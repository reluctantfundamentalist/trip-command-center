"""
Trip Claw Skill: compute_account_delta

Primarily rule-based comparison of previous and current account state,
with LLM summarization of changes. Called weekly during snapshot generation.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, asdict, field
from datetime import datetime, date

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from config.settings import settings

logger = logging.getLogger(__name__)


@dataclass
class AccountDelta:
    changes: list[str]
    new_offers: list[dict]
    resolved_items: list[dict]
    stalled_items: list[dict]
    state_recommendation: str


def _compute_rules_based_delta(
    previous_snapshot: dict,
    current_state: dict,
) -> dict:
    """
    Rule-based delta computation comparing previous snapshot to current state.
    Returns a raw delta dict before LLM summarization.
    """
    changes: list[str] = []
    new_offers: list[dict] = []
    resolved_items: list[dict] = []
    stalled_items: list[dict] = []

    # -- Meeting activity delta --
    prev_meetings = previous_snapshot.get("total_meetings", 0)
    curr_meetings = current_state.get("meetings_since", 0)
    if curr_meetings > 0:
        changes.append(f"{curr_meetings} new meeting(s) this period (total: {prev_meetings + curr_meetings})")
    elif prev_meetings > 0:
        changes.append("No new meetings this period")

    # -- Offer changes --
    prev_offer_ids = {o.get("id") for o in previous_snapshot.get("current_offers", []) if o.get("id")}
    for offer in current_state.get("current_offers", []):
        oid = offer.get("id")
        if oid and oid not in prev_offer_ids:
            new_offers.append(offer)
            changes.append(f"New offer: {offer.get('offer_type', 'unknown')} - {offer.get('description', '')[:60]}")
        elif oid and oid in prev_offer_ids:
            # Check for status change
            prev_offer = next(
                (o for o in previous_snapshot.get("current_offers", []) if o.get("id") == oid),
                None,
            )
            if prev_offer and prev_offer.get("status") != offer.get("status"):
                changes.append(
                    f"Offer {offer.get('offer_type', '')} status: "
                    f"{prev_offer.get('status', '?')} -> {offer.get('status', '?')}"
                )

    # -- Action item changes --
    prev_open_count = previous_snapshot.get("open_action_items", 0)
    curr_items = current_state.get("current_action_items", [])
    curr_open = [i for i in curr_items if i.get("status") in ("open", "in_progress")]
    curr_completed = [i for i in curr_items if i.get("status") == "completed"]
    curr_cancelled = [i for i in curr_items if i.get("status") == "cancelled"]

    for item in curr_completed:
        resolved_items.append(item)
    for item in curr_cancelled:
        resolved_items.append(item)

    if resolved_items:
        changes.append(f"{len(resolved_items)} action item(s) resolved")

    # -- Stalled items --
    today = date.today()
    overdue_threshold = 7
    for item in curr_open:
        due = item.get("due_date")
        if due:
            if isinstance(due, str):
                try:
                    due_date = date.fromisoformat(due)
                except ValueError:
                    continue
            elif isinstance(due, date):
                due_date = due
            else:
                continue

            days_overdue = (today - due_date).days
            if days_overdue > overdue_threshold:
                stalled_items.append({**item, "days_overdue": days_overdue})

    if stalled_items:
        changes.append(f"{len(stalled_items)} action item(s) overdue by >7 days")

    # -- Overdue action item trend --
    prev_overdue = previous_snapshot.get("overdue_action_items", 0)
    if len(stalled_items) > prev_overdue:
        changes.append(f"Overdue items increased: {prev_overdue} -> {len(stalled_items)}")
    elif len(stalled_items) < prev_overdue:
        changes.append(f"Overdue items decreased: {prev_overdue} -> {len(stalled_items)}")

    # -- State recommendation --
    last_contact = current_state.get("last_contact_date")
    days_since = None
    if last_contact:
        if isinstance(last_contact, str):
            try:
                last_dt = date.fromisoformat(last_contact[:10])
                days_since = (today - last_dt).days
            except ValueError:
                pass
        elif isinstance(last_contact, (date, datetime)):
            d = last_contact if isinstance(last_contact, date) else last_contact.date()
            days_since = (today - d).days

    if days_since is None or days_since > 90:
        state_rec = "DORMANT"
    elif days_since > 30:
        state_rec = "COLD"
    elif stalled_items:
        state_rec = "STALLED"
    elif days_since <= 14 and not stalled_items:
        state_rec = "ACTIVE"
    elif curr_open:
        state_rec = "WARM"
    else:
        state_rec = "ACTIVE"

    if not changes:
        changes.append("No significant changes detected")

    return {
        "changes": changes,
        "new_offers": new_offers,
        "resolved_items": resolved_items,
        "stalled_items": stalled_items,
        "state_recommendation": state_rec,
    }


SUMMARIZATION_PROMPT = """\
You are a concise sales intelligence summarizer for Trip.com Flights.
Given a rule-based delta between an airline account's previous and current state,
produce a short human-readable summary (2-4 sentences) of the most important changes.
Focus on: deal progression, risk signals, and required actions.

Return ONLY valid JSON: {"summary": "..."}
"""


@retry(
    stop=stop_after_attempt(2),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    reraise=True,
)
async def _summarize_delta(delta_dict: dict) -> str:
    """Call Anthropic API to produce a narrative summary of the delta."""
    auth_token = os.environ.get("ANTHROPIC_AUTH_TOKEN")
    base_url = os.environ.get("ANTHROPIC_BASE_URL", "https://api.anthropic.com")

    if not auth_token:
        raise RuntimeError("ANTHROPIC_AUTH_TOKEN not set in environment")

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(
            f"{base_url}/v1/messages",
            headers={
                "Authorization": f"Bearer {auth_token}",
                "Content-Type": "application/json",
                "x-api-key": auth_token,
                "anthropic-version": "2023-06-01",
            },
            json={
                "model": "claude-sonnet-4-6",
                "max_tokens": 1024,
                "system": SUMMARIZATION_PROMPT,
                "messages": [
                    {"role": "user", "content": json.dumps(delta_dict, default=str)}
                ],
            },
        )
        resp.raise_for_status()
        content = resp.json()["content"][0]["text"]
        parsed = json.loads(content)
        return parsed.get("summary", "No summary available.")


async def compute_account_delta(
    previous_snapshot: dict,
    current_state: dict,
) -> AccountDelta:
    """
    Compute the delta between previous and current account state.

    Args:
        previous_snapshot: Last week's account_snapshots row as dict.
            Keys: total_meetings, open_offers, open_action_items,
                  overdue_action_items, current_offers (list[dict]), state.
        current_state: Current metrics dict.
            Keys: meetings_since, current_offers (list[dict]),
                  current_action_items (list[dict]), last_contact_date.

    Returns:
        AccountDelta with changes, new/resolved/stalled items,
        and a state recommendation.
    """
    raw_delta = _compute_rules_based_delta(previous_snapshot, current_state)

    # Attempt LLM summarization; fall back to rule-based changes
    try:
        narrative = await _summarize_delta(raw_delta)
        raw_delta["changes"].insert(0, f"[Summary] {narrative}")
    except Exception as e:
        logger.warning("LLM summarization failed, using rule-based changes only: %s", e)

    return AccountDelta(
        changes=raw_delta["changes"],
        new_offers=raw_delta["new_offers"],
        resolved_items=raw_delta["resolved_items"],
        stalled_items=raw_delta["stalled_items"],
        state_recommendation=raw_delta["state_recommendation"],
    )
