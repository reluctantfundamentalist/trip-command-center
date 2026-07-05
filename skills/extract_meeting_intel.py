"""
Trip Claw Skill: extract_meeting_intel

Extracts structured intelligence from email bodies and attachments:
  - Offers (new or updates)
  - Action items with owners and deadlines
  - Key contacts and their roles
  - Overall interaction sentiment
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, asdict
from typing import Any, Literal

import httpx
import os
from tenacity import retry, stop_after_attempt, wait_exponential

from config.settings import settings

logger = logging.getLogger(__name__)


@dataclass
class Offer:
    offer_type: Literal["discount", "joint_marketing", "inventory_bundle", "exclusive_pricing", "other"]
    description: str
    terms: dict
    proposed_by: Literal["trip", "airline"]
    status: Literal["proposed", "negotiating", "accepted", "rejected"]


@dataclass
class ActionItem:
    description: str
    owner: str
    owner_side: Literal["trip", "airline"]
    due_date: str | None
    urgency: Literal["high", "medium", "low"]


@dataclass
class Contact:
    name: str
    email: str | None
    title: str | None
    role_in_conversation: str


@dataclass
class MeetingIntel:
    offers: list[Offer]
    action_items: list[ActionItem]
    key_contacts: list[Contact]
    sentiment: Literal["positive", "neutral", "negative", "mixed"]


SYSTEM_PROMPT = """\
You are a sales intelligence extraction engine for Trip.com's Flights organization.
Given an email body, optional attachment texts, and account context, extract:

1. **Offers**: Any commercial offers discussed (discounts, joint marketing, inventory bundles, exclusive pricing).
   For each offer, determine:
   - offer_type: one of "discount", "joint_marketing", "inventory_bundle", "exclusive_pricing", "other"
   - description: brief description of the offer
   - terms: a dict with relevant terms (discount_pct, start_date, end_date, volume_commitment, etc.)
   - proposed_by: "trip" or "airline" depending on who proposed it
   - status: "proposed", "negotiating", "accepted", or "rejected"

2. **Action Items**: Tasks or commitments mentioned.
   For each:
   - description: what needs to be done
   - owner: name or email of the responsible person
   - owner_side: "trip" or "airline"
   - due_date: ISO date string (YYYY-MM-DD) or null
   - urgency: "high", "medium", or "low"

3. **Key Contacts**: People mentioned or involved.
   For each:
   - name: full name
   - email: email address if available, else null
   - title: job title if mentioned, else null
   - role_in_conversation: "decision_maker", "technical", "commercial", "coordinator", etc.

4. **Sentiment**: Overall sentiment of the interaction: "positive", "neutral", "negative", or "mixed".

Use the account_context to distinguish new offers from references to existing ones.
Return ONLY valid JSON matching the schema. No markdown, no explanation.

Schema:
{
  "offers": [...],
  "action_items": [...],
  "key_contacts": [...],
  "sentiment": "positive|neutral|negative|mixed"
}
"""


def _build_user_prompt(
    email_body: str,
    attachments: list[str],
    account_context: dict,
) -> str:
    parts = [f"## Email Body\n{email_body}"]

    if attachments:
        for i, att_text in enumerate(attachments, 1):
            # Truncate very long attachment texts to stay within context
            truncated = att_text[:8000] if len(att_text) > 8000 else att_text
            parts.append(f"## Attachment {i}\n{truncated}")

    parts.append(f"## Account Context\n{json.dumps(account_context, indent=2, default=str)}")

    return "\n\n".join(parts)


def _parse_response(raw: str) -> MeetingIntel:
    """Parse LLM JSON response into typed dataclasses."""
    # Strip markdown fences if present
    text = raw.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        lines = [l for l in lines if not l.strip().startswith("```")]
        text = "\n".join(lines)

    data = json.loads(text)

    offers = [
        Offer(
            offer_type=o.get("offer_type", "other"),
            description=o.get("description", ""),
            terms=o.get("terms", {}),
            proposed_by=o.get("proposed_by", "airline"),
            status=o.get("status", "proposed"),
        )
        for o in data.get("offers", [])
    ]

    action_items = [
        ActionItem(
            description=a.get("description", ""),
            owner=a.get("owner", "unknown"),
            owner_side=a.get("owner_side", "trip"),
            due_date=a.get("due_date"),
            urgency=a.get("urgency", "medium"),
        )
        for a in data.get("action_items", [])
    ]

    key_contacts = [
        Contact(
            name=c.get("name", ""),
            email=c.get("email"),
            title=c.get("title"),
            role_in_conversation=c.get("role_in_conversation", "unknown"),
        )
        for c in data.get("key_contacts", [])
    ]

    sentiment = data.get("sentiment", "neutral")
    if sentiment not in ("positive", "neutral", "negative", "mixed"):
        sentiment = "neutral"

    return MeetingIntel(
        offers=offers,
        action_items=action_items,
        key_contacts=key_contacts,
        sentiment=sentiment,
    )


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=15),
    reraise=True,
)
async def _call_llm(system: str, user: str) -> str:
    """Call Anthropic API with retry logic."""
    auth_token = os.environ.get("ANTHROPIC_AUTH_TOKEN")
    base_url = os.environ.get("ANTHROPIC_BASE_URL", "https://api.anthropic.com")

    if not auth_token:
        raise RuntimeError("ANTHROPIC_AUTH_TOKEN not set in environment")

    async with httpx.AsyncClient(timeout=60.0) as client:
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
                "max_tokens": 4096,
                "system": system,
                "messages": [
                    {"role": "user", "content": user}
                ],
            },
        )
        resp.raise_for_status()
        data = resp.json()
        return data["content"][0]["text"]


async def extract_meeting_intel(
    email_body: str,
    attachments: list[str],
    account_context: dict,
) -> MeetingIntel:
    """
    Extract structured intelligence from an email and its attachments.

    Args:
        email_body: Plain text of the email.
        attachments: List of extracted text per attachment.
        account_context: Dict with keys: airline_name, recent_offers,
                         open_action_items, last_meeting_summary.

    Returns:
        MeetingIntel with offers, action items, contacts, and sentiment.
    """
    user_prompt = _build_user_prompt(email_body, attachments, account_context)

    for attempt in range(settings.trip_claw.max_retries):
        try:
            raw_response = await _call_llm(SYSTEM_PROMPT, user_prompt)
            result = _parse_response(raw_response)
            logger.info(
                "Extracted: %d offers, %d action items, %d contacts, sentiment=%s",
                len(result.offers),
                len(result.action_items),
                len(result.key_contacts),
                result.sentiment,
            )
            return result
        except (json.JSONDecodeError, KeyError) as e:
            logger.warning("Parse failed (attempt %d): %s", attempt + 1, e)
            if attempt == settings.trip_claw.max_retries - 1:
                logger.error("All parse attempts failed, returning empty intel")
                return MeetingIntel(
                    offers=[],
                    action_items=[],
                    key_contacts=[],
                    sentiment="neutral",
                )

    # Unreachable but satisfies type checker
    return MeetingIntel(offers=[], action_items=[], key_contacts=[], sentiment="neutral")
