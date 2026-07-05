"""
Trip Claw Skill: generate_proactive_guidance

Synthesizes patterns from similar accounts and best practices
to generate actionable guidance for a specific account.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from config.settings import settings

logger = logging.getLogger(__name__)


@dataclass
class ProactiveGuidance:
    recommended_approaches: list[str]
    suggested_offers: list[dict]  # {offer_type, rationale, template_terms}
    risk_flags: list[str]


SYSTEM_PROMPT = """\
You are a senior sales strategy advisor for Trip.com's Flights organization.
Your job is to generate specific, actionable guidance for an executive managing
an airline account.

Given:
1. The account's history (meetings, offers, action items, state transitions)
2. Similar accounts that had successful outcomes
3. Best practices mined from across the organization

Generate:
1. **recommended_approaches**: 3-5 specific tactical suggestions (e.g., "Schedule a QBR within 10 days focusing on Q3 volume commitments")
2. **suggested_offers**: 1-3 offer templates that could advance the relationship, each with:
   - offer_type: "discount", "joint_marketing", "inventory_bundle", "exclusive_pricing", or "other"
   - rationale: why this offer makes sense for this account
   - template_terms: dict with suggested terms based on similar successful deals
3. **risk_flags**: 0-3 early warning signals observed in this account's trajectory

Be concrete. Reference specific data from the inputs. No generic advice.

Return ONLY valid JSON:
{
  "recommended_approaches": [...],
  "suggested_offers": [...],
  "risk_flags": [...]
}
"""


def _build_user_prompt(
    account_history: dict,
    similar_accounts: list[dict],
    best_practices: dict,
) -> str:
    """Assemble user prompt from the three inputs, with truncation."""

    # Truncate meeting history to last 20
    history = dict(account_history)
    if "meetings" in history and len(history["meetings"]) > 20:
        history["meetings"] = history["meetings"][-20:]

    # Truncate similar accounts to top 5
    similar = similar_accounts[:5] if len(similar_accounts) > 5 else similar_accounts

    parts = [
        "## Account History",
        json.dumps(history, indent=2, default=str),
        "",
        "## Similar Accounts (successful outcomes)",
        json.dumps(similar, indent=2, default=str),
        "",
        "## Best Practices",
        json.dumps(best_practices, indent=2, default=str),
    ]
    return "\n".join(parts)


def _parse_response(raw: str) -> ProactiveGuidance:
    """Parse LLM JSON into ProactiveGuidance dataclass."""
    text = raw.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        lines = [l for l in lines if not l.strip().startswith("```")]
        text = "\n".join(lines)

    data = json.loads(text)

    recommended = data.get("recommended_approaches", [])
    if not isinstance(recommended, list):
        recommended = [str(recommended)]

    suggested = data.get("suggested_offers", [])
    if not isinstance(suggested, list):
        suggested = []
    # Normalize each offer
    normalized_offers = []
    for s in suggested:
        if isinstance(s, dict):
            normalized_offers.append({
                "offer_type": s.get("offer_type", "other"),
                "rationale": s.get("rationale", ""),
                "template_terms": s.get("template_terms", {}),
            })

    risk_flags = data.get("risk_flags", [])
    if not isinstance(risk_flags, list):
        risk_flags = [str(risk_flags)]

    return ProactiveGuidance(
        recommended_approaches=recommended,
        suggested_offers=normalized_offers,
        risk_flags=risk_flags,
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
        return resp.json()["content"][0]["text"]


async def generate_proactive_guidance(
    account_history: dict,
    similar_accounts: list[dict],
    best_practices: dict,
) -> ProactiveGuidance:
    """
    Generate actionable guidance for a specific airline account.

    Args:
        account_history: Dict with keys: meetings (list), offers (list),
                         action_items (list), state_transitions (list).
        similar_accounts: List of account dicts with same airline_category
                          that had positive outcomes.
        best_practices: Dict from best_practices table (active practices
                        for this airline category).

    Returns:
        ProactiveGuidance with approaches, suggested offers, and risk flags.
    """
    user_prompt = _build_user_prompt(account_history, similar_accounts, best_practices)

    for attempt in range(settings.trip_claw.max_retries):
        try:
            raw_response = await _call_llm(SYSTEM_PROMPT, user_prompt)
            result = _parse_response(raw_response)
            logger.info(
                "Guidance generated: %d approaches, %d offers, %d risks",
                len(result.recommended_approaches),
                len(result.suggested_offers),
                len(result.risk_flags),
            )
            return result
        except (json.JSONDecodeError, KeyError) as e:
            logger.warning("Guidance parse failed (attempt %d): %s", attempt + 1, e)
            if attempt == settings.trip_claw.max_retries - 1:
                logger.error("All guidance attempts failed, returning defaults")
                return ProactiveGuidance(
                    recommended_approaches=["Schedule a check-in meeting within the next 7 days."],
                    suggested_offers=[],
                    risk_flags=["Unable to generate detailed guidance — review account manually."],
                )

    return ProactiveGuidance(
        recommended_approaches=[],
        suggested_offers=[],
        risk_flags=[],
    )
