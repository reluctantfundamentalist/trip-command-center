"""
Trip Claw Skill: extract_attachment_intel

Extracts structured data and relevant excerpts from attachment text.
Handles PDF contracts, PPTX presentations, XLSX pricing tables, DOCX documents.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, asdict
from typing import Literal

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from config.settings import settings

logger = logging.getLogger(__name__)


@dataclass
class AttachmentIntel:
    structured_data: dict    # {tables: [...], key_figures: {...}, contract_terms: {...}}
    relevant_excerpts: list[str]  # verbatim passages relevant to the deal


SYSTEM_PROMPTS: dict[str, str] = {
    "pdf": """\
You are a contract and document analysis engine for Trip.com's Flights sales team.
Analyze the following PDF text and extract:
1. **structured_data**: A dict containing:
   - contract_terms: key contract terms (parties, duration, renewal, penalties, etc.)
   - key_figures: numerical data points (percentages, volumes, prices, dates)
   - tables: any tabular data found, each as a list of row-dicts
2. **relevant_excerpts**: Verbatim quotes (max 10) that are directly relevant to commercial terms, obligations, or deal structure.

Return ONLY valid JSON. No markdown fences, no explanation.
Schema: {"structured_data": {...}, "relevant_excerpts": [...]}
""",
    "pptx": """\
You are a presentation analysis engine for Trip.com's Flights sales team.
Analyze the following presentation text (slide by slide) and extract:
1. **structured_data**: A dict containing:
   - slides: list of {slide_number, key_points: [...], data_points: [...]}
   - key_figures: aggregate numerical data across all slides
   - tables: any tabular data found
2. **relevant_excerpts**: Verbatim quotes (max 10) containing proposals, commitments, or key data points.

Return ONLY valid JSON. No markdown fences, no explanation.
Schema: {"structured_data": {...}, "relevant_excerpts": [...]}
""",
    "xlsx": """\
You are a spreadsheet analysis engine for Trip.com's Flights sales team.
Analyze the following spreadsheet data and extract:
1. **structured_data**: A dict containing:
   - tables: parsed data tables with headers and rows
   - key_figures: summary statistics (totals, averages, min/max for pricing columns)
   - pricing_tiers: if pricing data is present, extract tier structures
2. **relevant_excerpts**: Key data rows or cells that relate to pricing, volumes, or commitments (as formatted strings).

Return ONLY valid JSON. No markdown fences, no explanation.
Schema: {"structured_data": {...}, "relevant_excerpts": [...]}
""",
    "docx": """\
You are a document analysis engine for Trip.com's Flights sales team.
Analyze the following document text and extract:
1. **structured_data**: A dict containing:
   - key_figures: numerical data points referenced in the document
   - contract_terms: any terms, conditions, or commitments described
   - tables: any tabular data found
2. **relevant_excerpts**: Verbatim quotes (max 10) that are directly relevant to commercial terms, deal structure, or obligations.

Return ONLY valid JSON. No markdown fences, no explanation.
Schema: {"structured_data": {...}, "relevant_excerpts": [...]}
""",
}

DEFAULT_SYSTEM_PROMPT = SYSTEM_PROMPTS["pdf"]  # fallback


def _build_user_prompt(
    attachment_text: str,
    attachment_type: str,
    account_context: dict,
) -> str:
    # Truncate to stay within context window
    max_chars = settings.trip_claw.max_context_tokens * 3  # rough chars-to-tokens
    truncated = attachment_text[:max_chars] if len(attachment_text) > max_chars else attachment_text

    return (
        f"## Attachment Content ({attachment_type.upper()})\n"
        f"{truncated}\n\n"
        f"## Account Context\n"
        f"{json.dumps(account_context, indent=2, default=str)}"
    )


def _parse_response(raw: str) -> AttachmentIntel:
    """Parse LLM JSON response into AttachmentIntel."""
    text = raw.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        lines = [l for l in lines if not l.strip().startswith("```")]
        text = "\n".join(lines)

    data = json.loads(text)

    return AttachmentIntel(
        structured_data=data.get("structured_data", {}),
        relevant_excerpts=data.get("relevant_excerpts", []),
    )


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=15),
    reraise=True,
)
async def _call_llm(system: str, user: str) -> str:
    async with httpx.AsyncClient(timeout=90.0) as client:
        resp = await client.post(
            f"{settings.trip_claw.api_url}/chat/completions",
            headers={
                "Authorization": f"Bearer {settings.trip_claw.api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": settings.trip_claw.model,
                "messages": [
                    {"role": "system", "content": system},
                    {"role": "user", "content": user},
                ],
                "temperature": 0.1,
                "response_format": {"type": "json_object"},
            },
        )
        resp.raise_for_status()
        return resp.json()["choices"][0]["message"]["content"]


async def extract_attachment_intel(
    attachment_text: str,
    attachment_type: Literal["pdf", "pptx", "xlsx", "docx"],
    account_context: dict,
) -> AttachmentIntel:
    """
    Extract structured intelligence from an attachment.

    Args:
        attachment_text: Extracted plain text from the attachment.
        attachment_type: Type of the attachment file.
        account_context: Dict with airline_name, recent_offers,
                         open_action_items, last_meeting_summary.

    Returns:
        AttachmentIntel with structured_data and relevant_excerpts.
    """
    system_prompt = SYSTEM_PROMPTS.get(attachment_type, DEFAULT_SYSTEM_PROMPT)
    user_prompt = _build_user_prompt(attachment_text, attachment_type, account_context)

    for attempt in range(settings.trip_claw.max_retries):
        try:
            raw_response = await _call_llm(system_prompt, user_prompt)
            result = _parse_response(raw_response)
            logger.info(
                "Attachment extracted: %d excerpts, %d structured keys (%s)",
                len(result.relevant_excerpts),
                len(result.structured_data),
                attachment_type,
            )
            return result
        except (json.JSONDecodeError, KeyError) as e:
            logger.warning(
                "Attachment parse failed (attempt %d/%d): %s",
                attempt + 1, settings.trip_claw.max_retries, e,
            )
            if attempt == settings.trip_claw.max_retries - 1:
                logger.error("All attachment parse attempts failed, returning empty")
                return AttachmentIntel(structured_data={}, relevant_excerpts=[])

    return AttachmentIntel(structured_data={}, relevant_excerpts=[])
