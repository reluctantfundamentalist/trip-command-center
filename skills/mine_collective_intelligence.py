"""
Trip Claw Skill: mine_collective_intelligence

Runs over the full dataset using a map-reduce pattern:
  1. MAP: Process accounts in batches of 50, extract local patterns
  2. REDUCE: Aggregate patterns, compute confidence scores

Output is written to the best_practices table.
"""

from __future__ import annotations

import json
import logging
import math
from collections import defaultdict
from dataclasses import dataclass, field, asdict
from typing import Any

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from config.settings import settings

logger = logging.getLogger(__name__)


@dataclass
class CollectiveInsights:
    offer_effectiveness: list[dict]    # {airline_category, offer_type, success_rate, avg_terms}
    cadence_patterns: list[dict]       # {airline_category, optimal_meeting_freq_days, correlation}
    framing_patterns: list[dict]       # {pattern_description, example, outcome_correlation}
    early_warnings: list[dict]         # {signal_description, lead_time_days, confidence}


# ── Map phase: per-batch pattern extraction ───────────────────

MAP_SYSTEM_PROMPT = """\
You are a sales analytics engine for Trip.com Flights.
Given a batch of airline account snapshots (including meeting history,
offer outcomes, action item completion rates, and state transitions),
identify LOCAL patterns in this batch:

1. **offer_effectiveness**: For each (airline_category, offer_type) pair present,
   compute success_rate = accepted / (accepted + rejected). Include avg_terms.
2. **cadence_patterns**: For accounts that progressed well (ACTIVE state, accepted offers),
   what was the average meeting frequency? Report as optimal_meeting_freq_days.
3. **framing_patterns**: Any notable language or approach patterns in successful vs failed negotiations.
4. **early_warnings**: Behavioral signals that preceded negative state transitions (ACTIVE->COLD, etc.)

Return ONLY valid JSON:
{
  "offer_effectiveness": [...],
  "cadence_patterns": [...],
  "framing_patterns": [...],
  "early_warnings": [...]
}
"""


REDUCE_SYSTEM_PROMPT = """\
You are a sales analytics aggregation engine for Trip.com Flights.
Given multiple batches of local pattern extractions, aggregate them into
organization-wide insights:

1. Merge offer_effectiveness across batches: weighted-average success rates by sample size.
2. Merge cadence_patterns: compute overall optimal frequency per airline_category.
3. Consolidate framing_patterns: deduplicate similar patterns, keep highest-correlation ones.
4. Consolidate early_warnings: deduplicate, assign confidence based on how many batches reported each signal.

Apply minimum sample size threshold: only include insights backed by >= 10 data points.

Return ONLY valid JSON:
{
  "offer_effectiveness": [...],
  "cadence_patterns": [...],
  "framing_patterns": [...],
  "early_warnings": [...]
}
"""


def chunk(lst: list, size: int = 50) -> list[list]:
    """Split a list into chunks of the given size."""
    return [lst[i : i + size] for i in range(0, len(lst), size)]


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    reraise=True,
)
async def _call_llm(system: str, user: str, timeout: float = 120.0) -> str:
    """Call Anthropic API with retry logic."""
    auth_token = os.environ.get("ANTHROPIC_AUTH_TOKEN")
    base_url = os.environ.get("ANTHROPIC_BASE_URL", "https://api.anthropic.com")

    if not auth_token:
        raise RuntimeError("ANTHROPIC_AUTH_TOKEN not set in environment")

    async with httpx.AsyncClient(timeout=timeout) as client:
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


def _parse_insights_json(raw: str) -> dict:
    """Parse raw LLM response into a dict with the four insight categories."""
    text = raw.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        lines = [l for l in lines if not l.strip().startswith("```")]
        text = "\n".join(lines)

    data = json.loads(text)
    return {
        "offer_effectiveness": data.get("offer_effectiveness", []),
        "cadence_patterns": data.get("cadence_patterns", []),
        "framing_patterns": data.get("framing_patterns", []),
        "early_warnings": data.get("early_warnings", []),
    }


def _rule_based_offer_effectiveness(snapshots: list[dict]) -> list[dict]:
    """
    Pure rule-based computation of offer effectiveness as a fallback
    and supplement to LLM extraction.
    """
    # Bucket by (airline_category, offer_type)
    buckets: dict[tuple[str, str], dict[str, int]] = defaultdict(
        lambda: {"accepted": 0, "rejected": 0, "total": 0}
    )

    for snap in snapshots:
        category = snap.get("airline_category", "unknown")
        for offer in snap.get("offers", []):
            otype = offer.get("offer_type", "other")
            status = offer.get("status", "")
            key = (category, otype)
            buckets[key]["total"] += 1
            if status == "accepted":
                buckets[key]["accepted"] += 1
            elif status == "rejected":
                buckets[key]["rejected"] += 1

    results = []
    for (cat, otype), counts in buckets.items():
        denominator = counts["accepted"] + counts["rejected"]
        if denominator >= 5:  # minimum sample
            results.append({
                "airline_category": cat,
                "offer_type": otype,
                "success_rate": round(counts["accepted"] / denominator, 3),
                "sample_size": denominator,
                "avg_terms": {},
            })

    return sorted(results, key=lambda x: x["success_rate"], reverse=True)


def _rule_based_cadence(snapshots: list[dict]) -> list[dict]:
    """
    Rule-based computation of optimal meeting cadence per airline category.
    """
    category_intervals: dict[str, list[float]] = defaultdict(list)

    for snap in snapshots:
        category = snap.get("airline_category", "unknown")
        meetings = snap.get("meetings", [])
        if len(meetings) < 2:
            continue

        # Sort meetings by date, compute intervals
        dates = sorted(
            m.get("occurred_at", "") for m in meetings if m.get("occurred_at")
        )
        for i in range(1, len(dates)):
            try:
                from datetime import datetime as dt
                d1 = dt.fromisoformat(str(dates[i - 1]).replace("Z", "+00:00"))
                d2 = dt.fromisoformat(str(dates[i]).replace("Z", "+00:00"))
                interval = (d2 - d1).days
                if 0 < interval < 180:
                    category_intervals[category].append(interval)
            except (ValueError, TypeError):
                continue

    results = []
    for cat, intervals in category_intervals.items():
        if len(intervals) >= 10:
            avg = sum(intervals) / len(intervals)
            # Simple correlation proxy: lower variance = stronger pattern
            variance = sum((x - avg) ** 2 for x in intervals) / len(intervals)
            std = math.sqrt(variance) if variance > 0 else 0
            correlation = max(0, 1.0 - (std / avg)) if avg > 0 else 0

            results.append({
                "airline_category": cat,
                "optimal_meeting_freq_days": round(avg, 1),
                "correlation": round(correlation, 3),
                "sample_size": len(intervals),
            })

    return results


def aggregate_insights(batch_results: list[dict]) -> CollectiveInsights:
    """
    REDUCE phase: aggregate local patterns from all batches
    into organization-wide insights.
    """
    # Merge offer effectiveness with weighted averaging
    offer_map: dict[tuple[str, str], dict] = {}
    for batch in batch_results:
        for oe in batch.get("offer_effectiveness", []):
            key = (oe.get("airline_category", ""), oe.get("offer_type", ""))
            if key not in offer_map:
                offer_map[key] = {
                    "airline_category": key[0],
                    "offer_type": key[1],
                    "weighted_success": 0.0,
                    "total_sample": 0,
                    "avg_terms": oe.get("avg_terms", {}),
                }
            sample = oe.get("sample_size", 1)
            rate = oe.get("success_rate", 0)
            offer_map[key]["weighted_success"] += rate * sample
            offer_map[key]["total_sample"] += sample

    offer_effectiveness = []
    for v in offer_map.values():
        if v["total_sample"] >= 10:
            offer_effectiveness.append({
                "airline_category": v["airline_category"],
                "offer_type": v["offer_type"],
                "success_rate": round(v["weighted_success"] / v["total_sample"], 3),
                "sample_size": v["total_sample"],
                "avg_terms": v["avg_terms"],
            })

    # Merge cadence patterns
    cadence_map: dict[str, dict] = {}
    for batch in batch_results:
        for cp in batch.get("cadence_patterns", []):
            cat = cp.get("airline_category", "")
            if cat not in cadence_map:
                cadence_map[cat] = {"total_freq": 0.0, "total_corr": 0.0, "count": 0}
            cadence_map[cat]["total_freq"] += cp.get("optimal_meeting_freq_days", 0)
            cadence_map[cat]["total_corr"] += cp.get("correlation", 0)
            cadence_map[cat]["count"] += 1

    cadence_patterns = []
    for cat, v in cadence_map.items():
        if v["count"] > 0:
            cadence_patterns.append({
                "airline_category": cat,
                "optimal_meeting_freq_days": round(v["total_freq"] / v["count"], 1),
                "correlation": round(v["total_corr"] / v["count"], 3),
            })

    # Deduplicate framing patterns (keep unique descriptions)
    seen_framing: set[str] = set()
    framing_patterns = []
    for batch in batch_results:
        for fp in batch.get("framing_patterns", []):
            desc = fp.get("pattern_description", "")
            if desc and desc not in seen_framing:
                seen_framing.add(desc)
                framing_patterns.append(fp)

    # Deduplicate early warnings, boost confidence by occurrence count
    warning_counts: dict[str, dict] = {}
    for batch in batch_results:
        for ew in batch.get("early_warnings", []):
            sig = ew.get("signal_description", "")
            if sig not in warning_counts:
                warning_counts[sig] = {**ew, "occurrences": 0}
            warning_counts[sig]["occurrences"] += 1

    total_batches = max(len(batch_results), 1)
    early_warnings = []
    for v in warning_counts.values():
        confidence = min(1.0, v["occurrences"] / total_batches)
        v["confidence"] = round(confidence, 3)
        early_warnings.append(v)

    early_warnings.sort(key=lambda x: x.get("confidence", 0), reverse=True)

    return CollectiveInsights(
        offer_effectiveness=offer_effectiveness,
        cadence_patterns=cadence_patterns,
        framing_patterns=framing_patterns[:20],  # cap
        early_warnings=early_warnings[:15],      # cap
    )


async def mine_collective_intelligence(
    all_account_snapshots: list[dict],
    lookback_days: int = 180,
) -> CollectiveInsights:
    """
    Mine collective intelligence across all accounts using map-reduce.

    Args:
        all_account_snapshots: All snapshots within the lookback window.
            Each snapshot dict should include: airline_category, meetings,
            offers, action_items, state, state_transitions.
        lookback_days: How far back to analyze (default 180 days).

    Returns:
        CollectiveInsights with offer effectiveness, cadence patterns,
        framing patterns, and early warning signals.
    """
    if not all_account_snapshots:
        logger.warning("No snapshots provided, returning empty insights")
        return CollectiveInsights(
            offer_effectiveness=[],
            cadence_patterns=[],
            framing_patterns=[],
            early_warnings=[],
        )

    # Compute rule-based metrics first (always available)
    rule_offer_eff = _rule_based_offer_effectiveness(all_account_snapshots)
    rule_cadence = _rule_based_cadence(all_account_snapshots)

    # MAP phase: batch accounts and call LLM for pattern extraction
    batches = chunk(all_account_snapshots, size=50)
    batch_results: list[dict] = []

    for i, batch in enumerate(batches):
        try:
            user_content = json.dumps(batch, default=str)
            # Truncate if too large
            if len(user_content) > 100_000:
                user_content = user_content[:100_000] + "\n... (truncated)"

            raw = await _call_llm(MAP_SYSTEM_PROMPT, user_content, timeout=120.0)
            parsed = _parse_insights_json(raw)
            batch_results.append(parsed)
            logger.info("MAP batch %d/%d complete", i + 1, len(batches))
        except Exception as e:
            logger.warning("MAP batch %d failed: %s — using rule-based fallback", i + 1, e)
            # Inject rule-based results for this batch
            batch_results.append({
                "offer_effectiveness": rule_offer_eff,
                "cadence_patterns": rule_cadence,
                "framing_patterns": [],
                "early_warnings": [],
            })

    # REDUCE phase
    if len(batch_results) > 1:
        try:
            reduce_input = json.dumps(batch_results, default=str)
            if len(reduce_input) > 100_000:
                reduce_input = reduce_input[:100_000] + "\n... (truncated)"

            raw = await _call_llm(REDUCE_SYSTEM_PROMPT, reduce_input, timeout=120.0)
            final_parsed = _parse_insights_json(raw)
            logger.info("REDUCE phase complete")

            return CollectiveInsights(
                offer_effectiveness=final_parsed.get("offer_effectiveness", rule_offer_eff),
                cadence_patterns=final_parsed.get("cadence_patterns", rule_cadence),
                framing_patterns=final_parsed.get("framing_patterns", []),
                early_warnings=final_parsed.get("early_warnings", []),
            )
        except Exception as e:
            logger.warning("REDUCE phase failed: %s — using programmatic aggregation", e)

    # Programmatic aggregation fallback
    aggregated = aggregate_insights(batch_results)

    # Supplement with rule-based results where LLM data is sparse
    if not aggregated.offer_effectiveness:
        aggregated.offer_effectiveness = rule_offer_eff
    if not aggregated.cadence_patterns:
        aggregated.cadence_patterns = rule_cadence

    logger.info(
        "Mining complete: %d offer patterns, %d cadence, %d framing, %d warnings",
        len(aggregated.offer_effectiveness),
        len(aggregated.cadence_patterns),
        len(aggregated.framing_patterns),
        len(aggregated.early_warnings),
    )
    return aggregated
