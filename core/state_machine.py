"""
Account State Machine and Nudge Engine.

States: ACTIVE, WARM, STALLED, COLD, DORMANT
Evaluated every 15 minutes by nudge_engine_tick().

State logic (from spec):
  DORMANT  — no contact >90 days
  COLD     — no contact >30 days
  STALLED  — any action item overdue by 7+ days
  ACTIVE   — meeting in last 14 days AND no overdue items
  WARM     — recent meeting (<=30 days) with open action items
  fallback — ACTIVE
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, date, timezone
from dataclasses import dataclass
from typing import Literal
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from core.database import async_session_factory

logger = logging.getLogger(__name__)

StateType = Literal["ACTIVE", "WARM", "COLD", "STALLED", "DORMANT"]

# ── Nudge configuration defaults ─────────────────────────────

DEFAULT_NUDGE_CONFIG = {
    "cold_threshold_days": 30,
    "dormant_threshold_days": 90,
    "action_item_overdue_threshold_days": 7,
    "offer_stall_threshold_days": 14,
    "escalation_delay_days": 3,
    "nudge_cooldown_hours": 48,
    "max_nudges_per_day_per_exec": 5,
}

# ── Nudge trigger severity mapping by transition ─────────────

TRANSITION_NUDGE_MAP: dict[tuple[str, str], dict] = {
    ("ACTIVE", "WARM"):    {"notify_exec": False, "notify_mgr": False, "exec_severity": None,    "mgr_severity": None},
    ("WARM", "STALLED"):   {"notify_exec": True,  "notify_mgr": False, "exec_severity": "warning", "mgr_severity": None},
    ("WARM", "COLD"):      {"notify_exec": True,  "notify_mgr": True,  "exec_severity": "urgent",  "mgr_severity": "info"},
    ("ACTIVE", "COLD"):    {"notify_exec": True,  "notify_mgr": True,  "exec_severity": "urgent",  "mgr_severity": "warning"},
    ("COLD", "DORMANT"):   {"notify_exec": True,  "notify_mgr": True,  "exec_severity": "urgent",  "mgr_severity": "urgent"},
}


@dataclass
class NudgeTrigger:
    """A single nudge trigger to fire."""
    nudge_type: str
    severity: Literal["info", "warning", "urgent"]
    message: str
    target_executive_id: UUID
    target_manager_id: UUID | None = None


# ── Pure state evaluation ─────────────────────────────────────

def evaluate_state(
    days_since_contact: int | None,
    has_overdue_items: bool,
    has_open_items: bool,
) -> StateType:
    """
    Pure function: compute recommended state from signals.
    If days_since_contact is None (no meetings ever), treat as DORMANT.
    """
    if days_since_contact is None or days_since_contact > 90:
        return "DORMANT"
    if days_since_contact > 30:
        return "COLD"
    if has_overdue_items:
        return "STALLED"
    if days_since_contact <= 14 and not has_overdue_items:
        return "ACTIVE"
    if has_open_items:
        return "WARM"
    return "ACTIVE"


# ── DB queries ────────────────────────────────────────────────

async def _get_last_meeting_date(session: AsyncSession, airline_id: UUID) -> datetime | None:
    result = await session.execute(
        text("SELECT MAX(occurred_at) FROM meetings WHERE airline_id = :aid"),
        {"aid": str(airline_id)},
    )
    row = result.scalar_one_or_none()
    return row


async def _get_overdue_action_items(
    session: AsyncSession, airline_id: UUID, threshold_days: int = 7
) -> list[dict]:
    cutoff = date.today() - timedelta(days=threshold_days)
    result = await session.execute(
        text(
            "SELECT id, description, due_date, owner_id "
            "FROM action_items "
            "WHERE airline_id = :aid AND status IN ('open','in_progress') "
            "  AND due_date IS NOT NULL AND due_date < :cutoff"
        ),
        {"aid": str(airline_id), "cutoff": cutoff},
    )
    return [dict(r._mapping) for r in result.fetchall()]


async def _get_open_action_items(session: AsyncSession, airline_id: UUID) -> list[dict]:
    result = await session.execute(
        text(
            "SELECT id, description, due_date, owner_id "
            "FROM action_items "
            "WHERE airline_id = :aid AND status IN ('open','in_progress')"
        ),
        {"aid": str(airline_id)},
    )
    return [dict(r._mapping) for r in result.fetchall()]


async def _get_stalled_offers(
    session: AsyncSession, airline_id: UUID, threshold_days: int = 14
) -> list[dict]:
    cutoff = datetime.now(timezone.utc) - timedelta(days=threshold_days)
    result = await session.execute(
        text(
            "SELECT id, offer_type, status, updated_at "
            "FROM offers "
            "WHERE airline_id = :aid AND status = 'negotiating' AND updated_at < :cutoff"
        ),
        {"aid": str(airline_id), "cutoff": cutoff},
    )
    return [dict(r._mapping) for r in result.fetchall()]


async def _get_account_executives(session: AsyncSession, airline_id: UUID) -> list[dict]:
    result = await session.execute(
        text(
            "SELECT ea.executive_id, ea.role, e.full_name, e.email, e.manager_id "
            "FROM executive_accounts ea "
            "JOIN executives e ON e.id = ea.executive_id "
            "WHERE ea.airline_id = :aid"
        ),
        {"aid": str(airline_id)},
    )
    return [dict(r._mapping) for r in result.fetchall()]


async def _get_global_executives_including_children(
    session: AsyncSession, global_airline_id: UUID
) -> list[dict]:
    """For a global account, include executives from all local child accounts too."""
    result = await session.execute(
        text(
            "SELECT ea.executive_id, ea.role, e.full_name, e.email, e.manager_id, "
            "       ea.airline_id AS source_account_id "
            "FROM executive_accounts ea "
            "JOIN executives e ON e.id = ea.executive_id "
            "WHERE ea.airline_id = :global_id "
            "   OR ea.airline_id IN ("
            "       SELECT id FROM airline_accounts WHERE parent_account_id = :global_id"
            "   )"
        ),
        {"global_id": str(global_airline_id)},
    )
    return [dict(r._mapping) for r in result.fetchall()]


async def _get_local_account_ids_including_parent(
    session: AsyncSession, local_airline_id: UUID
) -> list[str]:
    """Return [local_id, parent_global_id] if this is a local account with a global parent."""
    result = await session.execute(
        text(
            "SELECT id, parent_account_id FROM airline_accounts WHERE id = :aid"
        ),
        {"aid": str(local_airline_id)},
    )
    row = result.one_or_none()
    if not row:
        return [str(local_airline_id)]
    m = row._mapping
    if m["parent_account_id"]:
        return [str(m["id"]), str(m["parent_account_id"])]
    return [str(m["id"])]


async def _get_child_account_ids(session: AsyncSession, global_airline_id: UUID) -> list[str]:
    """Return all local child account IDs for a global account."""
    result = await session.execute(
        text("SELECT id FROM airline_accounts WHERE parent_account_id = :global_id"),
        {"global_id": str(global_airline_id)},
    )
    return [str(r._mapping["id"]) for r in result.fetchall()]


async def _recently_nudged(
    session: AsyncSession,
    airline_id: UUID,
    nudge_type: str,
    cooldown_hours: int = 48,
) -> bool:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=cooldown_hours)
    result = await session.execute(
        text(
            "SELECT COUNT(*) FROM nudge_log "
            "WHERE airline_id = :aid AND nudge_type = :ntype AND created_at > :cutoff"
        ),
        {"aid": str(airline_id), "ntype": nudge_type, "cutoff": cutoff},
    )
    return (result.scalar_one() or 0) > 0


async def _count_nudges_today(session: AsyncSession, executive_id: UUID) -> int:
    today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    result = await session.execute(
        text(
            "SELECT COUNT(*) FROM nudge_log "
            "WHERE executive_id = :eid AND created_at >= :today"
        ),
        {"eid": str(executive_id), "today": today_start},
    )
    return result.scalar_one() or 0


async def _record_state_transition(
    session: AsyncSession,
    airline_id: UUID,
    old_state: str,
    new_state: str,
) -> None:
    await session.execute(
        text(
            "UPDATE airline_accounts "
            "SET state = :new_state, state_changed_at = now(), updated_at = now() "
            "WHERE id = :aid"
        ),
        {"new_state": new_state, "aid": str(airline_id)},
    )
    logger.info("State transition: %s -> %s for account %s", old_state, new_state, airline_id)


async def _insert_nudge(
    session: AsyncSession,
    airline_id: UUID,
    executive_id: UUID,
    nudge_type: str,
    severity: str,
    message: str,
    delivered_via: list[str],
) -> None:
    await session.execute(
        text(
            "INSERT INTO nudge_log (airline_id, executive_id, nudge_type, severity, message, delivered_via) "
            "VALUES (:aid, :eid, :ntype, :sev, :msg, :via)"
        ),
        {
            "aid": str(airline_id),
            "eid": str(executive_id),
            "ntype": nudge_type,
            "sev": severity,
            "msg": message,
            "via": delivered_via,
        },
    )


# ── Trigger evaluation ────────────────────────────────────────

async def _evaluate_triggers(
    session: AsyncSession,
    airline_id: UUID,
    airline_name: str,
    current_state: str,
    days_since_contact: int | None,
    overdue_items: list[dict],
    stalled_offers: list[dict],
    executives: list[dict] | None = None,
) -> list[NudgeTrigger]:
    """Evaluate all nudge trigger conditions for a single account."""
    cfg = DEFAULT_NUDGE_CONFIG
    triggers: list[NudgeTrigger] = []
    if executives is None:
        executives = await _get_account_executives(session, airline_id)
    if not executives:
        return triggers

    owner = next((e for e in executives if e["role"] == "owner"), executives[0])
    exec_id = owner["executive_id"]
    mgr_id = owner.get("manager_id")

    # No meeting in N days
    if days_since_contact is not None and days_since_contact > cfg["cold_threshold_days"]:
        triggers.append(NudgeTrigger(
            nudge_type="cold_account",
            severity="warning",
            message=f"No meeting with {airline_name} in {days_since_contact} days.",
            target_executive_id=exec_id,
            target_manager_id=mgr_id,
        ))

    # Overdue action items
    for item in overdue_items:
        triggers.append(NudgeTrigger(
            nudge_type="overdue_action",
            severity="warning",
            message=f"Action item overdue for {airline_name}: {item['description'][:80]}",
            target_executive_id=exec_id,
            target_manager_id=mgr_id,
        ))

    # Stalled offers
    for offer in stalled_offers:
        triggers.append(NudgeTrigger(
            nudge_type="stalled_offer",
            severity="info",
            message=f"Offer ({offer['offer_type']}) for {airline_name} has had no update in 14+ days.",
            target_executive_id=exec_id,
            target_manager_id=mgr_id,
        ))

    # State regression to COLD or DORMANT
    if current_state in ("COLD", "DORMANT"):
        triggers.append(NudgeTrigger(
            nudge_type="state_regression",
            severity="urgent",
            message=f"Account {airline_name} is now {current_state}. Immediate attention needed.",
            target_executive_id=exec_id,
            target_manager_id=mgr_id,
        ))

    return triggers


def _delivery_channels(severity: str) -> list[str]:
    """Determine which channels to deliver a nudge through based on severity."""
    channels = ["dashboard"]
    if severity in ("warning", "urgent"):
        channels.append("email")
    if severity == "urgent":
        channels.append("im")
    return channels


# ── Escalation check ──────────────────────────────────────────

async def _check_escalations(session: AsyncSession) -> None:
    """Find unacknowledged nudges past the escalation delay and escalate."""
    delay_days = DEFAULT_NUDGE_CONFIG["escalation_delay_days"]
    cutoff = datetime.now(timezone.utc) - timedelta(days=delay_days)

    result = await session.execute(
        text(
            "SELECT nl.id, nl.airline_id, nl.executive_id, nl.nudge_type, nl.severity, nl.message, "
            "       e.manager_id "
            "FROM nudge_log nl "
            "JOIN executives e ON e.id = nl.executive_id "
            "WHERE nl.acknowledged_at IS NULL "
            "  AND nl.escalated = FALSE "
            "  AND nl.created_at < :cutoff "
            "  AND e.manager_id IS NOT NULL"
        ),
        {"cutoff": cutoff},
    )
    unacked = result.fetchall()

    for row in unacked:
        r = row._mapping
        # Bump severity by one level
        escalated_severity = "urgent" if r["severity"] in ("info", "warning") else "urgent"

        await _insert_nudge(
            session,
            airline_id=r["airline_id"],
            executive_id=r["manager_id"],
            nudge_type=f"escalation:{r['nudge_type']}",
            severity=escalated_severity,
            message=f"[ESCALATED] {r['message']}",
            delivered_via=_delivery_channels(escalated_severity),
        )

        # Mark original as escalated
        await session.execute(
            text("UPDATE nudge_log SET escalated = TRUE WHERE id = :nid"),
            {"nid": r["id"]},
        )

    if unacked:
        logger.info("Escalated %d unacknowledged nudges", len(unacked))


# ── Main tick function ────────────────────────────────────────

async def nudge_engine_tick() -> dict:
    """
    Main entry point: evaluate every account, update states, fire nudges.
    Returns summary stats.
    """
    stats = {
        "accounts_evaluated": 0,
        "state_transitions": 0,
        "nudges_sent": 0,
        "escalations_checked": True,
    }
    cfg = DEFAULT_NUDGE_CONFIG

    async with async_session_factory() as session:
        # Get all accounts with hierarchy info
        result = await session.execute(
            text(
                "SELECT id, airline_name, state, is_global, parent_account_id "
                "FROM airline_accounts"
            )
        )
        accounts = result.fetchall()

        now = datetime.now(timezone.utc)

        for acct_row in accounts:
            acct = acct_row._mapping
            airline_id = acct["id"]
            airline_name = acct["airline_name"]
            old_state = acct["state"]
            is_global = acct["is_global"]

            stats["accounts_evaluated"] += 1

            # Determine which accounts to aggregate signals from
            account_ids_to_query = [str(airline_id)]
            if is_global:
                child_ids = await _get_child_account_ids(session, airline_id)
                account_ids_to_query.extend(child_ids)

            # Gather signals from self (+ children for global accounts)
            last_meeting = None
            overdue_items: list[dict] = []
            open_items: list[dict] = []

            for aid in account_ids_to_query:
                lm = await _get_last_meeting_date(session, UUID(aid))
                if lm and (last_meeting is None or lm > last_meeting):
                    last_meeting = lm
                overdue_items.extend(await _get_overdue_action_items(
                    session, UUID(aid), cfg["action_item_overdue_threshold_days"]
                ))
                open_items.extend(await _get_open_action_items(session, UUID(aid)))

            days_since = (now - last_meeting).days if last_meeting else None

            # Evaluate state
            new_state = evaluate_state(
                days_since_contact=days_since,
                has_overdue_items=len(overdue_items) > 0,
                has_open_items=len(open_items) > 0,
            )

            # Record state transition
            if new_state != old_state:
                await _record_state_transition(session, airline_id, old_state, new_state)
                stats["state_transitions"] += 1

                # Fire transition-specific nudge
                transition_key = (old_state, new_state)
                if transition_key in TRANSITION_NUDGE_MAP:
                    tn = TRANSITION_NUDGE_MAP[transition_key]
                    # For global accounts, include executives from all children
                    if is_global:
                        executives = await _get_global_executives_including_children(
                            session, airline_id
                        )
                    else:
                        executives = await _get_account_executives(session, airline_id)
                    if executives:
                        owner = next(
                            (e for e in executives if e["role"] == "owner"),
                            executives[0],
                        )
                        if tn["notify_exec"] and tn["exec_severity"]:
                            await _insert_nudge(
                                session,
                                airline_id=airline_id,
                                executive_id=owner["executive_id"],
                                nudge_type="state_regression",
                                severity=tn["exec_severity"],
                                message=f"Account {airline_name}: {old_state} -> {new_state}",
                                delivered_via=_delivery_channels(tn["exec_severity"]),
                            )
                            stats["nudges_sent"] += 1
                        if tn["notify_mgr"] and tn["mgr_severity"] and owner.get("manager_id"):
                            await _insert_nudge(
                                session,
                                airline_id=airline_id,
                                executive_id=owner["manager_id"],
                                nudge_type="state_regression",
                                severity=tn["mgr_severity"],
                                message=f"[Manager] Account {airline_name}: {old_state} -> {new_state}",
                                delivered_via=_delivery_channels(tn["mgr_severity"]),
                            )
                            stats["nudges_sent"] += 1

            # Evaluate independent triggers
            # For global accounts, include child stalled offers too
            stalled_offers: list[dict] = []
            for aid in account_ids_to_query:
                stalled_offers.extend(
                    await _get_stalled_offers(session, UUID(aid), cfg["offer_stall_threshold_days"])
                )

            # Use all executives (including children) for trigger evaluation on global accounts
            if is_global:
                trigger_executives = await _get_global_executives_including_children(
                    session, airline_id
                )
            else:
                trigger_executives = await _get_account_executives(session, airline_id)

            triggers = await _evaluate_triggers(
                session, airline_id, airline_name, new_state,
                days_since, overdue_items, stalled_offers,
                executives=trigger_executives,
            )

            for trigger in triggers:
                if await _recently_nudged(
                    session, airline_id, trigger.nudge_type, cfg["nudge_cooldown_hours"]
                ):
                    continue

                today_count = await _count_nudges_today(session, trigger.target_executive_id)
                if today_count >= cfg["max_nudges_per_day_per_exec"]:
                    logger.warning(
                        "Nudge cap reached for exec %s, skipping",
                        trigger.target_executive_id,
                    )
                    continue

                await _insert_nudge(
                    session,
                    airline_id=airline_id,
                    executive_id=trigger.target_executive_id,
                    nudge_type=trigger.nudge_type,
                    severity=trigger.severity,
                    message=trigger.message,
                    delivered_via=_delivery_channels(trigger.severity),
                )
                stats["nudges_sent"] += 1

        # Check escalations
        await _check_escalations(session)

        await session.commit()

    logger.info(
        "Nudge engine tick complete: %d accounts, %d transitions, %d nudges",
        stats["accounts_evaluated"],
        stats["state_transitions"],
        stats["nudges_sent"],
    )
    return stats
