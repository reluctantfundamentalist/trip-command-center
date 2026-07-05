"""
FastAPI application — Trip.com Flights Sales Intelligence Dashboard API.

All 15 endpoints from the spec:
  GET  /api/v1/accounts
  GET  /api/v1/accounts/{id}
  GET  /api/v1/accounts/{id}/timeline
  GET  /api/v1/accounts/{id}/snapshots
  GET  /api/v1/executives
  GET  /api/v1/executives/{id}/summary
  GET  /api/v1/executives/{id}/guidance
  GET  /api/v1/offers
  GET  /api/v1/action-items
  GET  /api/v1/nudges
  PATCH /api/v1/nudges/{id}/acknowledge
  GET  /api/v1/dashboard/overview
  GET  /api/v1/dashboard/weekly-digest
  POST /api/v1/dashboard/weekly-digest/generate
  GET  /api/v1/best-practices
  GET  /api/v1/metrics
"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta, timezone
from typing import Any

from fastapi import FastAPI, Depends, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from config.settings import settings
from core.database import get_session, engine, async_session_factory
from core.cache import cache
from core.state_machine import nudge_engine_tick

logger = logging.getLogger(__name__)


# ── Lifespan ──────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    await cache.connect()
    logger.info("Application started")
    yield
    await cache.close()
    await engine.dispose()
    logger.info("Application shutdown")


app = FastAPI(
    title=settings.app_name,
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Pydantic response models ─────────────────────────────────

class PaginatedResponse(BaseModel):
    items: list[dict[str, Any]]
    total: int


class HealthResponse(BaseModel):
    status: str
    version: str


class DigestGenerateRequest(BaseModel):
    week: date


class DigestGenerateResponse(BaseModel):
    job_id: str


# ── Helper ────────────────────────────────────────────────────

def _rows_to_dicts(result) -> list[dict]:
    return [dict(r._mapping) for r in result.fetchall()]


def _serialize(obj: Any) -> Any:
    """Make objects JSON-serializable."""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, uuid.UUID):
        return str(obj)
    if isinstance(obj, dict):
        return {k: _serialize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_serialize(i) for i in obj]
    return obj


# ── Health ────────────────────────────────────────────────────

@app.get("/health", response_model=HealthResponse)
async def health():
    return {"status": "ok", "version": "1.0.0"}


# ═══════════════════════════════════════════════════════════════
# 1. GET /api/v1/accounts
# ═══════════════════════════════════════════════════════════════

@app.get("/api/v1/accounts")
async def list_accounts(
    exec_id: str | None = Query(None),
    state: str | None = Query(None),
    category: str | None = Query(None),
    global_only: bool = Query(False),
    include_children: bool = Query(False),
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
    session: AsyncSession = Depends(get_session),
):
    conditions = []
    params: dict[str, Any] = {}

    if exec_id:
        conditions.append(
            "a.id IN (SELECT airline_id FROM executive_accounts WHERE executive_id = :exec_id)"
        )
        params["exec_id"] = exec_id
    if state:
        conditions.append("a.state = :state")
        params["state"] = state
    if category:
        conditions.append("a.category = :category")
        params["category"] = category
    if global_only:
        conditions.append("a.is_global = TRUE")

    where = "WHERE " + " AND ".join(conditions) if conditions else ""
    offset = (page - 1) * limit
    params["limit"] = limit
    params["offset"] = offset

    count_q = f"SELECT COUNT(*) FROM airline_accounts a {where}"
    total = (await session.execute(text(count_q), params)).scalar_one()

    data_q = (
        f"SELECT a.* FROM airline_accounts a {where} "
        f"ORDER BY a.airline_name ASC LIMIT :limit OFFSET :offset"
    )
    rows = _rows_to_dicts(await session.execute(text(data_q), params))

    # If include_children, fetch and nest local accounts under their global parents
    if include_children and not global_only:
        # Fetch all local accounts
        local_rows = _rows_to_dicts(await session.execute(
            text(
                "SELECT a.* FROM airline_accounts a "
                "WHERE a.is_global = FALSE ORDER BY a.airline_name"
            )
        ))
        # Group by parent_account_id
        children_map: dict[str, list[dict]] = {}
        for child in local_rows:
            parent_id = child.get("parent_account_id")
            if parent_id:
                children_map.setdefault(str(parent_id), []).append(child)

        for row in rows:
            row_id = str(row["id"])
            row["children"] = children_map.get(row_id, [])
    elif include_children and global_only:
        # global_only already returns only roots; add children arrays
        children_map: dict[str, list[dict]] = {}
        local_rows = _rows_to_dicts(await session.execute(
            text("SELECT id, parent_account_id FROM airline_accounts WHERE is_global = FALSE")
        ))
        for child in local_rows:
            pid = child.get("parent_account_id")
            if pid:
                children_map.setdefault(str(pid), []).append(child)
        for row in rows:
            row["children"] = children_map.get(str(row["id"]), [])

    return {"items": _serialize(rows), "total": total}


# ═══════════════════════════════════════════════════════════════
# 2. GET /api/v1/accounts/{id}
# ═══════════════════════════════════════════════════════════════

@app.get("/api/v1/accounts/{account_id}")
async def get_account(
    account_id: str,
    session: AsyncSession = Depends(get_session),
):
    result = await session.execute(
        text("SELECT * FROM airline_accounts WHERE id = :aid"),
        {"aid": account_id},
    )
    row = result.first()
    if not row:
        raise HTTPException(404, "Account not found")
    account = dict(row._mapping)

    # Nested: recent meetings
    meetings = _rows_to_dicts(await session.execute(
        text(
            "SELECT * FROM meetings WHERE airline_id = :aid "
            "ORDER BY occurred_at DESC LIMIT 20"
        ),
        {"aid": account_id},
    ))

    # Nested: offers
    offers = _rows_to_dicts(await session.execute(
        text(
            "SELECT * FROM offers WHERE airline_id = :aid ORDER BY created_at DESC LIMIT 20"
        ),
        {"aid": account_id},
    ))

    # Nested: action items
    action_items = _rows_to_dicts(await session.execute(
        text(
            "SELECT * FROM action_items WHERE airline_id = :aid ORDER BY created_at DESC LIMIT 20"
        ),
        {"aid": account_id},
    ))

    account["meetings"] = meetings
    account["offers"] = offers
    account["action_items"] = action_items

    # Children (local accounts under this global account)
    children = _rows_to_dicts(await session.execute(
        text(
            "SELECT * FROM airline_accounts WHERE parent_account_id = :aid"
        ),
        {"aid": account_id},
    ))
    account["children"] = children

    # Parent (global account, if this is a local account)
    if account.get("parent_account_id"):
        parent_row = await session.execute(
            text("SELECT * FROM airline_accounts WHERE id = :pid"),
            {"pid": account["parent_account_id"]},
        )
        parent = parent_row.first()
        if parent:
            account["parent"] = dict(parent._mapping)

    return _serialize(account)


# ═══════════════════════════════════════════════════════════════
# 3. GET /api/v1/accounts/{id}/timeline
# ═══════════════════════════════════════════════════════════════

@app.get("/api/v1/accounts/{account_id}/timeline")
async def get_account_timeline(
    account_id: str,
    since: date | None = Query(None),
    until: date | None = Query(None),
    session: AsyncSession = Depends(get_session),
):
    params: dict[str, Any] = {"aid": account_id}
    # airline_id filter goes inside each subquery; only event_at filters go in outer WHERE
    outer_conditions: list[str] = []

    if since:
        outer_conditions.append("event_at >= :since")
        params["since"] = since
    if until:
        outer_conditions.append("event_at <= :until")
        params["until"] = until

    outer_where = "1=1" + (" AND " + " AND ".join(outer_conditions) if outer_conditions else "")

    # Union meetings, offers, action_items, nudges into timeline
    query = f"""
    SELECT * FROM (
        SELECT id, 'meeting' AS event_type, subject AS title,
               summary AS description, occurred_at AS event_at
        FROM meetings WHERE airline_id = :aid

        UNION ALL

        SELECT id, 'offer' AS event_type, offer_type AS title,
               status AS description, created_at AS event_at
        FROM offers WHERE airline_id = :aid

        UNION ALL

        SELECT id, 'action_item' AS event_type, description AS title,
               status AS description, created_at AS event_at
        FROM action_items WHERE airline_id = :aid

        UNION ALL

        SELECT id, 'nudge' AS event_type, nudge_type AS title,
               message AS description, created_at AS event_at
        FROM nudge_log WHERE airline_id = :aid
    ) events
    WHERE {outer_where}
    ORDER BY event_at DESC
    LIMIT 200
    """

    rows = _rows_to_dicts(await session.execute(text(query), params))
    return _serialize(rows)


# ═══════════════════════════════════════════════════════════════
# 4. GET /api/v1/accounts/{id}/snapshots
# ═══════════════════════════════════════════════════════════════

@app.get("/api/v1/accounts/{account_id}/snapshots")
async def get_account_snapshots(
    account_id: str,
    weeks: int = Query(12, ge=1, le=52),
    session: AsyncSession = Depends(get_session),
):
    rows = _rows_to_dicts(await session.execute(
        text(
            "SELECT * FROM account_snapshots "
            "WHERE airline_id = :aid "
            "ORDER BY snapshot_week DESC LIMIT :weeks"
        ),
        {"aid": account_id, "weeks": weeks},
    ))
    return _serialize(rows)


# ═══════════════════════════════════════════════════════════════
# 5. GET /api/v1/executives
# ═══════════════════════════════════════════════════════════════

@app.get("/api/v1/executives")
async def list_executives(
    role: str | None = Query(None),
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
    session: AsyncSession = Depends(get_session),
):
    params: dict[str, Any] = {"limit": limit, "offset": (page - 1) * limit}
    where = ""
    if role:
        where = "WHERE role = :role"
        params["role"] = role

    total = (await session.execute(
        text(f"SELECT COUNT(*) FROM executives {where}"), params
    )).scalar_one()

    rows = _rows_to_dicts(await session.execute(
        text(f"SELECT * FROM executives {where} ORDER BY full_name LIMIT :limit OFFSET :offset"),
        params,
    ))
    return {"items": _serialize(rows), "total": total}


# ═══════════════════════════════════════════════════════════════
# 6. GET /api/v1/executives/{id}/summary
# ═══════════════════════════════════════════════════════════════

@app.get("/api/v1/executives/{exec_id}/summary")
async def get_exec_summary(
    exec_id: str,
    week: date | None = Query(None),
    session: AsyncSession = Depends(get_session),
):
    # Executive info
    exec_row = (await session.execute(
        text("SELECT * FROM executives WHERE id = :eid"), {"eid": exec_id}
    )).first()
    if not exec_row:
        raise HTTPException(404, "Executive not found")
    executive = dict(exec_row._mapping)

    # Target week (default: current ISO week Monday)
    if not week:
        today = date.today()
        week = today - timedelta(days=today.weekday())

    week_start = week
    week_end = week + timedelta(days=7)

    # Accounts summary
    accounts_result = await session.execute(
        text(
            "SELECT a.id, a.airline_name, a.state, a.state_changed_at, "
            "       (SELECT COUNT(*) FROM meetings m "
            "        WHERE m.airline_id = a.id AND m.executive_id = :eid "
            "        AND m.occurred_at >= :ws AND m.occurred_at < :we) AS meetings_this_week, "
            "       (SELECT COUNT(*) FROM offers o "
            "        WHERE o.airline_id = a.id "
            "        AND o.status IN ('proposed','negotiating')) AS open_offers, "
            "       (SELECT COUNT(*) FROM action_items ai "
            "        WHERE ai.airline_id = a.id AND ai.owner_id = :eid "
            "        AND ai.status = 'open' AND ai.due_date < CURRENT_DATE) AS overdue_items "
            "FROM airline_accounts a "
            "JOIN executive_accounts ea ON ea.airline_id = a.id "
            "WHERE ea.executive_id = :eid "
            "ORDER BY a.airline_name"
        ),
        {"eid": exec_id, "ws": week_start, "we": week_end},
    )

    accounts = []
    highlights = []
    risks = []
    for r in accounts_result.fetchall():
        row = r._mapping
        acct = {
            "airline_name": row["airline_name"],
            "state": row["state"],
            "state_change": None,
            "meetings_this_week": row["meetings_this_week"],
            "open_offers": row["open_offers"],
            "overdue_items": row["overdue_items"],
        }
        accounts.append(acct)

        if row["meetings_this_week"] > 0:
            highlights.append(f"{row['airline_name']}: {row['meetings_this_week']} meeting(s) this week")
        if row["overdue_items"] > 0:
            risks.append(f"{row['airline_name']}: {row['overdue_items']} overdue action item(s)")
        if row["state"] in ("COLD", "DORMANT", "STALLED"):
            risks.append(f"{row['airline_name']} is {row['state']}")

    return _serialize({
        "executive": executive,
        "accounts": accounts,
        "highlights": highlights,
        "risks": risks,
    })


# ═══════════════════════════════════════════════════════════════
# 7. GET /api/v1/executives/{id}/guidance
# ═══════════════════════════════════════════════════════════════

@app.get("/api/v1/executives/{exec_id}/guidance")
async def get_exec_guidance(
    exec_id: str,
    session: AsyncSession = Depends(get_session),
):
    # Check cache first
    cached = await cache.get_guidance(exec_id, "__all__")
    if cached:
        return _serialize(cached)

    # Fetch from DB: get accounts for this exec, return any stored guidance
    accounts_result = await session.execute(
        text(
            "SELECT a.id, a.airline_name, a.category "
            "FROM airline_accounts a "
            "JOIN executive_accounts ea ON ea.airline_id = a.id "
            "WHERE ea.executive_id = :eid"
        ),
        {"eid": exec_id},
    )
    accounts = _rows_to_dicts(accounts_result)

    guidance_list = []
    for acct in accounts:
        cached_g = await cache.get_guidance(exec_id, str(acct["id"]))
        if cached_g:
            guidance_list.append({
                "airline_id": str(acct["id"]),
                "airline_name": acct["airline_name"],
                "guidance": cached_g,
            })

    return _serialize(guidance_list)


# ═══════════════════════════════════════════════════════════════
# 8. GET /api/v1/offers
# ═══════════════════════════════════════════════════════════════

@app.get("/api/v1/offers")
async def list_offers(
    status: str | None = Query(None),
    type: str | None = Query(None),
    airline_id: str | None = Query(None),
    exec_id: str | None = Query(None),
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
    session: AsyncSession = Depends(get_session),
):
    conditions = []
    params: dict[str, Any] = {"limit": limit, "offset": (page - 1) * limit}

    if status:
        conditions.append("o.status = :status")
        params["status"] = status
    if type:
        conditions.append("o.offer_type = :type")
        params["type"] = type
    if airline_id:
        conditions.append("o.airline_id = :airline_id")
        params["airline_id"] = airline_id
    if exec_id:
        conditions.append(
            "o.airline_id IN (SELECT airline_id FROM executive_accounts WHERE executive_id = :exec_id)"
        )
        params["exec_id"] = exec_id

    where = "WHERE " + " AND ".join(conditions) if conditions else ""

    total = (await session.execute(
        text(f"SELECT COUNT(*) FROM offers o {where}"), params
    )).scalar_one()

    rows = _rows_to_dicts(await session.execute(
        text(
            f"SELECT o.* FROM offers o {where} "
            f"ORDER BY o.created_at DESC LIMIT :limit OFFSET :offset"
        ),
        params,
    ))
    return {"items": _serialize(rows), "total": total}


# ═══════════════════════════════════════════════════════════════
# 9. GET /api/v1/action-items
# ═══════════════════════════════════════════════════════════════

@app.get("/api/v1/action-items")
async def list_action_items(
    status: str | None = Query(None),
    owner_id: str | None = Query(None),
    overdue: bool | None = Query(None),
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
    session: AsyncSession = Depends(get_session),
):
    conditions = []
    params: dict[str, Any] = {"limit": limit, "offset": (page - 1) * limit}

    if status:
        conditions.append("ai.status = :status")
        params["status"] = status
    if owner_id:
        conditions.append("ai.owner_id = :owner_id")
        params["owner_id"] = owner_id
    if overdue:
        conditions.append("ai.due_date < CURRENT_DATE AND ai.status IN ('open','in_progress')")

    where = "WHERE " + " AND ".join(conditions) if conditions else ""

    total = (await session.execute(
        text(f"SELECT COUNT(*) FROM action_items ai {where}"), params
    )).scalar_one()

    rows = _rows_to_dicts(await session.execute(
        text(
            f"SELECT ai.* FROM action_items ai {where} "
            f"ORDER BY ai.due_date ASC NULLS LAST LIMIT :limit OFFSET :offset"
        ),
        params,
    ))
    return {"items": _serialize(rows), "total": total}


# ═══════════════════════════════════════════════════════════════
# 10. GET /api/v1/nudges
# ═══════════════════════════════════════════════════════════════

@app.get("/api/v1/nudges")
async def list_nudges(
    exec_id: str | None = Query(None),
    acknowledged: bool | None = Query(None),
    session: AsyncSession = Depends(get_session),
):
    conditions = []
    params: dict[str, Any] = {}

    if exec_id:
        conditions.append("nl.executive_id = :exec_id")
        params["exec_id"] = exec_id
    if acknowledged is not None:
        if acknowledged:
            conditions.append("nl.acknowledged_at IS NOT NULL")
        else:
            conditions.append("nl.acknowledged_at IS NULL")

    where = "WHERE " + " AND ".join(conditions) if conditions else ""

    rows = _rows_to_dicts(await session.execute(
        text(
            f"SELECT nl.* FROM nudge_log nl {where} "
            f"ORDER BY nl.created_at DESC LIMIT 100"
        ),
        params,
    ))
    return _serialize(rows)


# ═══════════════════════════════════════════════════════════════
# 11. PATCH /api/v1/nudges/{id}/acknowledge
# ═══════════════════════════════════════════════════════════════

@app.patch("/api/v1/nudges/{nudge_id}/acknowledge")
async def acknowledge_nudge(
    nudge_id: str,
    session: AsyncSession = Depends(get_session),
):
    result = await session.execute(
        text("SELECT * FROM nudge_log WHERE id = :nid"),
        {"nid": nudge_id},
    )
    row = result.first()
    if not row:
        raise HTTPException(404, "Nudge not found")

    await session.execute(
        text("UPDATE nudge_log SET acknowledged_at = now() WHERE id = :nid"),
        {"nid": nudge_id},
    )
    await session.commit()

    updated = (await session.execute(
        text("SELECT * FROM nudge_log WHERE id = :nid"), {"nid": nudge_id}
    )).first()
    return _serialize(dict(updated._mapping))


# ═══════════════════════════════════════════════════════════════
# 12. GET /api/v1/dashboard/overview
# ═══════════════════════════════════════════════════════════════

@app.get("/api/v1/dashboard/overview")
async def dashboard_overview(
    session: AsyncSession = Depends(get_session),
):
    # Check cache
    cached = await cache.get_dashboard_overview("__global__")
    if cached:
        return cached

    # Total accounts
    total = (await session.execute(text("SELECT COUNT(*) FROM airline_accounts"))).scalar_one()

    # Accounts by state
    state_rows = await session.execute(
        text("SELECT state, COUNT(*) AS cnt FROM airline_accounts GROUP BY state")
    )
    accounts_by_state = {r._mapping["state"]: r._mapping["cnt"] for r in state_rows.fetchall()}

    # Offers by status
    offer_rows = await session.execute(
        text("SELECT status, COUNT(*) AS cnt FROM offers GROUP BY status")
    )
    offers_by_status = {r._mapping["status"]: r._mapping["cnt"] for r in offer_rows.fetchall()}

    # Action items
    ai_open = (await session.execute(
        text("SELECT COUNT(*) FROM action_items WHERE status IN ('open','in_progress')")
    )).scalar_one()

    ai_overdue = (await session.execute(
        text(
            "SELECT COUNT(*) FROM action_items "
            "WHERE status IN ('open','in_progress') AND due_date < CURRENT_DATE"
        )
    )).scalar_one()

    # Avg meeting frequency
    avg_freq = (await session.execute(
        text(
            "SELECT AVG(days_between) FROM ("
            "  SELECT airline_id, "
            "    EXTRACT(EPOCH FROM (MAX(occurred_at) - MIN(occurred_at))) / "
            "    GREATEST(COUNT(*) - 1, 1) / 86400 AS days_between "
            "  FROM meetings "
            "  GROUP BY airline_id "
            "  HAVING COUNT(*) > 1"
            ") sub"
        )
    )).scalar_one()

    # Executives with cold accounts
    cold_execs = (await session.execute(
        text(
            "SELECT COUNT(DISTINCT ea.executive_id) "
            "FROM executive_accounts ea "
            "JOIN airline_accounts a ON a.id = ea.airline_id "
            "WHERE a.state IN ('COLD','DORMANT')"
        )
    )).scalar_one()

    overview = {
        "total_accounts": total,
        "accounts_by_state": accounts_by_state,
        "offers_by_status": offers_by_status,
        "action_items_open": ai_open,
        "action_items_overdue": ai_overdue,
        "avg_meeting_frequency_days": round(float(avg_freq), 1) if avg_freq else None,
        "executives_with_cold_accounts": cold_execs,
    }

    await cache.set_dashboard_overview("__global__", overview)
    return overview


# ═══════════════════════════════════════════════════════════════
# 13. GET /api/v1/dashboard/weekly-digest
# ═══════════════════════════════════════════════════════════════

@app.get("/api/v1/dashboard/weekly-digest")
async def get_weekly_digest(
    week: date | None = Query(None),
    exec_id: str | None = Query(None),
    session: AsyncSession = Depends(get_session),
):
    if not week:
        today = date.today()
        week = today - timedelta(days=today.weekday())

    week_str = week.isoformat()
    cached = await cache.get_weekly_digest(week_str)
    if cached:
        if exec_id:
            # Filter to specific exec
            cached["executive_summaries"] = [
                s for s in cached.get("executive_summaries", [])
                if str(s.get("executive", {}).get("id", "")) == exec_id
            ]
        return cached

    return {"week": week_str, "status": "not_generated", "message": "Digest not yet generated. POST to generate."}


# ═══════════════════════════════════════════════════════════════
# 14. POST /api/v1/dashboard/weekly-digest/generate
# ═══════════════════════════════════════════════════════════════

@app.post("/api/v1/dashboard/weekly-digest/generate")
async def generate_weekly_digest(
    body: DigestGenerateRequest,
    session: AsyncSession = Depends(get_session),
):
    job_id = str(uuid.uuid4())

    # Fire-and-forget: schedule digest generation
    asyncio.create_task(_generate_digest_async(body.week, job_id))

    return {"job_id": job_id}


async def _generate_digest_async(week: date, job_id: str) -> None:
    """Background task to generate the weekly digest."""
    try:
        week_start = week
        week_end = week + timedelta(days=7)

        async with async_session_factory() as session:
            # Get all executives
            exec_rows = _rows_to_dicts(
                await session.execute(text("SELECT * FROM executives ORDER BY full_name"))
            )

            executive_summaries = []
            org_highlights = []
            accounts_at_risk = []
            top_deals = []

            for ex in exec_rows:
                eid = str(ex["id"])
                accounts_result = await session.execute(
                    text(
                        "SELECT a.id, a.airline_name, a.state, "
                        "  (SELECT COUNT(*) FROM meetings m "
                        "   WHERE m.airline_id = a.id AND m.executive_id = :eid "
                        "   AND m.occurred_at >= :ws AND m.occurred_at < :we) AS meetings_this_week, "
                        "  (SELECT COUNT(*) FROM offers o "
                        "   WHERE o.airline_id = a.id AND o.status IN ('proposed','negotiating')) AS open_offers, "
                        "  (SELECT COUNT(*) FROM action_items ai "
                        "   WHERE ai.airline_id = a.id AND ai.owner_id = :eid "
                        "   AND ai.status = 'open' AND ai.due_date < CURRENT_DATE) AS overdue_items "
                        "FROM airline_accounts a "
                        "JOIN executive_accounts ea ON ea.airline_id = a.id "
                        "WHERE ea.executive_id = :eid ORDER BY a.airline_name"
                    ),
                    {"eid": eid, "ws": week_start, "we": week_end},
                )

                acct_summaries = []
                exec_highlights = []
                exec_risks = []

                for r in accounts_result.fetchall():
                    row = r._mapping
                    acct_summaries.append({
                        "airline_name": row["airline_name"],
                        "state": row["state"],
                        "state_change": None,
                        "meetings_this_week": row["meetings_this_week"],
                        "open_offers": row["open_offers"],
                        "overdue_items": row["overdue_items"],
                    })
                    if row["meetings_this_week"] > 0:
                        exec_highlights.append(
                            f"{row['airline_name']}: {row['meetings_this_week']} meeting(s)"
                        )
                    if row["state"] in ("COLD", "DORMANT", "STALLED"):
                        exec_risks.append(f"{row['airline_name']} is {row['state']}")
                        accounts_at_risk.append({"id": str(row["id"]), "airline_name": row["airline_name"], "state": row["state"]})

                if acct_summaries:
                    executive_summaries.append({
                        "executive": _serialize(ex),
                        "accounts": acct_summaries,
                        "highlights": exec_highlights,
                        "risks": exec_risks,
                    })
                    org_highlights.extend(exec_highlights)

            # Top deals progressing
            top_deals_result = _rows_to_dicts(await session.execute(
                text(
                    "SELECT * FROM offers "
                    "WHERE status IN ('negotiating', 'accepted') "
                    "AND updated_at >= :ws "
                    "ORDER BY updated_at DESC LIMIT 10"
                ),
                {"ws": week_start},
            ))

            # Best practice of the week
            bp_result = await session.execute(
                text(
                    "SELECT * FROM best_practices WHERE active = TRUE "
                    "ORDER BY confidence DESC LIMIT 1"
                )
            )
            bp_row = bp_result.first()
            best_practice = dict(bp_row._mapping) if bp_row else None

            digest = {
                "week": week.isoformat(),
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "executive_summaries": executive_summaries,
                "org_highlights": org_highlights[:20],
                "top_deals_progressing": _serialize(top_deals_result),
                "accounts_at_risk": accounts_at_risk,
                "best_practice_of_week": _serialize(best_practice),
            }

            await cache.set_weekly_digest(week.isoformat(), digest)

        logger.info("Weekly digest generated for week %s (job_id=%s)", week, job_id)

    except Exception as e:
        logger.error("Digest generation failed (job_id=%s): %s", job_id, e, exc_info=True)


# ═══════════════════════════════════════════════════════════════
# 15. GET /api/v1/best-practices
# ═══════════════════════════════════════════════════════════════

@app.get("/api/v1/best-practices")
async def list_best_practices(
    category: str | None = Query(None),
    airline_category: str | None = Query(None),
    session: AsyncSession = Depends(get_session),
):
    conditions = ["bp.active = TRUE"]
    params: dict[str, Any] = {}

    if category:
        conditions.append("bp.category = :category")
        params["category"] = category
    if airline_category:
        conditions.append("(bp.airline_category = :acategory OR bp.airline_category IS NULL)")
        params["acategory"] = airline_category

    where = "WHERE " + " AND ".join(conditions)
    rows = _rows_to_dicts(await session.execute(
        text(
            f"SELECT bp.* FROM best_practices bp {where} "
            f"ORDER BY bp.confidence DESC LIMIT 50"
        ),
        params,
    ))
    return _serialize(rows)


# ═══════════════════════════════════════════════════════════════
# 16. GET /api/v1/metrics
# ═══════════════════════════════════════════════════════════════

@app.get("/api/v1/metrics")
async def get_metrics(
    start: date | None = Query(None, alias="from"),
    to: date | None = Query(None),
    group_by: str | None = Query(None),
    session: AsyncSession = Depends(get_session),
):
    if not start:
        start = date.today() - timedelta(days=90)
    if not to:
        to = date.today()

    # State distribution over time (from snapshots)
    state_series = _rows_to_dicts(await session.execute(
        text(
            "SELECT snapshot_week, state, COUNT(*) AS count "
            "FROM account_snapshots "
            "WHERE snapshot_week >= :start AND snapshot_week <= :end "
            "GROUP BY snapshot_week, state "
            "ORDER BY snapshot_week"
        ),
        {"start": start, "end": to},
    ))

    # Offer metrics
    offer_series = _rows_to_dicts(await session.execute(
        text(
            "SELECT DATE_TRUNC('week', created_at)::date AS week, "
            "       status, COUNT(*) AS count "
            "FROM offers "
            "WHERE created_at >= :start AND created_at <= :end "
            "GROUP BY week, status "
            "ORDER BY week"
        ),
        {"start": start, "end": to},
    ))

    # Meeting frequency
    meeting_freq = _rows_to_dicts(await session.execute(
        text(
            "SELECT DATE_TRUNC('week', occurred_at)::date AS week, "
            "       COUNT(*) AS meeting_count "
            "FROM meetings "
            "WHERE occurred_at >= :start AND occurred_at <= :end "
            "GROUP BY week ORDER BY week"
        ),
        {"start": start, "end": to},
    ))

    # Action item completion rate
    ai_completion = _rows_to_dicts(await session.execute(
        text(
            "SELECT DATE_TRUNC('week', created_at)::date AS week, "
            "  SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS completed, "
            "  SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled, "
            "  SUM(CASE WHEN status IN ('open','in_progress') THEN 1 ELSE 0 END) AS open, "
            "  COUNT(*) AS total "
            "FROM action_items "
            "WHERE created_at >= :start AND created_at <= :end "
            "GROUP BY week ORDER BY week"
        ),
        {"start": start, "end": to},
    ))

    return _serialize({
        "period": {"from": start, "to": to},
        "state_distribution": state_series,
        "offers": offer_series,
        "meeting_frequency": meeting_freq,
        "action_item_completion": ai_completion,
    })


# ── Nudge engine trigger endpoint (for manual/cron invocation) ──

@app.post("/api/v1/nudge-engine/tick")
async def trigger_nudge_tick():
    """Manually trigger a nudge engine evaluation cycle."""
    stats = await nudge_engine_tick()
    return stats


# ── Entry point ───────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "api.main:app",
        host="0.0.0.0",
        port=8000,
        reload=settings.debug,
        log_level=settings.log_level.lower(),
    )
