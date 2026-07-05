# Project State Summary — Trip.com Flights Sales Intelligence Platform

**Date**: 2026-07-05
**Status**: Active development — account hierarchy implemented, CEO dashboard running

---

## What was built

A full-stack AI-powered sales intelligence platform for Trip.com's Flights organization.

### Architecture
- **Backend**: FastAPI + PostgreSQL + Redis + Kafka
- **Ingestion**: Microsoft Graph delta API (Outlook email poller)
- **AI Extraction**: Anthropic API (LLM-powered entity extraction from emails)
- **State Machine**: Account health tracking with nudge engine
- **Frontend**: CXO dashboard (`trip-cxo-dashboard.html`) + Intel feed (`trip-intel-feed.html`)

### Key features delivered

1. **Email ingestion pipeline** — Microsoft Graph delta API polls Outlook, extracts airline-relevant emails, sends extraction jobs to Kafka

2. **LLM extraction worker** — Kafka consumer, extracts meetings, offers, action items, contacts, sentiment from emails using Anthropic API

3. **Account hierarchy** — Self-referential hierarchy in `airline_accounts`:
   - `parent_account_id` — links local accounts to global parent
   - `scope` — 'global' or 'local'
   - `is_global` — TRUE for root accounts
   - Domain matching routes emails to global parent when local has no domain match
   - Cache invalidation cascades to parent on extraction complete
   - State machine aggregates child signals for global accounts

4. **CEO Dashboard** (`trip-cxo-dashboard.html`) — Real-time view with:
   - KPI strip (total accounts, offers, action items, meeting frequency)
   - Health map by region
   - Alert feed (cold/stalled/overdue accounts)
   - Velocity trends

5. **Account hierarchy seeded**:
   - Emirates (global, `ab761948-2007-4951-862b-f1806c2eae48`) — parent
     - Emirates UAE (local, `f7650f20-ec09-4037-b4d2-a475feaefc18`)
     - Emirates India (local, `5b5627a8-6444-48d5-9fd0-0996bc95469a`)

6. **Executive assigned**: Anuj Bansal (`E001`) owns all 8 accounts via `executive_accounts`

---

## Outstanding issues

1. **CXO dashboard sparse** — All 8 accounts show as ACTIVE, no regional grouping (accounts lack `region` field), no cold/stalled nudges firing yet
2. **Metrics API** — Limited data: `meeting_frequency` only has 1 week, `state_distribution` and `action_item_completion` are empty
3. **Nudges** — `/api/v1/nudges` returns empty; state machine evaluation may not be running on a schedule
4. **Guidance API** — `/api/v1/executives/{id}/guidance` not yet tested
5. **Weekly digest** — `/api/v1/dashboard/weekly-digest` exists but not triggered

---

## API endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/dashboard/overview` | Aggregate stats across all accounts |
| GET | `/api/v1/executives/{id}/summary` | CEO dashboard — per-account breakdown + highlights + risks |
| GET | `/api/v1/accounts` | List all accounts |
| GET | `/api/v1/accounts?include_children=true` | List accounts with children nested |
| GET | `/api/v1/accounts?global_only=true` | List only global (root) accounts |
| GET | `/api/v1/accounts/{id}` | Account detail with `children` (if global) or `parent` (if local) |
| GET | `/api/v1/accounts/{id}/timeline` | Ordered events (meetings, offers, action_items, nudges) |
| GET | `/api/v1/accounts/{id}/snapshots` | Historical health snapshots |
| GET | `/api/v1/accounts/{id}/dashboard` | Per-account dashboard (NOT YET IMPLEMENTED — returns 404) |
| GET | `/api/v1/executives` | List all executives |
| GET | `/api/v1/executives/{id}/summary` | Executive summary with account health |
| GET | `/api/v1/executives/{id}/guidance` | Proactive AI guidance per executive |
| GET | `/api/v1/dashboard/weekly-digest` | Weekly digest |
| POST | `/api/v1/dashboard/weekly-digest/generate` | Generate weekly digest |
| GET | `/api/v1/nudges` | Active nudges (triggers) |
| GET | `/api/v1/metrics` | Trends and metrics |
| GET | `/api/v1/offers` | Offers with filters |
| GET | `/api/v1/action-items` | Action items with urgency |

---

## To resume work

1. **Start backend**: `python -m uvicorn api.main:app --host 0.0.0.0 --port 8000`
2. **Start extraction worker**: `python -m workers.extraction_worker`
3. **Start Outlook poller**: `python -m workers.outlook_poller`
4. **Open dashboard**: `frontend/trip-cxo-dashboard.html` (file:// URL)

---

## Pending tasks (from last session)

- [ ] Enhance per-account dashboard (`/accounts/{id}/dashboard`)
- [ ] Add region field to accounts for health map grouping
- [ ] Trigger weekly digest generation
- [ ] Test guidance endpoint
- [ ] Verify state machine evaluation runs on schedule
- [ ] Seed more realistic account data (contacts, historical meetings)
