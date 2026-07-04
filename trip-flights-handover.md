# Trip.com Flights Sales Intelligence Platform — Build Handover

**Date**: July 4, 2026
**Repo**: https://github.com/shadowfax1312/trip-flights-sales-intel
**Reviewed by**: claude-fable-5
**Handover target**: Next build agent / development team

---

## Intent

Build an AI-powered sales intelligence layer on top of Trip Claw (Trip.com's internal fork of OpenClaw) for the Flights Business Unit. The system automatically reads Outlook emails and attachments for 500 sales executives managing ~7,500 airline accounts, extracts commercial intelligence (offers, commitments, action items), tracks account health via a state machine, and surfaces collective patterns across the org to each executive.

**The business goal**: zero manual CRM entry, automated account health monitoring, pre-meeting intelligence, and collective wisdom from cross-account pattern mining.

---

## Architecture Overview

```
Outlook (Microsoft Graph delta API)
    ↓
Trip Claw email ingestion (per-exec, every 10 min)
    ↓
Kafka: extraction.jobs topic
    ↓
Extraction Worker (aiokafka consumer)
    → extract_meeting_intel skill    → LLM: offers, action items, contacts, sentiment
    → extract_attachment_intel skill → LLM: PDF/XLSX/PPTX/DOCX commercial terms
    ↓
PostgreSQL (accounts, meetings, offers, action_items, etc.)
    ↓
State Machine (runs every 15 min via cron/scheduler)
    → Scores each account: ACTIVE/WARM/STALLED/COLD/DORMANT
    → Fires nudges → nudge_log table → delivery via email/Lark/DingTalk
    ↓
FastAPI (16 endpoints) → Redis cache → Frontend dashboards
    ↓
Weekly collective intelligence miner (mine_collective_intelligence skill)
    → best_practices table → surfaced to execs pre-meeting
```

---

## What Is Built (Fully Implemented)

### Database (`schema.sql` — 202 lines, 10 tables)

| Table | Purpose |
|-------|---------|
| `airline_accounts` | Master airline account per executive (exec_id + airline_id, state, last_contact, health_score) |
| `executives` | Sales executive profiles (name, email, team, region) |
| `executive_accounts` | Many-to-many: exec → airline with assignment metadata |
| `raw_emails` | Every ingested email (subject, body, sender, attachments JSON, processed flag) |
| `meetings` | Extracted meeting records per account (sentiment, action_items_count, offers_count) |
| `offers` | Individual offer records (type, terms JSON, status, deadline, source_email_id) |
| `action_items` | Action items per account (description, due_date, owner, status) |
| `account_snapshots` | Weekly health snapshots (state, health_score, open_offers, overdue_items) |
| `nudge_log` | All fired nudges (type, message, delivered_at, acknowledged_at) |
| `best_practices` | Collective intelligence output (pattern, evidence_count, confidence, airline_category) |

### Skills (`skills/` — all 5 implemented)

| Skill | Input → Output | LLM role |
|-------|---------------|----------|
| `extract_meeting_intel.py` | email body + headers → `MeetingIntel` (offers, action_items, contacts, sentiment) | Structured extraction |
| `extract_attachment_intel.py` | file bytes + MIME type → `AttachmentIntel` (structured_data, excerpts) | PDF/PPTX/XLSX/DOCX parsing + extraction |
| `compute_account_delta.py` | old snapshot + new activities → `AccountDelta` (changes, state recommendation) | Rule-based + LLM summarisation |
| `generate_proactive_guidance.py` | account_history + similar_accounts + best_practices → `ProactiveGuidance` | Synthesis + recommendation |
| `mine_collective_intelligence.py` | batch of accounts → `CollectiveInsights` (patterns, confidence scores) | Map-reduce pattern mining |

### API (`api/main.py` — 16 endpoints)

```
GET  /health
GET  /api/v1/accounts                         — list with state/region/exec filters + pagination
GET  /api/v1/accounts/{id}                    — full account detail with timeline summary
GET  /api/v1/accounts/{id}/timeline           — ordered events (emails, meetings, offers, nudges)
GET  /api/v1/accounts/{id}/snapshots          — historical health snapshots
GET  /api/v1/executives                       — list execs with account health summary
GET  /api/v1/executives/{id}/summary          — exec dashboard: account breakdown by state
GET  /api/v1/executives/{id}/guidance         — calls generate_proactive_guidance for exec's accounts
GET  /api/v1/offers                           — offers with status/type/exec filters
GET  /api/v1/action-items                     — overdue/pending items with urgency scoring
GET  /api/v1/nudges                           — nudge feed with type/exec/state filters
PATCH /api/v1/nudges/{id}/acknowledge         — mark nudge as read
GET  /api/v1/dashboard/overview               — CXO view: KPIs + state distribution by region
GET  /api/v1/dashboard/weekly-digest          — latest generated digest (cached 30 days)
POST /api/v1/dashboard/weekly-digest/generate — trigger digest generation (async background task)
GET  /api/v1/best-practices                   — collective intelligence patterns with filters
GET  /api/v1/metrics                          — time-series: state distribution, offers, meeting freq
POST /api/v1/nudge-engine/tick                — manual trigger of state machine evaluation cycle
```

### Other components
- `core/state_machine.py` — full ACTIVE/WARM/STALLED/COLD/DORMANT logic + `nudge_engine_tick()`
- `core/cache.py` — Redis TTLs: 1h (account context), 4h (email thread), 7d (guidance), 5m (dashboard), 30d (digest)
- `core/database.py` — SQLAlchemy async engine + session factory
- `workers/extraction_worker.py` — aiokafka consumer for `extraction.jobs` topic
- `config/settings.py` — Pydantic BaseSettings (all config vars, env-variable driven)
- `docker-compose.yml` — PostgreSQL 15, Zookeeper, Kafka (confluentinc/cp-kafka:7.6.0), Redis 7, app + worker
- `Dockerfile` — Python 3.12-slim, includes poppler-utils + tesseract-ocr for PDF/image extraction

---

## Open Items — What Is NOT Built

### 1. Email ingestion trigger (HIGH PRIORITY)
The Kafka `extraction.jobs` topic expects messages, but **nothing currently writes to it**. Need:
- Microsoft Graph delta API poller (per-executive, 10-min interval)
- Reads new emails from each exec's Outlook mailbox
- Downloads attachments
- Publishes `{"exec_id": "...", "email_id": "...", "attachments": [...]}` to Kafka

**Where it lives in Trip Claw**: The delta API polling pattern should already exist in Trip Claw's core. Wire this as a scheduled task or adapt the existing Trip Claw email reader.

### 2. Nudge delivery (MEDIUM PRIORITY)
`nudge_engine_tick()` writes nudges to `nudge_log` but **never delivers them**. Need a delivery adapter:
- Lark webhook (preferred for Trip.com internal comms)
- DingTalk webhook (alternative)
- Email fallback (SMTP)

Config already has `NotificationSettings` in `settings.py` with `lark_webhook_url`, `dingtalk_webhook_url`, `smtp_*`. Just needs the send logic.

### 3. Cron scheduler (`cron/nudge_scheduler.py`) — partially
The state machine `nudge_engine_tick()` is built but not wired to a cron. Need:
- APScheduler or system cron: runs `nudge_engine_tick()` every 15 minutes
- Weekly digest generation trigger (currently manual via POST endpoint)
- Collective intelligence mining trigger (weekly, Sunday)

### 4. Authentication / multi-tenancy (MEDIUM)
No auth layer exists. All endpoints are open. For production need:
- JWT or internal SSO (Trip.com uses internal auth — needs integration)
- Per-executive data scoping (an exec should only see their own accounts by default, directors see team)

### 5. Attachment download from Graph API
`extract_attachment_intel.py` expects raw file bytes, but no code fetches attachment bytes from Microsoft Graph. The Outlook poller (#1 above) needs to also download attachment bytes and pass them through.

### 6. `generate` endpoint for weekly digest
`POST /api/v1/dashboard/weekly-digest/generate` creates a background task but the actual generation logic calls `mine_collective_intelligence` skill — this is async and may need a task queue (Celery or asyncio) rather than a FastAPI background task for reliability.

---

## Dependencies — Full List

### Python packages (pinned in `requirements.txt`)
```
fastapi==0.115.6, uvicorn[standard]==0.34.0
pydantic==2.10.4, pydantic-settings==2.7.1
sqlalchemy[asyncio]==2.0.36, asyncpg==0.30.0, psycopg2-binary==2.9.10
redis==5.2.1
aiokafka==0.12.0
httpx==0.28.1          # Trip Claw API calls
python-multipart==0.0.20
python-docx==1.1.2, python-pptx==1.0.2, openpyxl==3.1.5, pdfplumber==0.11.4
pillow==11.1.0, pytesseract==0.3.13    # image-based PDF OCR
orjson==3.10.13, structlog==24.4.0, tenacity==9.0.0
```

### System dependencies (in Dockerfile)
- `poppler-utils` — PDF rendering (pdfplumber dependency)
- `tesseract-ocr` — OCR for image-heavy PDFs

### Infrastructure
| Service | Version | Purpose |
|---------|---------|---------|
| PostgreSQL | 15 | Primary datastore |
| Redis | 7 | Caching (5 TTL tiers) |
| Kafka | confluentinc/cp-kafka:7.6.0 | Async extraction job queue |
| Zookeeper | confluentinc/cp-zookeeper:7.6.0 | Kafka coordination |

---

## External Integrations

### 1. Microsoft Graph (Outlook)
- **Auth**: OAuth2 client credentials flow (tenant_id, client_id, client_secret in settings)
- **Scope**: `https://graph.microsoft.com/Mail.Read` per-user delegated
- **Pattern**: Delta API (`GET /users/{id}/mailFolders/inbox/messages/delta`) — incremental sync
- **NOT YET BUILT** — poller needs to be written

### 2. Trip Claw LLM API
- **Interface**: OpenAI-compatible chat completions endpoint
- **Config**: `TRIP_CLAW_API_URL`, `TRIP_CLAW_API_KEY`, `TRIP_CLAW_MODEL` env vars
- **Used by**: All 5 skills for structured extraction + synthesis
- **Pattern**: POST to `/chat/completions` with `response_format: {type: "json_object"}`
- **Retry**: tenacity (3 attempts, exponential backoff)

### 3. Lark / DingTalk (nudge delivery)
- **NOT YET BUILT** — webhook URLs in config, send logic missing
- Lark: POST to `https://open.feishu.cn/open-apis/bot/v2/hook/{token}`
- DingTalk: POST to `https://oapi.dingtalk.com/robot/send?access_token={token}`

---

## Assumptions & Modifications Needed for Trip Claw / yClaw

### Assumption 1: Trip Claw exposes an OpenAI-compatible endpoint
The skills call `{TRIP_CLAW_API_URL}/chat/completions` with OpenAI-compatible JSON. If Trip Claw uses a different API format (e.g., custom `/generate` endpoint, different message schema), each skill's `_call_llm()` function needs updating.

### Assumption 2: Microsoft Graph OAuth is available
Trip Claw already has Outlook integration via Microsoft Graph delta API. This codebase assumes you can reuse those OAuth tokens/credentials. If Trip Claw uses a different auth approach (service account vs delegated), adjust `config/settings.py` → `OutlookSettings`.

### Assumption 3: Kafka is the right queue
Used Kafka to match Trip Claw's existing infra patterns. If Trip Claw uses a different queue (RabbitMQ, Redis Streams, SQS), swap `aiokafka` for the appropriate client. The extraction worker interface is clean — just swap the consumer loop in `workers/extraction_worker.py`.

### Assumption 4: Account states (needs domain review)
Current 5 states (ACTIVE/WARM/STALLED/COLD/DORMANT) are generic. For airline B2B, consider adding:
- `NEGOTIATING` — active deal in progress (differentiate from just "warm contact")
- `SEASONAL` — account cyclically inactive (avoid spurious COLD alerts during airline planning blackouts)
- `BLOCKED` — external hold (regulatory, airline internal freeze)
- `ESCALATED` — flagged for director/VP attention

The state machine (`core/state_machine.py`) is designed for extension — add new states to the `AccountState` enum and add evaluation logic in `evaluate_account_state()`.

### Assumption 5: Trip Claw skill registration
Skills are standalone Python modules (async functions). In Trip Claw, if skills need to be registered in a skill registry or annotated with metadata decorators, add those wrappers. The core logic is in each skill's main function with clear signatures.

### Assumption 6: `airline_id` master data
The schema has `airline_accounts.airline_id` as a foreign key to an `airlines` table — this table is **not in the schema** (it's assumed to exist in Trip.com's existing data infrastructure). Either:
- Add an `airlines` table to `schema.sql` and seed it
- Or connect to Trip.com's existing airline master data

---

## Environment Variables Required

```env
# PostgreSQL
DATABASE_HOST=localhost
DATABASE_PORT=5432
DATABASE_NAME=trip_flights
DATABASE_USER=trip_flights_user
DATABASE_PASSWORD=...

# Redis
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_DB=0

# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_EXTRACTION_TOPIC=extraction.jobs
KAFKA_CONSUMER_GROUP=extraction-workers

# Microsoft Graph (Outlook)
OUTLOOK_TENANT_ID=...
OUTLOOK_CLIENT_ID=...
OUTLOOK_CLIENT_SECRET=...

# Trip Claw LLM
TRIP_CLAW_API_URL=http://trip-claw-internal/v1
TRIP_CLAW_API_KEY=...
TRIP_CLAW_MODEL=qwen3-72b   # or whatever Trip.com uses

# Notifications
LARK_WEBHOOK_URL=...
DINGTALK_WEBHOOK_URL=...

# Nudge thresholds (optional, have defaults)
NUDGE_WARM_DAYS=14
NUDGE_STALLED_DAYS=7
NUDGE_COLD_DAYS=30
NUDGE_DORMANT_DAYS=90
NUDGE_COOLDOWN_HOURS=48
NUDGE_DAILY_CAP=5
```

---

## How to Run Locally

```bash
git clone https://github.com/shadowfax1312/trip-flights-sales-intel
cd trip-flights-sales-intel
cp .env.example .env   # fill in values
docker-compose up -d
# Apply schema
docker exec -i trip-flights-db psql -U trip_flights_user trip_flights < schema.sql
# Start API
docker-compose up app
# API at http://localhost:8000, docs at http://localhost:8000/docs
```

---

## Suggested Build Order for Next Agent

1. **Write the Outlook delta poller** — feeds the Kafka queue; nothing flows without this
2. **Wire nudge delivery** — Lark/DingTalk webhooks; 10 lines per adapter
3. **Add the `airlines` master table** to schema.sql and seed with IATA data
4. **Wire nudge_scheduler.py** — APScheduler cron, 15-min tick
5. **Auth layer** — JWT middleware, per-executive data scoping
6. **Extend account states** — add NEGOTIATING/SEASONAL/BLOCKED/ESCALATED after domain review with Anuj
