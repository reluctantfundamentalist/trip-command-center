# Trip.com Flights Sales Intelligence Platform

AI-powered sales enablement platform for Trip.com's Flights organization. Ingests executive email communications, extracts structured intelligence (offers, action items, contacts, sentiment), manages account health via a state machine, and surfaces proactive guidance through a management dashboard.

## Architecture Overview

```
Outlook (Exchange Online)
    |  Microsoft Graph API (delta sync)
    v
Email Ingestion Worker --> Kafka (extraction.jobs) --> Extraction Worker Pool
                                                           |
                                                           v
                                                      PostgreSQL
                                                     /     |     \
                                                    /      |      \
                                           Nudge Engine  Mining   Dashboard API
                                           (cron 15m)   Pipeline  (FastAPI)
                                                        (weekly)
```

## Project Structure

```
.
├── schema.sql                      # Complete PostgreSQL schema (10 tables)
├── api/
│   └── main.py                     # FastAPI app with 15+ endpoints
├── core/
│   ├── database.py                 # SQLAlchemy async engine
│   ├── cache.py                    # Redis caching layer with TTLs
│   └── state_machine.py            # Account state machine + nudge engine
├── skills/
│   ├── extract_meeting_intel.py    # Email/meeting intelligence extraction
│   ├── extract_attachment_intel.py # Attachment analysis (PDF/PPTX/XLSX/DOCX)
│   ├── compute_account_delta.py    # Weekly account delta computation
│   ├── generate_proactive_guidance.py  # AI-driven sales guidance
│   └── mine_collective_intelligence.py # Map-reduce pattern mining
├── workers/
│   └── extraction_worker.py        # Kafka consumer for extraction jobs
├── config/
│   └── settings.py                 # Pydantic BaseSettings configuration
├── docker-compose.yml              # PostgreSQL 15, Kafka, Redis, app
├── Dockerfile                      # Python 3.12 container
└── requirements.txt                # Pinned dependencies
```

## Quick Start

### Prerequisites

- Docker and Docker Compose
- Python 3.12+ (for local development)

### Run with Docker Compose

```bash
# Start all services (PostgreSQL, Kafka, Redis, app, extraction worker)
docker compose up -d

# The schema is automatically applied on first PostgreSQL start
# API is available at http://localhost:8000
# Health check: http://localhost:8000/health
```

### Local Development

```bash
# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Start infrastructure only
docker compose up -d postgres redis kafka zookeeper

# Apply schema manually (if not using docker-compose init)
psql postgresql://flights:flights@localhost:5432/flights_sales -f schema.sql

# Run the API server
uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload

# Run the extraction worker (separate terminal)
python -m workers.extraction_worker
```

## Configuration

All configuration is via environment variables. See `config/settings.py` for full list.

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | `postgresql+asyncpg://flights:flights@localhost:5432/flights_sales` | PostgreSQL connection |
| `REDIS_URL` | `redis://localhost:6379/0` | Redis connection |
| `KAFKA_BROKERS` | `localhost:9092` | Kafka broker list |
| `TRIP_CLAW_API_URL` | `http://localhost:8080/v1` | Trip Claw LLM API endpoint |
| `TRIP_CLAW_API_KEY` | (empty) | Trip Claw API key |
| `OUTLOOK_CLIENT_ID` | (empty) | Microsoft Graph OAuth client ID |
| `OUTLOOK_CLIENT_SECRET` | (empty) | Microsoft Graph OAuth secret |
| `OUTLOOK_TENANT_ID` | (empty) | Azure AD tenant ID |
| `LARK_WEBHOOK_URL` | (empty) | Lark/DingTalk webhook for urgent nudges |
| `SMTP_RELAY_HOST` | (empty) | Internal SMTP relay for email nudges |

## API Endpoints

Base path: `/api/v1`

| Method | Path | Description |
|--------|------|-------------|
| GET | `/accounts` | List accounts with filters (exec_id, state, category) |
| GET | `/accounts/{id}` | Full account detail with nested offers, action items, meetings |
| GET | `/accounts/{id}/timeline` | Chronological events for account |
| GET | `/accounts/{id}/snapshots` | Weekly snapshot history |
| GET | `/executives` | List executives |
| GET | `/executives/{id}/summary` | Weekly summary for one executive |
| GET | `/executives/{id}/guidance` | Latest proactive guidance per account |
| GET | `/offers` | Filter offers across accounts |
| GET | `/action-items` | Filter action items (supports overdue filter) |
| GET | `/nudges` | List nudges (supports acknowledged filter) |
| PATCH | `/nudges/{id}/acknowledge` | Mark nudge as seen |
| GET | `/dashboard/overview` | Aggregate stats for management |
| GET | `/dashboard/weekly-digest` | Generated weekly digest |
| POST | `/dashboard/weekly-digest/generate` | Trigger digest generation |
| GET | `/best-practices` | Active best practices |
| GET | `/metrics` | Time series metrics |

## Account State Machine

Five states with automatic transitions evaluated every 15 minutes:

| State | Condition |
|-------|-----------|
| **ACTIVE** | Meeting in last 14 days AND no overdue items |
| **WARM** | Meeting in last 30 days AND open action items exist |
| **STALLED** | Any action item overdue by 7+ days |
| **COLD** | No meeting/email in 30 days |
| **DORMANT** | No meeting/email in 90 days |

The nudge engine sends alerts on state regressions with configurable cooldowns (48h default) and daily caps (5 per executive) to prevent notification fatigue. Unacknowledged nudges escalate to managers after 3 days.

## Caching Strategy

| Cache Key Pattern | TTL | Purpose |
|-------------------|-----|---------|
| `ctx:{airline_id}` | 1 hour | Account context for extraction |
| `thread:{thread_id}` | 4 hours | Thread summary chain |
| `guidance:{exec_id}:{airline_id}` | 7 days | Proactive guidance output |
| `dashboard:{exec_id}:overview` | 5 minutes | Dashboard overview |
| `digest:{week}` | 30 days | Weekly digest (immutable) |

Write-through invalidation on extraction completion.

## Skills (Trip Claw LLM Integration)

1. **extract_meeting_intel** -- Extracts offers, action items, contacts, sentiment from emails
2. **extract_attachment_intel** -- Structured data extraction from PDF/PPTX/XLSX/DOCX
3. **compute_account_delta** -- Rule-based + LLM weekly account comparison
4. **generate_proactive_guidance** -- AI-driven tactical recommendations
5. **mine_collective_intelligence** -- Map-reduce pattern mining across all accounts
