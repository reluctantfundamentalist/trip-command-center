-- Trip.com Flights Org — Sales Enablement Platform
-- Complete PostgreSQL Schema

-- Enable UUID generation
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ============================================================
-- airline_accounts
-- ============================================================
CREATE TABLE airline_accounts (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    airline_name        TEXT NOT NULL,
    iata_code           CHAR(2),
    category            TEXT NOT NULL CHECK (category IN ('lcc', 'full_service', 'regional', 'cargo', 'charter')),
    email_domains       TEXT[] NOT NULL,
    known_contacts      JSONB NOT NULL DEFAULT '[]',
    state               TEXT NOT NULL DEFAULT 'ACTIVE' CHECK (state IN ('ACTIVE','WARM','COLD','STALLED','DORMANT')),
    state_changed_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    parent_account_id   UUID REFERENCES airline_accounts(id),
    scope               TEXT NOT NULL DEFAULT 'local' CHECK (scope IN ('global', 'local')),
    is_global           BOOLEAN NOT NULL DEFAULT FALSE,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT no_parent_for_global CHECK (
        (is_global = TRUE AND parent_account_id IS NULL) OR
        (is_global = FALSE)
    )
);

CREATE INDEX idx_airline_accounts_state ON airline_accounts(state);
CREATE INDEX idx_airline_accounts_iata ON airline_accounts(iata_code);
CREATE INDEX idx_airline_accounts_parent ON airline_accounts(parent_account_id);
CREATE INDEX idx_airline_accounts_scope ON airline_accounts(scope) WHERE scope = 'global';
CREATE INDEX idx_airline_accounts_global ON airline_accounts(is_global) WHERE is_global = TRUE;

-- ============================================================
-- executives
-- ============================================================
CREATE TABLE executives (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    employee_id     TEXT UNIQUE NOT NULL,
    full_name       TEXT NOT NULL,
    email           TEXT UNIQUE NOT NULL,
    role            TEXT NOT NULL CHECK (role IN ('executive', 'manager', 'director')),
    manager_id      UUID REFERENCES executives(id),
    outlook_delta_token TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ============================================================
-- executive_accounts (many-to-many)
-- ============================================================
CREATE TABLE executive_accounts (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    executive_id    UUID NOT NULL REFERENCES executives(id),
    airline_id      UUID NOT NULL REFERENCES airline_accounts(id),
    role            TEXT NOT NULL DEFAULT 'owner' CHECK (role IN ('owner', 'support', 'observer')),
    assigned_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(executive_id, airline_id)
);

CREATE INDEX idx_exec_accounts_exec ON executive_accounts(executive_id);
CREATE INDEX idx_exec_accounts_airline ON executive_accounts(airline_id);

-- ============================================================
-- raw_emails
-- ============================================================
CREATE TABLE raw_emails (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    outlook_message_id TEXT UNIQUE NOT NULL,
    executive_id    UUID NOT NULL REFERENCES executives(id),
    airline_id      UUID REFERENCES airline_accounts(id),
    thread_id       TEXT,
    from_address    TEXT NOT NULL,
    to_addresses    TEXT[] NOT NULL,
    subject         TEXT,
    body_text       TEXT,
    received_at     TIMESTAMPTZ NOT NULL,
    attachments     JSONB DEFAULT '[]',
    processed       BOOLEAN NOT NULL DEFAULT FALSE,
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_raw_emails_unprocessed ON raw_emails(processed) WHERE processed = FALSE;
CREATE INDEX idx_raw_emails_airline ON raw_emails(airline_id);
CREATE INDEX idx_raw_emails_thread ON raw_emails(thread_id);

-- ============================================================
-- meetings
-- ============================================================
CREATE TABLE meetings (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    airline_id      UUID NOT NULL REFERENCES airline_accounts(id),
    executive_id    UUID NOT NULL REFERENCES executives(id),
    meeting_type    TEXT NOT NULL CHECK (meeting_type IN ('email_thread', 'calendar_meeting', 'call', 'in_person')),
    subject         TEXT,
    summary         TEXT,
    sentiment       TEXT CHECK (sentiment IN ('positive', 'neutral', 'negative', 'mixed')),
    key_contacts    JSONB DEFAULT '[]',
    occurred_at     TIMESTAMPTZ NOT NULL,
    source_email_ids UUID[] DEFAULT '{}',
    extraction_ts   TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_meetings_airline ON meetings(airline_id);
CREATE INDEX idx_meetings_exec ON meetings(executive_id);
CREATE INDEX idx_meetings_occurred ON meetings(occurred_at DESC);

-- ============================================================
-- offers
-- ============================================================
CREATE TABLE offers (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    airline_id      UUID NOT NULL REFERENCES airline_accounts(id),
    meeting_id      UUID REFERENCES meetings(id),
    offer_type      TEXT NOT NULL CHECK (offer_type IN ('discount', 'joint_marketing', 'inventory_bundle', 'exclusive_pricing', 'other')),
    status          TEXT NOT NULL DEFAULT 'proposed' CHECK (status IN ('proposed', 'negotiating', 'accepted', 'rejected', 'expired', 'superseded')),
    terms           JSONB NOT NULL,
    proposed_by     TEXT NOT NULL CHECK (proposed_by IN ('trip', 'airline')),
    proposed_at     TIMESTAMPTZ,
    resolved_at     TIMESTAMPTZ,
    source_email_id UUID REFERENCES raw_emails(id),
    source_attachment TEXT,
    extraction_ts   TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_offers_airline ON offers(airline_id);
CREATE INDEX idx_offers_status ON offers(status);
CREATE INDEX idx_offers_type ON offers(offer_type);

-- ============================================================
-- action_items
-- ============================================================
CREATE TABLE action_items (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    airline_id      UUID NOT NULL REFERENCES airline_accounts(id),
    meeting_id      UUID REFERENCES meetings(id),
    owner_id        UUID NOT NULL REFERENCES executives(id),
    description     TEXT NOT NULL,
    due_date        DATE,
    status          TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'in_progress', 'completed', 'cancelled')),
    completed_at    TIMESTAMPTZ,
    overdue_notified BOOLEAN NOT NULL DEFAULT FALSE,
    source_email_id UUID REFERENCES raw_emails(id),
    source_attachment TEXT,
    extraction_ts   TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_action_items_owner ON action_items(owner_id);
CREATE INDEX idx_action_items_status ON action_items(status);
CREATE INDEX idx_action_items_due ON action_items(due_date) WHERE status = 'open';

-- ============================================================
-- account_snapshots
-- ============================================================
CREATE TABLE account_snapshots (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    airline_id      UUID NOT NULL REFERENCES airline_accounts(id),
    snapshot_week   DATE NOT NULL,
    state           TEXT NOT NULL,
    total_meetings  INT NOT NULL DEFAULT 0,
    meetings_this_week INT NOT NULL DEFAULT 0,
    open_offers     INT NOT NULL DEFAULT 0,
    open_action_items INT NOT NULL DEFAULT 0,
    overdue_action_items INT NOT NULL DEFAULT 0,
    sentiment_trend TEXT,
    delta           JSONB,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(airline_id, snapshot_week)
);

CREATE INDEX idx_snapshots_week ON account_snapshots(snapshot_week DESC);

-- ============================================================
-- nudge_log
-- ============================================================
CREATE TABLE nudge_log (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    airline_id      UUID NOT NULL REFERENCES airline_accounts(id),
    executive_id    UUID NOT NULL REFERENCES executives(id),
    nudge_type      TEXT NOT NULL,
    severity        TEXT NOT NULL CHECK (severity IN ('info', 'warning', 'urgent')),
    message         TEXT NOT NULL,
    delivered_via   TEXT[] NOT NULL,
    escalated       BOOLEAN NOT NULL DEFAULT FALSE,
    acknowledged_at TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_nudge_log_exec ON nudge_log(executive_id, created_at DESC);

-- ============================================================
-- best_practices
-- ============================================================
CREATE TABLE best_practices (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    category        TEXT NOT NULL,
    airline_category TEXT,
    insight         TEXT NOT NULL,
    confidence      FLOAT NOT NULL CHECK (confidence BETWEEN 0 AND 1),
    supporting_evidence JSONB NOT NULL DEFAULT '[]',
    generated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at      TIMESTAMPTZ,
    active          BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE INDEX idx_bp_category ON best_practices(category) WHERE active = TRUE;
