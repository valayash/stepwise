-- Runs: one durable agent execution each.
CREATE TABLE runs (
    id               text        PRIMARY KEY,
    status           text        NOT NULL,
    spec             jsonb       NOT NULL,
    input            text        NOT NULL,
    output           text        NOT NULL DEFAULT '',
    error            text        NOT NULL DEFAULT '',
    failure_kind     text        NOT NULL DEFAULT '',
    budget_tokens    integer     NOT NULL,
    tokens_used      integer     NOT NULL DEFAULT 0,
    attempts         integer     NOT NULL DEFAULT 0,  -- consecutive failures of the current step
    claims           integer     NOT NULL DEFAULT 0,  -- also the lease (fencing) token
    lease_owner      text        NOT NULL DEFAULT '',
    lease_expires_at timestamptz,
    available_at     timestamptz NOT NULL DEFAULT now(),
    parent_run_id    text        REFERENCES runs (id),
    parent_step_id   bigint,
    webhook_url      text        NOT NULL DEFAULT '',
    metadata         jsonb,
    idempotency_key  text        UNIQUE,
    triage           jsonb,
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),
    completed_at     timestamptz,
    CONSTRAINT runs_status_check CHECK (status IN ('queued', 'running', 'waiting', 'completed', 'failed', 'cancelled'))
);

-- Workers look for claimable runs in two places; partial indexes keep both cheap
-- no matter how many finished runs pile up.
CREATE INDEX runs_queued_idx  ON runs (available_at)     WHERE status = 'queued';
CREATE INDEX runs_leased_idx  ON runs (lease_expires_at) WHERE status = 'running';
CREATE INDEX runs_parent_idx  ON runs (parent_run_id)    WHERE parent_run_id IS NOT NULL;
CREATE INDEX runs_status_idx  ON runs (status, created_at DESC);
CREATE INDEX runs_created_idx ON runs (created_at DESC);

-- Steps: the saved history a run is replayed from.
CREATE TABLE steps (
    id           bigserial   PRIMARY KEY,
    run_id       text        NOT NULL REFERENCES runs (id) ON DELETE CASCADE,
    seq          integer     NOT NULL,
    kind         text        NOT NULL,
    status       text        NOT NULL,
    attempts     integer     NOT NULL DEFAULT 0,
    tool_call_id text        NOT NULL DEFAULT '',
    tool_name    text        NOT NULL DEFAULT '',
    tool_input   jsonb,
    llm          jsonb,
    result       text        NOT NULL DEFAULT '',
    is_error     boolean     NOT NULL DEFAULT false,
    approval     jsonb,
    child_run_id text,
    duration_ms  bigint      NOT NULL DEFAULT 0,
    created_at   timestamptz NOT NULL DEFAULT now(),
    completed_at timestamptz,
    UNIQUE (run_id, seq)
);

CREATE INDEX steps_blocking_idx ON steps (run_id) WHERE status IN ('pending_approval', 'waiting_child');

-- Transactional outbox for webhooks.
CREATE TABLE webhook_deliveries (
    id              bigserial   PRIMARY KEY,
    run_id          text        NOT NULL REFERENCES runs (id) ON DELETE CASCADE,
    url             text        NOT NULL,
    event           text        NOT NULL,
    payload         jsonb       NOT NULL,
    status          text        NOT NULL DEFAULT 'pending',
    attempts        integer     NOT NULL DEFAULT 0,
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    leased_until    timestamptz,
    last_error      text        NOT NULL DEFAULT '',
    created_at      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX deliveries_due_idx ON webhook_deliveries (next_attempt_at) WHERE status = 'pending';
CREATE INDEX deliveries_run_idx ON webhook_deliveries (run_id);
