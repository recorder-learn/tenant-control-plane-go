-- Two tables. Tenants are never deleted, so UNIQUE(slug) also covers destroyed
-- tenants. Tasks double as the transactional outbox: published_at IS NULL means
-- the task still has to go to the broker.

CREATE TABLE IF NOT EXISTS tenants (
    id          uuid PRIMARY KEY,
    slug        text NOT NULL UNIQUE
                CHECK (slug ~ '^[a-z][-a-z0-9]{1,26}[a-z0-9]$'),
    name        text NOT NULL CHECK (btrim(name) <> ''),
    status      text NOT NULL CHECK (status IN
                ('provisioning','active','updating','destroying','destroyed','failed')),
    version     integer NOT NULL CHECK (version >= 1),
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS tasks (
    id               uuid PRIMARY KEY,
    tenant_id        uuid NOT NULL REFERENCES tenants(id),
    type             text NOT NULL CHECK (type IN ('deploy','update','destroy')),
    status           text NOT NULL CHECK (status IN
                     ('accepted','in_progress','done','failed')),
    error            text,
    last_message_id  uuid,
    published_at     timestamptz,
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS tasks_unpublished ON tasks (id) WHERE published_at IS NULL;
CREATE INDEX IF NOT EXISTS tasks_tenant ON tasks (tenant_id, created_at DESC);
CREATE INDEX IF NOT EXISTS tenants_created ON tenants (created_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS tasks_created ON tasks (created_at DESC, id DESC);
