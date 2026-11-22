# Tenant Control Plane (Go)

A small asynchronous control plane for tenant provisioning. Creating, updating or
destroying a tenant returns immediately with a task; a worker does the "slow work"
and reports back over RabbitMQ.

The whole implementation is about 1,700 lines of Go (plus tests) and two tables.
Everything runs with Docker Compose, so you do not need Go installed to try it.

## What it demonstrates

- **Slug uniqueness under races** — a `UNIQUE` index is the arbiter, not a
  check-then-insert.
- **Optimistic locking** — `PATCH` carries the version the client saw; a
  conditional `UPDATE` rejects stale writes.
- **State machines** — both tenants and tasks move only through allowed states.
- **Publish after commit** — the task row is the outbox. Nothing is published
  until the transaction that wrote it has committed.
- **At-least-once messaging** — the relay retries, the worker may redeliver, and
  the consumer is idempotent and order-tolerant.
- **Poison handling** — bad messages are dead-lettered without stopping the
  consumer.

## Architecture

```text
              POST /v1/tenants
                     |
                     v
  +------------------+------------------+
  |  HTTP API (net/http)                |
  |  validation, error mapping          |
  +------------------+------------------+
                     | one transaction
                     v
  +------------------+------------------+
  |  Postgres                           |
  |  tenants(id, slug UNIQUE, version)  |
  |  tasks(... published_at)  <- outbox |
  +------------------+------------------+
                     ^
       relay (200 ms poll, FOR UPDATE SKIP LOCKED, publisher confirms)
                     |
                     v
              worker.tasks  (RabbitMQ)
                     |
                     v
  +------------------+------------------+
  |  worker simulator                   |
  |  in_progress -> done | failed       |
  +------------------+------------------+
                     |
                     v
       controlplane.task-updates (DLQ on failure)
                     |
                     v
  consumer: one conditional UPDATE, then tenant transition
```

The control plane and the worker are separate binaries built from one module. The
control plane also runs the relay and the update consumer.

## Run it

```sh
docker compose up --build -d --wait
```

That starts Postgres, RabbitMQ, the control plane on <http://localhost:8080> and
one worker. Then create a tenant:

```sh
curl -s -X POST localhost:8080/v1/tenants \
  -H 'Content-Type: application/json' \
  -d '{"slug":"acme","name":"Acme Inc"}'
```

The response is `202` with `{"tenant": ..., "task": ...}`. Within a second or two
the tenant becomes `active`:

```sh
curl -s localhost:8080/v1/tenants/<tenant-id>
```

Update it (use the current version), then delete it:

```sh
curl -s -X PATCH localhost:8080/v1/tenants/<tenant-id> \
  -H 'Content-Type: application/json' \
  -d '{"name":"Acme Corp","version":2}'

curl -s -X DELETE localhost:8080/v1/tenants/<tenant-id>
```

Run the quality gates (gofmt, `go vet`, `go test -race`, including live-stack
integration tests) in a container:

```sh
docker compose --profile tools run --rm --build check
```

Stop everything, keeping data, with `docker compose down`; add `-v` to drop the
volumes too. RabbitMQ's management UI is at <http://localhost:15672>
(`tenants` / `tenants`).

## API

| Method | Path                | Purpose                                   |
| ------ | ------------------- | ----------------------------------------- |
| POST   | `/v1/tenants`       | create; returns tenant + deploy task      |
| GET    | `/v1/tenants`       | list; `status`, `limit`, `offset`         |
| GET    | `/v1/tenants/{id}`  | read one                                  |
| PATCH  | `/v1/tenants/{id}`  | rename; body needs `name` and `version`   |
| DELETE | `/v1/tenants/{id}`  | destroy; returns tenant + destroy task    |
| GET    | `/v1/tasks`         | list; `tenant_id`, `status`, `limit`, `offset` |
| GET    | `/v1/tasks/{id}`    | read one                                  |
| GET    | `/healthz`          | checks the database                       |

`POST` and `PATCH` return `202 Accepted` with `Location` on create. Slugs are
3–28 lowercase letters, digits and hyphens, starting with a letter and ending with
a letter or digit. `limit` is 1–100 and defaults to 50.

Errors use one shape:

```json
{"error": {"code": "tenant_version_conflict", "message": "version 2 is stale; current version is 3"}}
```

| Status | Code                       | When                                        |
| ------ | -------------------------- | ------------------------------------------- |
| 400    | `validation_error`         | malformed JSON, bad slug/name/version/page  |
| 404    | `tenant_not_found`         | unknown tenant id                           |
| 404    | `task_not_found`           | unknown task id                             |
| 404    | `not_found`                | unknown route                               |
| 405    | `method_not_allowed`       | known route, wrong method                   |
| 409    | `tenant_already_exists`    | slug already taken (also for destroyed ones)|
| 409    | `tenant_update_not_allowed`| tenant is in the wrong state / racing write |
| 409    | `tenant_version_conflict`  | stale `version` on `PATCH`                  |

## State machines

Tenants:

```text
create -> provisioning -> active
active -> updating -> active | failed
active | failed -> destroying -> destroyed | failed
```

Tasks:

```text
accepted -> in_progress -> done | failed
accepted -----------------> done | failed
```

Terminal tasks never move again, so a late `in_progress` cannot resurrect one.
The tenant moves in the same transaction as its task, guarded by the tenant's
in-flight status. Deleting is a soft destroy: the row stays, which is why the
slug stays reserved.

## Messaging

The relay claims up to 20 unpublished tasks in a transaction, publishes each with
publisher confirms, sets `published_at`, then commits. A crash before the commit
republishes the batch; a crash after it is fine. That is the outbox guarantee:
at-least-once, never silent loss.

Every update message carries a fresh random `message_id`, plus `task_id`,
`status` and an optional `error`. The consumer stores the last `message_id` on the
task (`tasks.last_message_id`) and applies an update with **one** SQL statement
that checks all of:

- the message is not the one already applied,
- `in_progress` only follows `accepted`,
- `done` / `failed` only follow `accepted` / `in_progress`.

Zero rows means duplicate, stale, or unknown. A duplicate is acknowledged; an
unknown task is permanently rejected. If the database fails, the consumer retries
three times with a short backoff and then dead-letters. JSON garbage and invalid
payloads go straight to `controlplane.task-updates.dlq`.

Because the update is a single conditional write, order does not matter: a
`done` that arrives before `in_progress` wins, and the later `in_progress` is
ignored. The worker acknowledges the task message **after** both updates have been
confirmed, so a worker crash redelivers the task instead of losing it.

## Worker simulation

```sh
docker compose run --rm worker --fail-rate=0.3
```

| Flag            | Default | Meaning                                    |
| --------------- | ------- | ------------------------------------------ |
| `--fail-rate`   | `0`     | probability a task ends `failed`           |
| `--min-delay-ms`| `200`   | minimum simulated work time                |
| `--max-delay-ms`| `600`   | maximum simulated work time                |
| `--duplicate`   | `false` | publish the terminal update twice          |
| `--reorder`     | `false` | publish the outcome before `in_progress`   |

Good things to try on camera:

```sh
# Duplicates and out-of-order updates: tenant still ends active, version bumps once.
docker compose run --rm worker --duplicate --reorder

# Failure path: tenant becomes failed, DELETE still works from failed.
docker compose run --rm worker --fail-rate=1
```

(The `worker` service already runs; `docker compose run` starts an extra
one-off worker, which is the easiest way to change flags without editing files.)

## Decisions and trade-offs

| Decision | Why | What I would do with more time |
| -------- | --- | --- |
| Unique index on `slug` | Correct under concurrency with no locks or retries | Keep; map the constraint to a stable 409 |
| Version guard in `UPDATE` | Cheap optimistic locking, no row locks held across requests | Add ETags / `If-Match` |
| `tasks` doubles as outbox | One write path, no dual-write risk, no extra table | Move to CDC (Debezium) at high volume |
| Single conditional `UPDATE` for updates | Duplicate, stale and invalid transitions are one round trip | Keep |
| `last_message_id` instead of an inbox table | Enough for one producer per task, far less code | Inbox table if many producers or replay needs |
| Soft delete | Keeps task history and reserves the slug | Retention job / hard delete later |
| At-least-once + idempotent consumer | Simpler than exactly-once and honest about failure | Quorum queues for durability |
| Offset pagination | Fine for an interview-sized dataset | Keyset pagination |
| No auth / TLS | Out of scope for the brief | API gateway in front |

Known limits, stated plainly: the relay polls every 200 ms (add `LISTEN/NOTIFY`
for lower latency), a publish confirm and the `mark published` write are not
atomic with each other (hence at-least-once), and there is no rate limiting or
authentication.

## Layout

```text
cmd/controlplane/main.go     HTTP server, relay, update consumer, shutdown
cmd/worker/main.go           worker simulator and its flags
internal/app/
  model.go                   types, transition rules, validation, errors
  store.go                   schema, queries, conditional updates
  service.go                 transactions and state transitions
  http.go                    routes and error mapping
  broker.go                  queues, publisher confirms, relay
  consumer.go                retries and dead-lettering
  schema.sql                 two tables and indexes
  model_test.go              unit tests
  integration_test.go        live-stack tests (skipped without API_URL)
scripts/check.sh             gofmt + vet + test -race
compose.yaml                 the whole stack and the check profile
Dockerfile                   one image, two binaries
```

## If I had another day

- `LISTEN/NOTIFY` instead of polling for the outbox.
- Per-tenant rate limits and an idempotency key on `POST`.
- Metrics (queue depth, task latency, failure rate) and tracing.
- A retention policy for destroyed tenants and finished tasks.
