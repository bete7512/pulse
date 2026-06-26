# pulse

**A distributed, event-sourced job queue with CQRS and at-least-once delivery — in Go.**

pulse runs background jobs reliably across a pool of workers: a job is **never lost**, its
**full history is auditable**, and the system **recovers jobs whose worker crashes mid-run**.
Developers integrate through a small SDK and a single URL — Postgres, dispatch, and recovery
are all internal to the server.

> Status: working end-to-end (submit → dispatch → execute → report, with retries, dead-letter,
> and crash recovery). This is a portfolio/learning project built to demonstrate event sourcing,
> CQRS, and distributed-systems reliability — **the reasoning is documented in [`docs/adr/`](docs/adr/).**

---

## Why it's interesting

- **Event-sourced core.** A job's state is an append-only stream of immutable events
  (`JobSubmitted → JobStarted → JobCompleted/Failed → …`). Current status is **computed by
  folding the events, never stored** — so you get a complete, replayable audit trail for free.
- **CQRS.** The write side (the event log) is separate from a **read model** (`jobs` table)
  kept current by an asynchronous projector — fast queries without re-folding every time.
- **Real command-side invariants.** Every state change reconstitutes the aggregate from the
  log, checks a rule (you can't start a canceled job, complete one that isn't running…), then
  appends. A `UNIQUE(job_id, sequence)` constraint + optimistic retry make the **claim race
  safe** — two workers can't run the same job.
- **Reliable distributed dispatch.** Workers connect over gRPC and stream jobs; handlers run in
  the *developer's* process. A **heartbeat + watchdog** recovers jobs whose worker dies mid-run,
  and failures **retry with exponential backoff** before going to a **dead-letter** state.
- **Honest delivery semantics.** At-least-once delivery + idempotent handlers = effectively-once.
  Exactly-once across a network is impossible, and pulse says so (see
  [ADR-0004](docs/adr/0004-command-side-invariants.md)).
- **Scheduling** — run a job once at a time (`At`/`After`), on an interval (`Every`), or on a
  `Cron` expression. Timing lives in a mutable `schedules` table (a future run isn't a *fact*, so
  it's config, not an event); a scheduler loop fires due rows with `FOR UPDATE SKIP LOCKED` and a
  deterministic `uuidv5(schedule_id|occurrence)` id — **exactly-once per occurrence, crash-safe,
  multi-instance**, reusing the same `UNIQUE(job_id, sequence)` invariant. Spawned jobs are
  ordinary jobs tagged with `schedule_id`, so you get full lineage (which jobs a schedule produced,
  when it fired).
- **Priority dispatch.** A job can be submitted with a priority (`WithPriority(n)`, default 0);
  the dispatch head-scan orders `priority DESC, created_at ASC` — higher priority first, ties broken
  by arrival (FIFO). Priority is a submit-time *fact* on `JobSubmitted` (like topic), fully
  backward-compatible, and backed by a partial index so the planner needn't sort the pending set.

---

## Quick start

**1. Postgres + migrations**
```bash
# .env: DB_HOST=postgres://user:pass@localhost:5432/pulse
make migrate_up
```

**2. Run the server**
```bash
make run          # starts pulsed on :50051
```

**3. Use the SDK** (`go get github.com/bete7512/pulse`)
```go
type EmailArgs struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
}

p, _ := pulse.New("localhost:50051", pulse.WithConcurrency(20))
defer p.Close()

// register a handler — a plain typed function:
pulse.Register(p, "send-email", func(ctx context.Context, a EmailArgs) error {
	return sendEmail(a.To, a.Subject)
})

// enqueue a job by name:
pulse.Enqueue(ctx, p, "send-email", EmailArgs{To: "a@b.com", Subject: "Welcome"})

// ...or jump the queue with a higher priority (default 0):
pulse.Enqueue(ctx, p, "send-email", EmailArgs{To: "vip@b.com"}, pulse.WithPriority(10))

p.Run(ctx)        // process jobs until ctx is cancelled
```

**Scheduling** — same SDK, spawn jobs later / repeatedly:
```go
id, _ := p.Schedule(ctx, "reminder",  payload, pulse.At(tomorrow9am))      // once, at a time
id, _ := p.Schedule(ctx, "reconcile", payload, pulse.Every(5*time.Minute)) // on an interval
id, _ := p.Schedule(ctx, "rollup",    payload, pulse.Cron("0 * * * *"))    // hourly, on the hour

p.PauseSchedule(ctx, id); p.ResumeSchedule(ctx, id); p.DeleteSchedule(ctx, id)
p.ListScheduleJobs(ctx, id)   // jobs this schedule spawned (+ status)
p.ListScheduleFires(ctx, id)  // when it fired → which job (lineage)
```
A scheduled job arrives at the *same* `Register`ed handler — workers never know it came from a
schedule. (`pulse.ScheduleJob[T]` is the typed mirror of `Enqueue`.)

See [`examples/`](examples/) for a runnable producer + worker.

---

## Architecture

```
   YOUR APP — the SDK                              PULSE SERVER — pulsed
   ┌──────────────────────┐                       ┌─────────────────────────┐
   │ Enqueue(name, args)  │ ──────  SubmitJob ───► │ append "submitted"      │
   │                      │                       │                         │
   │ Register(name, fn)   │ ──────  StreamJobs ──► │ poll → claim            │
   │ Run()                │ ◄─────  job ────────── │ push to this worker     │
   │   run fn() LOCALLY    │                       │                         │
   │   ReportResult       │ ───  ReportResult ───► │ append done / failed    │
   │   Heartbeat          │ ──────  Heartbeat ───► │ renew liveness          │
   └──────────────────────┘                       └─────────────────────────┘
     handlers run in your process            all state is one append-only
     — only gRPC data crosses the wire       event log  =  the source of truth

   inside the server, background loops run:
     • projector → keeps the jobs read model current      (CQRS, reads the log)
     • watchdog  → re-dispatches jobs whose worker died   (via liveness)
     • scheduler → fires due schedules → appends new jobs (reads the schedules table)
```

The event store is the single source of truth; everything else (read model, dispatch, recovery)
is derived from it. Full details, package layout, and request flows: **[ARCHITECTURE.md](ARCHITECTURE.md)**.

---

## Design decisions (ADRs)

| # | Decision |
|---|---|
| [0001](docs/adr/0001-why-event-sourcing.md) | Why event sourcing (vs. a status column) |
| [0002](docs/adr/0002-architecture.md) | Overall architecture |
| [0003](docs/adr/0003-read-model-projection.md) | Async reconciliation read model (vs. sync / global cursor) |
| [0004](docs/adr/0004-command-side-invariants.md) | Command-side invariants, the claim race, idempotency |

---

## Development

```bash
make migrate_up      # apply DB migrations (goose)
make migrate_down    # roll back
make run             # run the server (cmd/pulsed)
go test -race ./...  # tests
go build ./...       # build everything
```

**Stack:** Go 1.25 · gRPC · Postgres (`pgx`) · goose migrations.

## Roadmap

Done: event-sourced core · Postgres event store · CQRS read model · command-side invariants ·
gRPC server + SDK · retries/backoff/dead-letter · heartbeat + watchdog crash recovery.
Next: observability (OpenTelemetry/Prometheus) · benchmarks · Kubernetes · optional push-based
dispatch (NATS) for lower latency at scale. See [BACKLOG.md](BACKLOG.md).
