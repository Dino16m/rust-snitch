# Snitch

A lightweight **cron job monitoring service** written in Rust. Think "Dead Man's Snitch" for your scheduled tasks — register a job with a cron schedule and a leeway window, and snitch alerts you (via webhook) if the job ever misses its check-in.

## Features

- **Cron-based scheduling** — register jobs with standard cron expressions
- **Punctuality monitoring** — detects missed check-ins within a configurable grace period
- **Webhook alerts** — fires an HTTP POST to a configurable URL when a job is tardy
- **Deduplication** — avoids spamming the same alert for the same missed event
- **RESTful API** — create, check in, query, and remove monitored jobs
- **SQLite persistence** — zero-config, no external database required
- **Docker support** — multi-stage build for production deployment
- **In-memory scheduler** — fast tick loop with periodic persistence to disk

## Architecture

Three threads communicate via `mpsc` channels:

```
[HTTP Server (Rocket)] ←─mpsc─→ [Scheduler] ←─mpsc─→ [Worker]
       │                                                  │
       │                                                  ├─ SQLite CRUD
       └──────────────────────────────────────────────────└─ HTTP webhooks
```

- **Rocket server** handles API requests and forwards actions to the scheduler
- **Scheduler** runs a tick loop, tracks job state in a `HashMap`, detects missed check-ins
- **Worker** executes workloads: persists jobs to SQLite and sends snitch webhooks via `ureq`

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Language | Rust (edition 2024) |
| Web framework | Rocket 0.5 |
| Database | SQLite via microrm |
| Scheduling | cron crate |
| HTTP client | ureq |
| Logging | paris |
| Deployment | Docker / docker-compose |

## API

| Method | Route | Description |
|--------|-------|-------------|
| `POST` | `/api/snitch/jobs` | Create a monitored job |
| `POST` | `/api/snitch/jobs/<id>` | Report a job ran |
| `GET` | `/api/snitch/job/<id>` | Get job details + punctuality status |
| `DELETE` | `/api/snitch/remove/<id>` | Remove a job |

## Quick Start

```bash
# Build and run with Cargo
cargo run

# Or with Docker
docker compose up --build
```

The server starts on `localhost:8000` by default.

## Testing

```bash
cargo test
```

A Node.js test client (`client.js`) is provided to simulate 500 jobs with randomized check-in patterns. Run `server.js` alongside it to observe webhook traffic.

## About

Built with an emphasis on correctness, minimal dependencies, and clean concurrency. The scheduler runs at 20ms resolution, with alert checks every 60 seconds and DB persistence every 3 minutes.
