# MongoDB Fleet Healthchecker

[![CI](https://github.com/erenkaya98/mongodb-fleet-healthchecker/actions/workflows/ci.yml/badge.svg)](https://github.com/erenkaya98/mongodb-fleet-healthchecker/actions/workflows/ci.yml)
[![Python](https://img.shields.io/badge/python-3.11%2B-blue)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](LICENSE)

Health monitoring for a fleet of MongoDB servers. It scrapes `mongodb_exporter` `/metrics`
endpoints and/or connects directly to MongoDB, evaluates a configurable rule set, sends webhook
alerts with deduplication, and serves an HTMX dashboard — all from a single async process.

Built for DBAs who need one page that answers "is anything wrong right now?" across dozens of
nodes, without standing up a full Prometheus + Alertmanager + Grafana stack.

**What it checks:** replica set state and member health, replication lag, oplog window, connection
saturation, WiredTiger cache and ticket pressure, global lock queue, disk and memory usage, slow
queries from `system.profile`, EOL MongoDB versions, FCV mismatches, and auth/bindIp exposure.

> **Note:** the web UI is in Turkish; code, configuration and documentation are in English.

## Architecture

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│  Exporter 1 │    │  Exporter 2 │    │  Exporter N │
│  :9216      │    │  :9216      │    │  :9216      │
└──────┬──────┘    └──────┬──────┘    └──────┬──────┘
       │  HTTP GET         │                  │
       └──────────────────┬┴──────────────────┘
                          │ aiohttp (async)
                   ┌──────▼──────┐
                   │ Healthchecker│─────── Webhook (Slack/Teams)
                   │  FastAPI     │
                   │  APScheduler │─────── Meta MongoDB
                   │  Rule Engine │
                   └─────────────┘
```

- **Data collection**: two modes — `mongodb_exporter` `/metrics` scrape, and/or a direct MongoDB connection via the target's `mongo_uri` (`serverStatus`, WiredTiger, `buildInfo`/FCV, `getCmdLineOpts`, `dbStats`, `hostInfo`, `system.profile`). Direct mode also produces slow-query snapshots and index suggestions.
- **Concurrency**: `asyncio` + `aiohttp.ClientSession` + `asyncio.Semaphore`
- **Scheduler**: `APScheduler` `AsyncIOScheduler`, 5-minute cron, leader lock via meta DB
- **Metadata**: Separate MongoDB cluster (`motor` async driver)
- **UI**: FastAPI + HTMX + Jinja2 server-side rendering, dark theme

## Quick Start (container)

```bash
# 1. Build the image (build context = repo root)
docker build -f .cicd/Dockerfile -t mongo-healthchecker:latest .

# 2. Configure
cp .env.example .env
chmod 600 .env
#   Fill in HC_META_MONGO_URI (and optionally HC_WEBHOOK_URL)

# 3. Run
docker run -d --name mongo-healthchecker \
  --network host \
  --env-file .env \
  --restart unless-stopped \
  mongo-healthchecker:latest

# 4. Verify
curl -s http://localhost:8000/healthz
curl -s http://localhost:8000/api/status | python3 -m json.tool
```

## Deployment

The production image is `.cicd/Dockerfile`, built with the **repo root** as context. Python
dependencies come from `requirements.txt` and are installed at build time, so the build host needs
access to PyPI or an internal mirror.

`--network host` is recommended: the container must reach every fleet node (`mongodb_exporter` on
9216 and/or MongoDB on 27017) and the metadata MongoDB, while inbound port 8000 serves the UI.

### 1. Prerequisites

- Docker
- `mongodb_exporter` (`:9216`) on each target node — or `mongo_uri` credentials for direct checks
- A separate metadata MongoDB, supplied via `HC_META_MONGO_URI`

### 2. Build

```bash
docker build -f .cicd/Dockerfile -t mongo-healthchecker:latest .
```

### 3. Configure

```bash
cp .env.example .env
chmod 600 .env
# Set HC_META_MONGO_URI, and HC_WEBHOOK_URL if you want alerts
```

`HC_META_MONGO_URI` must point at a metadata cluster separate from the fleet being monitored. The
database name is read from `HC_META_MONGO_DB`, **not** from the URI path (`app/db.py`). The target
list never goes into `.env`: prepare a CSV carrying each target's `mongo_uri` and import it through
`POST /api/targets/bulk`.

### 4. Run

```bash
docker run -d --name mongo-healthchecker \
  --network host \
  --env-file .env \
  --restart unless-stopped \
  mongo-healthchecker:latest
```

### 5. Verify

```bash
curl -s http://localhost:8000/healthz                              # -> ok
curl -s http://localhost:8000/api/status | python3 -m json.tool    # total should be > 0
```

### 6. Load the fleet

```bash
curl -sS -X POST http://localhost:8000/api/targets/bulk \
  -H "Content-Type: text/csv" \
  --data-binary @targets.csv
curl -sS -X POST http://localhost:8000/api/run-now
rm -f targets.csv   # contains credentials — delete after import
```

CSV columns:

```csv
ip,port,mongo_port,hostname,cluster,role,env,enabled,check_profile,exporter_url,mongo_uri,notes
```

### Security notes

- `.env` and credential-bearing CSVs are never committed and never enter the image
  (`.dockerignore` excludes `.env*` and `*.csv`; `.gitignore` does the same for git).
- The image runs as the non-root `appuser` and listens on port 8000.
- Give the healthchecker's MongoDB user the minimum it needs: `clusterMonitor` for metrics, plus
  read access to `system.profile` on the databases you want slow-query data from.

## Manual Install

### Prerequisites

- Python 3.11+
- MongoDB 7+ (separate metadata cluster)
- `mongodb_exporter` on every target node (optional when using direct mode)

### Setup

```bash
# 1. Clone
git clone https://github.com/erenkaya98/mongodb-fleet-healthchecker.git
cd mongodb-fleet-healthchecker

# 2. Virtual env
python3.11 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# 3. Configure
cp .env.example .env
chmod 600 .env
# Edit .env — at minimum set HC_META_MONGO_URI

# 4. Run
python -m app.main

# 5. Load targets (separate terminal)
curl -sS -X POST http://localhost:8000/api/targets/bulk \
  -H "Content-Type: text/csv" --data-binary @targets.csv
```

## Configuration

All settings via `HC_`-prefixed environment variables (see `.env.example`):

| Variable | Default | Description |
|---|---|---|
| `HC_META_MONGO_URI` | `mongodb://localhost:27017/healthchecker` | Metadata MongoDB URI |
| `HC_META_MONGO_DB` | `healthchecker` | Metadata DB name (overrides the URI path) |
| `HC_SCRAPE_CONCURRENCY` | `10` | Parallel scrape limit |
| `HC_SCRAPE_TIMEOUT_TOTAL` | `30` | Total HTTP timeout (seconds) |
| `HC_DIRECT_CONNECT_TIMEOUT` | `10` | Direct MongoDB connect timeout (seconds) |
| `HC_DIRECT_SERVER_SELECTION_TIMEOUT` | `5` | Direct MongoDB server selection timeout (seconds) |
| `HC_SLOW_QUERY_THRESHOLD_MS` | `100` | Minimum duration counted as "slow" in `system.profile` |
| `HC_SLOW_QUERY_WINDOW_SECONDS` | `300` | Look-back window in `system.profile` (seconds) |
| `HC_SLOW_QUERY_SNAPSHOT_MIN_MS` | `1000` | Minimum duration for a full query snapshot |
| `HC_SCHEDULER_CRON` | `*/5` | Cron **minute** field only |
| `HC_SCHEDULER_LOCK_TTL` | `120` | Leader lock TTL (seconds) |
| `HC_DISPLAY_TIMEZONE` | `UTC` | UI timezone (IANA zoneinfo name); invalid values fall back to UTC |
| `HC_RULES_FILE` | `rules.yml` | Rule file path (relative to CWD) |
| `HC_WEBHOOK_URL` | (empty) | Slack/Teams webhook URL |
| `HC_WEBHOOK_FORMAT` | `generic` | `slack`, `teams`, `generic` |
| `HC_ALERT_FOR_CONSECUTIVE` | `2` | Consecutive failures before firing |
| `HC_ALERT_RENOTIFY_MINUTES` | `60` | Re-notify interval (0 = every time) |
| `HC_DASHBOARD_BASE_URL` | `http://localhost:8000` | Deep-link base URL |

## Rules (`rules.yml`)

Profile-based rule sets, selected per target via the `check_profile` field:

| Profile | Rules | Intended for |
|---|---|---|
| `default` | 25 | Replica-set members and everything else |
| `standalone` | 16 | Standalone mongods (no replica-set rules) |
| `configsvr` | 2 | Config servers |
| `mongos` | 2 | `mongos` routers |
| `arbiter` | 2 | Arbiters |

```yaml
profiles:
  default:
    - id: mongodb_up
      title: MongoDB process up
      metric: [mongodb_up]
      aggregation: value
      op: ne
      critical: 1
      enabled: true
```

**Fields:**
- `metric`: Alternative metric names (first-found wins; missing → UNKNOWN)
- `aggregation`: `value`, `sum`, `max`, `min`, `count`, `ratio`, `oplog_window`, `delta`
- `op`: `gt`, `gte`, `lt`, `lte`, `eq`, `ne`, `absent`
- `skip_when_absent`: emit nothing instead of UNKNOWN when the metric does not exist on that
  MongoDB major (e.g. WiredTiger ticket metrics on 8.0 → the rule is skipped, not alarmed)

**Built-in rules (default profile):**
- Reachability: `exporter_reachable`, `scrape_latency`, `mongodb_up`
- Replica set: `replset_state`, `replset_health`, `replication_lag`, `oplog_window_hours`, `member_count`
- Connections / resources: `connections_saturation`, `cursor_timeouts`, `global_lock_queue`, `disk_usage`, `memory_usage`
- WiredTiger: `wt_cache_used_percent`, `wt_cache_dirty_percent`, `wt_read_tickets`, `wt_write_tickets`
- Version: `mongod_version_eol`, `fcv_mismatch`
- Security: `security_authorization`, `security_bindip_public`, `security_auth_exposed`
- Slow queries: `slow_query_count`, `slow_query_max_ms`
- Other: `assert_warnings`

Thresholds (warning/critical) live in `rules.yml`. Tune them for your own fleet —
edit them there, not in code.

## API

| Endpoint | Method | Description |
|---|---|---|
| `/healthz` | GET | Liveness probe |
| `/readyz` | GET | Readiness probe (DB connected + last run exists) |
| `/metrics` | GET | Own Prometheus metrics |
| `/api/status` | GET | Fleet summary (OK/WARN/CRIT/UNK counts) |
| `/api/targets` | GET/POST | List/add targets |
| `/api/targets/{id}` | PUT/DELETE | Update/delete target |
| `/api/targets/{id}/toggle` | POST | Enable/disable toggle |
| `/api/targets/{id}/history?hours=24` | GET | Target result timeline |
| `/api/targets/bulk` | POST | CSV/JSON bulk import |
| `/api/runs` | GET | Run history |
| `/api/alerts?state=firing` | GET | Active alerts |
| `/api/alerts/{fp}/silence?minutes=N` | POST | Silence alert |
| `/api/alerts/bulk-silence` | POST | Silence several alerts by fingerprint |
| `/api/slow-queries` | GET | Slow-query snapshots |
| `/api/slow-queries/summary` | GET | Slow queries grouped by host |
| `/api/index-suggestions` | GET | Heuristic index suggestions |
| `/api/run-now` | POST | Trigger manual run |

## UI Pages

- **`/`** — Dashboard: cluster grid, summary counters, 15s auto-refresh
- **`/targets`** — Target management: inline add/edit/delete, bulk import, enable/disable toggle
- **`/targets/{id}`** — Target detail: 24h timeline, check history, OS/WiredTiger metrics, error log
- **`/alerts`** — Active + resolved alerts (per-severity cards), silence button
- **`/runs`** — Run history, timing and status breakdown
- **`/slow-queries`** — Slow-query snapshots, full query view
- **`/index-suggestions`** — Heuristic `createIndex` suggestions (validate with `.explain()` before applying)

## systemd Unit

For a container-less install. `WorkingDirectory` matters: `rules.yml` and `.env` are read from the CWD.

```ini
[Unit]
Description=MongoDB Fleet Healthchecker
After=network.target

[Service]
Type=simple
User=healthchecker
WorkingDirectory=/opt/healthchecker
EnvironmentFile=/opt/healthchecker/.env
ExecStart=/opt/healthchecker/venv/bin/python -m app.main
Restart=always
RestartSec=10
KillSignal=SIGTERM

[Install]
WantedBy=multi-user.target
```

## mongodb_exporter Setup

On each MongoDB node:

```bash
# Download
wget https://github.com/percona/mongodb_exporter/releases/download/v0.40.0/mongodb_exporter-0.40.0.linux-amd64.tar.gz
tar xzf mongodb_exporter-0.40.0.linux-amd64.tar.gz

# Run (point to your mongod)
./mongodb_exporter \
  --mongodb.uri=mongodb://localhost:27017/admin \
  --web.listen-address=:9216 \
  --collect-all
```

## Project Structure

```
app/
  config.py         # pydantic-settings, HC_ env vars
  models.py         # Status enum, pydantic models
  db.py             # motor connection, ensure_indexes()
  store.py          # Target CRUD, run/state/alert persistence
  promparse.py      # Prometheus text format parser (no deps)
  collector.py      # aiohttp scraper + direct MongoDB checks
  rules.py          # Rule engine from rules.yml
  alerting.py       # Webhook notifier + state machine
  scheduler.py      # APScheduler + leader lock
  index_suggestions.py  # Heuristic index suggestions from slow queries
  api.py            # FastAPI REST + HTMX routes
  main.py           # Entry point, lifespan, uvicorn
  web/templates/    # Jinja2 templates
  static/           # CSS + HTMX.js
rules.yml           # Rule definitions and thresholds
requirements.txt    # Python dependencies
.env.example        # Environment variable template
.cicd/Dockerfile    # Production image (build context: repo root)
pytest.ini          # Test configuration
pyproject.toml      # ruff configuration
tests/              # Test suite (excluded from the image via .dockerignore)
.github/workflows/  # CI: ruff, pytest (3.11-3.13), docker build, gitleaks
.pre-commit-config.yaml  # gitleaks, ruff, detect-private-key
```


## Development

```bash
python3.11 -m venv venv && source venv/bin/activate
pip install -r requirements.txt pytest pytest-asyncio pre-commit
pre-commit install          # gitleaks, ruff, detect-private-key on every commit
```

### Tests

```bash
pytest tests/ -v            # 130 tests, no MongoDB required (aiohttp test server + mocks)
```

Coverage: the Prometheus parser (edge cases, escapes, Inf/NaN), the rule engine (OK/WARN/CRIT per
rule), the alert state machine (pending -> firing -> renotify -> escalation -> resolved), and the
collector (retry policy, timeouts, per-target failure isolation, WiredTiger across MongoDB 6/7/8).

### Linting

```bash
ruff check . && ruff format --check .
```

## Security

- Never commit `.env` or any CSV holding `mongo_uri` credentials. Both are covered by
  `.gitignore` and `.dockerignore`, and `pre-commit` runs gitleaks on every commit.
- Grant the healthchecker's MongoDB user the minimum it needs: `clusterMonitor` for metrics, plus
  read access to `system.profile` on the databases you want slow-query data from.
- Webhook URLs are masked in logs.

## Contributing

Issues and pull requests are welcome. Please run `pre-commit run --all-files` and `pytest` before
opening a PR.

## License

MIT — see [LICENSE](LICENSE).
