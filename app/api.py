# FastAPI router — REST API + HTMX partial endpoints
# ====================================================

from __future__ import annotations

import csv
import io
import json
from datetime import UTC, datetime
from zoneinfo import ZoneInfo

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse

from app.config import settings
from app.models import (
    TargetCreate,
    TargetUpdate,
)
from app.scheduler import INSTANCE_ID, get_scheduler
from app.store import (
    bulk_import_targets,
    create_target,
    delete_target,
    get_active_alerts,
    get_all_slow_query_docs,
    get_fleet_status,
    get_last_run,
    get_resolved_alerts,
    get_run,
    get_run_results,
    get_runs,
    get_slow_queries,
    get_slow_query_summary,
    get_target,
    get_target_history,
    get_targets,
    silence_alert,
    silence_alerts_bulk,
    toggle_target,
    update_target,
)

router = APIRouter()

# Jinja2 templates — custom subclass to avoid globals-cache-key issue
# Starlette >=0.28 passes context as globals to get_template(), and Jinja2
# tries to include the entire context dict in the cache key. Pydantic models
# with nested dict fields (clusters, envs, labels) are unhashable → TypeError.
from pathlib import Path

from fastapi.templating import Jinja2Templates as _Jinja2Templates


class _SafeTemplates(_Jinja2Templates):
    """Jinja2Templates subclass that skips globals in get_template() cache key."""

    def TemplateResponse(  # type: ignore[override]
        self,
        name: str,
        context: dict | None = None,
        **kwargs: object,
    ) -> HTMLResponse:
        # Bypass the globals-in-cache-key issue: load template without globals,
        # then render with the full context as usual.
        template = self.env.get_template(name)
        return HTMLResponse(template.render(context or {}), **kwargs)


_templates_dir = Path(__file__).parent / "web" / "templates"
templates = _SafeTemplates(directory=str(_templates_dir))


def _display_tz() -> ZoneInfo:
    """UI display timezone. Read from config; falls back to UTC when invalid."""
    try:
        return ZoneInfo(settings.display_timezone)
    except Exception:
        return ZoneInfo("UTC")


def _localtime(dt: datetime, fmt: str = "%Y-%m-%d %H:%M:%S") -> str:
    """Format a datetime in display_timezone (accepts naive or aware values)."""
    if dt is None:
        return ""
    if dt.tzinfo is None:
        # Naive datetimes are assumed to be UTC (as they come from the database)
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(_display_tz()).strftime(fmt)


templates.env.filters["localtime"] = _localtime


def _now() -> datetime:
    return datetime.now(UTC)


# ── Health ──────────────────────────────────────────────────────


@router.get("/healthz")
async def healthz():
    """Kubernetes liveness probe."""
    return {"status": "ok", "instance": INSTANCE_ID}


@router.get("/readyz")
async def readyz():
    """Kubernetes readiness probe."""
    try:
        await get_last_run()
        return {"status": "ready", "instance": INSTANCE_ID}
    except Exception as exc:
        raise HTTPException(503, "Not ready") from exc


@router.get("/metrics")
async def own_metrics():
    """Kendi Prometheus metriklerimiz."""
    fleet = await get_fleet_status()
    lines = [
        "# HELP healthchecker_targets_total Total enabled targets",
        "# TYPE healthchecker_targets_total gauge",
        f"healthchecker_targets_total {fleet.total}",
        "# HELP healthchecker_targets_status Status distribution",
        "# TYPE healthchecker_targets_status gauge",
        f'healthchecker_targets_status{{status="ok"}} {fleet.ok}',
        f'healthchecker_targets_status{{status="warning"}} {fleet.warning}',
        f'healthchecker_targets_status{{status="critical"}} {fleet.critical}',
        f'healthchecker_targets_status{{status="unknown"}} {fleet.unknown}',
        f"healthchecker_last_run_duration_ms {fleet.last_duration_ms}",
        f'healthchecker_info{{instance="{INSTANCE_ID}"}} 1',
    ]
    return JSONResponse(
        content="\n".join(lines) + "\n",
        media_type="text/plain",
    )


# ── Fleet Status ────────────────────────────────────────────────


@router.get("/api/status")
async def api_status():
    """Fleet status summary."""
    return await get_fleet_status()


# ── Targets CRUD ────────────────────────────────────────────────


@router.get("/api/targets")
async def api_targets(
    enabled_only: bool = False,
    cluster: str | None = None,
    env: str | None = None,
):
    """Target listesi."""
    return await get_targets(enabled_only=enabled_only, cluster=cluster, env=env)


@router.post("/api/targets")
async def api_create_target(data: TargetCreate):
    """Create a target."""
    return await create_target(data)


@router.put("/api/targets/{target_id}")
async def api_update_target(target_id: str, data: TargetUpdate):
    """Update a target."""
    result = await update_target(target_id, data)
    if not result:
        raise HTTPException(404, "Target not found")
    return result


@router.delete("/api/targets/{target_id}")
async def api_delete_target(target_id: str):
    """Target sil."""
    ok = await delete_target(target_id)
    if not ok:
        raise HTTPException(404, "Target not found")
    return {"ok": True}


@router.post("/api/targets/{target_id}/toggle")
async def api_toggle_target(target_id: str):
    """Target enable/disable."""
    result = await toggle_target(target_id)
    if not result:
        raise HTTPException(404, "Target not found")
    return result


@router.post("/api/targets/bulk")
async def api_bulk_import(request: Request):
    """CSV/JSON toplu target import."""
    body = await request.body()
    content_type = request.headers.get("content-type", "")

    targets: list[TargetCreate] = []

    if "application/json" in content_type:
        data = json.loads(body)
        if isinstance(data, list):
            for item in data:
                targets.append(TargetCreate(**item))
        elif isinstance(data, dict) and "targets" in data:
            for item in data["targets"]:
                targets.append(TargetCreate(**item))
    elif "text/csv" in content_type or "text/plain" in content_type:
        text = body.decode("utf-8")
        reader = csv.DictReader(io.StringIO(text))

        def _as_bool(v) -> bool:
            return str(v or "").strip().lower() in ("true", "1", "yes", "t", "on")

        def _as_int(v, default: int) -> int:
            try:
                return int(v)
            except (TypeError, ValueError):
                return default

        for row in reader:
            targets.append(
                TargetCreate(
                    ip=row["ip"],
                    port=_as_int(row.get("port"), 9216),
                    mongo_port=_as_int(row.get("mongo_port"), 27017),
                    hostname=row.get("hostname"),
                    cluster=row.get("cluster"),
                    role=row.get("role"),
                    env=row.get("env"),
                    enabled=_as_bool(row.get("enabled", True)),
                    exporter_url=row.get("exporter_url") or None,
                    mongo_uri=row.get("mongo_uri") or None,
                    check_profile=row.get("check_profile") or "default",
                    notes=row.get("notes") or None,
                )
            )
    else:
        raise HTTPException(400, "Unsupported content type. Use JSON or CSV.")

    result = await bulk_import_targets(targets)
    return result


# ── Targets History ────────────────────────────────────────────


@router.get("/api/targets/{target_id}/history")
async def api_target_history(target_id: str, hours: int = 24):
    """Historical results for a target."""
    return await get_target_history(target_id, hours=hours)


# ── Runs ───────────────────────────────────────────────────────


@router.get("/api/runs")
async def api_runs(limit: int = 20):
    """Run history."""
    return await get_runs(limit=limit)


@router.get("/api/runs/{run_id}")
async def api_run_detail(run_id: str):
    """Run detail."""
    result = await get_run(run_id)
    if not result:
        raise HTTPException(404, "Run not found")
    return result


@router.get("/api/runs/{run_id}/results")
async def api_run_results(run_id: str):
    """Per-target results of a single run."""
    if not await get_run(run_id):
        raise HTTPException(404, "Run not found")
    return await get_run_results(run_id)


# ── HTMX Form Partials ──────────────────────────────────────


@router.get("/targets/add-form", response_class=HTMLResponse)
async def add_target_form(request: Request):
    """Yeni target ekleme formu."""
    return templates.TemplateResponse(
        "target_form.html", {"request": request, "target": None, "action": "add"}
    )


@router.get("/targets/{target_id}/edit-form", response_class=HTMLResponse)
async def edit_target_form(request: Request, target_id: str):
    """Target edit form."""
    target = await get_target(target_id)
    if not target:
        raise HTTPException(404, "Target not found")
    return templates.TemplateResponse(
        "target_form.html", {"request": request, "target": target, "action": "edit"}
    )


@router.get("/targets/bulk-form", response_class=HTMLResponse)
async def bulk_import_form(request: Request):
    """Toplu import formu."""
    return templates.TemplateResponse("bulk_form.html", {"request": request})


@router.post("/targets/add", response_class=HTMLResponse)
async def handle_add_target(request: Request):
    """HTMX form submit handler — yeni target ekle."""
    form = await request.form()
    try:
        await create_target(
            TargetCreate(
                ip=str(form.get("ip", "")),
                port=int(form.get("port", 9216)),
                mongo_port=int(form.get("mongo_port", 27017)),
                hostname=str(form.get("hostname", "")) or None,
                cluster=str(form.get("cluster", "")) or None,
                role=str(form.get("role", "")) or None,
                env=str(form.get("env", "")) or None,
                enabled=form.get("enabled", "on") == "on",
                check_profile=str(form.get("check_profile", "default")),
                notes=str(form.get("notes", "")) or None,
                exporter_url=str(form.get("exporter_url", "")) or None,
                mongo_uri=str(form.get("mongo_uri", "")) or None,
            )
        )
        return HTMLResponse(
            '<script>document.getElementById("modal-overlay").classList.remove("active");'
            "window.location.reload();</script>"
        )
    except Exception as e:
        return HTMLResponse(f'<div class="toast toast-error">{e}</div>', status_code=400)


# ── Alerts ─────────────────────────────────────────────────────


# ── HTMX Form Partials ──────────────────────────────────────


@router.get("/api/alerts")
async def api_alerts(state: str = "firing"):
    """Alert listesi."""
    if state == "resolved":
        return await get_resolved_alerts()
    return await get_active_alerts()


@router.post("/api/alerts/{fingerprint}/silence")
async def api_silence_alert(fingerprint: str, minutes: int = 60):
    """Alert'i sustur."""
    ok = await silence_alert(fingerprint, minutes)
    if not ok:
        raise HTTPException(404, "Alert not found")
    return {"ok": True}


@router.post("/api/alerts/bulk-silence")
async def api_bulk_silence(request: Request):
    """Silence several alerts at once."""
    data = await request.json()
    fingerprints = data.get("fingerprints", [])
    if not fingerprints or not isinstance(fingerprints, list):
        raise HTTPException(400, "fingerprints list required")
    modified = await silence_alerts_bulk(fingerprints)
    return {"ok": True, "modified": modified}


# ── Run Now ────────────────────────────────────────────────────


@router.post("/api/run-now")
async def api_run_now():
    """Trigger a manual run."""
    scheduler = await get_scheduler()
    if scheduler.is_running:
        raise HTTPException(409, "A run is already in progress")
    run_id = await scheduler.run_now(triggered_by="manual")
    return {"run_id": run_id}


# ── UI Pages (HTMX) ───────────────────────────────────────────
# Pages are server-side rendered with Jinja2.


@router.get("/", response_class=HTMLResponse)
async def ui_dashboard(request: Request):
    """Main dashboard page."""
    fleet = await get_fleet_status()
    targets = await get_targets(enabled_only=True)
    return templates.TemplateResponse(
        "dashboard.html",
        {"request": request, "fleet": fleet, "targets": targets},
    )


@router.get("/dashboard/partial", response_class=HTMLResponse)
async def ui_dashboard_partial(request: Request):
    """Dashboard HTMX partial (15s auto-refresh)."""
    fleet = await get_fleet_status()
    targets = await get_targets(enabled_only=True)
    return templates.TemplateResponse(
        "dashboard_partial.html",
        {"request": request, "fleet": fleet, "targets": targets},
    )


@router.get("/targets", response_class=HTMLResponse)
async def ui_targets_page(request: Request):
    """Target management page."""
    targets = await get_targets()
    return templates.TemplateResponse(
        "targets.html",
        {"request": request, "targets": targets},
    )


@router.get("/targets/{target_id}", response_class=HTMLResponse)
async def ui_target_detail(request: Request, target_id: str):
    """Target detail page."""
    target = await get_target(target_id)
    if not target:
        raise HTTPException(404, "Target not found")
    history = await get_target_history(target_id, hours=24)
    return templates.TemplateResponse(
        "target_detail.html",
        {"request": request, "target": target, "history": history},
    )


@router.get("/alerts", response_class=HTMLResponse)
async def ui_alerts_page(request: Request):
    """Alerts page."""
    active = await get_active_alerts()
    resolved = await get_resolved_alerts(limit=50)
    return templates.TemplateResponse(
        "alerts.html",
        {"request": request, "active": active, "resolved": resolved},
    )


@router.get("/runs", response_class=HTMLResponse)
async def ui_runs_page(request: Request):
    """Run history page."""
    runs = await get_runs(limit=50)
    return templates.TemplateResponse(
        "runs.html",
        {"request": request, "runs": runs},
    )


@router.get("/runs/{run_id}", response_class=HTMLResponse)
async def ui_run_detail(request: Request, run_id: str):
    """Run detail page."""
    run = await get_run(run_id)
    if not run:
        raise HTTPException(404, "Run not found")
    results = await get_run_results(run_id)
    return templates.TemplateResponse(
        "run_detail.html",
        {"request": request, "run": run, "results": results},
    )


# ── Slow Queries ──────────────────────────────────────────────


@router.get("/api/slow-queries")
async def api_slow_queries(
    target_id: str | None = None,
    hostname: str | None = None,
    min_ms: int = 0,
    limit: int = 100,
):
    """Snapshotted slow queries (JSON)."""
    return await get_slow_queries(
        target_id=target_id,
        hostname=hostname,
        min_ms=min_ms,
        limit=limit,
    )


@router.get("/api/slow-queries/summary")
async def api_slow_query_summary(limit: int = 20):
    """Slow-query summary grouped by host."""
    return await get_slow_query_summary(limit=limit)


@router.get("/slow-queries", response_class=HTMLResponse)
async def ui_slow_queries_page(request: Request):
    """Slow-query analysis page."""
    summary = await get_slow_query_summary(limit=30)
    queries = await get_slow_queries(limit=100)
    docs = await get_all_slow_query_docs()
    from app.index_suggestions import analyze_slow_queries

    suggestions = analyze_slow_queries(docs)

    # host -> index suggestions for that host (ordered by severity)
    by_host: dict[str, list] = {}
    for s in suggestions:
        for h in s.get("hostnames", []):
            by_host.setdefault(h, []).append(s)

    for row in summary:
        row["suggestions"] = by_host.get(row["hostname"], [])

    return templates.TemplateResponse(
        "slow_queries.html",
        {"request": request, "summary": summary, "queries": queries},
    )


# ── Index Suggestions ─────────────────────────────────────────


@router.get("/api/index-suggestions")
async def api_index_suggestions():
    """Index suggestions derived from snapshots (JSON)."""
    docs = await get_all_slow_query_docs()
    from app.index_suggestions import analyze_slow_queries

    return analyze_slow_queries(docs)


@router.get("/index-suggestions", response_class=HTMLResponse)
async def ui_index_suggestions_page(request: Request):
    """Index suggestions page."""
    docs = await get_all_slow_query_docs()
    from app.index_suggestions import analyze_slow_queries

    suggestions = analyze_slow_queries(docs)
    return templates.TemplateResponse(
        "index_suggestions.html",
        {"request": request, "suggestions": suggestions},
    )
