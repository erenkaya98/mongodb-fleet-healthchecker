# Index suggestion engine - detects missing/ineffective indexes from slow_queries snapshots
# ==============================================================================
# Inspects system.profile details: COLLSCAN detection, low-selectivity IXSCAN
# detection, composite index suggestions from filter + sort fields, and emits a
# ready-to-run createIndex command.
#
# IMPORTANT: suggestions are HEURISTIC - they are derived from filter field names
# and sort direction only. Query VALUES and cardinality are unknown. Always verify
# with explain("executionStats") before applying anything in production.

from __future__ import annotations

from collections import defaultdict

# Severity precedence (used for ordering)
_SEVERITY_ORDER = {"critical": 0, "warning": 1, "info": 2}


def _is_field(name) -> bool:
    """Is this a real field name (not an operator)? Strips $and/$or/$exists prefixes."""
    return isinstance(name, str) and not name.startswith("$")


def build_index_keys(filter_keys: list, sort: dict) -> list[tuple[str, int]]:
    """Build an index key pattern from filter + sort fields.

    Heuristic: equality filter fields first (those not in the sort), then sort
    fields (with their direction). This follows MongoDB's equality -> sort -> range
    guideline and is a reasonable starting point.
    """
    keys: list[tuple[str, int]] = []
    sort_fields = [f for f in sort if f != "_id"] if isinstance(sort, dict) else []

    for f in filter_keys or []:
        # _id already has a unique index in every collection; skip it
        if _is_field(f) and f != "_id" and f not in sort_fields:
            keys.append((f, 1))

    if isinstance(sort, dict):
        for f, dirn in sort.items():
            if f == "_id":
                continue  # _id zaten index'li, sort'ta gereksiz
            if f not in [k for k, _ in keys]:
                keys.append((f, 1 if dirn and dirn > 0 else -1))

    return keys


def build_create_index_cmd(ns: str, keys: list[tuple[str, int]]) -> str | None:
    """Emit a createIndex command (background, named)."""
    if not keys:
        return None
    db, _, coll = ns.partition(".")
    if not db or not coll:
        return None
    key_str = ", ".join(f"{k}: {d}" for k, d in keys)
    name = "idx_" + coll + "_" + "_".join(k for k, _ in keys)
    return (
        f'db.getSiblingDB("{db}").{coll}.createIndex('
        f'{{ {key_str} }}, {{ name: "{name}", background: true }})'
    )


def suggest_for_query(q: dict) -> dict:
    """Derive an index suggestion from a single query detail."""
    ns = q.get("ns", "")
    coll = q.get("coll")
    op = q.get("op", "query")
    plan = (q.get("plan_summary") or "").strip()
    filter_keys = q.get("filter_keys") or []
    sort = q.get("sort") or {}
    docs = q.get("docs_examined")
    nret = q.get("nreturned")
    millis = q.get("millis") or 0

    # getMore: planSummary describes the cursor continuation, NOT the original plan.
    # A COLLSCAN here is misleading -> emit an informational note, not a suggestion.
    if op == "getmore":
        return {
            "ns": ns,
            "coll": coll,
            "severity": "info",
            "reason": (
                f"getMore (cursor devamı) — plan orijinal sorguya ait değil. "
                f"{millis}ms, {docs or '?'} doküman okundu"
            ),
            "plan_summary": plan,
            "millis": millis,
            "docs_examined": docs,
            "nreturned": nret,
            "filter_keys": filter_keys,
            "sort": sort,
            "suggested_keys": [],
            "create_index_cmd": None,
        }

    is_collscan = "COLLSCAN" in plan.upper()

    ratio = None
    if docs and nret:
        ratio = docs / max(nret, 1)

    if is_collscan:
        severity = "critical"
        reason = f"COLLSCAN — {docs or '?'} doküman full scan (uygun index yok)"
    elif ratio is not None and ratio >= 1000:
        severity = "critical"
        reason = f"Seçicilik çok düşük: {docs} tarandı → {nret} döndü ({ratio:.0f}x oran)"
    elif ratio is not None and ratio >= 100:
        severity = "warning"
        reason = f"Seçicilik düşük: {docs} tarandı → {nret} döndü ({ratio:.0f}x oran)"
    elif millis >= 5000:
        severity = "warning"
        reason = f"Yavaş sorgu ({millis}ms), index optimizasyonu değerlendirilmeli"
    else:
        severity = "info"
        reason = f"Yavaş sorgu ({millis}ms)"

    keys = build_index_keys(filter_keys, sort)

    return {
        "ns": ns,
        "coll": coll,
        "severity": severity,
        "reason": reason,
        "plan_summary": plan,
        "millis": millis,
        "docs_examined": docs,
        "nreturned": nret,
        "filter_keys": filter_keys,
        "sort": sort,
        "suggested_keys": [{"field": k, "direction": d} for k, d in keys],
        "create_index_cmd": build_create_index_cmd(ns, keys),
    }


def analyze_slow_queries(queries: list[dict]) -> list[dict]:
    """Inspect every snapshot; emit the most critical suggestion per namespace.

    Queries against the same namespace are grouped; the suggestion is derived from
    the slowest representative, annotated with how often it was seen.
    """
    by_ns: dict[str, list[dict]] = defaultdict(list)
    for q in queries:
        ns = q.get("ns")
        if ns:
            by_ns[ns].append(q)

    suggestions: list[dict] = []
    for _ns, qs in by_ns.items():
        # Pick a representative: prefer entries with a NON-EMPTY filter_keys (getMore
        # entries have empty filter_keys and carry a misleading plan), then the slowest.
        # getMore entries cannot produce suggestions - exclude them.
        informative = [q for q in qs if q.get("filter_keys")]
        candidates = informative or qs
        rep = max(candidates, key=lambda x: x.get("millis") or 0)
        s = suggest_for_query(rep)
        s["occurrences"] = len(qs)
        total_ms = sum(x.get("millis") or 0 for x in qs)
        s["avg_ms"] = round(total_ms / len(qs)) if qs else 0
        s["hostnames"] = sorted({x.get("hostname", "?") for x in qs})
        suggestions.append(s)

    suggestions.sort(
        key=lambda s: (
            _SEVERITY_ORDER.get(s["severity"], 3),
            -(s.get("millis") or 0),
        )
    )
    return suggestions
