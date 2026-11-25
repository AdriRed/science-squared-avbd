"""oa_fetch.py — Crawler unificado OpenAlex.

Una única llamada por batch (unión de `select()` de las 5 tablas).
`works.parquet` actúa como *commit marker*: se escribe el último; si está,
implica que las demás tablas también.

Tablas producidas en `../data/external/`:
    works.parquet             (1 fila / work)        ← commit marker
    work_sources.parquet      (1 fila / work)
    work_open_access.parquet  (1 fila / work)
    host_orgs.parquet         (1 fila / work)
    work_authorships.parquet  (1 fila / (work, author, institution))
    fields.parquet            (taxonomía estática, se refresca aparte)
"""

import os
import threading
from pathlib import Path

import pandas as pd
import requests
import pyalex
from pyalex import Works
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

# --- Config -----------------------------------------------------------------
pyalex.config.email   = os.environ["OPENALEX_EMAIL"]
pyalex.config.api_key = os.environ["OPENALEX_API_KEY"]

pyalex.config.max_retries      = 0
pyalex.config.retry_http_codes = [429, 500, 503]

# Unión de los `select()` que antes hacían los scripts sueltos.
FIELDS = [
    "ids",
    "apc_list",
    "apc_paid",
    "awards",
    "funders",
    "primary_topic",
    "topics",
    "referenced_works",
    "primary_location",
    "open_access",
    "authorships",
]

BATCH_SIZE        = 50
PER_PAGE          = 50          # must be >= BATCH_SIZE
MAX_WORKERS       = 5
MAX_RETRIES       = 5
CHECKPOINT_EVERY  = 50          # batches entre snapshots
LONG_WAIT_SECONDS = 120
CHECK_QUOTA_FIRST = True

DEFAULT_DATA_DIR  = Path("../data/external")
DEFAULT_DOIS_PATH = Path("../data/cache/dois.parquet")

# --- Esquemas ---------------------------------------------------------------
WORKS_COLS      = ["openalex_id", "doi",
                   "apc_paid_value_usd", "apc_list_value_usd",
                   "awards", "funders",
                   "primary_topic_field_id", "topics_field_ids",
                   "referenced_works"]
WORKS_LIST_COLS = ["awards", "funders", "topics_field_ids", "referenced_works"]

SOURCES_COLS = ["work_openalex_id", "source_id",
                "source_display_name", "source_type"]

OA_COLS      = ["work_openalex_id", "is_open_access", "open_access_status"]

HOST_COLS    = ["openalex_id", "host_organization_id",
                "host_organization_name"]

AUTH_COLS    = ["work_openalex_id", "author_id",
                "institution_id", "institution_country", "institution_code"]

FIELDS_COLS  = ["field_id", "field_name", "domain_id", "domain_name"]

# name -> (filename, columns, dedupe keys)
TABLE_SPECS = {
    "sources": ("work_sources.parquet",      SOURCES_COLS, ["work_openalex_id"]),
    "oa":      ("work_open_access.parquet",  OA_COLS,      ["work_openalex_id"]),
    "hosts":   ("host_orgs.parquet",         HOST_COLS,    ["openalex_id"]),
    "authors": ("work_authorships.parquet",  AUTH_COLS,
                ["work_openalex_id", "author_id", "institution_id"]),
    "works":   ("works.parquet",             WORKS_COLS,   ["openalex_id"]),
}

# `works` va el último → si se escribe, las demás ya están escritas.
COMMIT_ORDER = ["sources", "oa", "hosts", "authors", "works"]


class CrawlHalted(RuntimeError):
    """Stop duro: cuota diaria agotada o crawl abortado."""


# --- Helpers ----------------------------------------------------------------
def _normalize_doi(doi):
    if pd.isna(doi):
        return None
    doi = str(doi).strip().lower()
    for p in ("https://doi.org/", "http://doi.org/",
              "https://dx.doi.org/", "http://dx.doi.org/", "doi:"):
        if doi.startswith(p):
            doi = doi[len(p):]
    return doi.strip() or None


def _short_id(url):
    """'https://openalex.org/W123' -> 'W123'; None-safe."""
    if not url:
        return None
    return str(url).rstrip("/").rsplit("/", 1)[-1]


def _quota_reason(exc):
    resp = getattr(exc, "response", None)
    if getattr(resp, "status_code", None) != 429:
        return None
    h = getattr(resp, "headers", {}) or {}
    remaining   = h.get("X-RateLimit-Remaining")
    retry_after = h.get("Retry-After")
    try:
        if remaining is not None and int(float(remaining)) <= 0:
            return f"daily budget spent (X-RateLimit-Remaining={remaining})"
    except (TypeError, ValueError):
        pass
    try:
        if retry_after is not None and float(retry_after) > LONG_WAIT_SECONDS:
            return f"server asks for a {retry_after}s wait (Retry-After)"
    except (TypeError, ValueError):
        pass
    return None


# --- Extracción de filas por tabla ------------------------------------------
def _extract_rows(work):
    """Devuelve {tabla: [fila, ...]} para un work."""
    ids = work.get("ids") or {}
    wid = _short_id(ids.get("openalex"))
    if not wid:
        return {}

    # works
    apc_paid = (work.get("apc_paid") or {}).get("value_usd")
    apc_list = (work.get("apc_list") or {}).get("value_usd")
    awards   = work.get("awards") or []
    funders  = work.get("funders") or []
    topics   = work.get("topics") or []
    refs     = work.get("referenced_works") or []
    primary  = work.get("primary_topic") or {}
    pfield   = primary.get("field") or {}

    works_row = {
        "openalex_id":             wid,
        "doi":                     _normalize_doi(ids.get("doi")),
        "apc_paid_value_usd":      apc_paid,
        "apc_list_value_usd":      apc_list,
        "awards":                  [a.get("funder_award_id") for a in awards
                                    if a.get("funder_award_id")],
        "funders":                 [_short_id(f.get("id")) for f in funders
                                    if f.get("id")],
        "primary_topic_field_id":  _short_id(pfield.get("id")),
        "topics_field_ids": [
            _short_id((t.get("field") or {}).get("id"))
            for t in topics
            if (t.get("field") or {}).get("id")
        ],
        "referenced_works":        [_short_id(r) for r in refs if r],
    }

    # sources + hosts comparten primary_location.source
    source = (work.get("primary_location") or {}).get("source") or {}
    sources_row = {
        "work_openalex_id":    wid,
        "source_id":           _short_id(source.get("id")),
        "source_display_name": source.get("display_name"),
        "source_type":         source.get("type"),
    }
    hosts_row = {
        "openalex_id":            wid,
        "host_organization_id":   _short_id(source.get("host_organization")),
        "host_organization_name": source.get("host_organization_name"),
    }

    # open access
    oa = work.get("open_access") or {}
    oa_row = {
        "work_openalex_id":   wid,
        "is_open_access":     oa.get("is_oa"),
        "open_access_status": oa.get("oa_status"),
    }

    # authorships — un author sin institución no aparece (tu regla)
    auth_rows = []
    for a in work.get("authorships") or []:
        author_id = _short_id((a.get("author") or {}).get("id"))
        if not author_id:
            continue
        for inst in a.get("institutions") or []:
            auth_rows.append({
                "work_openalex_id":    wid,
                "author_id":           author_id,
                "institution_id":      _short_id(inst.get("id")),
                "institution_country": inst.get("country_code"),
                "institution_code":    inst.get("ror"),
            })

    return {
        "works":   [works_row],
        "sources": [sources_row],
        "oa":      [oa_row],
        "hosts":   [hosts_row],
        "authors": auth_rows,
    }


# --- Fetch ------------------------------------------------------------------
def _fetch_batch(dois, stop, attempt=0):
    if stop.is_set():
        return []
    try:
        return (Works()
                .filter_or(doi=dois)
                .select(FIELDS)
                .get(per_page=PER_PAGE))
    except Exception as e:
        status = getattr(getattr(e, "response", None), "status_code", None)
        if status == 429:
            reason = _quota_reason(e)
            if reason:
                stop.set()
                raise CrawlHalted(reason) from e
            if attempt >= MAX_RETRIES:
                stop.set()
                raise CrawlHalted(
                    f"persistent 429 after {MAX_RETRIES} retries") from e
            if stop.wait(min(2 ** attempt, 30)):
                raise CrawlHalted("stop requested during backoff") from e
            return _fetch_batch(dois, stop, attempt + 1)
        if status is not None and 400 <= status < 500:
            print(f"  ! HTTP {status} on batch of {len(dois)} DOIs — skipping")
            return []
        if attempt >= MAX_RETRIES:
            raise
        if stop.wait(min(2 ** attempt, 30)):
            raise CrawlHalted("stop requested during backoff") from e
        return _fetch_batch(dois, stop, attempt + 1)


# --- Persistencia -----------------------------------------------------------
def _load_existing(path, cols):
    if not path.exists():
        print(f"No previous {path.name} — starting fresh")
        return pd.DataFrame(columns=cols)
    try:
        df = pd.read_parquet(path)
    except Exception as e:
        raise RuntimeError(
            f"Found {path} but could not read it ({e}). "
            f"Move or delete it to start fresh.") from e
    missing = set(cols) - set(df.columns)
    if missing:
        print(f"⚠ {path.name} is missing columns: {sorted(missing)}")
    return df


class _State:
    def __init__(self, existing):
        self.existing   = existing
        self.new        = {name: [] for name in COMMIT_ORDER}
        self.failed     = []                # [(batch, exc), ...]
        self.works_fetched = 0              # works devueltos por OpenAlex en esta run

    @property
    def works_committed(self):
        return len(self.existing["works"])


def _write_one(state, name, data_dir):
    fname, cols, dedup = TABLE_SPECS[name]
    new_df = pd.DataFrame(state.new[name])
    if name == "works" and "openalex_id" in new_df.columns:
        new_df = new_df[new_df["openalex_id"].notna()]

    combined = pd.concat([state.existing[name], new_df], ignore_index=True)
    combined = combined.drop_duplicates(subset=dedup, keep="last")
    combined = combined.reindex(columns=cols)

    if name == "works":
        for col in WORKS_LIST_COLS:
            combined[col] = combined[col].apply(
                lambda x: x if isinstance(x, list) else [])

    combined.to_parquet(data_dir / fname, index=False, engine="pyarrow")
    state.existing[name] = combined
    state.new[name] = []


def _commit(state, data_dir):
    """Escribe tablas con datos nuevos; `works` la última."""
    for name in COMMIT_ORDER:
        if state.new[name]:
            _write_one(state, name, data_dir)


# --- Crawl ------------------------------------------------------------------
def _crawl(dois, state, data_dir):
    """Devuelve (stopped, reason). Acumula en `state` y hace commit por checkpoint."""
    batches = [dois[i:i + BATCH_SIZE] for i in range(0, len(dois), BATCH_SIZE)]
    print(f"{len(dois)} unique DOIs → {len(batches)} batches")

    stop = threading.Event()
    stopped, reason = False, None
    futures = {}
    pool = ThreadPoolExecutor(max_workers=MAX_WORKERS)

    def _postfix(extra=""):
        base = (f"works={state.works_fetched} "
                f"failed={len(state.failed)} "
                f"cp={state.works_committed}")
        return f"{base} {extra}".strip()

    try:
        futures = {pool.submit(_fetch_batch, b, stop): b for b in batches}
        pbar = tqdm(as_completed(futures), total=len(futures),
                    desc="crawl", unit="batch", mininterval=0.5)
        try:
            for i, fut in enumerate(pbar, 1):
                try:
                    results = fut.result()
                except CrawlHalted as e:
                    stopped, reason = True, str(e)
                    stop.set()
                    pbar.set_postfix_str("HALTED")
                    tqdm.write(f"  ⚠ HALTED — {reason}")
                    break
                except Exception as e:
                    state.failed.append((futures[fut], e))
                    pbar.set_postfix_str(_postfix())
                    continue

                for w in results:
                    rows = _extract_rows(w)
                    for name, lst in rows.items():
                        state.new[name].extend(lst)
                    if rows.get("works"):
                        state.works_fetched += 1

                if i % CHECKPOINT_EVERY == 0:
                    try:
                        _commit(state, data_dir)
                        pbar.set_postfix_str(_postfix("💾"))
                    except Exception as ck_err:
                        tqdm.write(f"  ⚠ commit failed at batch {i}: {ck_err}")
                        pbar.set_postfix_str(_postfix("⚠"))
                else:
                    pbar.set_postfix_str(_postfix())
        except KeyboardInterrupt:
            stopped, reason = True, "user interrupt"
            stop.set()
            tqdm.write("  ⏹ interrupted — saving partial results")
    finally:
        stop.set()
        for f in futures:
            f.cancel()
        try:
            pool.shutdown(wait=True, cancel_futures=True)
        except TypeError:
            pool.shutdown(wait=True)

    return stopped, reason


# --- Cuota ------------------------------------------------------------------
def check_openalex_quota():
    try:
        r = requests.get("https://api.openalex.org/rate-limit",
                         params={"api_key": pyalex.config.api_key},
                         timeout=15)
        r.raise_for_status()
        data = r.json()
    except Exception:
        return None
    for key in ("remaining", "credits_remaining", "daily_remaining"):
        if key in data:
            return data[key]
    rl = data.get("rate_limit") or data.get("rateLimit") or {}
    for key in ("remaining", "credits_remaining"):
        if key in rl:
            return rl[key]
    return None


# --- API pública ------------------------------------------------------------
def run(data_dir=None, dois_path=None, dois_col="doi"):
    """Crawl unificado works + 4 tablas per-work.

    Reanuda por DOI: se omiten los DOIs presentes en works.parquet.
    Devuelve un dict con el resumen.
    """
    data_dir  = Path(data_dir)  if data_dir  else DEFAULT_DATA_DIR
    dois_path = Path(dois_path) if dois_path else DEFAULT_DOIS_PATH

    if CHECK_QUOTA_FIRST:
        remaining = check_openalex_quota()
        if remaining is not None:
            print(f"OpenAlex credits remaining today: {remaining}")
            if float(remaining) <= 0:
                print("Quota already exhausted — nothing fetched. "
                      "Existing data is untouched.")
                return {"stopped": True, "reason": "quota pre-exhausted",
                        "works_total": None, "works_fetched": 0,
                        "failed_batches": 0}

    # Cargar existentes
    existing = {}
    for name, (fname, cols, _) in TABLE_SPECS.items():
        existing[name] = _load_existing(data_dir / fname, cols)

    already_dois = {_normalize_doi(d)
                    for d in existing["works"].get("doi", pd.Series(dtype="str"))
                                       .dropna()}
    already_dois.discard(None)

    # DOIs a procesar
    df = pd.read_parquet(dois_path)
    if dois_col not in df.columns:
        raise SystemExit(f"{dois_path} no tiene columna '{dois_col}'")
    df["doi_norm"] = df[dois_col].map(_normalize_doi)
    todo = (df.loc[df["doi_norm"].notna()
                   & ~df["doi_norm"].isin(already_dois), "doi_norm"]
              .drop_duplicates().tolist())

    print(f"{len(todo)} DOIs to fetch "
          f"({len(df) - len(todo)} input rows already covered)")

    state = _State(existing)
    stopped, reason = False, None

    try:
        if todo:
            stopped, reason = _crawl(todo, state, data_dir)
        else:
            print("Nothing new to fetch — skipping API calls")
    finally:
        _commit(state, data_dir)

        if state.failed:
            print(f"\n{len(state.failed)} batches gave up after retries — "
                  f"their DOIs are NOT in works.parquet and will be retried "
                  f"next run:")
            for batch, err in state.failed[:5]:
                label = (f"{len(batch)} DOIs" if isinstance(batch, list)
                         else str(batch))
                print(f"  {label}: {err}")
            if len(state.failed) > 5:
                print(f"  … and {len(state.failed) - 5} more batches")

        if stopped:
            kind = ("user interrupt" if reason == "user interrupt"
                    else "OpenAlex quota/rate limit")
            print(f"\n⚠ STOPPED EARLY — {kind}.")
            print(f"  reason : {reason}")
            print(f"  saved  : {state.works_committed} works in works.parquet")
            print(f"  → rerun to resume exactly where it stopped.")

    summary = {
        "works_total":   state.works_committed,
        "works_fetched": state.works_fetched,
        "failed_batches": len(state.failed),
        "stopped":       stopped,
        "reason":        reason,
    }
    print(f"\nSummary: {summary}")
    return summary


def refresh_fields(data_dir=None):
    """Descarga /fields y reescribe fields.parquet. Independiente del crawl."""
    data_dir = Path(data_dir) if data_dir else DEFAULT_DATA_DIR
    r = requests.get(
        "https://api.openalex.org/fields",
        params={"per-page": 200, "api_key": pyalex.config.api_key},
        timeout=30,
    )
    r.raise_for_status()
    rows = []
    for f in r.json()["results"]:
        dom = f.get("domain") or {}
        rows.append({
            "field_id":    _short_id(f.get("id")),
            "field_name":  f.get("display_name"),
            "domain_id":   _short_id(dom.get("id")),
            "domain_name": dom.get("display_name"),
        })
    df = pd.DataFrame(rows).reindex(columns=FIELDS_COLS)
    df.to_parquet(data_dir / "fields.parquet", index=False, engine="pyarrow")
    print(f"Wrote {len(df)} fields → fields.parquet")
    return df