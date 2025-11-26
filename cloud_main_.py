#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Cloud Run Jobs entrypoint for urlcloner.
- Reads URL list from local or GCS
- Shards with CLOUD_RUN_TASK_INDEX/COUNT
- Calls scraper.scrape_one(url) for each assigned URL
- Writes output/articles.json
- Runs combine.py (translation+enrichment) -> output/articles_enriched.csv
- Optionally runs score_summrize.py -> <base>__with_jp_eval.csv
- Uploads per-shard CSV to gs://<bucket>/outputs/<run_id>/parts/
- Task 0 merges parts into final CSV and writes manifest.json

Env:
  GOOGLE_CLOUD_PROJECT  : required
  MASTER_CSV_GCS        : gs://urlcloner/data/master_media_data-utf8.csv
  OUTPUT_BUCKET         : gs://urlcloner/outputs
  RUN_ID                : optional (auto if missing)
  WAIT_SECS_FOR_PARTS   : optional, default 300
  INPUT_URLS_GCS        : optional; if positional "input" is omitted, use this as URL list path (local or gs://)
"""
from pathlib import Path
import os, sys, argparse, json, time, importlib, subprocess, uuid, datetime as dt
import requests

# --- Robust retry wrapper for flaky GCS operations ---
import traceback
def _retry(fn, *args, tries: int = 5, backoff: int = 2, what: str = "GCS op", **kwargs):
    last = None
    for i in range(1, tries + 1):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            last = e
            wait = backoff * i
            print(f"[RETRY] {what} failed ({i}/{tries}): {type(e).__name__}: {e}; sleep {wait}s", file=sys.stderr)
            time.sleep(wait)
    print(f"[FATAL] {what} failed after {tries} tries: {type(last).__name__}: {last}", file=sys.stderr)
    print("".join(traceback.format_exception(last)), file=sys.stderr)
    raise

from typing import List

from gcs_io import read_text, download_to_file, upload_file, write_bytes, list_prefix

# --- Slack & Signed URL helpers ---
def _post_slack(text: str) -> None:
    hook = os.getenv("SLACK_WEBHOOK_URL")
    if not hook:
        return
    try:
        requests.post(hook, json={"text": text}, timeout=10)
    except Exception as e:
        print(f"[SLACK] failed: {e}", file=sys.stderr)

def _parse_gcs_uri(gs_uri: str) -> tuple[str, str]:
    if not gs_uri.startswith("gs://"):
        raise ValueError(f"not a gs:// URI: {gs_uri}")
    rest = gs_uri[5:]
    bucket, key = rest.split("/", 1)
    return bucket, key

def _get_sa_email() -> str | None:
    # Prefer explicit override
    v = os.getenv("SIGNING_SA_EMAIL")
    if v:
        return v
    # Try metadata server (works on Cloud Run / GCE)
    try:
        import requests as _req
        r = _req.get(
            "http://metadata/computeMetadata/v1/instance/service-accounts/default/email",
            headers={"Metadata-Flavor": "Google"},
            timeout=2,
        )
        if r.ok:
            return r.text.strip()
    except Exception:
        pass
    return None


# --- GCS to Download URL helper ---
def _gcs_to_download_url(gs_uri: str) -> str:
    assert gs_uri.startswith("gs://"), gs_uri
    bucket, key = gs_uri[5:].split("/", 1)
    return f"https://storage.cloud.google.com/{bucket}/{key}"


WORKDIR = Path(".")
DATADIR = WORKDIR / "data"
OUTDIR  = WORKDIR / "output"
DATADIR.mkdir(parents=True, exist_ok=True)
OUTDIR.mkdir(parents=True, exist_ok=True)

# --- Robust local/GCS text reading with encoding fallbacks ---
# You can force a specific encoding via env `INPUT_FILE_ENCODING` (e.g., cp932).
CANDIDATE_ENCODINGS = [
    (os.getenv("INPUT_FILE_ENCODING") or "").strip() or None,
    "utf-8-sig",
    "utf-8",
    "cp932",
    "shift_jis",
    "euc_jp",
    "iso2022_jp",
]
CANDIDATE_ENCODINGS = [e for e in CANDIDATE_ENCODINGS if e]

def _read_text_local_smart(path: Path) -> str:
    data = path.read_bytes()
    for enc in CANDIDATE_ENCODINGS:
        try:
            txt = data.decode(enc)
            print(f"[INPUT] Decoded {path} with encoding={enc}")
            return txt
        except UnicodeDecodeError:
            continue
    # Last resort: keep going with replacement chars so pipeline doesn't crash
    txt = data.decode("utf-8", errors="replace")
    print("[INPUT] WARNING: Failed to detect encoding; using utf-8 with replacement (mojibake possible)")
    return txt

def _read_input_text(uri: str) -> str:
    if uri.startswith("gs://"):
        print(f"[INPUT] reading URL list from {uri}")
        tmp = DATADIR / "_input_urls.tmp"
        _retry(download_to_file, uri, str(tmp), what="download_to_file(input)")
        return _read_text_local_smart(tmp)
    else:
        return _read_text_local_smart(Path(uri))

SCRAPER_MODULE = "scraper"
COMBINE_SCRIPT = Path("combine.py")
SCORE_SCRIPT   = Path("score_summrize.py")

def _now_utc_ts() -> str:
    return dt.datetime.utcnow().strftime("%Y%m%d-%H%M%S")

# Prefer a shared execution-scoped ID so all tasks write to the same output prefix
def _get_run_id() -> str:
    # Prefer values that are constant across all tasks in one execution
    for k in ("RUN_ID", "CLOUD_RUN_EXECUTION", "CLOUD_RUN_EXECUTION_ID", "CLOUD_RUN_JOB_EXECUTION"):
        v = os.getenv(k)
        if v:
            return v
    # Fallback: per-process timestamp+uuid (may differ across tasks if not overridden)
    return _now_utc_ts() + "-" + uuid.uuid4().hex[:8]

def pick_my_shard(urls: List[str]) -> List[str]:
    idx  = int(os.getenv("CLOUD_RUN_TASK_INDEX", "0"))
    size = int(os.getenv("CLOUD_RUN_TASK_COUNT", "1"))
    if size <= 1:
        return urls
    return [u for i, u in enumerate(urls) if i % size == idx]

def import_scraper():
    if SCRAPER_MODULE in sys.modules:
        del sys.modules[SCRAPER_MODULE]
    return importlib.import_module(SCRAPER_MODULE)

def scrape_all(urls: List[str]) -> List[dict]:
    scraper = import_scraper()
    scrape_one = getattr(scraper, "scrape_one", None)
    if scrape_one is None:
        print("[FATAL] scraper.scrape_one(url) not found", file=sys.stderr)
        sys.exit(2)
    out = []
    total = len(urls)
    for i, u in enumerate(urls, 1):
        print(f"[SCRAPE] {i}/{total} → {u}")
        try:
            art = scrape_one(u)
            out.append(art)
            print(f"◯ {i}件目／合計 {total} 件が完了しました。")
        except KeyboardInterrupt:
            raise
        except Exception as e:
            print(f"[WARN] scrape failed: {u} :: {e}", file=sys.stderr)
    return out

def write_articles_json(records: List[dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fp:
        json.dump(records, fp, ensure_ascii=False, indent=2)

def run_combine(env: dict | None = None) -> int:
    if not COMBINE_SCRIPT.exists():
        print("[FATAL] combine.py not found", file=sys.stderr)
        return 2
    cmd = [sys.executable, "-u", str(COMBINE_SCRIPT)]
    print("[COMBINE] Running:", " ".join(cmd))
    return subprocess.call(cmd, env=env or os.environ.copy())

def run_score(input_csv: Path) -> Path:
    if not SCORE_SCRIPT.exists():
        print("[INFO] score_summrize.py not found; skipping")
        return input_csv
    cmd = [sys.executable, "-u", str(SCORE_SCRIPT), str(input_csv)]
    print(f"[SCORE] Input CSV: {input_csv}")
    print("[SCORE] Running:", " ".join(cmd))
    rc = subprocess.call(cmd)
    if rc != 0:
        print(f"[WARN] score_summrize exit={rc}; keep base CSV")
        return input_csv
    base, ext = os.path.splitext(str(input_csv))
    scored = Path(base + "__with_jp_eval" + (ext or ".csv"))
    return scored if scored.exists() else input_csv

def merge_parts_to_final(parts_prefix: str, final_gcs_path: str, timeout_secs: int = 300) -> int:
    start = time.time()
    expected = int(os.getenv("CLOUD_RUN_TASK_COUNT", "1"))
    print(f"[MERGE] Expecting up to {expected} parts under {parts_prefix}")

    seen = set()
    while True:
        items = list_prefix(parts_prefix)
        csvs = [(name, size) for (name, size) in items if name.endswith(".csv")]
        for name, _ in csvs:
            seen.add(name)
        ready = len(seen)
        if ready >= expected:
            print(f"[MERGE] Found {ready}/{expected} parts — proceeding")
            break
        if time.time() - start > timeout_secs:
            print(f"[MERGE] Timeout reached ({timeout_secs}s). Proceed with {ready} parts.")
            break
        time.sleep(5)

    # Concatenate in stable order with global renumbering of "NO." column
    import csv, sys
    from io import StringIO
    # Raise the CSV field size limit to safely read very large fields
    # (e.g., long article bodies or embedded JSON) without hitting the default 128 KiB cap.
    try:
        csv.field_size_limit(sys.maxsize)
    except OverflowError:
        # Fallback for platforms where sys.maxsize is too large for the underlying C long
        csv.field_size_limit(2**31 - 1)

    header = None
    no_col_idx = None
    global_no = 1

    out_buf = StringIO()
    writer = csv.writer(out_buf, lineterminator="\n")

    for name in sorted(seen):
        bucket = final_gcs_path[5:].split("/", 1)[0]
        part_gs = f"gs://{bucket}/{name}"
        tmpf = DATADIR / f"_merge_{os.path.basename(name).replace('/', '_')}"
        _retry(download_to_file, part_gs, str(tmpf), what=f"download_to_file(part {name})")
        data = _read_text_local_smart(tmpf)
        if not data:
            print(f"[MERGE] Skip empty: {name}")
            continue

        # Parse this part as CSV
        part_buf = StringIO(data)
        reader = csv.reader(part_buf)
        try:
            part_header = next(reader)
        except StopIteration:
            continue

        # Initialize header and locate/create NO. column
        if header is None:
            # Try common variants just in case
            candidates = {"NO.", "No.", "No", "no", "番号"}
            found_idx = None
            for idx, col in enumerate(part_header):
                if col.strip() in candidates:
                    found_idx = idx
                    break

            if found_idx is None:
                # Add a new NO. column at the beginning
                header = ["NO."] + part_header
                no_col_idx = 0
            else:
                header = part_header
                no_col_idx = found_idx

            writer.writerow(header)
        else:
            # If later parts have a different header length (shouldn't), pad rows below.
            pass

        # Stream rows, overwrite/insert NO. sequentially
        for row in reader:
            if not row:
                continue
            # If we created NO. at index 0 and this row lacks it (since it's from part files), prepend
            if no_col_idx == 0 and len(row) == len(header) - 1:
                row = [str(global_no)] + row
            else:
                # Ensure row has enough columns
                if no_col_idx >= len(row):
                    row = row + [""] * (no_col_idx - len(row) + 1)
                row[no_col_idx] = str(global_no)
            writer.writerow(row)
            global_no += 1

    # Write as Excel-friendly UTF-8 with BOM and explicit charset metadata
    csv_text = out_buf.getvalue()
    if not csv_text.startswith("\ufeff"):
        csv_text = "\ufeff" + csv_text
    _retry(write_bytes, final_gcs_path, csv_text.encode("utf-8"), what="write_bytes(final_csv)", content_type="text/csv; charset=utf-8")
    print(f"[MERGE] Renumbered NO. 1..{global_no - 1}")
    return 0

def main():
    try:
        p = argparse.ArgumentParser()
        p.add_argument("input", nargs="?", help='URL list path (local or gs://). If omitted, uses env INPUT_URLS_GCS')
        p.add_argument("--translate", type=int, choices=[0,1], default=1)
        p.add_argument("--score-summarize", type=int, choices=[0,1], default=int(os.getenv("SCORE_SUMMARIZE", "1")))
        args = p.parse_args()

        # Allow INPUT_URLS_GCS env as fallback when positional arg is omitted
        input_path = args.input or os.getenv("INPUT_URLS_GCS")
        if not input_path:
            print("[FATAL] input path not provided. Pass a positional 'input' or set env INPUT_URLS_GCS.", file=sys.stderr)
            sys.exit(2)

        run_id = _get_run_id()
        print(f"[RUN] run_id={run_id}")
        print(f"[DEBUG] effective SA (metadata or SIGNING_SA_EMAIL) = {_get_sa_email()}")
        output_bucket = os.getenv("OUTPUT_BUCKET")   # gs://urlcloner/outputs
        master_gcs    = os.getenv("MASTER_CSV_GCS")  # gs://urlcloner/data/master_media_data-utf8.csv
        if not output_bucket or not master_gcs:
            print("[FATAL] OUTPUT_BUCKET and MASTER_CSV_GCS must be set", file=sys.stderr)
            sys.exit(2)

        # 1) Read URLs (robust encoding with fallbacks)
        raw = _read_input_text(input_path)
        urls = [ln.strip() for ln in raw.splitlines() if ln.strip() and not ln.strip().startswith("#")]
        total = len(urls)
        my_urls = pick_my_shard(urls)
        print(f"[URLS] total={total}, assigned={len(my_urls)} (task_index={os.getenv('CLOUD_RUN_TASK_INDEX','0')})")

        # 2) Ensure local master CSV (for combine.py)
        _retry(download_to_file, master_gcs, str(DATADIR / "master_media_data-utf8.csv"), what="download_to_file(master)")

        # 3) Scrape this shard
        records = scrape_all(my_urls)
        articles_json_path = OUTDIR / "articles.json"
        write_articles_json(records, articles_json_path)

        # 4) Run combine.py (translation + enrichment)
        env = os.environ.copy()
        env["ENABLE_TRANSLATE"] = "1" if args.translate == 1 else "0"
        print(f"[COMBINE] ENABLE_TRANSLATE={env.get('ENABLE_TRANSLATE')}")
        print(f"[SCORE] ENABLE_SCORE_SUMMARIZE={'1' if args.score_summarize == 1 else '0'}")
        rc = run_combine(env)
        if rc != 0:
            sys.exit(rc)

        shard_csv = OUTDIR / "articles_enriched.csv"
        if not shard_csv.exists():
            shard_csv.write_text("", encoding="utf-8")

        # 5) Optionally run score summarize
        part_path_local = shard_csv
        if args.score_summarize == 1:
            part_path_local = run_score(shard_csv)

        # 6) Upload part
        task_index = int(os.getenv("CLOUD_RUN_TASK_INDEX", "0"))
        parts_prefix = f"{output_bucket.rstrip('/')}/{run_id}/parts"
        part_gcs = f"{parts_prefix}/articles_enriched.part-{task_index:03d}.csv"
        _retry(upload_file, str(part_path_local), part_gcs, what="upload_file(part_csv)", content_type="text/csv; charset=utf-8")
        print(f"[UPLOAD] part → {part_gcs}")

        if task_index == 0:
            final_filename = f"articles_enriched_{run_id}.csv"
            final_gcs = f"{output_bucket.rstrip('/')}/{run_id}/{final_filename}"
            wait_secs = int(os.getenv("WAIT_SECS_FOR_PARTS", "300"))
            print(f"[DEBUG] preparing to write final CSV to {final_gcs} as SA={_get_sa_email()}")
            merge_parts_to_final(parts_prefix, final_gcs, timeout_secs=wait_secs)

            # Generate browser-downloadable URL
            download_url = _gcs_to_download_url(final_gcs)

            manifest = {
                "run_id": run_id,
                "total_urls": total,
                "tasks": int(os.getenv("CLOUD_RUN_TASK_COUNT","1")),
                "created_at_utc": _now_utc_ts(),
                "input": input_path,
                "output_final": final_gcs,
                "parts_prefix": parts_prefix,
                "download_url": download_url,
            }
            _retry(
                write_bytes,
                f"{output_bucket.rstrip('/')}/{run_id}/manifest.json",
                json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8"),
                what="write_bytes(manifest.json)",
                content_type="application/json",
            )
            print(f"[DONE] Final: {final_gcs}")

            # Slack completion notification (optional)
            if os.getenv("SLACK_WEBHOOK_URL"):
                try:
                    lines = [
                        ":white_check_mark: chronoro genelator 完了",
                        f"Run ID: `{run_id}`",
                        f"URL件数: {total}",
                    ]
                    lines.append(f"CSV: <{download_url}|ダウンロード>")
                    _post_slack("\n".join(lines))
                except Exception as e:
                    print(f"[SLACK] post failed: {e}", file=sys.stderr)
    except Exception as e:
        # Send Slack notification on failure
        run_id = None
        try:
            run_id = os.getenv("RUN_ID") or os.getenv("CLOUD_RUN_EXECUTION") or os.getenv("CLOUD_RUN_EXECUTION_ID") or os.getenv("CLOUD_RUN_JOB_EXECUTION")
        except Exception:
            run_id = None
        emoji = ":x:"
        exc_type = type(e).__name__
        msg_lines = [
            f"{emoji} Cloud Run job failed",
            f"Run ID: `{run_id}`" if run_id else "",
            f"Exception: {exc_type}: {e}",
        ]
        msg = "\n".join(l for l in msg_lines if l)
        if os.getenv("SLACK_WEBHOOK_URL"):
            try:
                _post_slack(msg)
            except Exception as slack_exc:
                print(f"[SLACK] post failed: {slack_exc}", file=sys.stderr)
        # Optionally print traceback
        import traceback
        print("[FATAL] Exception in main():", file=sys.stderr)
        traceback.print_exc()
        # Re-raise or exit with code 1 to ensure job is marked as failed
        sys.exit(1)

if __name__ == "__main__":
    main()
