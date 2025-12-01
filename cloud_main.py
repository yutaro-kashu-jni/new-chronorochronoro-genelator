#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Cloud Run Jobsのエントリーポイント。
・URLリストをローカルまたはGCSから取得
・CLOUD_RUN_TASK_INDEX/COUNTで分割して並列処理
・各URLに対してscraper.scrape_one(url)を実行
・output/articles.jsonに結果を書き出し
・combine.pyで翻訳・情報付加 → output/articles_enriched.csv
・必要に応じてscore_summrize.pyで評価 → <base>__with_jp_eval.csv
・シャードごとのCSVをGCSにアップロード
・タスク0が全パートをマージし、最終CSVとmanifest.jsonを作成

【主な環境変数】
    GOOGLE_CLOUD_PROJECT  : 必須
    MASTER_CSV_GCS        : gs://urlcloner/data/master_media_data-utf8.csv
    OUTPUT_BUCKET         : gs://urlcloner/outputs
    RUN_ID                : 任意（未指定なら自動生成）
    WAIT_SECS_FOR_PARTS   : 任意、デフォルト300
    INPUT_URLS_GCS        : 任意。引数inputがなければこれをURLリストとして利用
"""
from pathlib import Path
import os, sys, argparse, json, time, importlib, subprocess, uuid, datetime as dt
import requests

# --- GCSなどの不安定な操作をリトライするラッパー関数 ---
import traceback
def _retry(fn, *args, tries: int = 5, backoff: int = 2, what: str = "GCS op", **kwargs):
    # GCSなどの不安定な操作をリトライするラッパー関数
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

# --- Slack通知や署名付きURL関連のヘルパー ---
def _post_slack(text: str) -> None:
    # Slack通知用。SLACK_WEBHOOK_URLが設定されていればメッセージを送信
    hook = os.getenv("SLACK_WEBHOOK_URL")
    if not hook:
        return
    try:
        requests.post(hook, json={"text": text}, timeout=10)
    except Exception as e:
        print(f"[SLACK] failed: {e}", file=sys.stderr)

def _parse_gcs_uri(gs_uri: str) -> tuple[str, str]:
    # gs://bucket/key 形式のURIをバケット名とキーに分割
    if not gs_uri.startswith("gs://"):
        raise ValueError(f"not a gs:// URI: {gs_uri}")
    rest = gs_uri[5:]
    bucket, key = rest.split("/", 1)
    return bucket, key

def _get_sa_email() -> str | None:
    # サービスアカウントのメールアドレスを取得（明示指定またはメタデータサーバ）
    # 明示的な環境変数があればそれを優先
    v = os.getenv("SIGNING_SA_EMAIL")
    if v:
        return v
    # メタデータサーバから取得（Cloud RunやGCE上で動作する場合）
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


# --- GCSのgs://パスをダウンロード用のhttps URLに変換するヘルパー ---
def _gcs_to_download_url(gs_uri: str) -> str:
    # GCSのgs://パスをダウンロード用のhttps URLに変換
    assert gs_uri.startswith("gs://"), gs_uri
    bucket, key = gs_uri[5:].split("/", 1)
    return f"https://storage.cloud.google.com/{bucket}/{key}"


WORKDIR = Path(".")
DATADIR = WORKDIR / "data"
OUTDIR  = WORKDIR / "output"
DATADIR.mkdir(parents=True, exist_ok=True)
OUTDIR.mkdir(parents=True, exist_ok=True)

    # ローカル/GCSテキストファイルのエンコーディング候補リスト
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
    # テキストファイルを複数エンコーディングで読み込み、失敗時は置換文字で復旧
    data = path.read_bytes()
    for enc in CANDIDATE_ENCODINGS:
        try:
            txt = data.decode(enc)
            print(f"[INPUT] Decoded {path} with encoding={enc}")
            return txt
        except UnicodeDecodeError:
            continue
    # どのエンコーディングでも失敗した場合、置換文字で復旧しパイプラインを止めない
    txt = data.decode("utf-8", errors="replace")
    print("[INPUT] WARNING: Failed to detect encoding; using utf-8 with replacement (mojibake possible)")
    return txt

def _read_input_text(uri: str) -> str:
    # GCSまたはローカルからURLリストを取得
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

# すべてのタスクが同じ出力プレフィックスに書き込むための実行単位IDを優先的に利用
def _get_run_id() -> str:
    # 1回の実行で全タスク共通となる値を優先的に利用
    for k in ("RUN_ID", "CLOUD_RUN_EXECUTION", "CLOUD_RUN_EXECUTION_ID", "CLOUD_RUN_JOB_EXECUTION"):
        v = os.getenv(k)
        if v:
            return v
    # 上記がなければプロセスごとのタイムスタンプ+uuidを利用（タスクごとに異なる可能性あり）
    return _now_utc_ts() + "-" + uuid.uuid4().hex[:8]

def pick_my_shard(urls: List[str]) -> List[str]:
    # タスクインデックス・タスク数に応じて担当URLを分割
    idx  = int(os.getenv("CLOUD_RUN_TASK_INDEX", "0"))
    size = int(os.getenv("CLOUD_RUN_TASK_COUNT", "1"))
    if size <= 1:
        return urls
    return [u for i, u in enumerate(urls) if i % size == idx]

def import_scraper():
    # scraperモジュールを動的にインポート
    if SCRAPER_MODULE in sys.modules:
        del sys.modules[SCRAPER_MODULE]
    return importlib.import_module(SCRAPER_MODULE)

def scrape_all(urls: List[str]) -> List[dict]:
    # 各URLをスクレイピングし、結果をリストで返す
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
    # 記事データをJSONファイルとして保存
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fp:
        json.dump(records, fp, ensure_ascii=False, indent=2)

def run_combine(env: dict | None = None) -> int:
    # combine.py（翻訳・情報付加）をサブプロセスで実行
    if not COMBINE_SCRIPT.exists():
        print("[FATAL] combine.py not found", file=sys.stderr)
        return 2
    cmd = [sys.executable, "-u", str(COMBINE_SCRIPT)]
    print("[COMBINE] Running:", " ".join(cmd))
    return subprocess.call(cmd, env=env or os.environ.copy())

def run_score(input_csv: Path) -> Path:
    # score_summrize.py（日本語評価）をサブプロセスで実行
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
    # 各シャードのCSVをGCSからダウンロードし、NO.列をグローバル連番でマージして最終CSVを作成
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

    # 安定した順序でNO.列をグローバル連番で付与しながらCSVを連結
    import csv, sys
    from io import StringIO
    # 非常に大きなフィールド（記事本文や埋め込みJSONなど）も読み込めるようにCSVフィールドサイズ上限を拡張
    try:
        csv.field_size_limit(sys.maxsize)
    except OverflowError:
        # 一部環境ではsys.maxsizeが大きすぎる場合のフォールバック
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

        # ヘッダー初期化とNO.列の位置決定・新規作成
        if header is None:
            # よくある表記ゆれも考慮
            candidates = {"NO.", "No.", "No", "no", "番号"}
            found_idx = None
            for idx, col in enumerate(part_header):
                if col.strip() in candidates:
                    found_idx = idx
                    break

            if found_idx is None:
                # 先頭にNO.列を新規追加
                header = ["NO."] + part_header
                no_col_idx = 0
            else:
                header = part_header
                no_col_idx = found_idx

            writer.writerow(header)
        else:
            # もし後続パートでヘッダー長が異なる場合は下でパディング
            pass

        # 各行をストリームし、NO.列をグローバル連番で上書き/挿入
        for row in reader:
            if not row:
                continue
            # 先頭NO.列を新規追加した場合、パートファイル由来の行はNO.列がないので付与
            if no_col_idx == 0 and len(row) == len(header) - 1:
                row = [str(global_no)] + row
            else:
                # 列数が足りない場合はパディング
                if no_col_idx >= len(row):
                    row = row + [""] * (no_col_idx - len(row) + 1)
                row[no_col_idx] = str(global_no)
            writer.writerow(row)
            global_no += 1

    # Excelで開きやすいようUTF-8 BOM付きで書き出し、明示的にcharsetを指定
    csv_text = out_buf.getvalue()
    if not csv_text.startswith("\ufeff"):
        csv_text = "\ufeff" + csv_text
    _retry(write_bytes, final_gcs_path, csv_text.encode("utf-8"), what="write_bytes(final_csv)", content_type="text/csv; charset=utf-8")
    print(f"[MERGE] Renumbered NO. 1..{global_no - 1}")
    return 0

def main():
    # メイン処理。引数・環境変数取得→URLリスト取得→分割→スクレイピング→集計→アップロード→マージ→通知まで一括実行
    try:
        p = argparse.ArgumentParser()
        p.add_argument("input", nargs="?", help='URL list path (local or gs://). If omitted, uses env INPUT_URLS_GCS')
        p.add_argument("--translate", type=int, choices=[0,1], default=1)
        p.add_argument("--score-summarize", type=int, choices=[0,1], default=int(os.getenv("SCORE_SUMMARIZE", "1")))
        args = p.parse_args()

        #位置引数が省略された場合、環境変数 INPUT_URLS_GCS を代わりに使用できるようにする
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

        # 1) URLリストを読み込む（エンコーディング自動判定付きで頑健に処理）
        raw = _read_input_text(input_path)
        urls = [ln.strip() for ln in raw.splitlines() if ln.strip() and not ln.strip().startswith("#")]
        total = len(urls)
        my_urls = pick_my_shard(urls)
        print(f"[URLS] total={total}, assigned={len(my_urls)} (task_index={os.getenv('CLOUD_RUN_TASK_INDEX','0')})")

        # 2) master CSVをローカルに取得（combine.py用）
        _retry(download_to_file, master_gcs, str(DATADIR / "master_media_data-utf8.csv"), what="download_to_file(master)")

        # 3) 担当分のURLをスクレイピング
        records = scrape_all(my_urls)
        articles_json_path = OUTDIR / "articles.json"
        write_articles_json(records, articles_json_path)

        # 4) combine.pyを実行（翻訳＋情報付加）
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

        # 5) 必要に応じてscore_summrize.pyで評価
        part_path_local = shard_csv
        if args.score_summarize == 1:
            part_path_local = run_score(shard_csv)

        # 6) シャードごとのCSVをアップロード
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

            # ブラウザでダウンロード可能なURLを生成
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

            # Slack通知（任意）
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
        # 失敗時はSlack通知
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
        # 必要に応じてトレースバックを出力
        import traceback
        print("[FATAL] Exception in main():", file=sys.stderr)
        traceback.print_exc()
        # ジョブが失敗として扱われるように、例外を再送出するか、終了コード 1 で終了する
        sys.exit(1)

if __name__ == "__main__":
    main()
    