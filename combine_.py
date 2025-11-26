#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
articles.json と master_media_data-utf8.csv を突合し、
指定の 7 項目を JSON 形式で出力するワンパススクリプト。
"""

from pathlib import Path
import json
import pandas as pd
from urllib.parse import urlparse

try:
    from google.cloud import translate  # v3
    _GCP_TRANSLATE_AVAILABLE = True
except Exception as e:
    _GCP_TRANSLATE_AVAILABLE = False
    _GCP_TRANSLATE_IMPORT_ERR = e

try:
    from google.cloud import aiplatform
    from google.protobuf.json_format import MessageToDict
    _GCP_AIPLATFORM_AVAILABLE = True
except Exception as e:
    _GCP_AIPLATFORM_AVAILABLE = False
    _GCP_AIPLATFORM_IMPORT_ERR = e

from google.api_core import exceptions as gax_exceptions


import os
# ── Load .env for local development (optional) ───────────────────────────────
# 優先度: 既存の環境変数 > .env/.env.local
try:
    from dotenv import load_dotenv  # type: ignore
    for _p in (Path(".env"), Path(".env.local")):
        if _p.exists():
            load_dotenv(dotenv_path=_p, override=False)
            break
except Exception:
    # フォールバック: 超簡易パーサ（KEY=VALUE 形式のみ／クォートは剥がす）
    for _p in (Path(".env"), Path(".env.local")):
        try:
            if _p.exists():
                for _line in _p.read_text(encoding="utf-8").splitlines():
                    _s = _line.strip()
                    if not _s or _s.startswith("#") or "=" not in _s:
                        continue
                    _k, _v = _s.split("=", 1)
                    os.environ.setdefault(_k.strip(), _v.strip().strip('"').strip("'"))
                break
        except Exception:
            pass

# ------------------------------------------------------------------------------
# 設定
# ------------------------------------------------------------------------------

TRANSLATE_BATCH_SIZE = 16

# Per-request size limits (codepoints) for Translation APIs
TRANSLATE_REQ_MAX_CODEPOINTS = int(os.getenv("TRANSLATE_REQ_MAX_CODEPOINTS", "30000"))  # API hard limit is 30720
TRANSLATE_REQ_HEADROOM = int(os.getenv("TRANSLATE_REQ_HEADROOM", "1024"))               # safety margin
# Max length of a single segment when we split long texts
TRANSLATE_SEGMENT_MAX_CODEPOINTS = int(os.getenv("TRANSLATE_SEGMENT_MAX_CODEPOINTS", "8000"))

# ------------------------------------------------------------------------------
# DEBUG スイッチ
# ------------------------------------------------------------------------------
DEBUG = os.getenv("DEBUG", "0") == "1"

def _dbg(*args):
    if DEBUG:
        print("[DEBUG]", *args)

# --- Helpers for safe batching under API codepoint limits --------------------
def _split_long_text(s: str, max_len: int = TRANSLATE_SEGMENT_MAX_CODEPOINTS):
    """
    Split a long string into segments not exceeding max_len codepoints.
    Prefers to cut on paragraph/newline/punctuation boundaries; falls back to hard cuts.
    """
    s = s or ""
    if len(s) <= max_len:
        return [s]

    segs = []
    cur = ""
    # First, try to build chunks by paragraphs
    for para in s.split("\n\n"):
        candidate = (cur + ("\n\n" if cur else "") + para) if cur else para
        if len(candidate) <= max_len:
            cur = candidate
        else:
            # If a single paragraph is too big, further split by single newlines or punctuation
            buf = (cur + ("\n\n" if cur else "")) if cur else ""
            part = para
            while part:
                # Try to cut near max_len on natural boundaries
                limit = max_len - len(buf)
                if limit <= 0:
                    segs.append(buf)
                    buf = ""
                    limit = max_len
                if len(part) <= limit:
                    buf += part
                    part = ""
                else:
                    window = part[:limit]
                    # look for last good breakpoint in window
                    cut = max(
                        window.rfind("\n"),
                        window.rfind("。"),
                        window.rfind("．"),
                        window.rfind("！"),
                        window.rfind("!"),
                        window.rfind("？"),
                        window.rfind("?"),
                        window.rfind("。 "),
                        window.rfind(". "),
                        window.rfind(" "),
                    )
                    if cut == -1 or cut < limit * 0.5:
                        # fallback: hard cut
                        cut = limit
                    buf += part[:cut]
                    part = part[cut:]
                    segs.append(buf)
                    buf = ""
            cur = ""
    if cur:
        segs.append(cur)

    # Final safety: if any segment still exceeds max_len, hard-split it
    out = []
    for t in segs:
        if len(t) <= max_len:
            out.append(t)
        else:
            start = 0
            while start < len(t):
                out.append(t[start:start + max_len])
                start += max_len
    return out

def _iter_packs_by_codepoints(strings, *, max_total: int = TRANSLATE_REQ_MAX_CODEPOINTS, headroom: int = TRANSLATE_REQ_HEADROOM):
    """
    Yield lists of strings such that the sum of codepoints in each list does not exceed (max_total - headroom).
    Ensures every individual string also respects the same limit (splitting if necessary).
    """
    limit = max(1024, max_total - headroom)
    batch, total = [], 0
    for s in strings:
        s = s or ""
        # ensure each piece is under the per-item limit
        pieces = _split_long_text(s, min(TRANSLATE_SEGMENT_MAX_CODEPOINTS, limit))
        for p in pieces:
            plen = len(p)
            if plen > limit:
                # very rare: hard split if still too big
                start = 0
                while start < plen:
                    sub = p[start:start + limit]
                    if batch and total + len(sub) > limit:
                        yield batch
                        batch, total = [], 0
                    batch.append(sub)
                    total += len(sub)
                    if total >= limit:
                        yield batch
                        batch, total = [], 0
                    start += limit
                continue

            if batch and total + plen > limit:
                yield batch
                batch, total = [], 0
            batch.append(p)
            total += plen
    if batch:
        yield batch

# Translation LLM 用の追加設定
TRANSLATION_ENGINE = os.getenv("TRANSLATION_ENGINE", "LLM")  # LLM or NMT
DETECT_LOCATION = os.getenv("GOOGLE_CLOUD_LOCATION_DETECT", "global")
DETECT_SAMPLE_CHARS = int(os.getenv("DETECT_SAMPLE_CHARS", "800"))  # 言語判定は先頭 N 文字で

# ------------------------------------------------------------------------------
# 1.  記事 JSON を読み込む
# ------------------------------------------------------------------------------
ARTICLES_PATH = Path("output/articles.json")
with ARTICLES_PATH.open(encoding="utf-8") as f:
    articles_raw = json.load(f)

# pandas データフレーム化（扱いやすさ重視）
df_articles = pd.json_normalize(articles_raw)

# --- articles.json のフィールド名ゆれを吸収（代表名に正規化） -----------------
# よくある別名: publishedAt/pubDate/date, link, description/summary/body/text 等

def _coalesce_col(df, target, candidates):
    # 既に target があればそれを優先（空は後で埋める）
    if target not in df.columns:
        for c in candidates:
            if c in df.columns:
                df[target] = df[c]
                break
    # 依然として無ければ空列を用意
    if target not in df.columns:
        df[target] = pd.NA


_coalesce_col(df_articles, "published", ["publishedAt", "pubDate", "date"])  # 日付
_coalesce_col(df_articles, "title", ["headline", "name"])                    # タイトル
_coalesce_col(df_articles, "url", ["link", "permalink"])                     # URL
_coalesce_col(df_articles, "content", ["description", "summary", "body", "text"])  # 本文

# --- sanitize: published に FETCH ERROR が含まれる場合は published と content を null ---
if "published" in df_articles.columns:
    _mask_pub_fetch = df_articles["published"].astype(str).str.contains("FETCH ERROR", case=False, na=False)
    if _mask_pub_fetch.any():
        _dbg("normalize: published contains FETCH ERROR rows:", int(_mask_pub_fetch.sum()))
        df_articles.loc[_mask_pub_fetch, ["published", "content"]] = pd.NA


# --- sanitize: title に FETCH ERROR がある場合は title と content を null ---
if "title" in df_articles.columns:
    _mask_title_fetch = df_articles["title"].astype(str).str.contains("FETCH ERROR", case=False, na=False)
    if _mask_title_fetch.any():
        _dbg("normalize: title contains FETCH ERROR rows:", int(_mask_title_fetch.sum()))
        df_articles.loc[_mask_title_fetch, ["title", "content"]] = pd.NA

# --- 欠損の統一: None / "" / "None" → <NA> に揃える（翻訳ショートサーキット用） ---
for _col in ["title", "content", "published"]:
    if _col in df_articles.columns:
        s = pd.Series(df_articles[_col], dtype="string")
        s = s.replace({"None": pd.NA}).str.strip()
        s = s.mask(s == "", pd.NA)
        df_articles[_col] = s

# URL がスキーム無しでも動く堅牢なドメイン抽出

def _to_domain(u):
    s = "" if u is None else str(u).strip()
    if not s:
        return ""
    if not (s.startswith("http://") or s.startswith("https://")):
        s = "http://" + s
    host = urlparse(s).netloc.lower()
    if host.startswith("www."):
        host = host[4:]
    return host.split(":")[0]

df_articles["domain"] = df_articles["url"].map(_to_domain)

# ------------------------------------------------------------------------------
# 2.  master_media_data-utf8.csv を読み込む
# ------------------------------------------------------------------------------
# GCS優先＋ローカルfallbackで媒体CSVを読む
_LOCAL_MEDIA = Path("data/master_media_data-utf8.csv")
_GCS_MEDIA   = "gs://urlcloner/data/master_media_data-utf8.csv"

def _read_media_csv():
    # 1) GCS（gcsfs経由で直接読み取り）
    try:
        # import inside function so the module is optional at import time
        import gcsfs  # noqa: F401
        print("[INFO] Loading media list from GCS:", _GCS_MEDIA)
        return pd.read_csv(_GCS_MEDIA, encoding="utf-8")
    except Exception as e_gcsfs:
        # 2) 公式クライアントでダウンロード → /tmp から読み取り
        try:
            from google.cloud import storage  # type: ignore
            client = storage.Client()
            bucket = client.bucket("urlcloner")
            blob = bucket.blob("data/master_media_data-utf8.csv")
            tmp = "/tmp/master_media_data-utf8.csv"
            blob.download_to_filename(tmp)
            print("[INFO] Loading media list from GCS via download:", "gs://urlcloner/data/master_media_data-utf8.csv")
            return pd.read_csv(tmp, encoding="utf-8")
        except Exception as e_storage:
            # 3) ローカルfallback
            if _LOCAL_MEDIA.exists():
                print("[WARN] GCS 読み込みに失敗。ローカルを使用します:", _LOCAL_MEDIA)
                return pd.read_csv(_LOCAL_MEDIA, encoding="utf-8")
            raise RuntimeError(
                "媒体リストの取得に失敗しました（GCS優先＋ローカルfallback）: "
                f"gcsfs_err={e_gcsfs!r}, storage_err={e_storage!r}"
            )

df_media = _read_media_csv()

# ★CSV 側の URL 列名が違う場合はここを書き換えてください
CSV_URL_COL = "URL"

# ドメイン列を作成
df_media["domain"] = df_media[CSV_URL_COL].map(_to_domain)

# media 側はドメイン単位で一意化（重複があると articles が増殖するため）
_media_cols = ["domain", "国", "資料源", "オフィシャル度"]
media_view = df_media[_media_cols].drop_duplicates("domain")
_dbg("media domains: total=", df_media["domain"].nunique(), "unique_used=", media_view["domain"].nunique())

# --- 記事ドメイン → media ドメインの最長サフィックス一致（subdomain 対応） ---
_media_domains_sorted = sorted(
    [d for d in media_view["domain"].dropna().astype(str).unique()],
    key=len, reverse=True
)

def _best_media_domain(article_domain):
    if article_domain is pd.NA or article_domain is None:
        return pd.NA
    ad = str(article_domain).lower()
    for md in _media_domains_sorted:
        mds = str(md).lower()
        if ad == mds or ad.endswith("." + mds):
            return md  # 最も長い一致を返す（先に長い順で探索）
    return pd.NA  # 一致なし

# 記事ごとに最適な media ドメインを付与
df_articles["media_domain"] = df_articles["domain"].map(_best_media_domain)
_dbg("articles with matched media domain:", int(df_articles["media_domain"].notna().sum()),
     "unmatched:", int(df_articles["media_domain"].isna().sum()))

# ------------------------------------------------------------------------------
# 3.  ドメインキーで結合（articles を左、media を右にする left join）
#     ─ 記事（articles.json）を主として保持し、該当がない場合は CSV 側は欠損のまま
#     ※ media 側ドメインは最長サフィックス一致で対応
# ------------------------------------------------------------------------------
df_joined = pd.merge(
    df_articles,
    media_view,
    left_on="media_domain",
    right_on="domain",
    how="left",
    suffixes=("_art", "_csv"),
)

# ------------------------------------------------------------------------------
# 4.  必要カラムを整形
#     （CSV 側の列名が異なる場合は同様に修正してください）
# ------------------------------------------------------------------------------
OUTPUT_COLS = {
    "国":           "国",          # ★CSV: 国名列
    "反応主体":     "資料源",      # ★CSV: 「資料源」列を出力「反応主体」へ
    "オフィシャル度":"オフィシャル度",# ★CSV: オフィシャル度列
    "日付":         "published",   # JSON: published
    "タイトル":     "title",       # JSON: title
    "URL":          "url",         # JSON: url
    "本文":         "content",     # JSON: content
}

# 英語 → 日本語 の向きで確実にリネーム
_cols_select = list(OUTPUT_COLS.values())
_rename_map = {v: k for k, v in OUTPUT_COLS.items()}

# まずは存在している列だけを安全に選択（欠けは後段で補完）
_existing = [c for c in _cols_select if c in df_joined.columns]
_missing  = [c for c in _cols_select if c not in df_joined.columns]
if _missing:
    _dbg("missing source cols before rename:", _missing)

df_out = df_joined[_existing].rename(columns=_rename_map)

# URL 重複はここで排除（翻訳の重複コストも抑制）
_before = len(df_out)
df_out = df_out.drop_duplicates(subset=["URL"], keep="first")
_dbg("drop_duplicates by URL:", _before, "→", len(df_out))

# 欠けているターゲット列を空で補完
for _src, _dst in _rename_map.items():
    if _dst not in df_out.columns:
        df_out[_dst] = ""

# CSV 側（国/反応主体/オフィシャル度）はそのまま残っているか確認 + エイリアス対応
_ALIAS_SRC = {
    "国": ["国"],
    "反応主体": ["反応主体", "資料源"],
    "オフィシャル度": ["オフィシャル度"],
}
for _dst in ["国", "反応主体", "オフィシャル度"]:
    if _dst in df_out.columns:
        continue
    filled = False
    for _src in _ALIAS_SRC.get(_dst, []):
        if _src in df_joined.columns:
            df_out[_dst] = df_joined[_src]
            filled = True
            break
    if not filled:
        df_out[_dst] = ""

# --- デバッグ出力: 件数やカラム
_dbg("rows: articles=", len(df_articles), "media=", len(df_media), "joined=", len(df_joined))
_dbg("df_joined columns:", list(df_joined.columns))
_dbg("df_out columns (pre-translate):", list(df_out.columns))

ENABLE_TRANSLATION = os.getenv("ENABLE_TRANSLATION", "1") != "0"
GCP_PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT") or os.getenv("GCP_PROJECT_ID")
VERTEX_LOCATION = os.getenv("VERTEX_LOCATION") or os.getenv("GOOGLE_CLOUD_LOCATION") or "us-central1"
TRANSLATE_LOCATION = os.getenv("TRANSLATE_LOCATION") or "us-central1"
TARGET_LANG = os.getenv("TARGET_LANG", "ja")
_dbg("locations:", {"vertex": VERTEX_LOCATION, "translate": TRANSLATE_LOCATION, "detect": DETECT_LOCATION})

# ------------------------------------------------------------------------------
# 4.5  翻訳（Translation LLM on Vertex AI で日本語化）
#       ・"タイトル" → "タイトル（日本語）"
#       ・"本文"     → "和訳"
#       備考:
#         - 言語判定は Cloud Translation v3 の detectLanguage を使用（locations/global）
#         - 翻訳は Vertex AI の PredictionService 経由で Translation LLM を使用
#           エンドポイント: publishers/google/models/cloud-translate-text:predict
#           モデル: projects/{PROJECT}/locations/{LOCATION}/models/general/translation-llm
# ------------------------------------------------------------------------------
if ENABLE_TRANSLATION:
    if not GCP_PROJECT_ID:
        raise RuntimeError(
            "Google Cloud のプロジェクト ID が未設定です。環境変数 GOOGLE_CLOUD_PROJECT もしくは GCP_PROJECT_ID を設定してください。"
        )

    # --- 言語判定 (Cloud Translation v3) --------------------------------------
    if not _GCP_TRANSLATE_AVAILABLE:
        raise ImportError(
            "google-cloud-translate が見つかりません。`pip install google-cloud-translate` を実行してください。"
        )
    lang_client = translate.TranslationServiceClient()
    detect_parent = f"projects/{GCP_PROJECT_ID}/locations/{DETECT_LOCATION}"

    def _detect_lang(text: str) -> str:
        text = (text or "")
        if not text:
            return "und"
        # 先頭のみで十分に判定できることが多い
        sample = text[:DETECT_SAMPLE_CHARS]
        resp = lang_client.detect_language(
            parent=detect_parent,
            content=sample,
            mime_type="text/plain",
        )
        langs = getattr(resp, "languages", [])
        return (langs[0].language_code if langs else "und")

    # --- Translation LLM 呼び出し (Vertex AI PredictionService) ---------------
    if not _GCP_AIPLATFORM_AVAILABLE:
        raise ImportError(
            "google-cloud-aiplatform が見つかりません。`pip install google-cloud-aiplatform` を実行してください。"
        )

    def _llm_translate_batch(texts, src_lang: str, *, target=TARGET_LANG):
        def _predict_with_location(loc: str):
            client = aiplatform.gapic.PredictionServiceClient(
                client_options={"api_endpoint": f"{loc}-aiplatform.googleapis.com"}
            )
            endpoint = f"projects/{GCP_PROJECT_ID}/locations/{loc}/publishers/google/models/cloud-translate-text"
            model = f"projects/{GCP_PROJECT_ID}/locations/{loc}/models/general/translation-llm"
            instances = [{
                "model": model,
                "source_language_code": src_lang,
                "target_language_code": target,
                "contents": list(texts),
            }]
            return client.predict(endpoint=endpoint, instances=instances)

        try:
            resp = _predict_with_location(VERTEX_LOCATION)
        except gax_exceptions.NotFound:
            if VERTEX_LOCATION != "us-central1":
                _dbg("LLM NotFound at", VERTEX_LOCATION, "→ retry us-central1")
                resp = _predict_with_location("us-central1")
            else:
                raise

        predictions = MessageToDict(resp._pb).get("predictions", [])
        if not predictions:
            return [""] * len(texts)
        translations = predictions[0].get("translations") if len(predictions) == 1 else None
        if translations is None:
            translations = []
            for p in predictions:
                translations.extend(p.get("translations", []))
        return [t.get("translatedText", "") for t in translations]

    # --- NMT フォールバック（任意） -----------------------------------------
    def _nmt_translate_list(values, *, target=TARGET_LANG):
        """
        Translate a list of strings using Cloud Translation (NMT) while respecting per-request codepoint limits.
        """
        out = []
        parent = f"projects/{GCP_PROJECT_ID}/locations/{TRANSLATE_LOCATION}"

        for pack in _iter_packs_by_codepoints(values):
            try:
                resp = lang_client.translate_text(
                    request={
                        "parent": parent,
                        "contents": pack,
                        "mime_type": "text/plain",
                        "target_language_code": target,
                    }
                )
                out.extend([t.translated_text for t in resp.translations])
            except gax_exceptions.InvalidArgument:
                # Location issues or hidden size overhead: try fallback locations then split further
                tried_locs = []
                ok = False
                for loc in ("us-central1", "global"):
                    if parent.endswith(f"/locations/{loc}"):
                        continue
                    tried_locs.append(loc)
                    parent_fb = f"projects/{GCP_PROJECT_ID}/locations/{loc}"
                    try:
                        resp = lang_client.translate_text(
                            request={
                                "parent": parent_fb,
                                "contents": pack,
                                "mime_type": "text/plain",
                                "target_language_code": target,
                            }
                        )
                        out.extend([t.translated_text for t in resp.translations])
                        parent = parent_fb
                        ok = True
                        break
                    except gax_exceptions.InvalidArgument:
                        # As an extra guard, split the pack into halves and retry
                        pass
                if not ok:
                    # final fallback: split this pack into smaller subpacks and retry recursively
                    if len(pack) > 1:
                        mid = len(pack) // 2
                        out.extend(_nmt_translate_list(pack[:mid], target=target))
                        out.extend(_nmt_translate_list(pack[mid:], target=target))
                    else:
                        # single item still too large → split the string further and retry
                        pieces = _split_long_text(pack[0], TRANSLATE_SEGMENT_MAX_CODEPOINTS // 2)
                        out.extend(_nmt_translate_list(pieces, target=target))
        return out

    def _nmt_translate_series(series, *, target=TARGET_LANG, batch_size=TRANSLATE_BATCH_SIZE):
        # batch_size is ignored; we pack by codepoints instead.
        values = series.fillna("").astype(str).tolist()
        return pd.Series(_nmt_translate_list(values, target=target), index=series.index)

    def _translate_series_with_llm(series, *, target=TARGET_LANG, batch_size=TRANSLATE_BATCH_SIZE):
        # 言語判定 → 同言語ごとにまとめて LLM 翻訳（長文・総量の上限に配慮）
        s = series.fillna("").astype(str)
        detected = s.map(_detect_lang)
        out = pd.Series([""] * len(s), index=s.index)

        for src_lang, idx in detected.groupby(detected).groups.items():
            chunk = s.loc[idx]
            if not src_lang or src_lang == "und" or src_lang == target:
                out.loc[idx] = chunk
                continue

            # 1) まず各テキストを安全な長さに分割してフラット化
            originals = chunk.tolist()
            seg_counts = []
            flat_segments = []
            for t in originals:
                segs = _split_long_text(t, TRANSLATE_SEGMENT_MAX_CODEPOINTS)
                seg_counts.append(len(segs))
                flat_segments.extend(segs)

            # 2) LLM でパック単位に翻訳（総コードポイントが閾値を超えないように送る）
            translated_segments = []
            for pack in _iter_packs_by_codepoints(flat_segments):
                try:
                    translated_segments.extend(_llm_translate_batch(pack, src_lang, target=target))
                except Exception:
                    # LLM 側でサイズエラー等 → 同じ pack を NMT で翻訳
                    translated_segments.extend(_nmt_translate_list(pack, target=target))

            # 3) セグメントを元の記事単位に再結合
            restored = []
            pos = 0
            for n in seg_counts:
                restored.append("".join(translated_segments[pos:pos + n]))
                pos += n

            out.loc[idx] = restored

        return out

    # 列を挿入（元列の直後）
    if "タイトル" in df_out.columns:
        df_out.insert(
            df_out.columns.get_loc("タイトル") + 1,
            "タイトル（日本語）",
            _translate_series_with_llm(df_out["タイトル"], target=TARGET_LANG),
        )
    if "本文" in df_out.columns:
        df_out.insert(
            df_out.columns.get_loc("本文") + 1,
            "和訳",
            _translate_series_with_llm(df_out["本文"], target=TARGET_LANG),
        )

# ------------------------------------------------------------------------------
# 4.9  出力カラム最終整形（列名・順序・NO./空列 など）
# ------------------------------------------------------------------------------
# 指定の列名にリネーム
df_out = df_out.rename(columns={
    "国": "国（地域）",
    "タイトル": "タイトル（原語）",
})

# 必須列を空でも作成
for _col in ["日付", "URL", "本文", "タイトル（原語）"]:
    if _col not in df_out.columns:
        df_out[_col] = ""

# 訳語列が存在しない場合は空で作成（保険）
for _col in ["タイトル（日本語）", "和訳"]:
    if _col not in df_out.columns:
        df_out[_col] = ""


# 追加の空列
for _col in ["キーワード", "備考欄"]:
    if _col not in df_out.columns:
        df_out[_col] = ""


# 日付フォーマット統一（例: 2025-05-28T15:46:08.772000+00:00 → 5月28日）
def _fmt_mmdd_jp(v):
    # 欠損や空文字は null（NA）を維持
    if pd.isna(v) or (isinstance(v, str) and v.strip() == ""):
        return pd.NA
    dt = pd.to_datetime(v, errors="coerce", utc=False)
    if pd.isna(dt):
        return pd.NA  # 解析できない場合も null に統一
    return f"{int(dt.month)}月{int(dt.day)}日"

if "日付" in df_out.columns:
    df_out["日付"] = df_out["日付"].map(_fmt_mmdd_jp)

# NO. 列（先頭に挿入）: 1,2,3... の昇順（半角数字=通常の整数）
df_out.insert(0, "NO.", range(1, len(df_out) + 1))

# 列の最終順序
_desired_cols = [
    "NO.",
    "国（地域）",
    "反応主体",
    "オフィシャル度",
    "日付",
    "タイトル（原語）",
    "タイトル（日本語）",
    "URL",
    "本文",
    "和訳",
    "キーワード",
    "備考欄",
]
# 念のため存在している列のみで並べ替え
_existing_cols = [c for c in _desired_cols if c in df_out.columns]
df_out = df_out[_existing_cols]

# ------------------------------------------------------------------------------
# 5.  JSON & CSV ファイルに書き出し（output ディレクトリ配下）
# ------------------------------------------------------------------------------
OUTDIR = Path("output")
OUTDIR.mkdir(parents=True, exist_ok=True)
OUTPUT_JSON = OUTDIR / "articles_enriched.json"
OUTPUT_CSV  = OUTDIR / "articles_enriched.csv"

# ── JSON ──────────────────────────────────────────────────────────────
OUTPUT_JSON.write_text(
    df_out.to_json(orient="records", force_ascii=False, indent=2),
    encoding="utf-8"
)

# ── CSV ───────────────────────────────────────────────────────────────
#   ・index=False で行番号を付けない  
#   ・Excel 互換を考慮して UTF‑8 BOM 付き (utf-8-sig) にするのが無難
df_out.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")

print(
    f"[INFO] 完了: {len(df_out)} 件を書き出しました → "
    f"{OUTPUT_JSON.name}, {OUTPUT_CSV.name}"
)