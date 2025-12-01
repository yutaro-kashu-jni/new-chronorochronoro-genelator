
#!/usr/bin/env python3
# =============================================================================
# このファイル combine_.py の役割
# -----------------------------------------------------------------------------
# ・articles.json（記事データ）と master_media_data-utf8.csv（媒体データ）を突合し、
#   指定の7項目を日本語化・整形してJSON/CSVで出力するスクリプトです。
# ・GCPの翻訳APIやVertex AI LLMを活用し、記事本文やタイトルの自動翻訳も行います。
# ・データの正規化、欠損補完、ドメイン名マッチング、出力整形なども一括で実施します。
# ・実行すると output/articles_enriched.json, articles_enriched.csv を生成します。
# =============================================================================
# -*- coding: utf-8 -*-



# --- 必要な標準・外部ライブラリのインポート ---
from pathlib import Path
import json
import pandas as pd
from urllib.parse import urlparse


# --- Google Cloud翻訳API/Vertex AIの利用可否を判定 ---
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


# --- Google API例外クラスのインポート ---
from google.api_core import exceptions as gax_exceptions



# --- ローカル開発用: .envファイルから環境変数を読み込む処理 ---
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
    # .envが無い場合の簡易パーサ
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


# --- 翻訳APIやバッチ処理の各種設定値 ---

TRANSLATE_BATCH_SIZE = 16


# --- 翻訳APIのリクエストサイズや分割単位の設定 ---
TRANSLATE_REQ_MAX_CODEPOINTS = int(os.getenv("TRANSLATE_REQ_MAX_CODEPOINTS", "30000"))  # API hard limit is 30720
TRANSLATE_REQ_HEADROOM = int(os.getenv("TRANSLATE_REQ_HEADROOM", "1024"))               # safety margin
# 長文を分割する際の1セグメントあたりの最大長
TRANSLATE_SEGMENT_MAX_CODEPOINTS = int(os.getenv("TRANSLATE_SEGMENT_MAX_CODEPOINTS", "8000"))


# --- デバッグ出力用スイッチと関数 ---
DEBUG = os.getenv("DEBUG", "0") == "1"

# デバッグ用: DEBUGが有効な場合のみ標準出力に内容を表示する関数
def _dbg(*args):
    if DEBUG:
        print("[DEBUG]", *args)


# --- 文字列をAPI制限内で安全に分割・バッチ化するヘルパー関数群 ---
 # 長いテキストを指定した最大長で安全に分割する関数
 # 長いテキストを指定した最大長で安全に分割する関数
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
    # まず段落単位でチャンク化を試みる
    for para in s.split("\n\n"):
        candidate = (cur + ("\n\n" if cur else "") + para) if cur else para
        if len(candidate) <= max_len:
            cur = candidate
        else:
            # 1段落が大きすぎる場合は改行や句読点でさらに分割
            buf = (cur + ("\n\n" if cur else "")) if cur else ""
            part = para
            while part:
                # max_len付近で自然な区切りを探して分割
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
                    # ウィンドウ内で最後の良い分割点を探す
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

    # 最終的な安全策：どのセグメントも max_len を超える場合は、強制的に分割する
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

 # 文字列リストを合計コードポイント制限内で分割し、バッチ化するイテレータ関数
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


# --- Vertex AI LLM翻訳用の追加設定 ---
TRANSLATION_ENGINE = os.getenv("TRANSLATION_ENGINE", "LLM")  # LLM or NMT
DETECT_LOCATION = os.getenv("GOOGLE_CLOUD_LOCATION_DETECT", "global")
DETECT_SAMPLE_CHARS = int(os.getenv("DETECT_SAMPLE_CHARS", "800"))  # 言語判定は先頭 N 文字で


# === 1. 記事JSONの読み込みと正規化 ===
ARTICLES_PATH = Path("output/articles.json")
with ARTICLES_PATH.open(encoding="utf-8") as f:
    articles_raw = json.load(f)


# --- JSONをpandas DataFrameに変換 ---
df_articles = pd.json_normalize(articles_raw)


# --- フィールド名の揺れを代表名に正規化（よくある別名に対応） ---

 # DataFrame内のカラム名の揺れを代表名に正規化する関数
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


# --- publishedにFETCH ERRORが含まれる場合のクリーニング ---
if "published" in df_articles.columns:
    _mask_pub_fetch = df_articles["published"].astype(str).str.contains("FETCH ERROR", case=False, na=False)
    if _mask_pub_fetch.any():
        _dbg("normalize: published contains FETCH ERROR rows:", int(_mask_pub_fetch.sum()))
        df_articles.loc[_mask_pub_fetch, ["published", "content"]] = pd.NA



# --- titleにFETCH ERRORが含まれる場合のクリーニング(title と content を null ) ---
if "title" in df_articles.columns:
    _mask_title_fetch = df_articles["title"].astype(str).str.contains("FETCH ERROR", case=False, na=False)
    if _mask_title_fetch.any():
        _dbg("normalize: title contains FETCH ERROR rows:", int(_mask_title_fetch.sum()))
        df_articles.loc[_mask_title_fetch, ["title", "content"]] = pd.NA


# --- 欠損値を<NA>に統一（翻訳処理のため） ---
for _col in ["title", "content", "published"]:
    if _col in df_articles.columns:
        s = pd.Series(df_articles[_col], dtype="string")
        s = s.replace({"None": pd.NA}).str.strip()
        s = s.mask(s == "", pd.NA)
        df_articles[_col] = s


# --- URLからドメイン名を抽出する関数 ---

 # URL文字列からドメイン名部分のみを抽出する関数
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


# === 2. 媒体CSVの読み込み（GCS優先＋ローカルfallback） ===

# --- GCSまたはローカルから媒体CSVを読み込む ---
_LOCAL_MEDIA = Path("data/master_media_data-utf8.csv")
_GCS_MEDIA   = "gs://urlcloner/data/master_media_data-utf8.csv"

 # GCSまたはローカルから媒体CSVを読み込む関数
def _read_media_csv():
    # 1) GCS（gcsfs経由で直接読み取り）
    try:
        # このモジュールは必須ではないため、読み込み時の依存を避けるために関数内でインポートする
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


# --- CSVのURL列名が異なる場合はここを修正 ---
CSV_URL_COL = "URL"


# --- 媒体CSVからドメイン列を作成 ---
df_media["domain"] = df_media[CSV_URL_COL].map(_to_domain)


# --- 媒体側はドメイン単位で一意化 ---
_media_cols = ["domain", "国", "資料源", "オフィシャル度"]
media_view = df_media[_media_cols].drop_duplicates("domain")
_dbg("media domains: total=", df_media["domain"].nunique(), "unique_used=", media_view["domain"].nunique())


# --- 記事ドメインと媒体ドメインの最長サフィックス一致ロジック ---
_media_domains_sorted = sorted(
    [d for d in media_view["domain"].dropna().astype(str).unique()],
    key=len, reverse=True
)

 # 記事ドメインに最もよく一致する媒体ドメインを返す関数
def _best_media_domain(article_domain):
    if article_domain is pd.NA or article_domain is None:
        return pd.NA
    ad = str(article_domain).lower()
    for md in _media_domains_sorted:
        mds = str(md).lower()
        if ad == mds or ad.endswith("." + mds):
            return md  # 最も長い一致を返す（先に長い順で探索）
    return pd.NA  # 一致なし


# --- 記事ごとに最適な媒体ドメインを付与 ---
df_articles["media_domain"] = df_articles["domain"].map(_best_media_domain)
_dbg("articles with matched media domain:", int(df_articles["media_domain"].notna().sum()),
     "unmatched:", int(df_articles["media_domain"].isna().sum()))


# === 3. 記事と媒体情報のドメインキーによる結合（left join） ===
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


# === 4. 必要カラムの整形・リネーム・欠損補完 ===（CSV 側の列名が異なる場合は同様に修正してください）
OUTPUT_COLS = {
    "国":           "国",          # ★CSV: 国名列
    "反応主体":     "資料源",      # ★CSV: 「資料源」列を出力「反応主体」へ
    "オフィシャル度":"オフィシャル度",# ★CSV: オフィシャル度列
    "日付":         "published",   # JSON: published
    "タイトル":     "title",       # JSON: title
    "URL":          "url",         # JSON: url
    "本文":         "content",     # JSON: content
}


# --- 英語→日本語のカラム名リネーム ---
_cols_select = list(OUTPUT_COLS.values())
_rename_map = {v: k for k, v in OUTPUT_COLS.items()}


# --- 存在する列のみ安全に選択 ---
_existing = [c for c in _cols_select if c in df_joined.columns]
_missing  = [c for c in _cols_select if c not in df_joined.columns]
if _missing:
    _dbg("missing source cols before rename:", _missing)

df_out = df_joined[_existing].rename(columns=_rename_map)


# --- URL重複排除（翻訳コスト削減） ---
_before = len(df_out)
df_out = df_out.drop_duplicates(subset=["URL"], keep="first")
_dbg("drop_duplicates by URL:", _before, "→", len(df_out))


# --- 欠損ターゲット列を空で補完 ---
for _src, _dst in _rename_map.items():
    if _dst not in df_out.columns:
        df_out[_dst] = ""


# --- CSV側のカラムが無い場合のエイリアス補完 ---
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


# --- デバッグ用: 件数やカラム情報出力 ---
_dbg("rows: articles=", len(df_articles), "media=", len(df_media), "joined=", len(df_joined))
_dbg("df_joined columns:", list(df_joined.columns))
_dbg("df_out columns (pre-translate):", list(df_out.columns))

ENABLE_TRANSLATION = os.getenv("ENABLE_TRANSLATION", "1") != "0"
GCP_PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT") or os.getenv("GCP_PROJECT_ID")
VERTEX_LOCATION = os.getenv("VERTEX_LOCATION") or os.getenv("GOOGLE_CLOUD_LOCATION") or "us-central1"
TRANSLATE_LOCATION = os.getenv("TRANSLATE_LOCATION") or "us-central1"
TARGET_LANG = os.getenv("TARGET_LANG", "ja")
_dbg("locations:", {"vertex": VERTEX_LOCATION, "translate": TRANSLATE_LOCATION, "detect": DETECT_LOCATION})


# === 4.5 Vertex AI/Cloud Translationによる自動翻訳処理 ===
# タイトル・本文を日本語へ翻訳し新カラムとして追加。言語判定やAPIエラー時のフォールバックも実装。
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


    # --- Cloud Translation v3による言語判定 ---
    if not _GCP_TRANSLATE_AVAILABLE:
        raise ImportError(
            "google-cloud-translate が見つかりません。`pip install google-cloud-translate` を実行してください。"
        )
    lang_client = translate.TranslationServiceClient()
    detect_parent = f"projects/{GCP_PROJECT_ID}/locations/{DETECT_LOCATION}"

    # テキストの言語をCloud Translation APIで判定する関数
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


    # --- Vertex AI PredictionServiceによるLLM翻訳 ---
    if not _GCP_AIPLATFORM_AVAILABLE:
        raise ImportError(
            "google-cloud-aiplatform が見つかりません。`pip install google-cloud-aiplatform` を実行してください。"
        )

    # Vertex AI PredictionServiceを使ってLLM翻訳を実行する関数
    def _llm_translate_batch(texts, src_lang: str, *, target=TARGET_LANG):
        # 指定ロケーションでPredictionServiceClientを呼び出す内部関数
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


    # --- NMT(従来型)翻訳APIによるフォールバック ---
    # Cloud Translation(NMT) APIでリストを翻訳し、サイズ超過時は分割再試行する関数
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
            # ロケーションの問題や隠れたサイズ超過が原因の可能性があるため、フォールバック先を試し、さらに分割して再試行する
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
                        # 追加の安全策として、パックを半分に分割して再試行する

                        pass
                if not ok:
                    # 最終フォールバック：このパックをさらに小さなサブパックに分割し、再帰的に再試行する
                    if len(pack) > 1:
                        mid = len(pack) // 2
                        out.extend(_nmt_translate_list(pack[:mid], target=target))
                        out.extend(_nmt_translate_list(pack[mid:], target=target))
                    else:
                        # 単一アイテムでもまだ大きすぎる場合 → 文字列をさらに分割して再試行する
                        pieces = _split_long_text(pack[0], TRANSLATE_SEGMENT_MAX_CODEPOINTS // 2)
                        out.extend(_nmt_translate_list(pieces, target=target))
        return out

    # pandas.SeriesをNMTで翻訳し新しいSeriesとして返す関数
    def _nmt_translate_series(series, *, target=TARGET_LANG, batch_size=TRANSLATE_BATCH_SIZE):
        # batch_size is ignored; we pack by codepoints instead.
        values = series.fillna("").astype(str).tolist()
        return pd.Series(_nmt_translate_list(values, target=target), index=series.index)

    # pandas.SeriesをLLMで翻訳し新しいSeriesとして返す関数
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


    # --- 翻訳済みカラムを元列の直後に挿入 ---
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


# === 4.9 出力カラムの最終整形（列名・順序・NO.付与など） ===

# --- 指定列名へのリネーム ---
df_out = df_out.rename(columns={
    "国": "国（地域）",
    "タイトル": "タイトル（原語）",
})


# --- 必須列が無い場合は空で作成 ---
for _col in ["日付", "URL", "本文", "タイトル（原語）"]:
    if _col not in df_out.columns:
        df_out[_col] = ""


# --- 訳語列が無い場合の保険 ---
for _col in ["タイトル（日本語）", "和訳"]:
    if _col not in df_out.columns:
        df_out[_col] = ""



# --- 追加の空列を作成 ---
for _col in ["キーワード", "備考欄"]:
    if _col not in df_out.columns:
        df_out[_col] = ""



# --- 日付フォーマットを「月日」表記に統一 ---
 # 日付を「月日」表記の日本語文字列に変換する関数
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


# --- NO.列（連番）を先頭に挿入 ---
df_out.insert(0, "NO.", range(1, len(df_out) + 1))


# --- 列の最終順序を指定 ---
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

# --- 存在している列のみで並べ替え ---
_existing_cols = [c for c in _desired_cols if c in df_out.columns]
df_out = df_out[_existing_cols]


# === 5. JSON/CSVファイルへの書き出し処理 ===
OUTDIR = Path("output")
OUTDIR.mkdir(parents=True, exist_ok=True)
OUTPUT_JSON = OUTDIR / "articles_enriched.json"
OUTPUT_CSV  = OUTDIR / "articles_enriched.csv"


# --- JSONファイル出力 ---
OUTPUT_JSON.write_text(
    df_out.to_json(orient="records", force_ascii=False, indent=2),
    encoding="utf-8"
)


# --- CSVファイル出力（Excel互換/BOM付き） ---
df_out.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")

print(
    f"[INFO] 完了: {len(df_out)} 件を書き出しました → "
    f"{OUTPUT_JSON.name}, {OUTPUT_CSV.name}"
)