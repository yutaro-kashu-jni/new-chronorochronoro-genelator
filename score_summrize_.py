
#!/usr/bin/env python3
# =============================================================================
# score_summrize_.py の役割
# -----------------------------------------------------------------------------
# 入力CSVの「和訳」カラム（または指定カラム）をVertex AI Gemini LLMに渡し、
# 「日本関連の評価」JSONを取得して新たなカラムとして追記する自動評価スクリプトです。
# - 日本への言及有無・スコア・強度・根拠引用などを自動判定し、CSV/Excelに出力します。
# - GCPプロジェクト/リージョン/モデル/カラム名等は環境変数で柔軟に指定可能です。
# - LLM API障害時はリージョンを順次フォールバックし、全失敗時も仕様に沿った空評価を返します。
# - Excel出力時は根拠引用等を改行区切りで見やすく整形します。
# =============================================================================
# -*- coding: utf-8 -*-
# -*- coding: utf-8 -*-
"""

# 1) gcloud を最新化（任意だが推奨）
gcloud components update

# 2) gcloud CLI のログイン（CLI用）※やっていない/古い場合のみ
gcloud auth login

# 3) **ADC の再認証（これが本命）**
gcloud auth application-default login

# 4) プロジェクトを明示
gcloud config set project dev-peak2
py
CSVに「日本関連の評価」カラムを追加するスクリプト。
- 入力CSVの「和訳」カラム（変更可）をLLMに渡し、評価JSONを受け取り、カラムとして追記します。
- Vertex AI Gemini (gemini-2.5-flash) を使用。リージョンはフォールバック可能。

使い方:
    python score_summrize.py input.csv

環境変数 (必要に応じて上書き可):
    GOOGLE_CLOUD_PROJECT=dev-peak2
    VERTEX_LOCATION=asia-northeast1
    VERTEX_LLM_MODEL=gemini-2.5-flash
    GENAI_LOCATIONS=asia-northeast1,us-central1,us-east4
    TEXT_MAX_CHARS=2000
    INPUT_COL=和訳
    # 出力ファイル名の接尾辞
    OUTPUT_SUFFIX=__with_jp_eval
"""
from __future__ import annotations

import os
import sys
import csv
import json
import math
import time
from typing import Any, Dict, List

import pandas as pd

# Google Gen AI SDK (Vertex AI 経由で使用)
from google import genai
from google.genai import types as genai_types

# from dotenv import load_dotenv
# load_dotenv()  # .env を環境変数に反映


# ===== ユーザー指定のプロンプト（そのまま使用） =====
PROMPT_HEADER = """指示：
与えられた本文のみを根拠に評価し、外部知識は使わないこと。出力は必ずJSONのみ。説明文・マークアップ・コードフェンスを出力してはいけない。指定のキー以外は出力しない。

日本への言及の定義（いずれかを満たせば true）：
- 「日本」「Japan」「Japanese」「Japon」「Япония」「일본」「Nhật Bản」等の“日本”の多言語表記への直接言及
- 日本の政府・機関・部隊（例：防衛省、自衛隊、JMSDF/JGSDF/JASDF、在日米軍/USFJ、日本政府/GoJ、外務省/MOFA、海上保安庁/JCG 等）の明示
- 日本の地名・海域・EEZ等が実体的に関与（例：東京、沖縄、北海道、尖閣、琉球、日本海/Sea of Japan、東シナ海の日本EEZ 等）
- 日本の政策・装備・外交・災害・安全保障・経済安全保障等を主題的に扱う記述

上記に該当すれば mentions_japan = true、該当しなければ false。

評価項目：
1. mentions_japan（boolean）: 日本に関する言及の有無
2. japan_relevance（string）: "direct"（明示的言及あり） / "ambiguous"（日本を示唆するが明示なし） / "none"（言及なし）
3. no_mention_reason（string, 任意）: mentions_japan=false または japan_relevance="ambiguous"/"none" のとき、簡潔に理由を1文で示す（例：「本文に日本や関連固有表現の明示がないため。」）。
4. score（number）: -5.0〜+5.0（0.5刻み）。マイナス=日本/防衛省への批判、プラス=支持
   - -5.0 = 最大限の批判、+5.0 = 最大限の支持
   - 端数は最も近い0.5に丸める（例：0.26→0.5、-0.26→-0.5）。範囲外は[-5,5]に丸め込み
5. intensity（integer）: 1〜5。1=非常に弱い、5=非常に強い
6. evidence（array, up to 3）: スコア判断の根拠（重要度順）
   - 各要素は「番号 + 根拠説明 + 改行（\\n） + 原文引用」の1文字列（**説明と引用の間に必ず改行**）
   - 根拠説明は常体（〜である／〜している）で記述し、「〜ため／〜ために」を使用しない。 **代わりに「〜と指摘している」「〜が問題視されている」「〜と述べた」「〜を批判している」等を用いる。**
   - **文末は必ず句点「。」で終える**。冗長な口語・係り受けは避ける。
   - **原文引用は必ず3文以上（短い断片禁止）。必要に応じて前後の文も含めてよい**
   
7. mention_sentences（array of strings, 引用サマリー）:
   - 記事の要旨を示す原文の「完全な1文」だけを 3〜5 件抽出する。言い換え・要約・語順変更・合成は禁止。
   - 各要素は1文のみ。省略記号（…）や追加の括弧・引用符・注釈は付けない。句読点・大文字小文字は原文のまま。
   - 本文の出現順に並べる。重複は除外する。見出し装飾・著作権表記・UI/広告・画像キャプションは除外する。
   - 選定基準：①リード/冒頭で主題を示す文 → ②中核事実（誰が/何を/いつ/どこで/どうした） → ③結果・背景・見通しを述べる文。
   - 日本への言及有無にかかわらず常時出力する。
   - 本文が3文未満の場合は取得可能な範囲で全ての文を出力する。

ガイドライン：
- ゲーティング：mentions_japan=false の場合、日本への評価は行わず **score=0、intensity=1、evidence=[]** とする。`japan_relevance` は "none" または文脈に応じて "ambiguous" を設定し、`no_mention_reason` を必ず1文で記載する。**`mention_sentences` は日本の言及有無にかかわらず抽出し、3〜5文を出力する。**
- あいまいケース：日本を示唆するが明示がない場合は `japan_relevance="ambiguous"` とし、`mentions_japan=false` のまま中立評価（score=0、intensity=1）。`no_mention_reason` に「示唆はあるが明示がない」旨を記載し、**`mention_sentences` は通常どおり抽出する。**
- 評価の範囲：日本や防衛省への評価のみをスコアに反映。他主体（他国・他組織）への評価は含めない。
- 整合性：evidence と score/intensity の論理整合を保つ。evidence の引用は mention_sentences と重複してもよい。
- **表現ルール（スタイル）**：根拠説明は常体で簡潔に。**「〜ため／〜ために」を禁止**し、代替として「〜と指摘している」「〜が問題視されている」「〜と述べた」等を用いる。**文末は句点「。」で統一**。推量・婉曲の多用を避け、必要な場合は「〜と報じている／〜と述べている」を使用。
- 説明と引用の間は **改行（\\n）** を入れる。引用部は「」で括る。

出力形式（JSONのみ）：
{
  "mentions_japan": true,
  "japan_relevance": "direct",
  "no_mention_reason": "",
  "score": 0.5,
  "intensity": 2,
  "evidence": [
    "1. （根拠説明）: 「（原文の3文以上の引用。必要なら前後の文も含める）」",
    "2. （根拠説明）: 「（原文の3文以上の引用）」"
  ],
  "mention_sentences": [
    "（必要に応じて最大5件まで）"
  ]
}

"""

PROMPT_INPUT_PREFIX = "入力：\n文章:"

# レスポンスのスキーマ（構造化出力を強制）
SCHEMA = genai_types.Schema(
    type="OBJECT",
    properties={
        "mentions_japan": genai_types.Schema(type="BOOLEAN"),
        "japan_relevance": genai_types.Schema(type="STRING"),
        "no_mention_reason": genai_types.Schema(type="STRING"),
        "score": genai_types.Schema(type="NUMBER"),
        "intensity": genai_types.Schema(type="INTEGER"),
        "evidence": genai_types.Schema(type="ARRAY", items=genai_types.Schema(type="STRING")),
        "mention_sentences": genai_types.Schema(type="ARRAY", items=genai_types.Schema(type="STRING")),
    },
    # 追加フィールドは後方互換のため required に含めない
    required=["mentions_japan", "score", "intensity", "evidence"],
)


 # 環境変数を取得し、未設定時はデフォルト値を返す関数
def getenv(key: str, default: str) -> str:
    v = os.environ.get(key, default)
    return v


 # カンマ区切りのリージョン文字列をリスト化し、重複を除去して順序を維持する関数
def parse_locations(env_val: str) -> List[str]:
    parts = [p.strip() for p in env_val.split(",") if p.strip()]
    # 重複排除を保ちつつ順序維持
    seen = set()
    ordered = []
    for p in parts:
        if p not in seen:
            seen.add(p)
            ordered.append(p)
    return ordered or ["asia-northeast1", "us-central1", "us-east4"]


 # スコア値を-5.0～5.0の範囲で0.5刻みに丸める関数
def round_to_half(x: float) -> float:
    try:
        x = float(x)
    except Exception:
        return 0.0
    # clamp
    x = max(min(x, 5.0), -5.0)
    # 最近傍0.5刻みに丸め（±0のときは0.0）
    sgn = 1.0 if x >= 0 else -1.0
    val = math.floor(abs(x) * 2.0 + 0.5) / 2.0
    return sgn * val


 # 強度値を1～5の整数に正規化する関数
def coerce_intensity(val: Any) -> int:
    try:
        v = int(round(float(val)))
    except Exception:
        v = 1
    return max(1, min(5, v))


 # モデル出力(JSON)を仕様に沿って正規化・補完し、後方互換も確保する関数
def ensure_schema(payload: Dict[str, Any]) -> Dict[str, Any]:
    """モデル出力(JSON)を仕様にアジャスト（後方互換を確保）"""
    mj = bool(payload.get("mentions_japan", False))

    # 正規化: japan_relevance
    raw_rel = payload.get("japan_relevance", None)
    rel = str(raw_rel).lower().strip() if isinstance(raw_rel, str) else ""
    if rel not in ("direct", "ambiguous", "none"):
        rel = "direct" if mj else "none"
    # mentions_japan=True で "none" が来た場合は "direct" に補正
    if mj and rel == "none":
        rel = "direct"

    # 正規化: no_mention_reason
    reason = payload.get("no_mention_reason", "")
    if not isinstance(reason, str):
        reason = str(reason or "")

    # 常に引用サマリー（mention_sentences）は保持
    mentions = payload.get("mention_sentences", [])
    if not isinstance(mentions, list):
        mentions = [str(mentions)]
    mentions = [str(x) for x in mentions][:5]

    # evidence 正規化（最大3件）
    evidence = payload.get("evidence", [])
    if not isinstance(evidence, list):
        evidence = [str(evidence)]
    evidence = [str(x) for x in evidence][:3]

    if not mj:
        if not reason:
            reason = "本文に日本や関連固有表現の明示がないため。"
        # 日本言及がない場合でも summary（mention_sentences）は出力対象
        return {
            "mentions_japan": False,
            "japan_relevance": rel,
            "no_mention_reason": reason,
            "score": 0.0,
            "intensity": 1,
            "evidence": [],
            "mention_sentences": mentions,
        }

    score = round_to_half(payload.get("score", 0.0))
    intensity = coerce_intensity(payload.get("intensity", 1))

    return {
        "mentions_japan": True,
        "japan_relevance": rel,
        "no_mention_reason": "",
        "score": score,
        "intensity": intensity,
        "evidence": evidence,
        "mention_sentences": mentions,
    }


 # 指定テキストをLLM用プロンプト形式に整形して返す関数
def build_prompt(text: str) -> str:
    return f"{PROMPT_HEADER}\n{PROMPT_INPUT_PREFIX}{text}"


 # 指定テキストをLLMに投げ、リージョン障害時は順次フォールバックして最初の成功結果を返す関数
 # すべて失敗した場合は仕様に沿った空評価を返す
def call_gemini_with_fallbacks(
    text: str,
    model_name: str,
    locations: List[str],
    project: str,
) -> Dict[str, Any]:
    """リージョンを順に試す。成功した最初の結果を返す。失敗時はデフォルトの空評価。"""
    for loc in locations:
        try:
            client = genai.Client(vertexai=True, project=project, location=loc)
            resp = client.models.generate_content(
                model=model_name,
                contents=[build_prompt(text)],
                config=genai_types.GenerateContentConfig(
                    response_mime_type="application/json",
                    response_schema=SCHEMA,
                ),
            )
            raw = (resp.text or "").strip()
            if not raw:
                raise RuntimeError("Empty response text")
            # 余分な囲い（```json ... ```など）が来た場合の軽微な修正
            if raw.startswith("```"):
                raw = raw.strip("` \n")
                raw = raw.replace("json\n", "", 1).strip("` \n")
            data = json.loads(raw)
            return ensure_schema(data)
        except Exception as e:
            print(f"[WARN] Vertex call failed at {loc}: {e}", file=sys.stderr)
            continue

    # 全リージョン失敗時のフォールバック（仕様に沿った空評価）
    return {
        "mentions_japan": False,
        "score": 0.0,
        "intensity": 1,
        "evidence": [],
        "mention_sentences": [],
    }


 # 入力CSVを読み込み、各行の指定カラムをLLMで評価し、評価結果カラムを追記して新CSV/Excelを出力する主処理関数
 # - 評価カラムの追加、重み付きスコア計算、根拠引用の整形、Excel出力も担当
def process_csv(input_path: str) -> str:
    project = getenv("GOOGLE_CLOUD_PROJECT", "dev-peak2")
    default_loc = getenv("VERTEX_LOCATION", "asia-northeast1")
    locations = parse_locations(getenv("GENAI_LOCATIONS", f"{default_loc},us-central1,us-east4"))
    model_name = getenv("VERTEX_LLM_MODEL", "gemini-2.5-flash")
    max_chars = int(getenv("TEXT_MAX_CHARS", "2000"))
    input_col = getenv("INPUT_COL", "和訳")
    output_suffix = getenv("OUTPUT_SUFFIX", "__with_jp_eval")

    print("=== 設定 ===")
    print(f"PROJECT           : {project}")
    print(f"MODEL             : {model_name}")
    print(f"LOCATIONS         : {locations}")
    print(f"TEXT_MAX_CHARS    : {max_chars}")
    print(f"INPUT_COL         : {input_col}")
    print(f"OUTPUT_SUFFIX     : {output_suffix}")
    print("================")

    # CSV読込
    df = pd.read_csv(input_path, dtype=str)  # 既存カラムを壊さないため文字列基準で読込
    total = len(df)
    if total == 0:
        raise SystemExit("入力CSVにデータがありません。")

    if input_col not in df.columns:
        raise SystemExit(f"入力カラム '{input_col}' が見つかりません。列名を確認するか、環境変数 INPUT_COL で指定してください。")

    # 追記するカラム名
    col_mj  = "jp_eval_mentions_japan"
    col_sc  = "jp_eval_score"
    col_int = "jp_eval_intensity"
    col_evd = "jp_eval_evidence_json"
    col_qsum = "quote_summary_json"
    col_wsc = "jp_eval_weighted_score"
    col_rel = "jp_eval_japan_relevance"
    col_reason = "jp_eval_no_mention_reason"

    # 既存の場合は上書き
    for c in (col_mj, col_wsc, col_sc, col_int, col_evd, col_qsum, col_rel, col_reason):
        if c not in df.columns:
            df[c] = ""

    print(f"処理対象レコード: {total} 件")
    completed = 0

    for i in range(total):
        text = df.at[i, input_col]
        if text is None or str(text).strip() == "" or str(text).lower() == "nan":
            # 空入力: 仕様のデフォルト値を設定
            res = {
                "mentions_japan": False,
                "score": 0.0,
                "intensity": 1,
                "evidence": [],
                "mention_sentences": [],
            }
        else:
            t = str(text).strip()
            if len(t) > max_chars:
                t = t[:max_chars]
            res = call_gemini_with_fallbacks(t, model_name, locations, project)

        # DataFrameに書き込み
        df.at[i, col_mj]  = str(bool(res.get("mentions_japan", False)))
        df.at[i, col_sc]  = str(res.get("score", 0.0))
        df.at[i, col_int] = str(res.get("intensity", 1))
        # 重み付きスコア（最終スコア案）: score * intensity
        try:
            s_val = float(res.get("score", 0.0))
            i_val = int(res.get("intensity", 1))
            w_val = round(s_val * i_val, 3)
        except Exception:
            w_val = 0.0
        df.at[i, col_wsc] = str(w_val)
        df.at[i, col_evd] = json.dumps(res.get("evidence", []), ensure_ascii=False)
        df.at[i, col_qsum] = json.dumps(res.get("mention_sentences", []), ensure_ascii=False)
        df.at[i, col_rel] = str(res.get("japan_relevance", "direct" if res.get("mentions_japan", False) else "none"))
        df.at[i, col_reason] = str(res.get("no_mention_reason", ""))

        completed += 1
        print(f"{completed} 件目／合計 {total} 件が完了しました。", flush=True)

    # 列順の調整: jp_eval_weighted_score を jp_eval_score の左側に移動
    try:
        if col_wsc in df.columns and col_sc in df.columns:
            cols = list(df.columns)
            if col_wsc in cols:
                cols.remove(col_wsc)
            insert_at = cols.index(col_sc)
            cols.insert(insert_at, col_wsc)
            df = df.loc[:, cols]
    except Exception as e:
        print(f"[WARN] column reordering skipped: {e}", file=sys.stderr)

    # CSV にも改行区切りの人間可読カラムを追加
    def _as_lines_csv(v):
        try:
            s = "" if v is None else str(v).strip()
            if not s or s.lower() == "nan":
                return ""
            if s.startswith("[") and s.endswith("]"):
                arr = json.loads(s)
                if isinstance(arr, list):
                    return "\n".join(str(x) for x in arr if str(x).strip() != "")
        except Exception:
            pass
        return str(v) if v is not None else ""

    if "jp_eval_evidence_json" in df.columns:
        df["jp_eval_evidence"] = df["jp_eval_evidence_json"].apply(_as_lines_csv)
    if "quote_summary_json" in df.columns:
        df["quote_summary_sentences"] = df["quote_summary_json"].apply(_as_lines_csv)
    elif "jp_eval_mention_sentences_json" in df.columns:  # backward compatibility
        df["quote_summary_sentences"] = df["jp_eval_mention_sentences_json"].apply(_as_lines_csv)

    # 出力パス決定
    base, ext = os.path.splitext(input_path)
    out_path = f"{base}{output_suffix}{ext or '.csv'}"

    # 最終CSVから JSON 配列カラムは除外する
    drop_cols = [
        "jp_eval_evidence_json",
        "quote_summary_json",
        "jp_eval_mention_sentences_json",  # legacy cleanup
    ]
    df_to_save = df.drop(columns=[c for c in drop_cols if c in df.columns], errors="ignore")
    df_to_save.to_csv(out_path, index=False, quoting=csv.QUOTE_MINIMAL)

    # 併せて Excel (人が読みやすい改行区切り) を出力 — OUTPUT_EXCEL が true のとき
    if os.environ.get("OUTPUT_EXCEL", "false").lower() in ("1", "true", "yes", "on"):
        df_x = df.copy()

        def _as_lines(v):
            try:
                s = "" if v is None else str(v).strip()
                if not s or s.lower() == "nan":
                    return ""
                if s.startswith("[") and s.endswith("]"):
                    arr = json.loads(s)
                    if isinstance(arr, list):
                        return "\n".join(str(x) for x in arr if str(x).strip() != "")
            except Exception:
                pass
            return str(v) if v is not None else ""

        # JSON 文字列カラムを改行区切りのテキストへ（新規カラムとして追加）
        if "jp_eval_evidence_json" in df_x.columns:
            df_x["jp_eval_evidence"] = df_x["jp_eval_evidence_json"].apply(_as_lines)
        if "quote_summary_json" in df_x.columns:
            df_x["quote_summary_sentences"] = df_x["quote_summary_json"].apply(_as_lines)
        elif "jp_eval_mention_sentences_json" in df_x.columns:  # backward compatibility
            df_x["quote_summary_sentences"] = df_x["jp_eval_mention_sentences_json"].apply(_as_lines)

        xlsx_path = f"{base}{output_suffix}.xlsx"
        try:
            # openpyxl があれば折返しを適用
            from openpyxl.styles import Alignment  # type: ignore
            with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
                df_x.to_excel(writer, index=False, sheet_name="Sheet1")
                ws = writer.book.active
                # 折返し対象の列に wrap_text を設定
                for name in ("jp_eval_evidence", "quote_summary_sentences"):
                    if name in df_x.columns:
                        col_idx = df_x.columns.get_loc(name) + 1  # 1-based index
                        for row in range(2, ws.max_row + 1):  # データ行のみ
                            cell = ws.cell(row=row, column=col_idx)
                            cell.alignment = Alignment(wrap_text=True)
        except Exception as e:
            # スタイリングに失敗した場合でも Excel 自体は出力
            try:
                with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
                    df_x.to_excel(writer, index=False, sheet_name="Sheet1")
            except Exception as e2:
                print(f"[WARN] Excel export failed: {e2}", file=sys.stderr)
        else:
            print(f"Excel出力: {xlsx_path}")

    print("=== 完了 ===")
    print(f"出力: {out_path}")
    return out_path


 # コマンドライン引数でCSVパスを受け取り、処理を実行するエントリポイント関数
def main():
    if len(sys.argv) < 2:
        print("使い方: python main.py input.csv", file=sys.stderr)
        sys.exit(2)
    input_path = sys.argv[1]
    if not os.path.exists(input_path):
        print(f"ファイルが見つかりません: {input_path}", file=sys.stderr)
        sys.exit(2)

    try:
        process_csv(input_path)
    except Exception as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
