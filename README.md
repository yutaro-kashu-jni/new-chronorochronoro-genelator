# chronoro-genelator

## 1. ツールの概要

`chronoro-genelator`は、URLリストから記事をスクレイピングし、マスターデータと結合して翻訳・評価を行い、日本語翻訳付きのCSVレポートを生成するバッチ処理ツールです。

### 主な機能

- **URLスクレイピング**: URLリストから記事のタイトル、本文、公開日時を抽出
- **マスターデータ結合**: メディアマスターデータ（オフィシャル度、国、資料源）と結合
- **多言語対応**: 英語、中国語、韓国語、ロシア語など多言語の記事に対応
- **自動翻訳**: Translation LLM（Vertex AI）またはCloud Translation API v3を使用して記事本文とタイトルを日本語に翻訳
- **評価・要約**: Gemini APIを使用して記事の日本関連評価と要約を生成（オプション）
- **並列処理**: Cloud Run Jobsを使用した分散処理により、大量データを効率的に処理
- **CSV出力**: 処理結果をCSV形式で出力（GCSまたはローカルファイル）

## 2. 前提環境のインストール手順

### 必要な環境

- Python 3.12以上
- Google Cloud Platform (GCP) アカウント
- Playwright（ブラウザ自動化用）

### 依存関係のインストール

```bash
# 仮想環境の作成（推奨）
python3.12 -m venv venv
source venv/bin/activate  # macOS/Linux
# または
venv\Scripts\activate  # Windows

# 依存パッケージのインストール
pip install --upgrade pip
pip install -r requirements.txt

# Playwrightブラウザのインストール
playwright install chromium
```

### 必要な環境変数

`.env`ファイルを作成し、以下の環境変数を設定してください：

```bash
# Google Cloud設定
GOOGLE_CLOUD_PROJECT=your-project-id
VERTEX_LOCATION=asia-northeast1
GENAI_LOCATIONS=asia-northeast1,us-central1,us-east4
TRANSLATE_LOCATION=us-central1

# データパス
MASTER_CSV_GCS=gs://urlcloner/data/master_media_data-utf8.csv
OUTPUT_BUCKET=gs://urlcloner/outputs

# オプション設定
ENABLE_TRANSLATION=1  # 翻訳を有効化（0で無効化）
TARGET_LANG=ja        # 翻訳先言語
SCORE_SUMMARIZE=1     # 評価・要約を有効化（0で無効化）
WAIT_SECS_FOR_PARTS=300  # シャードマージの待機時間（秒）

# 翻訳エンジン設定
TRANSLATION_ENGINE=LLM  # LLM または NMT
TRANSLATE_REQ_MAX_CODEPOINTS=30000
TRANSLATE_SEGMENT_MAX_CODEPOINTS=8000

# Slack通知（オプション）
SLACK_WEBHOOK_URL=your-slack-webhook-url
```

### Google Cloud認証

```bash
# Application Default Credentials (ADC) の設定
gcloud auth application-default login

# サービスアカウントを使用する場合
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account-key.json
```

## 3. 使用方法

### ローカル実行

```bash
# 基本的な実行（URLリストファイルを指定）
python cloud_main.py urls.txt

# 翻訳を無効化
python cloud_main.py urls.txt --translate 0

# 評価・要約を無効化
python cloud_main.py urls.txt --score-summarize 0

# GCS上のURLリストを使用
python cloud_main.py gs://urlcloner/inputs/urls.txt
```

### 環境変数による設定

```bash
# 入力URLリストを環境変数で指定
export INPUT_URLS_GCS=gs://urlcloner/inputs/urls.txt

# 出力先を指定
export OUTPUT_BUCKET=gs://urlcloner/outputs

# 実行IDを指定（デフォルト: タイムスタンプ）
export RUN_ID=20250115-120000

# 翻訳設定
export ENABLE_TRANSLATION=1
export TARGET_LANG=ja
export TRANSLATION_ENGINE=LLM

# 評価・要約設定
export SCORE_SUMMARIZE=1
export VERTEX_LLM_MODEL=gemini-2.5-flash
```

### Cloud Run Jobsでの実行

```bash
# Dockerイメージのビルド
PROJECT_ID=dev-peak
REGION=asia-northeast1

gcloud config set project $PROJECT_ID

# Artifact Registryリポジトリの作成（初回のみ）
gcloud artifacts repositories create containers \
  --repository-format=docker \
  --location=$REGION || true

# ビルド＆プッシュ
gcloud builds submit \
  --tag $REGION-docker.pkg.dev/$PROJECT_ID/containers/urlcloner:latest

# Cloud Run Jobのデプロイ
IMAGE="$REGION-docker.pkg.dev/$PROJECT_ID/containers/urlcloner:latest"
SA="urlcloner-job-sa@$PROJECT_ID.iam.gserviceaccount.com"

JOB=urlcloner-job
gcloud run jobs $CMD $JOB \
  --image="$IMAGE" \
  --region="$REGION" \
  --tasks=8 \
  --task-timeout=3600 \
  --max-retries=1 \
  --service-account="$SA" \
  --set-env-vars=GOOGLE_CLOUD_PROJECT=$PROJECT_ID,VERTEX_LOCATION=asia-northeast1,GENAI_LOCATIONS=asia-northeast1,us-central1,us-east4,MASTER_CSV_GCS=gs://urlcloner/data/master_media_data-utf8.csv,OUTPUT_BUCKET=gs://urlcloner/outputs

# ジョブの実行
gcloud run jobs execute urlcloner-job \
  --region=$REGION \
  --args="gs://urlcloner/inputs/urls.txt","--translate","1","--score-summarize","1"
```

### 使用例

#### 例1: ローカルでURLリストファイルを使用

```bash
# urls.txt にURLを1行ずつ記載
python cloud_main.py urls.txt
```

#### 例2: GCS上のURLリストを使用

```bash
# GCSにURLリストをアップロード
gsutil cp urls.txt gs://urlcloner/inputs/urls.txt

# 実行
python cloud_main.py gs://urlcloner/inputs/urls.txt
```

#### 例3: 翻訳なしで実行

```bash
python cloud_main.py urls.txt --translate 0
```

#### 例4: 評価・要約なしで実行

```bash
python cloud_main.py urls.txt --score-summarize 0
```

## 4. コード構造

```
chronoro-genelator/
├── cloud_main.py              # Cloud Run Jobsエントリーポイント（メイン処理）
├── scraper.py                  # 記事スクレイピングモジュール
├── combine.py                  # マスターデータ結合・翻訳処理
├── score_summrize.py           # 評価・要約処理（Gemini API）
├── gcs_io.py                   # GCS入出力ヘルパー
├── requirements.txt            # Python依存パッケージ一覧
├── Dockerfile                  # Dockerコンテナイメージ定義
├── README-cloudrun.md          # Cloud Run Jobsデプロイ手順
├── env.yaml.example            # 環境変数サンプル
├── memo.txt                    # デプロイコマンドメモ
├── data/                       # データディレクトリ
│   └── master_media_data-utf8.csv  # メディアマスターデータ
├── output/                     # 出力ディレクトリ
│   ├── articles.json           # スクレイピング結果（JSON）
│   ├── articles_enriched.csv   # 結合・翻訳後のCSV
│   └── articles_enriched__with_jp_eval.csv  # 評価・要約付きCSV
└── urls_smoke.txt              # テスト用URLリスト
```

### 各ファイルの役割

- **cloud_main.py**:
  - URLリストの読み込み（ローカル/GCS対応）
  - Cloud Run Jobs対応の分散処理（シャーディング）
  - スクレイピング処理の実行
  - `combine.py`の呼び出し（翻訳・結合）
  - `score_summrize.py`の呼び出し（評価・要約、オプション）
  - シャード結果のマージ
  - GCSへの出力とマニフェスト生成
  - Slack通知（オプション）

- **scraper.py**:
  - URLからの記事抽出（タイトル、本文、公開日時）
  - 複数のスクレイピングライブラリ（trafilatura, readability, newspaper3k）を使用
  - Playwrightによるブラウザ自動化（JavaScriptレンダリングが必要なサイト）
  - ドメイン別のカスタム抽出ロジック（プラグイン方式）

- **combine.py**:
  - `articles.json`とマスターデータCSVの結合
  - ドメインベースのマッチング（サブドメイン対応）
  - Cloud Translation API v3による言語判定
  - Translation LLM（Vertex AI）またはNMTによる日本語翻訳
  - CSV出力（UTF-8 BOM付き、Excel互換）

- **score_summrize.py**:
  - Gemini APIを使用した記事の日本関連評価
  - 評価項目: mentions_japan, japan_relevance, score, intensity, evidence
  - 要約文の抽出（mention_sentences）
  - CSVへの評価カラム追加

- **gcs_io.py**:
  - GCSへの読み書きヘルパー関数
  - テキスト/バイナリファイルのアップロード・ダウンロード
  - プレフィックス一覧取得

## 5. 設定ファイル

### 環境変数

| 変数名 | 説明 | 必須 | デフォルト値 |
|--------|------|------|--------------|
| `GOOGLE_CLOUD_PROJECT` | GCPプロジェクトID | 必須 | - |
| `MASTER_CSV_GCS` | メディアマスターデータCSVパス（GCS URI） | 必須 | `gs://urlcloner/data/master_media_data-utf8.csv` |
| `OUTPUT_BUCKET` | 出力先GCSバケットパス | 必須 | `gs://urlcloner/outputs` |
| `INPUT_URLS_GCS` | 入力URLリストパス（環境変数で指定する場合） | 任意 | - |
| `RUN_ID` | 実行ID（出力ファイル名に使用） | 任意 | タイムスタンプ |
| `ENABLE_TRANSLATION` | 翻訳を有効化 | 任意 | `1` |
| `TARGET_LANG` | 翻訳先言語 | 任意 | `ja` |
| `TRANSLATION_ENGINE` | 翻訳エンジン（LLM/NMT） | 任意 | `LLM` |
| `VERTEX_LOCATION` | Vertex AIリージョン | 任意 | `asia-northeast1` |
| `GENAI_LOCATIONS` | Gemini API利用可能リージョン（カンマ区切り） | 任意 | `asia-northeast1,us-central1,us-east4` |
| `TRANSLATE_LOCATION` | Cloud Translation APIリージョン | 任意 | `us-central1` |
| `SCORE_SUMMARIZE` | 評価・要約を有効化 | 任意 | `1` |
| `VERTEX_LLM_MODEL` | Geminiモデル名 | 任意 | `gemini-2.5-flash` |
| `CLOUD_RUN_TASK_COUNT` | Cloud Run Jobsのタスク数 | 自動 | `1` |
| `CLOUD_RUN_TASK_INDEX` | 現在のタスクインデックス（自動設定） | 自動 | `0` |
| `WAIT_SECS_FOR_PARTS` | シャードマージの待機時間（秒） | 任意 | `300` |
| `SLACK_WEBHOOK_URL` | Slack通知用Webhook URL | 任意 | - |
| `TRANSLATE_REQ_MAX_CODEPOINTS` | 翻訳APIリクエスト最大コードポイント数 | 任意 | `30000` |
| `TRANSLATE_SEGMENT_MAX_CODEPOINTS` | 翻訳セグメント最大コードポイント数 | 任意 | `8000` |
| `INPUT_FILE_ENCODING` | 入力ファイルのエンコーディング（強制指定） | 任意 | 自動検出 |

### マスターデータCSV

メディアのオフィシャル度、国、資料源をマッピングするCSVファイル。必須カラム：

- `URL`: メディアURL
- `オフィシャル度`: オフィシャル度の値
- `国`: 国名
- `資料源`: 資料源の値

### URLリストファイル

入力URLリストは、1行に1つのURLを記載したテキストファイルです。

- 空行は無視されます
- `#`で始まる行はコメントとして無視されます
- ローカルファイルまたはGCS URI（`gs://`）を指定可能
- エンコーディングは自動検出されます（UTF-8, CP932, Shift_JIS, EUC-JP, ISO-2022-JP）

## 6. 認証/アクセス制御

### Google Cloud認証

1. **Application Default Credentials (ADC)**:
   ```bash
   gcloud auth application-default login
   ```

2. **サービスアカウント**:
   - Cloud Run Jobsではサービスアカウントを使用
   - 必要な権限:
     - Cloud Storage: 読み書き（`roles/storage.objectAdmin`）
     - Vertex AI: Gemini API呼び出し（`roles/aiplatform.user`）
     - Cloud Translation: 翻訳API呼び出し（`roles/cloudtranslate.user`）
     - Cloud Run Jobs: ジョブ実行

### アクセス制御

- **GCSバケット**: 適切なIAMポリシーを設定
- **Cloud Run Jobs**: サービスアカウントに最小権限を付与
- **Slack通知**: Webhook URLはSecret Managerで管理（推奨）

## 7. 注意事項

### 実行時の注意点

1. **レート制限**:
   - Gemini APIにはレート制限があるため、大量データ処理時は注意
   - Cloud Run Jobsで複数タスクに分散することで処理時間を短縮可能

2. **メモリ使用量**:
   - 大量データ処理時はメモリ不足に注意
   - Cloud Run Jobsでは適切なメモリ設定を推奨

3. **タイムアウト**:
   - 長時間処理の場合は`task-timeout`を適切に設定
   - デフォルトは3600秒（1時間）

4. **並列処理**:
   - Cloud Run Jobsで複数タスクを実行する場合、データは自動的にシャーディングされる
   - タスク0がコーディネーターとしてシャードをマージ
   - `WAIT_SECS_FOR_PARTS`でマージ待機時間を調整可能

5. **出力ファイル**:
   - GCSに出力する場合、バケットへの書き込み権限が必要
   - ファイル名は`articles_enriched_<run_id>.csv`形式
   - 評価・要約付きの場合は`articles_enriched_<run_id>__with_jp_eval.csv`

6. **翻訳エンジン**:
   - `TRANSLATION_ENGINE=LLM`: Translation LLM（Vertex AI）を使用（高品質、高コスト）
   - `TRANSLATION_ENGINE=NMT`: Cloud Translation API（NMT）を使用（高速、低コスト）
   - LLMでエラーが発生した場合、自動的にNMTにフォールバック

7. **スクレイピング**:
   - JavaScriptレンダリングが必要なサイトはPlaywrightを使用
   - 一部のサイトはドメイン別のカスタム抽出ロジックが必要
   - スクレイピング失敗時はエラーログに記録され、処理は継続

### トラブルシューティング

- **認証エラー**: ADCの設定を確認（`gcloud auth application-default login`）
- **GCSアクセスエラー**: サービスアカウントの権限を確認
- **Gemini APIエラー**: プロジェクトのクォータとレート制限を確認
- **スクレイピング失敗**: `scraper.py`のログを確認、必要に応じてカスタム抽出ロジックを追加
- **翻訳エラー**: リージョン設定を確認（`VERTEX_LOCATION`, `TRANSLATE_LOCATION`）
- **シャードマージタイムアウト**: `WAIT_SECS_FOR_PARTS`を増やす、またはタスク数を減らす

### パフォーマンス最適化

- **タスク数の調整**: データ量に応じて`--tasks`を調整（デフォルト: 8）
- **翻訳エンジンの選択**: 品質重視ならLLM、速度重視ならNMT
- **評価・要約の無効化**: 不要な場合は`--score-summarize 0`で無効化
- **エンコーディング指定**: 入力ファイルのエンコーディングが分かっている場合は`INPUT_FILE_ENCODING`で指定

### 開発時の注意

- **ローカル開発**: 環境変数を適切に設定（`.env`ファイルを使用可能）
- **デバッグモード**: `DEBUG=1`で詳細ログを出力
- **テスト実行**: `urls_smoke.txt`で小規模テストを実行
- **エンコーディング**: 日本語を含むファイルはUTF-8で保存

