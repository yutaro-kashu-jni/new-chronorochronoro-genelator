# Cloud Run Jobs: URL Cloner Scaffold (bucket: urlcloner)

## 1) Build & push
```bash
PROJECT_ID=dev-peak
REGION=asia-northeast1

gcloud config set project $PROJECT_ID

# Create repo once
gcloud artifacts repositories create containers   --repository-format=docker --location=$REGION || true

# Build & push
gcloud builds submit   --tag $REGION-docker.pkg.dev/$PROJECT_ID/containers/urlcloner:latest
```

## 2) Create/Update the Job (with sharding)
```bash
IMAGE="$REGION-docker.pkg.dev/$PROJECT_ID/containers/urlcloner:latest"
SA="urlcloner-job-sa@$PROJECT_ID.iam.gserviceaccount.com"

# Roles (one-time)
# gcloud iam service-accounts create urlcloner-job-sa --display-name="URL Cloner Job SA"
# gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:$SA" --role="roles/storage.objectAdmin"
# gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:$SA" --role="roles/aiplatform.user"

JOB=urlcloner-job
gcloud run jobs describe $JOB --region=$REGION >/dev/null 2>&1   && CMD=update || CMD=create

gcloud run jobs $CMD $JOB   --image="$IMAGE"   --region="$REGION"   --tasks=8   --task-timeout=3600   --max-retries=1   --service-account="$SA"   --set-env-vars=GOOGLE_CLOUD_PROJECT=$PROJECT_ID,VERTEX_LOCATION=asia-northeast1,GENAI_LOCATIONS=asia-northeast1,us-central1,us-east4,MASTER_CSV_GCS=gs://urlcloner/data/master_media_data-utf8.csv,OUTPUT_BUCKET=gs://urlcloner/outputs
```

## 3) Run the Job on an input list in GCS
```bash
# Put your list here:
# gs://urlcloner/inputs/urls.txt
gcloud run jobs execute urlcloner-job --region=$REGION   --args="gs://urlcloner/inputs/urls.txt","--translate","1","--score-summarize","1"
```

## 4) Outputs
- Parts: `gs://urlcloner/outputs/<run_id>/parts/articles_enriched.part-000.csv` ...
- Final: `gs://urlcloner/outputs/<run_id>/articles_enriched.csv`
- Manifest: `gs://urlcloner/outputs/<run_id>/manifest.json`

## 5) Optional: control translation from combine.py
In **combine.py**, add at top:
```python
import os
ENABLE_TRANSLATE = os.getenv("ENABLE_TRANSLATE","1").lower() in ("1","true","yes","on")
```
Wrap translation insertions with `if ENABLE_TRANSLATE:` so `--translate 0` works.
