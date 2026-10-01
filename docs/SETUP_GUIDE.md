# CS / MBA / AI Job Postings — Full GCP Project
## Step-by-Step Setup, Deployment & Results Tracking Guide

**D.Eng. Research — AI in the Workforce & Labor Market**  
Penn State Great Valley | Rickey Patel

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         TRIGGER LAYER                                    │
│  Cloud Scheduler (weekly, Mon 6 AM UTC)                                 │
│         │                                                               │
│         ▼                                                               │
├─────────────────────────────────────────────────────────────────────────┤
│                        COLLECTION LAYER                                  │
│  Cloud Run Job: collector.py                                            │
│  ┌──────────────┐  ┌──────────────┐                                    │
│  │  USAJobs API  │  │  Jooble API  │                                    │
│  │  38 keywords  │  │  33 keywords │                                    │
│  │  ~9,500 raw   │  │  ~3,300 raw  │                                    │
│  └──────┬───────┘  └──────┬───────┘                                    │
│         │                 │                                             │
│         ▼                 ▼                                             │
│  ┌───────────────────────────────────────────────────────────────────┐  │
│  │              Pub/Sub Topic: job-postings-raw                      │  │
│  │              (one message per posting, JSON)                      │  │
│  └───────────────────────┬───────────────────────────────────────────┘  │
│                          │                                              │
│         Also → GCS raw/usajobs/*.json + raw/jooble/*.json              │
├─────────────────────────────────────────────────────────────────────────┤
│                       PROCESSING LAYER                                   │
│  Dataflow Streaming Pipeline (beam_pipeline.py)                         │
│  Pub/Sub → ParseJSON → EnrichAndFlatten → FilterRelevance               │
│         → BigQuery: job_postings.feature_postings                       │
│                                                                         │
│  Dataflow Batch Pipeline (triggered manually or post-collection)        │
│  GCS raw/*.json → same transforms → BigQuery + GCS processed/           │
├─────────────────────────────────────────────────────────────────────────┤
│                         ML LAYER                                         │
│  Vertex AI Pipelines (mlops_pipeline.py)                                │
│  BQ export → AutoML Tabular → evaluate → (if acc ≥ 0.82) → deploy      │
│  Model: domain classifier (CS / MBA / DATA_ANALYTICS / AI)             │
│  Endpoint: job_domain_endpoint                                          │
│  Pipeline runs logged to BQ: job_postings.pipeline_runs                 │
├─────────────────────────────────────────────────────────────────────────┤
│                       DASHBOARD LAYER                                    │
│  dashboard.html — static HTML, zero server required                     │
│  Import: MASTER_*.json from GCS, or live BigQuery API proxy             │
│  Filters: domain, source, era, tier, score, full-text search            │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Prerequisites

| Item | Required | Notes |
|---|---|---|
| GCP account with billing | ✅ | New accounts get $300 free credit |
| USAJobs API key | ✅ | Register at developer.usajobs.gov |
| Jooble API key | ✅ | Register at jooble.org/api (500 req/day free) |
| `gcloud` CLI | ✅ | Or use Cloud Shell (zero install) |
| Python 3.11+ | For local dev | Not needed if using Cloud Shell |

---

## Phase 1 — GCP Project Bootstrap

### Step 1.1 — Open Cloud Shell

Go to **console.cloud.google.com** → click **Activate Cloud Shell** (terminal icon top-right).

All commands below run in Cloud Shell unless noted otherwise.

### Step 1.2 — Set project variables

```bash
# ── Replace these values ──────────────────────────────────────────────────────
export PROJECT_ID="your-project-id"          # must be globally unique
export REGION="us-central1"
export USAJOBS_EMAIL="your@email.com"
export USAJOBS_KEY="your-usajobs-api-key"
export JOOBLE_KEY="your-jooble-api-key"

# ── Derived variables (do not edit) ──────────────────────────────────────────
export GCS_BUCKET="${PROJECT_ID}-job-pipeline"
export PUBSUB_TOPIC="projects/${PROJECT_ID}/topics/job-postings-raw"
export PUBSUB_SUB="projects/${PROJECT_ID}/subscriptions/job-postings-sub"
export AR_REPO="job-pipeline-repo"
export COLLECTOR_IMAGE="us-central1-docker.pkg.dev/${PROJECT_ID}/${AR_REPO}/collector:latest"
export PIPELINE_SA="${PROJECT_ID}@appspot.gserviceaccount.com"
```

### Step 1.3 — Set project & enable APIs

```bash
gcloud config set project ${PROJECT_ID}

gcloud services enable \
  run.googleapis.com \
  dataflow.googleapis.com \
  pubsub.googleapis.com \
  bigquery.googleapis.com \
  bigquerystorage.googleapis.com \
  storage.googleapis.com \
  aiplatform.googleapis.com \
  cloudscheduler.googleapis.com \
  secretmanager.googleapis.com \
  artifactregistry.googleapis.com \
  cloudbuild.googleapis.com \
  logging.googleapis.com \
  monitoring.googleapis.com

echo "APIs enabled ✓"
```

*Wait ~60 seconds for propagation.*

### Step 1.4 — Create GCS bucket

```bash
gcloud storage buckets create gs://${GCS_BUCKET} \
  --location=${REGION} \
  --uniform-bucket-level-access

# Create folder structure
for PREFIX in raw/usajobs raw/jooble processed/master vertex-datasets \
              vertex-pipelines dataflow-temp models metadata; do
  echo "" | gcloud storage cp - gs://${GCS_BUCKET}/${PREFIX}/.keep
done

echo "GCS bucket gs://${GCS_BUCKET} created ✓"
```

### Step 1.5 — Create Pub/Sub topic & subscription

```bash
gcloud pubsub topics create job-postings-raw \
  --message-retention-duration=7d

gcloud pubsub subscriptions create job-postings-sub \
  --topic=job-postings-raw \
  --ack-deadline=60 \
  --message-retention-duration=7d

# Dead-letter topic for failed messages
gcloud pubsub topics create job-postings-dead-letter

# FIX: `modify-push-config` only configures push-delivery settings and does
# not accept --dead-letter-topic/--max-delivery-attempts at all — and
# job-postings-sub is a pull subscription (no --push-endpoint above) anyway.
# `subscriptions update` is the correct subcommand for setting a
# dead-letter policy regardless of push/pull delivery mode.
gcloud pubsub subscriptions update job-postings-sub \
  --dead-letter-topic=job-postings-dead-letter \
  --max-delivery-attempts=5

# Required for the dead-letter policy above to actually work: the Pub/Sub
# service agent needs publish rights on the DLQ topic and subscribe rights
# on the source subscription. Without these, dead-lettering silently never
# triggers (messages just keep retrying against job-postings-sub forever).
PROJECT_NUMBER=$(gcloud projects describe ${PROJECT_ID} --format='value(projectNumber)')
PUBSUB_SA="service-${PROJECT_NUMBER}@gcp-sa-pubsub.iam.gserviceaccount.com"
gcloud pubsub topics add-iam-policy-binding job-postings-dead-letter \
  --member="serviceAccount:${PUBSUB_SA}" --role="roles/pubsub.publisher"
gcloud pubsub subscriptions add-iam-policy-binding job-postings-sub \
  --member="serviceAccount:${PUBSUB_SA}" --role="roles/pubsub.subscriber"

echo "Pub/Sub topic and subscription created ✓"
```

Note this is a different, complementary mechanism from `beam_pipeline.py`'s
own dead-lettering (`ParseJSON`/`DecodePubSub` routing malformed *message
content* to a DLQ via `--dlq_topic`, added in Phase 4). This Pub/Sub-level
policy instead catches messages that repeatedly fail to be acked at all
(e.g. the Dataflow job is down) — both matter, neither substitutes for the
other.

### Step 1.6 — Create BigQuery dataset and tables

```bash
bq --location=${REGION} mk --dataset ${PROJECT_ID}:job_postings

# feature_postings — main enriched postings table
bq mk --table ${PROJECT_ID}:job_postings.feature_postings \
  hash_id:STRING,job_title:STRING,company:STRING,location:STRING,\
domain:STRING,era:STRING,source:STRING,publication_date:STRING,\
application_close:STRING,salary:STRING,job_type:STRING,grade:STRING,\
relevance_score:INTEGER,relevance_tier:STRING,required_skills:STRING,\
onet_skills:STRING,onet_basic:INTEGER,onet_social:INTEGER,\
onet_technical:INTEGER,onet_systems:INTEGER,onet_resource:INTEGER,\
onet_complex:INTEGER,tech_skill_count:INTEGER,onet_skill_count:INTEGER,\
description:STRING,job_url:STRING,search_keyword:STRING,\
matched_terms:STRING,collected_at:TIMESTAMP,run_timestamp:STRING

# pipeline_runs — MLOps tracking table
bq mk --table ${PROJECT_ID}:job_postings.pipeline_runs \
  run_timestamp:STRING,logged_at:STRING,model_resource:STRING,\
eval_metric:FLOAT,endpoint:STRING,deployed:BOOLEAN

echo "BigQuery dataset and tables created ✓"
```

### Step 1.7 — Store API keys in Secret Manager

```bash
echo -n "${USAJOBS_EMAIL}" | \
  gcloud secrets create USAJOBS_EMAIL --data-file=- --replication-policy=automatic

echo -n "${USAJOBS_KEY}" | \
  gcloud secrets create USAJOBS_KEY --data-file=- --replication-policy=automatic

echo -n "${JOOBLE_KEY}" | \
  gcloud secrets create JOOBLE_KEY --data-file=- --replication-policy=automatic

echo "Secrets stored ✓"
```

### Step 1.8 — Create service account and grant permissions

```bash
SA_NAME="job-pipeline-sa"
SA_EMAIL="${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

gcloud iam service-accounts create ${SA_NAME} \
  --display-name="Job Pipeline Service Account"

# Roles needed for collector + Dataflow + Vertex AI
for ROLE in \
  roles/bigquery.dataEditor \
  roles/bigquery.jobUser \
  roles/dataflow.worker \
  roles/storage.objectAdmin \
  roles/pubsub.publisher \
  roles/pubsub.subscriber \
  roles/aiplatform.user \
  roles/secretmanager.secretAccessor \
  roles/logging.logWriter; do
  gcloud projects add-iam-policy-binding ${PROJECT_ID} \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="${ROLE}" --quiet
done

# Allow Dataflow service agent to use the SA
PROJECT_NUMBER=$(gcloud projects describe ${PROJECT_ID} --format='value(projectNumber)')
gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:service-${PROJECT_NUMBER}@dataflow-service-producer-prod.iam.gserviceaccount.com" \
  --role="roles/dataflow.serviceAgent"

echo "Service account ${SA_EMAIL} configured ✓"
export SA_EMAIL="${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
```

---

## Phase 2 — Upload Project Files

### Step 2.1 — Upload files to Cloud Shell

In Cloud Shell, click **⋮ → Upload** and upload the entire project folder, OR clone your repo:

```bash
# If you have a git repo:
git clone https://github.com/YOUR_USERNAME/job-postings-pipeline.git
cd job-postings-pipeline

# Or manually create the structure:
mkdir -p ~/job-pipeline/{collector,pipeline,dashboard,docs}
# Then use Cloud Shell upload to place files in the right folders
```

### Step 2.2 — Verify file structure

```bash
find ~/job-pipeline -type f | sort
# Expected output:
# ~/job-pipeline/bootstrap_refresh.sh
# ~/job-pipeline/collector/collector.py
# ~/job-pipeline/collector/requirements.txt
# ~/job-pipeline/collector/Dockerfile
# ~/job-pipeline/collector/job-collector.yaml
# ~/job-pipeline/pipeline/beam_pipeline.py
# ~/job-pipeline/pipeline/requirements.txt
# ~/job-pipeline/pipeline/Dockerfile
# ~/job-pipeline/pipeline/cloudbuild.yaml
# ~/job-pipeline/pipeline/metadata.json
# ~/job-pipeline/pipeline/mlops_pipeline.py
# ~/job-pipeline/pipeline/requirements-mlops.txt
# ~/job-pipeline/dashboard/dashboard.html
# ~/job-pipeline/dashboard/bq_proxy.py
# ~/job-pipeline/dashboard/requirements.txt
# ~/job-pipeline/dashboard/Dockerfile
# ~/job-pipeline/docs/SETUP_GUIDE.md
```

---

## Phase 3 — Build & Deploy the Collector (Cloud Run Job)

### Step 3.1 — Create Artifact Registry repository

```bash
gcloud artifacts repositories create ${AR_REPO} \
  --repository-format=docker \
  --location=${REGION} \
  --description="Job pipeline container images"

gcloud auth configure-docker ${REGION}-docker.pkg.dev --quiet

echo "Artifact Registry repo created ✓"
```

### Step 3.2 — Build and push collector Docker image

```bash
cd ~/job-pipeline/collector

gcloud builds submit \
  --tag ${COLLECTOR_IMAGE} \
  --project ${PROJECT_ID}

echo "Collector image pushed: ${COLLECTOR_IMAGE} ✓"
```

*Build typically takes 2–3 minutes.*

### Step 3.3 — Create Cloud Run Job

```bash
gcloud run jobs create job-collector \
  --image ${COLLECTOR_IMAGE} \
  --region ${REGION} \
  --service-account ${SA_EMAIL} \
  --memory 2Gi \
  --cpu 2 \
  --task-timeout 3600 \
  --max-retries 1 \
  --set-env-vars "GCP_PROJECT_ID=${PROJECT_ID},GCS_BUCKET=${GCS_BUCKET}" \
  --set-secrets "USAJOBS_EMAIL=USAJOBS_EMAIL:latest" \
  --set-secrets "USAJOBS_KEY=USAJOBS_KEY:latest" \
  --set-secrets "JOOBLE_KEY=JOOBLE_KEY:latest" \
  --set-env-vars "PUBSUB_TOPIC=projects/${PROJECT_ID}/topics/job-postings-raw" \
  --project ${PROJECT_ID}

echo "Cloud Run Job 'job-collector' created ✓"
```

### Step 3.4 — Test run the collector manually

```bash
gcloud run jobs execute job-collector \
  --region ${REGION} \
  --project ${PROJECT_ID} \
  --wait

echo "Test run complete. Check logs with:"
echo "  gcloud run jobs executions list --job=job-collector --region=${REGION}"
```

### Step 3.5 — Schedule weekly execution (Monday 6 AM UTC)

```bash
gcloud scheduler jobs create http weekly-job-collection \
  --location ${REGION} \
  --schedule "0 6 * * 1" \
  --description "Weekly job postings collection — CS/MBA/AI pipeline" \
  --uri "https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/job-collector:run" \
  --http-method POST \
  --oauth-service-account-email ${SA_EMAIL} \
  --time-zone "UTC"

echo "Cloud Scheduler job created: every Monday 6 AM UTC ✓"
```

---

## Phase 4 — Deploy Dataflow Pipelines

Both batch and streaming modes are ONE Dataflow Flex Template image
(`pipeline/Dockerfile`), launched with different `--parameters`. There is no
separate image per mode — `Dockerfile.batch`/`Dockerfile.stream` from
earlier iterations of this project were literal duplicates of the same
image and have been removed.

### Step 4.1 — Build and stage the Flex Template

```bash
cd ~/job-pipeline/pipeline

gcloud builds submit --config cloudbuild.yaml \
  --project ${PROJECT_ID}

gcloud dataflow flex-template build \
  gs://${GCS_BUCKET}/templates/pipeline-template.json \
  --image "us-central1-docker.pkg.dev/${PROJECT_ID}/${AR_REPO}/pipeline-dataflow:latest" \
  --sdk-language PYTHON \
  --metadata-file metadata.json \
  --project ${PROJECT_ID}

echo "Flex Template staged ✓"
```

*This only stages the template spec in GCS — it doesn't start any job by itself.*

### Step 4.2 — Launch the Streaming Pipeline (continuous Pub/Sub ingestion)

This pipeline runs continuously, consuming from Pub/Sub as the collector publishes records.

```bash
gcloud dataflow flex-template run "job-postings-streaming-$(date +%Y%m%d-%H%M)" \
  --template-file-gcs-location "gs://${GCS_BUCKET}/templates/pipeline-template.json" \
  --region ${REGION} \
  --project ${PROJECT_ID} \
  --service-account-email ${SA_EMAIL} \
  --parameters pipeline_mode=streaming,\
input_subscription="projects/${PROJECT_ID}/subscriptions/job-postings-sub",\
bq_table="${PROJECT_ID}:job_postings.feature_postings",\
dlq_topic="projects/${PROJECT_ID}/topics/job-postings-dead-letter",\
project="${PROJECT_ID}",region="${REGION}",\
temp_location="gs://${GCS_BUCKET}/dataflow-temp/temp",\
staging_location="gs://${GCS_BUCKET}/dataflow-temp/staging"
```

*The streaming job starts within ~3 minutes. Monitor at:*
`https://console.cloud.google.com/dataflow/jobs?project=YOUR_PROJECT_ID`

`dlq_topic` is optional but recommended — malformed Pub/Sub messages are
routed there instead of silently dropped (see beam_pipeline.py's
`ParseJSON`/`DecodePubSub`). Create it once if you haven't:
`gcloud pubsub topics create job-postings-dead-letter` (Step 1.5 already
does this).

### Step 4.3 — Run the Batch Pipeline (process historical GCS files)

Use this after any collection run to process raw JSON already in GCS:

```bash
gcloud dataflow flex-template run "job-postings-batch-$(date +%Y%m%d-%H%M)" \
  --template-file-gcs-location "gs://${GCS_BUCKET}/templates/pipeline-template.json" \
  --region ${REGION} \
  --project ${PROJECT_ID} \
  --service-account-email ${SA_EMAIL} \
  --parameters pipeline_mode=batch,\
gcs_input_pattern="gs://${GCS_BUCKET}/raw/*/*.json",\
gcs_output_path="gs://${GCS_BUCKET}/processed/postings_$(date +%Y%m%d)",\
bq_table="${PROJECT_ID}:job_postings.feature_postings",\
project="${PROJECT_ID}",region="${REGION}",\
temp_location="gs://${GCS_BUCKET}/dataflow-temp/temp",\
staging_location="gs://${GCS_BUCKET}/dataflow-temp/staging"
```

### Step 4.4 — Monitor Dataflow jobs

```bash
# List all Dataflow jobs
gcloud dataflow jobs list --region ${REGION} --project ${PROJECT_ID}

# Stream logs for a specific job
gcloud dataflow jobs describe JOB_ID \
  --region ${REGION} --project ${PROJECT_ID} \
  --format="json" | python3 -m json.tool | head -60

# Check for dead-lettered messages
gcloud pubsub subscriptions pull job-postings-dead-letter-sub \
  --project ${PROJECT_ID} --limit 5 2>/dev/null || \
  echo "(create a pull subscription on job-postings-dead-letter first if you want to inspect it)"

# Useful BigQuery check after pipeline runs:
bq query --use_legacy_sql=false "
SELECT
  source,
  domain,
  era,
  relevance_tier,
  COUNT(*) AS postings,
  ROUND(AVG(relevance_score),2) AS avg_score
FROM \`${PROJECT_ID}.job_postings.feature_postings\`
GROUP BY 1,2,3,4
ORDER BY 1,2,3,4
"
```

**Or just run `bootstrap_refresh.sh`** from the repo root, which does Phase
4.2's streaming launch (checking for an already-active job first) plus a
full collector-run + verification cycle in one script — see the file
reference at the end of this guide.

---

## Phase 5 — Deploy the Vertex AI MLOps Pipeline

### Step 5.1 — Compile the KFP pipeline

```bash
cd ~/job-pipeline/pipeline

pip install -r requirements-mlops.txt --quiet

export GCP_PROJECT_ID=${PROJECT_ID}
export GCP_REGION=${REGION}
export GCS_BUCKET=${GCS_BUCKET}

python mlops_pipeline.py compile

ls -lh pipeline.json
echo "Pipeline compiled ✓"
```

### Step 5.2 — Upload pipeline spec to GCS

```bash
gcloud storage cp pipeline.json \
  gs://${GCS_BUCKET}/vertex-pipelines/pipeline_spec_$(date +%Y%m%d).json

echo "Pipeline spec uploaded ✓"
```

### Step 5.3 — Submit the Vertex AI pipeline run

```bash
python mlops_pipeline.py run

echo "Pipeline submitted. Monitor at:"
echo "  https://console.cloud.google.com/vertex-ai/pipelines?project=${PROJECT_ID}"
```

*The full pipeline run (BQ export → AutoML train → evaluate → deploy) takes 1–2 hours due to AutoML training.*

### Step 5.4 — Check pipeline run status from CLI

```bash
# List recent pipeline runs
gcloud ai pipeline-jobs list \
  --region=${REGION} \
  --project=${PROJECT_ID} \
  --filter="displayName:job-mlops" \
  --limit=5

# Check the pipeline_runs BQ table for deployed models
bq query --use_legacy_sql=false "
SELECT run_timestamp, eval_metric, deployed, endpoint
FROM \`${PROJECT_ID}.job_postings.pipeline_runs\`
ORDER BY logged_at DESC
LIMIT 10
"
```

### Step 5.5 — Schedule weekly MLOps pipeline (runs after collection)

```bash
gcloud scheduler jobs create http weekly-mlops-pipeline \
  --location ${REGION} \
  --schedule "0 12 * * 1" \
  --description "Weekly MLOps pipeline — train and deploy domain classifier" \
  --uri "https://${REGION}-aiplatform.googleapis.com/v1/projects/${PROJECT_ID}/locations/${REGION}/pipelineJobs" \
  --message-body "$(cat pipeline.json | python3 -c 'import sys,json; d=json.load(sys.stdin); print(json.dumps({"displayName":"scheduled-run","templateUri":"gs://${GCS_BUCKET}/vertex-pipelines/pipeline_spec_latest.json","pipelineRootUri":"gs://${GCS_BUCKET}/vertex-pipelines"}))')" \
  --http-method POST \
  --oauth-service-account-email ${SA_EMAIL} \
  --time-zone "UTC"
```

---

## Phase 6 — Dashboard Setup

### Step 6.1 — Open the dashboard locally

The dashboard is a self-contained HTML file with zero dependencies.

1. Download `dashboard/dashboard.html` from Cloud Shell:
   - Click **⋮ → Download file** in Cloud Shell and enter the path
2. Open it in any browser — no server needed
3. Click **▶ Load Sample Data** to test with 420 synthetic records
4. Click **📂 Import JSON** to load a real `MASTER_*.json` from the pipeline

### Step 6.2 — Export data from BigQuery for the dashboard

```bash
# Export feature_postings as a dashboard-compatible JSON file
bq query --use_legacy_sql=false --format=json "
SELECT
  GENERATE_UUID() AS id,
  hash_id,
  job_title,
  company,
  location,
  domain,
  source,
  era,
  publication_date,
  application_close,
  salary,
  job_type,
  grade,
  relevance_score,
  relevance_tier,
  required_skills,
  onet_skills,
  onet_basic,
  onet_social,
  onet_technical,
  onet_systems,
  onet_resource,
  onet_complex,
  tech_skill_count,
  onet_skill_count,
  description,
  job_url,
  search_keyword,
  matched_terms
FROM \`${PROJECT_ID}.job_postings.feature_postings\`
ORDER BY relevance_score DESC
LIMIT 5000
" > dashboard_export.json

# Wrap in pipeline-compatible format
python3 -c "
import json, sys
rows = json.load(open('dashboard_export.json'))
# Convert pipe-delimited strings back to arrays
for r in rows:
    for k in ['required_skills','onet_skills']:
        if isinstance(r.get(k), str) and r[k]:
            r[k] = r[k].split('|')
        elif not r.get(k):
            r[k] = []
output = {'metadata':{'title':'BQ Export','total_records':len(rows)}, 'postings':rows}
with open('MASTER_BQ_export.json','w') as f:
    json.dump(output, f, indent=2)
print(f'Exported {len(rows)} records → MASTER_BQ_export.json')
"

echo "Download MASTER_BQ_export.json and import it into the dashboard."
```

### Step 6.3 — Deploy dashboard to Cloud Storage (optional public hosting)

```bash
# Make bucket publicly readable for static hosting
gcloud storage buckets update gs://${GCS_BUCKET} --web-main-page-suffix=dashboard.html

gcloud storage objects update \
  gs://${GCS_BUCKET}/dashboard/dashboard.html \
  --add-acl-grant=entity=AllUsers,role=READER

gcloud storage cp ~/job-pipeline/dashboard/dashboard.html \
  gs://${GCS_BUCKET}/dashboard/

echo "Dashboard URL: https://storage.googleapis.com/${GCS_BUCKET}/dashboard/dashboard.html"
```

---

## Phase 7 — Results Tracking

### Real-time BQ queries to monitor the pipeline

**Total postings by run:**
```sql
SELECT
  run_timestamp,
  source,
  COUNT(*) AS postings,
  COUNTIF(era = 'current') AS current_postings,
  ROUND(AVG(relevance_score), 2) AS avg_score,
  COUNTIF(relevance_tier = 'HIGH') AS high_tier
FROM `PROJECT_ID.job_postings.feature_postings`
GROUP BY 1, 2
ORDER BY 1 DESC, 2;
```

**Domain and skill trends:**
```sql
SELECT
  domain,
  skill,
  COUNT(*) AS freq,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY domain), 1) AS domain_pct
FROM `PROJECT_ID.job_postings.feature_postings`,
  UNNEST(SPLIT(required_skills, '|')) AS skill
WHERE skill != ''
GROUP BY 1, 2
ORDER BY 1, 3 DESC;
```

**O\*NET family profile across domains:**
```sql
SELECT
  domain,
  ROUND(AVG(onet_basic), 2)     AS basic_skills,
  ROUND(AVG(onet_technical), 2) AS technical_skills,
  ROUND(AVG(onet_systems), 2)   AS systems_skills,
  ROUND(AVG(onet_social), 2)    AS social_skills,
  ROUND(AVG(onet_resource), 2)  AS resource_mgmt,
  ROUND(AVG(onet_complex), 2)   AS complex_problem_solving
FROM `PROJECT_ID.job_postings.feature_postings`
GROUP BY 1
ORDER BY 1;
```

**MLOps pipeline run history:**
```sql
SELECT
  run_timestamp,
  ROUND(eval_metric, 4) AS accuracy,
  deployed,
  endpoint,
  model_resource
FROM `PROJECT_ID.job_postings.pipeline_runs`
ORDER BY logged_at DESC;
```

### Useful gcloud monitoring commands

```bash
# Check Pub/Sub message backlog
gcloud pubsub subscriptions describe job-postings-sub \
  --project=${PROJECT_ID} \
  --format="json" | python3 -m json.tool | grep -A5 "messageCount"

# GCS raw file sizes
gcloud storage du gs://${GCS_BUCKET}/raw/ --summarize

# Dataflow job metrics
gcloud dataflow metrics list JOB_ID \
  --region=${REGION} --project=${PROJECT_ID}

# Cloud Run job execution history
gcloud run jobs executions list \
  --job=job-collector \
  --region=${REGION} \
  --project=${PROJECT_ID} \
  --limit=10

# Latest Vertex AI models
gcloud ai models list \
  --region=${REGION} \
  --project=${PROJECT_ID} \
  --filter="displayName:domain_classifier"
```

---

## Quick Cost Estimate

| Component | Typical Monthly Cost |
|---|---|
| Cloud Run Job (1 execution/week, ~40 min) | ~$0.50 |
| Pub/Sub (12,800 msgs/week) | ~$0.01 |
| Dataflow Streaming (2 n1-standard-2 workers) | ~$80–120 |
| Dataflow Batch (weekly, 30 min) | ~$2–5 |
| BigQuery storage (10 GB) | ~$0.20 |
| BigQuery query (10 TB/mo free tier) | $0 |
| GCS storage (5 GB) | ~$0.10 |
| Vertex AI AutoML (1 node-hour training) | ~$22 |
| Vertex AI Endpoint (1 n1-standard-2 replica) | ~$50–70 |
| Secret Manager | ~$0.06 |
| Cloud Scheduler (2 jobs) | $0 (free tier) |
| **Total (with streaming pipeline + endpoint)** | **~$155–220/mo** |

> 💡 **Cost tip:** Stop the streaming Dataflow job and Vertex AI endpoint between research cycles.
> Use the batch pipeline + manual endpoint calls instead to reduce costs to ~$30/mo.

---

## Troubleshooting

| Symptom | Check | Fix |
|---|---|---|
| Collector exits immediately | Cloud Run logs | Verify secrets are accessible: `gcloud secrets versions access latest --secret=USAJOBS_KEY` |
| No data in BigQuery | Pub/Sub backlog | Check subscription: `gcloud pubsub subscriptions pull job-postings-sub --limit=5` |
| Dataflow job fails | Job logs in Console | Check worker logs, usually missing dependency or IAM issue |
| Rows missing from BigQuery, no Dataflow errors | `job-postings-dead-letter` topic | Malformed Pub/Sub messages are routed there (see `ParseJSON`/`DecodePubSub` in beam_pipeline.py) rather than silently dropped — inspect it if row counts look low |
| Step 9's signed URL fails ("SA may lack iam.serviceAccounts.signBlob") | `gcloud iam service-accounts get-iam-policy ${SA_EMAIL}` | Step 2 now self-grants `roles/iam.serviceAccountTokenCreator` on job-pipeline-sa to itself (and to whoever runs the script) — re-run bootstrap_refresh.sh if this predates that fix. Falls back to `cloudshell download` either way, so this is never blocking. |
| Dataflow job shows `JOB_STATE_FAILED` | `gcloud logging read 'resource.type="dataflow_step" AND resource.labels.job_id="JOB_ID" AND severity>=ERROR' --project=${PROJECT_ID} --freshness=1d` | Step 7 now runs this automatically and prints the actual error text when it detects a non-running job — check that output first before digging in the Console |
| AutoML training fails | Vertex AI console | Dataset must have ≥ 1,000 rows and balanced classes |
| Pipeline accuracy < 0.82 | BQ pipeline_runs | Collect more data; adjust `ACCURACY_THRESHOLD` in mlops_pipeline.py |
| Dashboard shows nothing | Browser console | Click "Load Sample Data" to verify UI, then import JSON |

---

## File Reference

```
job-pipeline/
├── bootstrap_refresh.sh      One-shot: build all images, launch/confirm streaming
│                              Dataflow job, run the collector, wait for BQ
│                              rows, publish the live dashboard + API, export
│                              a full JSON snapshot as a fallback
├── cloudbuild.yaml            Builds all three images (collector, pipeline-dataflow,
│                              bq-proxy) in one submission — what bootstrap_refresh.sh calls
├── collector/
│   ├── collector.py          API collector → Pub/Sub + GCS
│   ├── requirements.txt      Python dependencies
│   ├── Dockerfile            Cloud Run container
│   └── job-collector.yaml    Cloud Run Job manifest (gcloud run jobs replace)
├── pipeline/
│   ├── beam_pipeline.py      Apache Beam: batch + streaming transforms
│   ├── requirements.txt      Dataflow worker/launcher dependencies
│   ├── Dockerfile            Flex Template image (batch AND streaming — one image)
│   ├── cloudbuild.yaml       Builds the image above
│   ├── metadata.json         Flex Template parameter definitions
│   ├── mlops_pipeline.py     Vertex AI KFP pipeline: train + deploy
│   └── requirements-mlops.txt   Dependencies for `python mlops_pipeline.py compile|run`
├── dashboard/
│   ├── dashboard.html        Interactive HTML dashboard (zero server)
│   ├── bq_proxy.py           Cloud Run service: serves BigQuery data as JSON
│   ├── requirements.txt      bq_proxy.py dependencies
│   └── Dockerfile            Cloud Run container for bq_proxy.py
└── docs/
    └── SETUP_GUIDE.md        This file
```

Generated artifacts (`pipeline.json`, `MASTER_BQ_export*.json`,
`dashboard_export.json`) are build/export output, not source — regenerated
by `mlops_pipeline.py compile` and Phase 6/`bootstrap_refresh.sh`'s export
steps respectively. Worth adding to `.gitignore` if this repo is under
version control, so they don't accumulate as stale snapshots alongside real
source changes.
