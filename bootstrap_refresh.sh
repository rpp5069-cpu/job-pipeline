#!/usr/bin/env bash
# =============================================================================
#  bootstrap_refresh.sh (FINAL STABLE VERSION)
#
#  Usage:
#    bash bootstrap_refresh.sh                # full setup
#    bash bootstrap_refresh.sh --skip-build   # skip Cloud Build (use existing images)
#    bash bootstrap_refresh.sh --data-only    # only ingest data (all infra already exists)
# =============================================================================


set -euo pipefail

# ─────────────────────────────────────────────────────────────────────────────
# ARGUMENT PARSING
# ─────────────────────────────────────────────────────────────────────────────
SKIP_BUILD=false
DATA_ONLY=false
for arg in "$@"; do
  case $arg in
    --skip-build)
      SKIP_BUILD=true
      shift
      ;;
    --data-only)
      DATA_ONLY=true
      SKIP_BUILD=true # --data-only implies --skip-build
      shift
      ;;
  esac
done

# ─────────────────────────────────────────────────────────────────────────────
# COLOUR HELPERS
# ─────────────────────────────────────────────────────────────────────────────
RED='\033[0;31m'; YEL='\033[1;33m'; GRN='\033[0;32m'
BLU='\033[0;34m'; CYN='\033[0;36m'; NC='\033[0m'
info()    { echo -e "${BLU}[INFO]${NC}  $*"; }
success() { echo -e "${GRN}[OK]${NC}    $*"; }
warn()    { echo -e "${YEL}[WARN]${NC}  $*"; }
error()   { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }
step()    { echo -e "\n${CYN}━━━ $* ━━━${NC}"; }

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────
PROJECT_ID="job-postings-full-gcp-project"
# FIX: previously REGION/RUN_REGION defaulted to us-east1 while Artifact
# Registry and the actual deployed job-collector live in us-central1 — Step
# 5's "search other regions" fallback was masking this drift run after run
# rather than it ever getting fixed. One canonical region now; the fallback
# search stays in Step 5 as a safety net, not as the primary mechanism.
REGION="us-central1"
RUN_REGION="$REGION"
RUN_TIMESTAMP=$(date -u +"%Y%m%d-%H%M%S")

# Derived naming conventions
GCS_BUCKET="${PROJECT_ID}-job-pipeline"
SA_EMAIL="job-pipeline-sa@${PROJECT_ID}.iam.gserviceaccount.com"
PUBSUB_SUB="projects/${PROJECT_ID}/subscriptions/job-postings-sub"
BQ_DATASET="job_postings"
BQ_TABLE_FEAT="${PROJECT_ID}:${BQ_DATASET}.extracted_features"
DATAFLOW_JOB_NAME="job-pipeline-refresh-${RUN_TIMESTAMP}"
DASHBOARD_EXPORT="MASTER_BQ_export_${RUN_TIMESTAMP}.json"

# Dataflow parameters
DF_TEMP="gs://${GCS_BUCKET}/dataflow-temp"
DF_STAGE="gs://${GCS_BUCKET}/dataflow-temp/staging"
DF_OUTPUT="gs://${GCS_BUCKET}/processed/master"  # only used for batch-mode gcs_output_path; not passed by Step 4's streaming launch

# Artifact Registry config — defaults match the error log (us-central1/job-pipeline-repo)
# FIX: auto-detected in Step 2 so mismatches are caught automatically
AR_LOCATION="us-central1"
AR_REPO="job-pipeline-repo"
COLLECTOR_IMAGE="${AR_LOCATION}-docker.pkg.dev/${PROJECT_ID}/${AR_REPO}/collector:latest"
DATAFLOW_IMAGE="${AR_LOCATION}-docker.pkg.dev/${PROJECT_ID}/${AR_REPO}/pipeline-dataflow:latest"
BQ_PROXY_IMAGE="${AR_LOCATION}-docker.pkg.dev/${PROJECT_ID}/${AR_REPO}/bq-proxy:latest"

DLQ_TOPIC="projects/${PROJECT_ID}/topics/job-postings-dead-letter"
DASHBOARD_BUCKET="${PROJECT_ID}-dashboard"

# FIX: initialize to avoid unbound-variable errors in the final summary block
DATAFLOW_JOB_ID=""

echo ""
echo -e "${CYN}╔══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${CYN}║   Job Postings Pipeline — Bootstrap Refresh (STABLE)         ║${NC}"
echo -e "${CYN}║   Project: ${PROJECT_ID}                     ║${NC}"
echo -e "${CYN}╚══════════════════════════════════════════════════════════════╝${NC}"

# ─────────────────────────────────────────────────────────────────────────────
# STEP 0 — Project Validation
# ─────────────────────────────────────────────────────────────────────────────
step "STEP 0 — Project Validation"
gcloud config set project "$PROJECT_ID" --quiet
success "Active Project: $PROJECT_ID"

# ─────────────────────────────────────────────────────────────────────────────
# STEP 1 — Infrastructure Health Check
# ─────────────────────────────────────────────────────────────────────────────
step "STEP 1 — Infrastructure Health Check"

if [[ "$DATA_ONLY" == "true" ]]; then
    warn "Skipping infrastructure checks (--data-only flag set)."
    # We continue to the next step
else

info "Checking GCS bucket: gs://${GCS_BUCKET}..."
if gcloud storage ls "gs://${GCS_BUCKET}/" > /dev/null 2>&1; then
    success "Bucket verified."
else
    error "GCS bucket gs://${GCS_BUCKET} not found. Please create it first."
fi

info "Checking BigQuery table: ${BQ_TABLE_FEAT}..."
if bq show --quiet "${BQ_TABLE_FEAT}" > /dev/null 2>&1; then
    success "BigQuery table verified."
else
    error "BigQuery table ${BQ_TABLE_FEAT} not found."
fi

info "Checking Pub/Sub subscription..."
if gcloud pubsub subscriptions describe job-postings-sub --project="$PROJECT_ID" > /dev/null 2>&1; then
    success "Pub/Sub subscription verified."
else
    error "Pub/Sub subscription 'job-postings-sub' not found."
fi

fi # End of DATA_ONLY check for Step 1

# ─────────────────────────────────────────────────────────────────────────────
# STEP 2 — IAM & Artifact Registry Permissions
# FIX: The Dataflow launcher VM runs as the Compute Engine DEFAULT SERVICE ACCOUNT,
#      not as job-pipeline-sa. That default SA was never granted
#      roles/artifactregistry.reader, causing the Docker image pull to fail with:
#        "Permission 'artifactregistry.repositories.downloadArtifacts' denied"
#      This step grants the right roles BEFORE Step 4 launches Dataflow.
# ─────────────────────────────────────────────────────────────────────────────
step "STEP 2 — IAM & Artifact Registry Permissions"

if [[ "$DATA_ONLY" == "true" ]]; then
    warn "Skipping IAM and Artifact Registry permission checks (--data-only flag set)."
    # We continue to the next step
else

PROJECT_NUM=$(gcloud projects describe "$PROJECT_ID" --format="value(projectNumber)")
COMPUTE_SA="${PROJECT_NUM}-compute@developer.gserviceaccount.com"

info "Project number  : ${PROJECT_NUM}"
info "Pipeline SA     : ${SA_EMAIL}"
info "Compute Engine SA: ${COMPUTE_SA}"

# Grant roles/artifactregistry.reader at project level to both SAs.
# The Dataflow launcher VM uses the Compute Engine default SA for image pulls.
for member in "serviceAccount:${SA_EMAIL}" "serviceAccount:${COMPUTE_SA}"; do
    info "Granting artifactregistry.reader to ${member}..."
    if gcloud projects add-iam-policy-binding "$PROJECT_ID"             --member="$member"             --role="roles/artifactregistry.reader"             --condition=None             --quiet 2>/dev/null; then
        success "Granted roles/artifactregistry.reader → ${member}"
    else
        warn "Binding may already exist for ${member} (non-fatal)"
    fi
done

# Also bind directly on the AR repository object (belt-and-suspenders).
# If the repo is in a different location/name this is caught by the auto-detect below.
info "Binding AR reader directly on repository: ${AR_REPO} (${AR_LOCATION})..."
gcloud artifacts repositories add-iam-policy-binding "$AR_REPO"     --location="$AR_LOCATION"     --project="$PROJECT_ID"     --member="serviceAccount:${COMPUTE_SA}"     --role="roles/artifactregistry.reader"     --quiet 2>/dev/null     && success "Repository-level reader binding applied"     || warn "Repository-level binding skipped (repo may not match default name — see auto-detect below)"

gcloud artifacts repositories add-iam-policy-binding "$AR_REPO"     --location="$AR_LOCATION"     --project="$PROJECT_ID"     --member="serviceAccount:${SA_EMAIL}"     --role="roles/artifactregistry.reader"     --quiet 2>/dev/null || true

# Auto-detect the actual AR repo name and location — overrides defaults above
DETECTED_REPO=$(gcloud artifacts repositories list     --project="$PROJECT_ID"     --filter="name~job-pipeline"     --format="value(name)" 2>/dev/null | head -1 | awk -F/ '{print $NF}' || echo "")
DETECTED_LOC=$(gcloud artifacts repositories list     --project="$PROJECT_ID"     --filter="name~job-pipeline"     --format="value(location)" 2>/dev/null | head -1 || echo "")

if [[ -n "$DETECTED_REPO" && -n "$DETECTED_LOC" ]]; then
    AR_REPO="$DETECTED_REPO"
    AR_LOCATION="$DETECTED_LOC"
    COLLECTOR_IMAGE="${AR_LOCATION}-docker.pkg.dev/${PROJECT_ID}/${AR_REPO}/collector:latest"
    success "Auto-detected AR repo: ${AR_LOCATION}/${AR_REPO}"
    info "Collector image : ${COLLECTOR_IMAGE}"

    # Apply repo-level binding again using the confirmed repo name
    for member in "serviceAccount:${SA_EMAIL}" "serviceAccount:${COMPUTE_SA}"; do
        gcloud artifacts repositories add-iam-policy-binding "$AR_REPO"             --location="$AR_LOCATION"             --project="$PROJECT_ID"             --member="$member"             --role="roles/artifactregistry.reader"             --quiet 2>/dev/null || true
    done
    success "Repository-level bindings applied to confirmed repo: ${AR_REPO}"
else
    warn "Could not auto-detect AR repo — using defaults (${AR_LOCATION}/${AR_REPO})"
fi

# Configure Docker for the detected AR host (needed for any local docker pull/push)
AR_HOST="${AR_LOCATION}-docker.pkg.dev"
gcloud auth configure-docker "$AR_HOST" --quiet 2>/dev/null     && success "Docker configured for ${AR_HOST}"     || warn "docker configure-docker failed (non-fatal in Cloud Shell)"

# Verify the collector image exists and is accessible
info "Verifying collector image accessibility..."
if gcloud artifacts docker images describe "${COLLECTOR_IMAGE}" --project="$PROJECT_ID" &>/dev/null; then
    success "Collector image accessible: ${COLLECTOR_IMAGE}"
else
    warn "Cannot describe collector image right now."
    warn "IAM changes can take up to 60s to propagate — proceeding anyway."
    warn "If Step 4 still fails with a permission error, wait 60s and re-run."
fi

# FIX: Step 9's `gcloud storage sign-url --impersonate-service-account`
# needs iam.serviceAccounts.signBlob, granted via
# roles/iam.serviceAccountTokenCreator — same "even self-impersonation needs
# an explicit grant" GCP rule as service-account actAs. Without this,
# every signed-URL attempt in Step 9 fails silently (falls back to the
# 'download via Cloud Shell' path, which still works, just less convenient).
info "Ensuring ${SA_EMAIL} can sign URLs as itself (roles/iam.serviceAccountTokenCreator)..."
gcloud iam service-accounts add-iam-policy-binding "$SA_EMAIL" \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="roles/iam.serviceAccountTokenCreator" \
    --project="$PROJECT_ID" --quiet \
    && success "Token Creator self-grant confirmed." \
    || warn "Could not verify Token Creator grant — Step 9's signed URL may fail (non-fatal, falls back to Cloud Shell download)."

# Also needed: whoever is RUNNING this script (their own user account, not
# job-pipeline-sa) must be able to impersonate job-pipeline-sa in the first
# place. Grant it defensively if missing — safe to attempt even if already
# present.
CURRENT_USER="$(gcloud config get-value account 2>/dev/null || echo "")"
if [[ -n "$CURRENT_USER" && "$CURRENT_USER" != "(unset)" ]]; then
    gcloud iam service-accounts add-iam-policy-binding "$SA_EMAIL" \
        --member="user:${CURRENT_USER}" \
        --role="roles/iam.serviceAccountTokenCreator" \
        --project="$PROJECT_ID" --quiet 2>/dev/null \
        && success "Token Creator grant confirmed for ${CURRENT_USER}." \
        || warn "Could not grant ${CURRENT_USER} Token Creator on ${SA_EMAIL} — may need an admin to run this once."
fi

fi # End of DATA_ONLY check for Step 2

# ─────────────────────────────────────────────────────────────────────────────
# STEP 3 — Drain Stale Messages
# ─────────────────────────────────────────────────────────────────────────────
step "STEP 3 — Drain Stale Pub/Sub Messages"
gcloud pubsub subscriptions seek job-postings-sub --project="$PROJECT_ID" --time="$(date -u +%Y-%m-%dT%H:%M:%SZ)" --quiet
success "Subscription reset to current time."

# ─────────────────────────────────────────────────────────────────────────────
# STEP 3.5 — Build & Push All Images, Stage the Flex Template
# ─────────────────────────────────────────────────────────────────────────────
# FIX: this used to rebuild the Dataflow image from an ad hoc inline
# Dockerfile with its own hardcoded, independently pinned dependency list
# (drifting from pipeline/requirements.txt), tagged it "beam-pipeline" (every
# other file in this repo expects "pipeline-dataflow"), and passed
# beam_pipeline.py itself as --metadata-file to `flex-template build` (which
# silently failed and fell back to a template with zero declared parameters).
# Replaced with one `gcloud builds submit` using the repo's own
# collector/Dockerfile, pipeline/Dockerfile, and dashboard/Dockerfile as the
# single source of truth — builds all three service images in one
# submission, per requirements.txt each file actually ships with.
step "STEP 3.5 — Build & Push All Images"

if [[ "${SKIP_BUILD}" == "true" ]]; then
  warn "Skipping image build and template staging (--skip-build or --data-only flag set)."
else
    REPO_ROOT=""
    for candidate_dir in "$(pwd)" "$(pwd)/job-pipeline" "${HOME}/job-pipeline" "${HOME}"; do
        if [[ -f "${candidate_dir}/cloudbuild.yaml" && -d "${candidate_dir}/collector" && -d "${candidate_dir}/pipeline" ]]; then
            REPO_ROOT="$candidate_dir"
            info "Found repo root: ${REPO_ROOT}"
            break
        fi
    done

    if [[ -z "$REPO_ROOT" ]]; then
        warn "Could not locate the repo root (needs cloudbuild.yaml + collector/ + pipeline/ together)."
        warn "Skipping image build — Step 4 will use whatever images already exist in Artifact Registry."
        warn "To fix: cd into the job-pipeline repo root before running this script,"
        warn "or clone/upload it to Cloud Shell first."
    else
        info "Building collector, pipeline-dataflow, and bq-proxy images (this takes 3-6 minutes)..."
        if gcloud builds submit "${REPO_ROOT}" \
            --config="${REPO_ROOT}/cloudbuild.yaml" \
            --project="$PROJECT_ID" \
            --substitutions="_AR_LOCATION=${AR_LOCATION},_AR_REPO=${AR_REPO}" \
            --timeout=1800s \
            --quiet; then
            success "All three images built and pushed."
        else
            error "Image build failed. Check Cloud Build logs: https://console.cloud.google.com/cloud-build/builds?project=${PROJECT_ID}"
        fi

        info "Staging Flex Template → gs://${GCS_BUCKET}/templates/pipeline-template.json"
        if gcloud dataflow flex-template build \
            "gs://${GCS_BUCKET}/templates/pipeline-template.json" \
            --project="$PROJECT_ID" \
            --image="$DATAFLOW_IMAGE" \
            --sdk-language=PYTHON \
            --metadata-file="${REPO_ROOT}/pipeline/metadata.json" \
            --quiet; then
            success "Flex Template staged with metadata.json (parameters visible in Console)."
        else
            warn "flex-template build with metadata.json failed — staging without it."
            gcloud dataflow flex-template build \
                "gs://${GCS_BUCKET}/templates/pipeline-template.json" \
                --project="$PROJECT_ID" \
                --image="$DATAFLOW_IMAGE" \
                --sdk-language=PYTHON \
                --quiet \
                && success "Flex Template staged (no metadata — parameters still work via --parameters, just not shown in Console)." \
                || error "Could not stage the Flex Template at all. Check GCS permissions on ${GCS_BUCKET}."
        fi
    fi
fi # End of SKIP_BUILD check


# ─────────────────────────────────────────────────────────────────────────────
# STEP 4 — Dataflow Streaming Pipeline
# ─────────────────────────────────────────────────────────────────────────────
step "STEP 4 — Dataflow Streaming Pipeline"

# Check for ANY active Dataflow job in the project/region
ACTIVE_DF_JOB=$(gcloud dataflow jobs list --project="$PROJECT_ID" --region="$REGION" --status=active --format="value(JOB_ID)" | head -1 || echo "")

if [[ -n "$ACTIVE_DF_JOB" ]]; then
    success "Found existing active Dataflow job: $ACTIVE_DF_JOB"
    DATAFLOW_JOB_ID="$ACTIVE_DF_JOB"
else
    info "Starting new Dataflow Flex Template..."
    DATAFLOW_JOB_ID=$(gcloud dataflow flex-template run "$DATAFLOW_JOB_NAME" \
        --template-file-gcs-location="gs://${GCS_BUCKET}/templates/pipeline-template.json" \
        --region="$REGION" \
        --worker-machine-type="e2-standard-2" \
        --parameters pipeline_mode=streaming,input_subscription="${PUBSUB_SUB}",bq_table="${BQ_TABLE_FEAT}",dlq_topic="${DLQ_TOPIC}",project="${PROJECT_ID}",region="${REGION}",temp_location="${DF_TEMP}",staging_location="${DF_STAGE}" \
        --service-account-email="$SA_EMAIL" \
        --project="$PROJECT_ID" \
        --format="value(job.id)" 2>/dev/null)

    if [[ -z "$DATAFLOW_JOB_ID" ]]; then
        error "Dataflow job submission failed. Check gcloud logs for errors like ZONE_RESOURCE_POOL_EXHAUSTED and try a different --region or --worker-zone."
    fi

    success "Dataflow job started: $DATAFLOW_JOB_ID"

    # FIX: a blind `sleep 60` isn't enough — Dataflow streaming jobs commonly
    # take 3-5+ minutes to actually reach RUNNING (VM provisioning, SDK
    # harness startup, graph construction). Poll the real job state instead
    # of guessing a fixed wait, so Step 5 doesn't fire the collector before
    # Dataflow is actually ready to consume.
    info "Waiting for job to reach RUNNING state (up to 6 minutes)..."
    DF_READY=false
    for i in $(seq 1 36); do
        DF_STATE=$(gcloud dataflow jobs describe "$DATAFLOW_JOB_ID" --region="$REGION" \
            --project="$PROJECT_ID" --format="value(currentState)" 2>/dev/null || echo "")
        if [[ "$DF_STATE" == "JOB_STATE_RUNNING" ]]; then
            success "Dataflow job is RUNNING (took ~$((i * 10))s)."
            DF_READY=true
            break
        fi
        sleep 10
    done
    if [[ "$DF_READY" != "true" ]]; then
        warn "Dataflow job did not reach RUNNING within 6 minutes (last state: ${DF_STATE:-unknown})."
        warn "Proceeding anyway — check the job in the Console if Step 7 finds no new rows:"
        warn "  https://console.cloud.google.com/dataflow/jobs/${REGION}/${DATAFLOW_JOB_ID}?project=${PROJECT_ID}"
    fi
fi

# ─────────────────────────────────────────────────────────────────────────────
# STEP 5 — Trigger Cloud Run Ingestion Job
# ─────────────────────────────────────────────────────────────────────────────
step "STEP 5 — Trigger Cloud Run Ingestion Job"

# FIX: The job was NOT_FOUND because it either was never deployed to this region
#      or it lives in a different region. This block:
#        1. Checks $RUN_REGION first (configured region)
#        2. Searches common Cloud Run regions if not found there
#        3. Deploys job-collector from the AR image if it doesn't exist anywhere

EFFECTIVE_RUN_REGION=""

# 1 — Check the configured region
if gcloud run jobs describe job-collector --region="$RUN_REGION"         --project="$PROJECT_ID" &>/dev/null; then
    EFFECTIVE_RUN_REGION="$RUN_REGION"
    info "job-collector found in configured region: ${RUN_REGION}"
else
    warn "job-collector not found in ${RUN_REGION}. Searching other regions..."

    # 2 — Scan common Cloud Run regions
    for search_region in us-central1 us-west1 us-west2 us-east4 us-east5                          europe-west1 europe-west2 asia-east1 asia-northeast1; do
        if [[ "$search_region" == "$RUN_REGION" ]]; then continue; fi
        if gcloud run jobs describe job-collector --region="$search_region"                 --project="$PROJECT_ID" &>/dev/null; then
            EFFECTIVE_RUN_REGION="$search_region"
            warn "Found job-collector in different region: ${search_region}"
            warn "Using ${search_region} for this run."
            break
        fi
    done
fi

# 3 — Not found anywhere — deploy it now from the AR collector image
if [[ -z "$EFFECTIVE_RUN_REGION" ]]; then
    warn "job-collector not found in any region. Deploying from: ${COLLECTOR_IMAGE}"

    if ! gcloud artifacts docker images describe "${COLLECTOR_IMAGE}"             --project="$PROJECT_ID" &>/dev/null; then
        error "Cannot deploy: image ${COLLECTOR_IMAGE} not found in Artifact Registry. Build and push the collector image, then re-run."
    fi

    gcloud run jobs create job-collector         --project="$PROJECT_ID"         --region="$RUN_REGION"         --image="$COLLECTOR_IMAGE"         --service-account="$SA_EMAIL"         --memory=2Gi         --cpu=2         --task-timeout=3600s         --max-retries=2         --parallelism=1         --set-env-vars="GCP_PROJECT_ID=${PROJECT_ID},GCS_BUCKET=${GCS_BUCKET},GCS_RAW_PREFIX=raw,PUBSUB_TOPIC=projects/${PROJECT_ID}/topics/job-postings-raw"         --set-secrets="USAJOBS_EMAIL=USAJOBS_EMAIL:latest,USAJOBS_KEY=USAJOBS_KEY:latest,JOOBLE_KEY=JOOBLE_KEY:latest"         --quiet         && success "job-collector deployed to ${RUN_REGION}"         || error "Failed to deploy job-collector. Check IAM permissions and image availability."

    EFFECTIVE_RUN_REGION="$RUN_REGION"
fi

info "Executing Cloud Run Job: job-collector in ${EFFECTIVE_RUN_REGION}..."
if gcloud run jobs execute job-collector         --region="$EFFECTIVE_RUN_REGION"         --project="$PROJECT_ID"         --wait; then
    success "job-collector finished execution."
else
    error "job-collector failed. Logs: https://console.cloud.google.com/run/jobs/details/${EFFECTIVE_RUN_REGION}/job-collector/executions?project=${PROJECT_ID}"
fi

success "Pipeline triggered successfully. Data is now flowing."
# ─────────────────────────────────────────────────────────────────────────────
# STEP 6 — Verify messages reached Pub/Sub
# ─────────────────────────────────────────────────────────────────────────────
step "STEP 6 — Verify Pub/Sub Ingestion"

# FIX: `--auto-ack` permanently removes whatever it pulls from the
# subscription. job-postings-sub is the SAME subscription Dataflow's
# streaming job reads from (Step 4) — every message this "sample" grabs is
# one Dataflow will never see, competing with (and stealing from) the real
# consumer it's supposed to be checking on. Pull WITHOUT acking, capture the
# ack IDs, then immediately release them (ack-deadline=0) so they're
# redelivered to Dataflow right away instead of being lost or sitting
# invisible for the full 60s ack deadline.
#
# FIX 2: only request `ackId`, never `data`. The previous version requested
# `value(ackId,data)` and split on the tab gcloud uses between columns —
# but `data` is the raw job-posting JSON payload, and if any sampled
# message's text contains a literal tab or embedded newline, `cut -f1`
# silently returns garbage instead of a real ack ID, and
# modify-ack-deadline fails with "invalid ack ID" (exactly what showed up
# in the log). We don't need the message content for this check at all —
# only whether messages exist and their ack IDs.
ACK_IDS_RAW=$(gcloud pubsub subscriptions pull job-postings-sub \
    --project="$PROJECT_ID" \
    --limit=3 \
    --format="value(ackId)" 2>/dev/null || echo "")

if [[ -n "$ACK_IDS_RAW" ]]; then
    MSG_COUNT=$(echo "$ACK_IDS_RAW" | grep -c .)
    success "Pub/Sub is receiving messages. Sampled ${MSG_COUNT} messages (released back, not consumed)."

    ACK_ID_CSV=$(echo "$ACK_IDS_RAW" | paste -sd, -)
    gcloud pubsub subscriptions modify-ack-deadline job-postings-sub \
        --project="$PROJECT_ID" \
        --ack-ids="$ACK_ID_CSV" \
        --ack-deadline=0 --quiet 2>/dev/null \
        && info "Released sampled messages back to the subscription for Dataflow to consume." \
        || warn "Could not release sampled messages early — they'll still redeliver after the normal 60s ack deadline."
else
    warn "No messages sampled from Pub/Sub — subscription backlog is currently empty."
    warn "This is expected if Dataflow already drained everything the collector published."
    warn "It's a problem only if Step 7 also shows zero new BigQuery rows."
fi

# ─────────────────────────────────────────────────────────────────────────────
# STEP 7 — Wait for Dataflow to flush to BigQuery
# ─────────────────────────────────────────────────────────────────────────────
step "STEP 7 — Waiting for Dataflow → BigQuery Flush"
info "Checking Pub/Sub subscription backlog..."
BACKLOG_SIZE=$(gcloud pubsub subscriptions describe "${PUBSUB_SUB}" \
    --format="value(num_undelivered_messages)" 2>/dev/null || echo "0")

if [[ "$BACKLOG_SIZE" -eq "0" ]]; then
    success "Pub/Sub backlog is 0. All messages appear to be processed."
    success "Skipping BigQuery flush polling."
else
    warn "Pub/Sub backlog is ${BACKLOG_SIZE}. Waiting for Dataflow to process these messages."
    info "Dataflow batches records per BigQuery write. Waiting up to 15 minutes."
    info "Polling BigQuery every 60 seconds for new rows..."

    POLL_ATTEMPTS=15
    NEW_ROWS=0

    info "Capturing baseline BigQuery row count..."
    # Get a baseline of rows collected *today* before this run's data arrives.
    PREV_COUNT=$(bq query --use_legacy_sql=false --quiet --format=csv \
        "SELECT COUNT(*) FROM \`${PROJECT_ID}.${BQ_DATASET}.extracted_features\` WHERE DATE(collected_at) = CURRENT_DATE()" \
        2>/dev/null | tail -1 || echo "0")
    PREV_COUNT="${PREV_COUNT//[^0-9]/}"
    PREV_COUNT="${PREV_COUNT:-0}"
    info "Baseline row count for today: ${PREV_COUNT}"

    for i in $(seq 1 $POLL_ATTEMPTS); do
        sleep 60
        # Check the current count of rows collected *today*.
        CURRENT_COUNT=$(bq query --use_legacy_sql=false --quiet --format=csv \
            "SELECT COUNT(*) FROM \`${PROJECT_ID}.${BQ_DATASET}.extracted_features\` WHERE DATE(collected_at) = CURRENT_DATE()" \
            2>/dev/null | tail -1 || echo "0")
        CURRENT_COUNT="${CURRENT_COUNT//[^0-9]/}"
        CURRENT_COUNT="${CURRENT_COUNT:-0}"
        NEW_ROWS=$(( CURRENT_COUNT - PREV_COUNT ))
        info "Poll ${i}/${POLL_ATTEMPTS} — Rows collected today: ${CURRENT_COUNT} (+${NEW_ROWS} new)"
        if [[ "$NEW_ROWS" -gt "0" ]]; then
            success "Data is flowing into BigQuery. ${NEW_ROWS} new rows so far."
            break
        fi
    done

    if [[ "$NEW_ROWS" -eq "0" ]]; then
        warn "No new rows detected in BigQuery after ${POLL_ATTEMPTS} minutes."

        LIVE_DF_STATE=$(gcloud dataflow jobs describe "$DATAFLOW_JOB_ID" --region="$REGION" \
            --project="$PROJECT_ID" --format="value(currentState)" 2>/dev/null || echo "unknown")
        warn "Dataflow job current state: ${LIVE_DF_STATE}"

        TODAY_COUNT=$(bq query --use_legacy_sql=false --quiet --format=csv \
            "SELECT COUNT(*) FROM \`${PROJECT_ID}.${BQ_DATASET}.extracted_features\` WHERE DATE(collected_at) = CURRENT_DATE()" \
            2>/dev/null | tail -1 || echo "0")
        TODAY_COUNT="${TODAY_COUNT//[^0-9]/}"
        warn "Rows collected today so far: ${TODAY_COUNT:-0}"

        if [[ "$LIVE_DF_STATE" != "JOB_STATE_RUNNING" ]]; then
            warn "→ Root cause is likely here: Dataflow is not RUNNING (state: ${LIVE_DF_STATE})."
            warn "   Check: https://console.cloud.google.com/dataflow/jobs/${REGION}/${DATAFLOW_JOB_ID}?project=${PROJECT_ID}"

            info "Fetching the actual error from Cloud Logging..."
            DF_ERROR_LOG=$(gcloud logging read \
                "resource.type=\"dataflow_step\" AND resource.labels.job_id=\"${DATAFLOW_JOB_ID}\" AND severity>=ERROR" \
                --project="$PROJECT_ID" \
                --freshness=1d \
                --limit=5 \
                --format="value(timestamp,textPayload)" 2>/dev/null || echo "")
            if [[ -n "$DF_ERROR_LOG" ]]; then
                warn "Most recent error(s) from Dataflow job logs:"
                echo "$DF_ERROR_LOG" | while IFS= read -r line; do warn "    $line"; done
            else
                warn "No ERROR-severity log entries found yet — the failure may still be propagating to Cloud Logging."
            fi
        else
            warn "Dataflow IS running but nothing new landed. Possible causes:"
            warn "  1. Collector wrote to GCS but not Pub/Sub — check collector logs."
            warn "  2. All new messages are invalid and were sent to the dead-letter topic."
            warn "  3. Rows landed but with a `collected_at` timestamp outside today."
        fi
    fi
fi

# ─────────────────────────────────────────────────────────────────────────────
# STEP 8 — Export fresh data for dashboard
# ─────────────────────────────────────────────────────────────────────────────
step "STEP 8 — Export Dashboard JSON"

# FIX: `bq query` silently defaults --max_rows to 100 when the flag is
# omitted entirely — simply deleting a hardcoded limit would have made this
# WORSE than the previous 5000 cap, not unlimited. Determine the actual
# current row count and pass it explicitly (with headroom for rows written
# between this count and the export query) so every row is returned.
EXPORT_ROW_COUNT=$(bq query --use_legacy_sql=false --quiet --format=csv \
    "SELECT COUNT(*) FROM \`${PROJECT_ID}.${BQ_DATASET}.extracted_features\`" \
    2>/dev/null | tail -1 || echo "0")
EXPORT_ROW_COUNT="${EXPORT_ROW_COUNT//[^0-9]/}"
EXPORT_ROW_COUNT="${EXPORT_ROW_COUNT:-0}"
EXPORT_MAX_ROWS=$(( EXPORT_ROW_COUNT + 1000 ))  # headroom for concurrent writes

info "Querying BigQuery for ALL records (${EXPORT_ROW_COUNT} currently, requesting up to ${EXPORT_MAX_ROWS})..."

bq query \
    --use_legacy_sql=false \
    --format=json \
    --max_rows="${EXPORT_MAX_ROWS}" \
    --project_id="$PROJECT_ID" \
    "
SELECT
    GENERATE_UUID()        AS id,
    hash_id,
    job_title              AS title,
    company,
    location,
    domain,
    source,
    era,
    publication_date       AS posted_date,
    application_close,
    salary,
    job_type,
    grade,
    CAST(relevance_score AS INT64)   AS relevance_score,
    relevance_tier,
    required_skills,
    onet_skills,
    CAST(onet_basic AS INT64)        AS onet_basic,
    CAST(onet_social AS INT64)       AS onet_social,
    CAST(onet_technical AS INT64)    AS onet_technical,
    CAST(onet_systems AS INT64)      AS onet_systems,
    CAST(onet_resource AS INT64)     AS onet_resource,
    CAST(onet_complex AS INT64)      AS onet_complex,
    CAST(tech_skill_count AS INT64)  AS tech_skill_count,
    CAST(onet_skill_count AS INT64)  AS onet_skill_count,
    description,
    job_url,
    search_keyword,
    matched_terms
FROM \`${PROJECT_ID}.${BQ_DATASET}.extracted_features\`
ORDER BY relevance_score DESC, collected_at DESC
" > /tmp/bq_raw_export.json 2>/dev/null || {
    warn "BigQuery export failed — possibly no data yet. Trying with fewer rows..."
    bq query --use_legacy_sql=false --format=json --max_rows=100 --project_id="$PROJECT_ID" \
        "SELECT * FROM \`${PROJECT_ID}.${BQ_DATASET}.extracted_features\` LIMIT 100" \
        > /tmp/bq_raw_export.json 2>/dev/null || echo "[]" > /tmp/bq_raw_export.json;
} 

# Wrap in the format dashboard.html expects and convert pipe strings → arrays
python3 - << PYEOF
import json, sys, os

with open("/tmp/bq_raw_export.json") as f:
    try:
        rows = json.load(f)
    except json.JSONDecodeError:
        rows = []

if not isinstance(rows, list):
    rows = []

# Convert pipe-delimited skill strings back to arrays
for r in rows:
    for field in ["required_skills", "onet_skills"]:
        raw = r.get(field, "")
        if isinstance(raw, str) and raw:
            r[field] = [s.strip() for s in raw.split("|") if s.strip()]
        elif not raw:
            r[field] = []

    # Ensure numeric fields are ints not strings (BQ JSON sometimes returns strings)
    for int_field in [
        "relevance_score", "onet_basic", "onet_social", "onet_technical",
        "onet_systems", "onet_resource", "onet_complex",
        "tech_skill_count", "onet_skill_count"
    ]:
        try:
            r[int_field] = int(float(r.get(int_field, 0) or 0))
        except (ValueError, TypeError):
            r[int_field] = 0

output = {
    "metadata": {
        "title":           "CS / MBA / Data Analytics / AI — Job Postings (Refresh)",
        "researcher":      "Rickey Patel — Penn State Great Valley D.Eng.",
        "generated_at":    "${RUN_TIMESTAMP}",
        "total_records":   len(rows),
        "current_records": sum(1 for r in rows if r.get("era") == "current"),
        "historical_records": sum(1 for r in rows if r.get("era") == "historical"),
        "sources":         list(set(r.get("source","") for r in rows if r.get("source"))),
        "domains":         list(set(r.get("domain","") for r in rows if r.get("domain"))),
        "schema_version":  "2.0",
        "export_type":     "BigQuery refresh ${RUN_TIMESTAMP}",
    },
    "postings": rows,
}

out_path = "${DASHBOARD_EXPORT}"
with open(out_path, "w", encoding="utf-8") as f:
    json.dump(output, f, indent=2, ensure_ascii=False)

print(f"Exported {len(rows)} records → {out_path}")
PYEOF

# ─────────────────────────────────────────────────────────────────────────────
# STEP 9 — Upload dashboard export to GCS
# ─────────────────────────────────────────────────────────────────────────────
step "STEP 9 — Upload to GCS"

if [[ -f "$DASHBOARD_EXPORT" ]]; then
    EXPORT_SIZE=$(wc -c < "$DASHBOARD_EXPORT")
    info "Export size: ${EXPORT_SIZE} bytes"

    # FIX: gsutil is deprecated in favor of `gcloud storage` (this was
    # triggering Google's own migration warning on every run).
    gcloud storage cp "$DASHBOARD_EXPORT" \
        "gs://${GCS_BUCKET}/processed/master/${DASHBOARD_EXPORT}" --quiet
    success "Dashboard export uploaded: gs://${GCS_BUCKET}/processed/master/${DASHBOARD_EXPORT}"

    # FIX: gsutil signurl requires a P12/JSON key file, which is not available
    # when running under OAuth in Cloud Shell. Use gcloud storage sign-url with
    # --impersonate-service-account instead — works with standard OAuth credentials.
    info "Generating signed download URL (valid 7 days)..."
    # FIX: gcloud storage sign-url outputs column 'url', not 'signed_url'.
    # Try value(url) first; fall back to parsing the raw output line.
    SIGNED_URL=$(gcloud storage sign-url \
        "gs://${GCS_BUCKET}/processed/master/${DASHBOARD_EXPORT}" \
        --duration=7d \
        --impersonate-service-account="${SA_EMAIL}" \
        --format="value(url)" \
        2>/dev/null || echo "")
    # Secondary fallback: grab the https:// URL from raw output
    if [[ -z "$SIGNED_URL" ]]; then
        SIGNED_URL=$(gcloud storage sign-url \
            "gs://${GCS_BUCKET}/processed/master/${DASHBOARD_EXPORT}" \
            --duration=7d \
            --impersonate-service-account="${SA_EMAIL}" \
            2>/dev/null | grep -o "https://[^ ]*" | head -1 || echo "")
    fi

    if [[ -n "$SIGNED_URL" ]]; then
        info "Signed download URL (valid 7 days):"
        echo "$SIGNED_URL"
    else
        warn "Could not generate signed URL (SA may lack iam.serviceAccounts.signBlob)."
        warn "Download directly from Cloud Shell:  cloudshell download ${DASHBOARD_EXPORT}"
        info "Or browse GCS: https://console.cloud.google.com/storage/browser/${GCS_BUCKET}/processed/master?project=${PROJECT_ID}"
    fi
else
    warn "Export file not found: $DASHBOARD_EXPORT"
fi

# ─────────────────────────────────────────────────────────────────────────────
# STEP 9.5 — Publish Live Dashboard + BigQuery API (no manual JSON upload needed)
# ─────────────────────────────────────────────────────────────────────────────
step "STEP 9.5 — Publish Live Dashboard + BigQuery API"

if [[ "$DATA_ONLY" == "true" ]]; then
    warn "Skipping dashboard and API deployment (--data-only flag set)."
    # Set placeholder URLs to prevent errors in the summary
    BQ_PROXY_URL=""
    DASHBOARD_URL=""
else
# Set target table and row limit variables
TARGET_TABLE="extracted_features"
ROW_LIMIT="200000"

BQ_PROXY_URL=""
if gcloud run services describe bq-proxy --region="$REGION" --project="$PROJECT_ID" &>/dev/null; then
    info "bq-proxy already deployed — updating to the freshly built image..."
    # FIX 1: Point BQ_TABLE to the live 'extracted_features' table where Dataflow writes
    # FIX 2: Set MAX_ROWS=100000 to prevent defaulting to 20,000 rows
    # FIX 3: Add --memory=2Gi to allocate enough RAM to handle large JSON payloads without crashing (OOM)
    gcloud run deploy bq-proxy \
        --image="$BQ_PROXY_IMAGE" \
        --region="$REGION" --project="$PROJECT_ID" \
        --service-account="$SA_EMAIL" \
        --set-env-vars="GCP_PROJECT_ID=${PROJECT_ID},BQ_DATASET=${BQ_DATASET},BQ_TABLE=${TARGET_TABLE},MAX_ROWS=${ROW_LIMIT}" \
        --memory=2Gi \
        --allow-unauthenticated --quiet \
        && success "bq-proxy updated." \
        || warn "bq-proxy update failed — dashboard will fall back to the GCS JSON export."
else
    info "Deploying bq-proxy for the first time..."
    # FIX 1: Point BQ_TABLE to the live 'extracted_features' table where Dataflow writes
    # FIX 2: Set MAX_ROWS=100000 to prevent defaulting to 20,000 rows
    # FIX 3: Add --memory=2Gi to allocate enough RAM to handle large JSON payloads without crashing (OOM)
    gcloud run deploy bq-proxy \
        --image="$BQ_PROXY_IMAGE" \
        --region="$REGION" --project="$PROJECT_ID" \
        --service-account="$SA_EMAIL" \
        --set-env-vars="GCP_PROJECT_ID=${PROJECT_ID},BQ_DATASET=${BQ_DATASET},BQ_TABLE=${TARGET_TABLE},MAX_ROWS=${ROW_LIMIT}" \
        --memory=2Gi \
        --allow-unauthenticated --quiet \
        && success "bq-proxy deployed." \
        || warn "bq-proxy deploy failed — dashboard will fall back to the GCS JSON export."
fi
BQ_PROXY_URL=$(gcloud run services describe bq-proxy --region="$REGION" --project="$PROJECT_ID" \
    --format="value(status.url)" 2>/dev/null || echo "")

# Make sure dashboard-proxy also gets updated with the lifted row limit if it's deployed
if gcloud run services describe dashboard-proxy --region="$REGION" --project="$PROJECT_ID" &>/dev/null; then
    info "Updating dashboard-proxy to support up to ${ROW_LIMIT} rows..."
    gcloud run services update dashboard-proxy \
        --update-env-vars="MAX_ROWS=${ROW_LIMIT}" \
        --region="$REGION" --project="$PROJECT_ID" --quiet \
        && success "dashboard-proxy row limit updated."
fi

DASHBOARD_URL=""
if [[ -f "dashboard/dashboard.html" || -f "${REPO_ROOT:-}/dashboard/dashboard.html" ]]; then
    DASHBOARD_SRC="dashboard/dashboard.html"
    [[ -f "${REPO_ROOT:-}/dashboard/dashboard.html" ]] && DASHBOARD_SRC="${REPO_ROOT}/dashboard/dashboard.html"

    if ! gcloud storage buckets describe "gs://${DASHBOARD_BUCKET}" --project="$PROJECT_ID" &>/dev/null; then
        info "Creating public static-hosting bucket gs://${DASHBOARD_BUCKET}..."
        gcloud storage buckets create "gs://${DASHBOARD_BUCKET}" \
            --project="$PROJECT_ID" --location="$REGION" --uniform-bucket-level-access --quiet
        gcloud storage buckets update "gs://${DASHBOARD_BUCKET}" --web-main-page-suffix=dashboard.html --quiet
        gcloud storage buckets add-iam-policy-binding "gs://${DASHBOARD_BUCKET}" \
            --member=allUsers --role=roles/storage.objectViewer --quiet
    fi

    gcloud storage cp "$DASHBOARD_SRC" "gs://${DASHBOARD_BUCKET}/dashboard.html" --quiet \
        && success "Dashboard published." \
        || warn "Dashboard upload failed — use the local copy of dashboard.html instead."
    DASHBOARD_URL="https://storage.googleapis.com/${DASHBOARD_BUCKET}/dashboard.html"
else
    warn "dashboard/dashboard.html not found — skipping dashboard hosting. Open it locally instead."
fi

fi # End of DATA_ONLY check for Step 9.5

# ─────────────────────────────────────────────────────────────────────────────
# STEP 10 — BQ row counts by domain and source
# ─────────────────────────────────────────────────────────────────────────────
step "STEP 10 — Final BigQuery Summary"

echo ""
echo -e "${CYN}Postings collected today (${RUN_TIMESTAMP:0:8}):${NC}"
bq query --use_legacy_sql=false --quiet --format=pretty \
    " \
SELECT
    source,
    domain,
    COUNT(*)                                    AS total_postings,
    COUNTIF(era = 'current')                   AS current_postings,
    ROUND(AVG(relevance_score), 2)             AS avg_score,
    COUNTIF(relevance_tier = 'HIGH')           AS high_tier,
    ROUND(AVG(tech_skill_count), 1)            AS avg_skills
FROM \`${PROJECT_ID}.${BQ_DATASET}.extracted_features\`
WHERE DATE(collected_at) = CURRENT_DATE() \
GROUP BY 1, 2
ORDER BY 1, 2
" 2>/dev/null || warn "No rows with today's date yet — collection may still be running."

echo ""
echo -e "${CYN}Total historical postings (all time):${NC}"
bq query --use_legacy_sql=false --quiet --format=pretty \
    " \
SELECT
    source,
    COUNT(*)                            AS total,
    COUNTIF(era = 'current')           AS current_count,
    COUNTIF(era = 'historical')        AS historical_count,
    MIN(DATE(collected_at))            AS first_collected,
    MAX(DATE(collected_at))            AS last_collected
FROM \`${PROJECT_ID}.${BQ_DATASET}.extracted_features\`
GROUP BY 1
ORDER BY 1
" 2>/dev/null || warn "Unable to query historical totals."

# ─────────────────────────────────────────────────────────────────────────────
# SUMMARY
# ─────────────────────────────────────────────────────────────────────────────
echo ""
echo -e "${GRN}╔══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${GRN}║   Bootstrap Refresh Complete                                ║${NC}"
echo -e "${GRN}╚══════════════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "${GRN}What was done:${NC}"
echo "  ✅  Infrastructure verified (GCS, BigQuery, Pub/Sub, Secrets)"
if [[ "$SKIP_BUILD" != "true" ]]; then
echo "  ✅  All three images built and pushed (collector, pipeline-dataflow, bq-proxy)"
fi
echo "  ✅  Stale Pub/Sub messages drained"
echo "  ✅  Dataflow streaming job: ${DATAFLOW_JOB_ID:-none — check Step 4 output above}"
# FIX: this used to hardcode "ingest-usajobs & ingest-jooble" regardless of
# what actually ran — Step 5 executes exactly one Cloud Run Job (job-collector),
# so say that, not two jobs that were never invoked.
echo "  ✅  Cloud Run ingestion triggered: job-collector (region: ${EFFECTIVE_RUN_REGION:-$RUN_REGION})"
echo "  ✅  Dashboard export written: ${DASHBOARD_EXPORT} (all rows, no 5,000-row cap)"
echo ""

if [[ -n "$BQ_PROXY_URL" || -n "$DASHBOARD_URL" ]]; then
    echo -e "${YEL}Live dashboard (preferred — no download/upload needed):${NC}"
    echo ""
    if [[ -n "$DASHBOARD_URL" ]]; then
        echo "  Dashboard : ${DASHBOARD_URL}"
    fi
    if [[ -n "$BQ_PROXY_URL" ]]; then
        echo "  BigQuery API : ${BQ_PROXY_URL}/postings"
        echo "    → Open the dashboard above, click 'Connect to BigQuery', and paste the API URL."
        echo "      It fetches current data directly — no JSON download/import needed, ever again."
    fi
    echo ""
fi

echo -e "${YEL}Manual fallback (point-in-time snapshot, if the live dashboard is unavailable):${NC}"
echo ""
echo "  1. Download the export file from Cloud Shell:"
echo "       cloudshell download ${DASHBOARD_EXPORT}"
echo ""
echo "  2. Open dashboard.html in your browser (local copy, or the hosted one above)."
echo ""
echo "  3. Click  📂 Import JSON  and select  ${DASHBOARD_EXPORT}"
echo ""
echo -e "${YEL}Useful monitoring links:${NC}"
# FIX: only print the Dataflow link when we actually have a job ID;
# an empty DATAFLOW_JOB_ID produces a broken URL that confuses the console
if [[ -n "$DATAFLOW_JOB_ID" ]]; then
    echo "  Dataflow job  : https://console.cloud.google.com/dataflow/jobs/${REGION}/${DATAFLOW_JOB_ID}?project=${PROJECT_ID}"
else
    echo "  Dataflow jobs : https://console.cloud.google.com/dataflow/jobs?project=${PROJECT_ID}&region=${REGION}"
fi
echo "  Cloud Run logs: https://console.cloud.google.com/run?project=${PROJECT_ID}"
echo "  BigQuery table: https://console.cloud.google.com/bigquery?project=${PROJECT_ID}&ws=!1m5!1m4!4m3!1s${PROJECT_ID}!2s${BQ_DATASET}!3sextracted_features"
echo "  GCS export    : https://console.cloud.google.com/storage/browser/${GCS_BUCKET}/processed/master?project=${PROJECT_ID}"
echo "  Dead-letter   : https://console.cloud.google.com/cloudpubsub/topic/detail/job-postings-dead-letter?project=${PROJECT_ID}"
echo ""
echo -e "${BLU}Run timestamp: ${RUN_TIMESTAMP}${NC}"