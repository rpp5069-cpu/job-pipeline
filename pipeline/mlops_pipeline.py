"""
mlops_pipeline.py
=================
Vertex AI Pipelines (KFP v2) definition for the CS/MBA/AI Job Postings project.

Pipeline DAG:
  export_bq_to_gcs → register_dataset → automl_train → evaluate_model
                                                              │
                                                     accuracy >= threshold?
                                                              │
                                                      deploy_to_endpoint

Run locally:
  python mlops_pipeline.py compile     # writes pipeline.json
  python mlops_pipeline.py run         # submits to Vertex AI
"""

import argparse
import json
import os
from kfp import dsl
from google_cloud_pipeline_components.types import artifact_types
from google_cloud_pipeline_components.types.artifact_types import VertexModel

from datetime import datetime

PROJECT_ID   = "job-postings-full-gcp-project"
REGION       = os.environ.get("GCP_REGION", "us-central1")
GCS_BUCKET   = os.environ.get("GCS_BUCKET", f"{PROJECT_ID}-job-pipeline")
# Service account the pipeline actually runs as. Without this, Vertex AI
# defaults to the project's default Compute Engine service account
# ({PROJECT_NUMBER}-compute@developer.gserviceaccount.com), which is NOT
# the job-pipeline-sa created in SETUP_GUIDE.md Step 1.8 and does not have
# the IAM roles granted there — this is the exact cause of the
# "Service account ...-compute@developer.gserviceaccount.com does not have
# [storage.objects.create] IAM permission(s)" error.
PIPELINE_SA  = os.environ.get("PIPELINE_SA", f"job-pipeline-sa@{PROJECT_ID}.iam.gserviceaccount.com")
PIPELINE_ROOT   = f"gs://{GCS_BUCKET}/vertex-pipelines"
BQ_DATASET      = "job_postings"
BQ_TABLE     = "raw_postings"
VERTEX_DATASET  = "job_postings_dataset"
VERTEX_MODEL    = "domain_classifier"
VERTEX_ENDPOINT = "job_domain_endpoint"
PIPELINE_FILE   = "pipeline.json"
RUN_TS          = datetime.now().strftime("%Y%m%d_%H%M")
ACCURACY_THRESHOLD = 0.82

# Column name used for the stratified predefined split (see export_bq_to_gcs).
# Vertex AI's default random split is NOT stratified by class — with an
# imbalanced target (e.g. far fewer real-world "AI" postings than "CS" ones),
# a random 10% test slice can land zero rows of the rarest class, which is
# exactly the "Test split contains no sample" failure. Assigning TRAIN/
# VALIDATE/TEST per-domain at export time guarantees every class has at
# least one row in both the validation and test sets.
SPLIT_COLUMN_NAME = "ml_split"


# ── KFP components ────────────────────────────────────────────────────────────
def _build_pipeline():
    """
    Import KFP inside function to avoid hard dependency at module load time.
    Returns a compiled pipeline function.
    """
    from kfp import dsl
    from kfp.v2 import compiler
    from google_cloud_pipeline_components.v1.automl.training_job import (
        AutoMLTabularTrainingJobRunOp,
    )

    @dsl.component(
        base_image="python:3.11-slim",
        packages_to_install=["google-cloud-bigquery", "db-dtypes", "pyarrow", "gcsfs"],
    )
    def export_bq_to_gcs(
        project_id: str,
        bq_dataset: str,
        bq_table: str,
        gcs_bucket: str,
        run_ts: str,
        split_column_name: str,
        target_column: str = "domain",
    ) -> str:
        """
        Export feature_postings to GCS as CSV, with an added stratified
        predefined-split column so every value of `target_column` is
        guaranteed at least one row in the validation and test splits,
        regardless of class size. Vertex AI's default random split does not
        stratify — see SPLIT_COLUMN_NAME comment at module level for why
        that matters here specifically.

        Approach: rank rows within each target_column group, then assign the
        first ~10% (min 1) to TEST, the next ~10% (min 1) to VALIDATE, and
        the rest to TRAIN. This is computed once, materialized into a
        staging table, then extracted — extract_table() can't add computed
        columns directly, so a query-to-table step is required first.
        """
        from google.cloud import bigquery

        client = bigquery.Client(project=project_id)
        source_table = f"{project_id}.{bq_dataset}.{bq_table}"
        staging_table = f"{project_id}.{bq_dataset}.export_staging_{run_ts}"
        gcs_uri = f"gs://{gcs_bucket}/vertex-datasets/postings_{run_ts}.csv"

        split_query = f"""
        CREATE OR REPLACE TABLE `{staging_table}` AS
        WITH ranked AS (
          SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY {target_column} ORDER BY hash_id) AS rn,
            COUNT(*) OVER (PARTITION BY {target_column}) AS group_count
          FROM `{source_table}`
          WHERE {target_column} IS NOT NULL
        )
        SELECT
          * EXCEPT(rn, group_count),
          CASE
            WHEN rn <= GREATEST(1, CAST(ROUND(group_count * 0.1) AS INT64)) THEN 'TEST'
            WHEN rn <= 2 * GREATEST(1, CAST(ROUND(group_count * 0.1) AS INT64)) THEN 'VALIDATE'
            ELSE 'TRAIN'
          END AS {split_column_name}
        FROM ranked
        """
        client.query(split_query).result()

        # Sanity check: fail loudly here rather than downstream in AutoML if
        # any class ended up with zero rows in a split (e.g. a class with
        # only 1 total row can't populate both TEST and VALIDATE).
        check_query = f"""
        SELECT {target_column}, {split_column_name}, COUNT(*) AS n
        FROM `{staging_table}`
        GROUP BY 1, 2
        ORDER BY 1, 2
        """
        rows = list(client.query(check_query).result())
        by_class = {}
        for r in rows:
            by_class.setdefault(r[target_column], {})[r[split_column_name]] = r["n"]
        print("Split counts by class:")
        for cls, counts in by_class.items():
            print(f"  {cls}: {counts}")
            missing = [s for s in ("TRAIN", "VALIDATE", "TEST") if counts.get(s, 0) == 0]
            if missing:
                raise ValueError(
                    f"Class '{cls}' has zero rows in split(s) {missing} even after "
                    f"stratified assignment — this class likely has too few total "
                    f"rows in {source_table} (needs at least 3 to populate all three "
                    f"splits). Collect more data for this class before training."
                )

        destination = bigquery.TableReference.from_string(staging_table)
        job_config = bigquery.ExtractJobConfig()
        job_config.destination_format = bigquery.DestinationFormat.CSV
        job_config.print_header = True

        job = client.extract_table(destination, gcs_uri, job_config=job_config)
        job.result()
        print(f"Exported {staging_table} (with stratified '{split_column_name}' column) -> {gcs_uri}")
        return gcs_uri

    @dsl.component(
        base_image="python:3.11-slim",
        packages_to_install=[
            "google-cloud-aiplatform==1.57.0",
            "google-cloud-pipeline-components==2.13.1",
        ],
    )
    def register_vertex_dataset(
        gcs_csv_uri: str,
        project_id: str,
        region: str,
        display_name: str,
        dataset: dsl.Output[artifact_types.VertexDataset],
    ):
        """Register a Vertex AI TabularDataset."""
        from google.cloud import aiplatform

        aiplatform.init(project=project_id, location=region)
        ds = aiplatform.TabularDataset.create(
            display_name=display_name, gcs_source=gcs_csv_uri
        )
        dataset.metadata['resourceName'] = ds.resource_name
        dataset.uri = f'https://{region}-aiplatform.googleapis.com/v1/{ds.resource_name}'
        print(f"Dataset registered: {ds.resource_name}")

    @dsl.component(
        base_image="python:3.11-slim",

        packages_to_install=[
            "google-cloud-aiplatform==1.57.0",
            "google-cloud-pipeline-components==2.13.1",
        ],
    )
    def evaluate_model(
        model: dsl.Input[VertexModel],
        project_id: str,
        region: str,
    ) -> float:
        """Retrieve best model evaluation metric (auPrc or accuracy)."""
        from google.cloud import aiplatform

        aiplatform.init(project=project_id, location=region)
        model_resource_name = model.metadata.get('resourceName')
        model = aiplatform.Model(model_resource_name)
        for ev in model.list_model_evaluations():
            m = ev.metrics
            val = m.get("auPrc", m.get("auRoc", m.get("accuracy", 0.0)))
            print(f"Model eval metric: {val}")
            return float(val)
        return 0.0

    @dsl.component(
        base_image="python:3.11-slim",
        packages_to_install=[
            "google-cloud-aiplatform==1.57.0",
            "google-cloud-pipeline-components==2.13.1",
        ],
    )
    def deploy_model_endpoint(
        model: dsl.Input[VertexModel],
        project_id: str,
        region: str,
        endpoint_display_name: str,
    ) -> str:
        """Deploy model to a Vertex AI Endpoint. Returns endpoint resource name."""
        from google.cloud import aiplatform

        aiplatform.init(project=project_id, location=region)
        model_resource_name = model.metadata.get('resourceName')
        model = aiplatform.Model(model_resource_name)

        # Check if endpoint already exists
        endpoints = aiplatform.Endpoint.list(
            filter=f'display_name="{endpoint_display_name}"',
            project=project_id,
            location=region,
        )
        if endpoints:
            endpoint = endpoints[0]
            print(f"Reusing endpoint: {endpoint.resource_name}")
        else:
            endpoint = aiplatform.Endpoint.create(
                display_name=endpoint_display_name,
                project=project_id,
                location=region,
            )
            print(f"Endpoint created: {endpoint.resource_name}")

        endpoint.deploy(
            model=model,
            deployed_model_display_name=f"{endpoint_display_name}_v{datetime.now().strftime('%Y%m%d')}",
            machine_type="n1-standard-2",
            min_replica_count=1,
            max_replica_count=3,
            traffic_percentage=100,
            sync=True,
        )
        print(f"Model deployed to: {endpoint.resource_name}")
        return endpoint.resource_name

    @dsl.component(
        base_image="python:3.11-slim",
        packages_to_install=[
            "google-cloud-bigquery==3.22.0",
            "google-cloud-pipeline-components==2.13.1",
            "db-dtypes==1.2.0",
        ],
    )
    def log_pipeline_run(
        project_id: str,
        bq_dataset: str,
        run_ts: str,
        model: dsl.Input[VertexModel],
        eval_metric: float,
        endpoint_resource_name: str,
        deployed: bool,
    ) -> None:
        """Write pipeline run metadata to BigQuery for tracking."""
        from datetime import datetime as _dt
        from google.cloud import bigquery

        model_resource_name = model.metadata.get('resourceName')
        client = bigquery.Client(project=project_id)
        table  = f"{project_id}.{bq_dataset}.pipeline_runs"

        rows = [{
            "run_timestamp":   run_ts,
            "logged_at":       _dt.utcnow().isoformat() + "Z",
            "model_resource":  model_resource_name,
            "eval_metric":     eval_metric,
            "endpoint":        endpoint_resource_name,
            "deployed":        deployed,
        }]

        errors = client.insert_rows_json(table, rows)
        if errors:
            print(f"BQ log errors: {errors}")
        else:
            print(f"Pipeline run logged to {table}")

    @dsl.pipeline(
        name="job-postings-mlops-pipeline",
        description="CS/MBA/AI job postings: BQ export → AutoML → evaluate → deploy",
        pipeline_root=PIPELINE_ROOT,
    )
    def job_pipeline(
        project_id: str = PROJECT_ID,
        region: str = REGION,
        gcs_bucket: str = GCS_BUCKET,
        bq_dataset: str = BQ_DATASET,
        bq_table: str = BQ_TABLE,
        vertex_dataset_name: str = VERTEX_DATASET,
        model_display_name: str = VERTEX_MODEL,
        endpoint_display_name: str = VERTEX_ENDPOINT,
        accuracy_threshold: float = ACCURACY_THRESHOLD,
        run_ts: str = RUN_TS,
        split_column_name: str = SPLIT_COLUMN_NAME,
    ):
        # Step 1: Export BQ → GCS CSV (with stratified predefined split)
        export_op = export_bq_to_gcs(
            project_id=project_id,
            bq_dataset=bq_dataset,
            bq_table=bq_table,
            gcs_bucket=gcs_bucket,
            run_ts=run_ts,
            split_column_name=split_column_name,
        )

        # Step 2: Register Vertex AI Dataset
        dataset_op = register_vertex_dataset(
            gcs_csv_uri=export_op.output,
            project_id=project_id,
            region=region,
            display_name=vertex_dataset_name,
        ).after(export_op)

        # Step 3: AutoML Tabular Training
        # predefined_split_column_name uses the stratified TRAIN/VALIDATE/TEST
        # column added in export_bq_to_gcs, instead of Vertex AI's default
        # non-stratified random split — this is the actual fix for the
        # "Test split contains no sample" failure on the imbalanced domain
        # classes (CS postings vastly outnumber AI postings in raw volume).
        train_op = AutoMLTabularTrainingJobRunOp(
            project=project_id,
            location=region,
            display_name=model_display_name,
            optimization_prediction_type="classification",
            dataset=dataset_op.outputs['dataset'],
            target_column="domain",
            predefined_split_column_name=split_column_name,
            budget_milli_node_hours=1000,
            column_transformations=[
                {"categorical": {"column_name": "source"}},
                {"categorical": {"column_name": "era"}},
                {"categorical": {"column_name": "relevance_tier"}},
                {"numeric":     {"column_name": "relevance_score"}},
                {"numeric":     {"column_name": "tech_skill_count"}},
                {"numeric":     {"column_name": "onet_skill_count"}},
                {"numeric":     {"column_name": "onet_basic"}},
                {"numeric":     {"column_name": "onet_technical"}},
                {"numeric":     {"column_name": "onet_systems"}},
                {"numeric":     {"column_name": "onet_resource"}},
            ],
        ).after(dataset_op)

        # Step 4: Evaluate
        eval_op = evaluate_model(
            model=train_op.outputs["model"],
            project_id=project_id,
            region=region,
        )

        # Step 5: Deploy if accuracy meets threshold
        with dsl.Condition(
            eval_op.output >= accuracy_threshold,
            name="accuracy-gate",
        ):
            deploy_op = deploy_model_endpoint(
                model=train_op.outputs["model"],
                project_id=project_id,
                region=region,
                endpoint_display_name=endpoint_display_name,
            )

            log_pipeline_run(
                project_id=project_id,
                bq_dataset=bq_dataset,
                run_ts=run_ts,
                model=train_op.outputs["model"],
                eval_metric=eval_op.output,
                endpoint_resource_name=deploy_op.output,
                deployed=True,
            )

    return job_pipeline, compiler


def compile_pipeline(output_file=PIPELINE_FILE):
    """Compile the KFP pipeline to JSON."""
    job_pipeline, compiler = _build_pipeline()
    compiler.Compiler().compile(pipeline_func=job_pipeline, package_path=output_file)
    print(f"Pipeline compiled → {output_file}")
    return output_file


def run_pipeline(pipeline_file=PIPELINE_FILE):
    """Submit the compiled pipeline to Vertex AI."""
    from google.cloud import aiplatform
    from google.cloud.aiplatform import pipeline_jobs

    aiplatform.init(project=PROJECT_ID, location=REGION, staging_bucket=f"gs://{GCS_BUCKET}")

    job = pipeline_jobs.PipelineJob(
        project=PROJECT_ID,
        display_name=f"job-mlops-{RUN_TS}",
        template_path=pipeline_file,
        pipeline_root=PIPELINE_ROOT,
        parameter_values={
            "project_id":             PROJECT_ID,
            "region":                 REGION,
            "gcs_bucket":             GCS_BUCKET,
            "bq_dataset":             BQ_DATASET,
            "bq_table":               BQ_TABLE,
            "vertex_dataset_name":    VERTEX_DATASET,
            "model_display_name":     VERTEX_MODEL,
            "endpoint_display_name":  VERTEX_ENDPOINT,
            "accuracy_threshold":     ACCURACY_THRESHOLD,
            "run_ts":                 RUN_TS,
            "split_column_name":      SPLIT_COLUMN_NAME,
        },
        enable_caching=True,
    )
    job.submit(service_account=PIPELINE_SA)
    print(f"Pipeline submitted: {job.resource_name}")
    print(f"Running as service account: {PIPELINE_SA}")
    print(f"Monitor: https://console.cloud.google.com/vertex-ai/pipelines?project={PROJECT_ID}")
    return job


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["compile", "run", "compile-run"])
    args = parser.parse_args()

    if args.action in ("compile", "compile-run"):
        compile_pipeline()
    if args.action in ("run", "compile-run"):
        run_pipeline()
