"""
beam_pipeline.py
================
Apache Beam pipeline for the CS/MBA/AI Job Postings GCP project.
Runs on Cloud Dataflow in BOTH batch and streaming modes.

Batch  : triggered after each API collection run
         GCS raw JSON  →  enrich  →  BigQuery + GCS Parquet
Streaming : continuous Pub/Sub ingestion
         Pub/Sub topic →  enrich  →  BigQuery (streaming insert)

Fixes:
  • Added the module-level run() function, wrapping what used to live
    directly under `if __name__ == "__main__":`. This repo also had an
    orphaned pipeline/launcher/launch.py doing `from beam_pipeline import
    run` — but Dockerfile's FLEX_TEMPLATE_PYTHON_PY_FILE pointed straight at
    this file, not at launcher/launch.py, so that import was never actually
    exercised by the real build. launcher/ has been removed as dead code
    rather than half-wired in; run() stays because it makes this file
    cleanly importable/testable on its own merits.
  • ParseJSON / DecodePubSub no longer silently drop malformed messages;
    both route failures to a tagged 'dead_letter' output, forwarded to a
    Pub/Sub DLQ topic when --dlq_topic is provided.

Launch parameters (used consistently by beam_pipeline.py's argparse,
metadata.json, and bootstrap_refresh.sh — see docs/SETUP_GUIDE.md):
  --pipeline_mode        batch | streaming
  --project, --region, --staging_location, --temp_location   (always required)
  --bq_table              BigQuery table, "dataset.table" (always required)
  --gcs_input_pattern, --gcs_output_path                      (batch mode)
  --input_subscription, --dlq_topic                           (streaming mode)
"""

import argparse
import hashlib
import json
import logging
import os
import re
from datetime import datetime

import apache_beam as beam
from apache_beam.io import WriteToBigQuery
from apache_beam.io import fileio as beam_fileio
from apache_beam.io.gcp.bigquery import BigQueryDisposition
from apache_beam.options.pipeline_options import (
    GoogleCloudOptions,
    PipelineOptions,
    SetupOptions,
    StandardOptions,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────
CURRENT_EPOCH = datetime(2026, 1, 1)
MIN_SCORE = 2
ONET_VERSION = "O*NET 24.2"

SKILL_TAXONOMY = [
    "Python","R","SQL","Java","Scala","Go","JavaScript","TypeScript",
    "C++","Rust","MATLAB","Julia","Bash","Shell","SAS","SPSS",
    "PyTorch","TensorFlow","Keras","Scikit-learn","XGBoost","LightGBM",
    "Hugging Face","JAX","ONNX","LangChain","LlamaIndex",
    "Machine Learning","Deep Learning","NLP","Computer Vision",
    "Reinforcement Learning","MLOps","Feature Engineering","Model Deployment",
    "A/B Testing","Large Language Models","Generative AI","RAG",
    "Prompt Engineering","Fine-tuning","RLHF","Federated Learning",
    "Spark","Kafka","Airflow","dbt","Databricks","Snowflake",
    "BigQuery","Redshift","ETL","ELT","Data Warehouse","Data Lake",
    "Delta Lake","Flink","Dataflow","Pub/Sub","Dataproc",
    "Tableau","Power BI","Looker","Qlik","Excel","Statistics",
    "Pandas","NumPy","Matplotlib","Seaborn","Plotly","Statsmodels",
    "AWS","Azure","GCP","Docker","Kubernetes","Terraform",
    "Helm","ArgoCD","Cloud Run","Vertex AI","BigQuery ML",
    "REST API","GraphQL","CI/CD","Git","Linux","Microservices",
    "Cybersecurity","gRPC","OpenAPI",
    "Financial Modeling","Six Sigma","Agile","Scrum",
    "Project Management","Strategic Planning","Business Intelligence",
    "OKRs","Stakeholder Management","Change Management",
]

DOMAIN_TITLE_T1 = [
    "data analyst","senior data analyst","analytics engineer",
    "business intelligence","bi developer","bi analyst",
    "quantitative analyst","data engineer","machine learning engineer",
    "ml engineer","applied scientist","research scientist","ai engineer",
    "nlp engineer","deep learning engineer","computer vision engineer",
    "mlops engineer","data scientist","ai researcher","generative ai",
    "llm engineer","natural language processing","software engineer",
    "systems architect","cloud engineer","devops engineer","platform engineer",
    "site reliability engineer","backend engineer","cybersecurity analyst",
    "security engineer","solutions architect","computer scientist",
    "network engineer","systems administrator","information technology specialist",
    "strategy consultant","management consultant","business analyst",
    "product manager","operations manager","financial analyst",
    "management analyst","program analyst","budget analyst",
    "management and program analyst","program manager","policy analyst",
]
DOMAIN_TITLE_T2 = [
    "engineer","developer","analyst","scientist","architect",
    "consultant","manager","director","specialist",
    "data","cloud","ai","ml","machine learning","automation",
    "analytics","intelligence","research","technology","software",
]
DOMAIN_DESC_TERMS = [
    "machine learning","deep learning","neural network","nlp",
    "natural language processing","computer vision","pytorch","tensorflow",
    "scikit-learn","hugging face","llm","large language model",
    "generative ai","rag","mlops","vertex ai","dataflow","bigquery",
    "python","sql","scala","java","data pipeline","etl","data warehouse",
    "spark","kafka","airflow","dbt","databricks","snowflake",
    "bigquery","redshift","tableau","power bi","looker",
    "aws","azure","gcp","docker","kubernetes","terraform",
    "a/b testing","statistics","mba","strategy","financial modeling",
    "agile","scrum","rest api","devops","cybersecurity","federal","clearance",
]
DOMAIN_ORG_NAMES = [
    "department of defense","dod","national security agency","nsa","nasa",
    "national science foundation","department of energy",
    "department of veterans affairs","centers for disease control","cdc",
    "department of treasury","irs","general services administration","gsa",
    "department of commerce","census bureau","department of homeland security",
    "google","amazon","meta","apple","microsoft","openai","anthropic",
    "deepmind","nvidia","intel","ibm","oracle","salesforce",
    "databricks","snowflake","palantir","mckinsey","bcg","bain",
    "deloitte","accenture","pwc","booz allen","leidos","mitre",
    "jpmorgan","goldman sachs","bloomberg",
]

ONET_SKILLS_MAP = {
    "Reading Comprehension":["documentation","technical document","specification","interpret report","review document","written material","policy document","literature review"],
    "Active Listening":["active listening","stakeholder","requirements gathering","client meetings","user interview","discovery session"],
    "Writing":["writing","written communication","documentation","technical writing","report","proposal","white paper","draft","document findings"],
    "Speaking":["presentation","verbal communication","present","speak","briefing","communicate ideas","public speaking","demo","pitch"],
    "Mathematics":["math","mathematics","statistics","quantitative","calculus","algebra","probability","linear algebra","regression","optimization"],
    "Science":["scientific","research","experiment","hypothesis","empirical","research methodology","scientific method","study design"],
    "Critical Thinking":["critical thinking","analytical","analysis","evaluate","assess","problem-solving","logical","reasoning","root cause","synthesize"],
    "Active Learning":["active learning","continuous learning","self-directed","upskill","reskill","adapt","emerging technology","professional development"],
    "Learning Strategies":["learning strategies","training program","curriculum","instructional design","onboarding plan","coaching","knowledge management"],
    "Monitoring":["monitor","track","KPI","metrics","dashboard","alerting","observability","logging","reporting","SLA","telemetry"],
    "Social Perceptiveness":["emotional intelligence","empathy","interpersonal","cultural awareness","soft skills","relationship building"],
    "Coordination":["coordinate","cross-functional","collaboration","teamwork","sync","align","liaison","partner with","multi-team"],
    "Persuasion":["persuasion","influence","convince","advocate","change management","buy-in","gain alignment","drive adoption"],
    "Negotiation":["negotiation","negotiate","contract","vendor management","procurement","SLA negotiation"],
    "Instructing":["instruct","mentor","coach","teach","train","guide","knowledge transfer","pair programming","onboard"],
    "Service Orientation":["customer service","client-facing","user support","service delivery","customer success","support ticket"],
    "Complex Problem Solving":["complex problem","problem solving","debug","troubleshoot","root cause analysis","incident response","triage","ambiguity"],
    "Operations Analysis":["operations analysis","business analysis","requirements analysis","workflow analysis","process improvement","gap analysis","cost-benefit"],
    "Technology Design":["technology design","system design","architecture","solution design","data modeling","schema design","design pattern"],
    "Equipment Selection":["tool selection","technology selection","platform selection","tool evaluation","vendor evaluation","build vs buy"],
    "Installation":["install","deploy","configure","provisioning","implementation","rollout","CI/CD","pipeline setup","containerize","Terraform"],
    "Programming":["programming","coding","software development","software engineering","develop","script","Python","Java","SQL","C++","JavaScript","Scala","algorithm","data structure","object-oriented"],
    "Operations Monitoring":["system monitoring","infrastructure monitoring","Datadog","Prometheus","Grafana","CloudWatch","APM","log aggregation"],
    "Operation and Control":["automation","DevOps","orchestration","Kubernetes","Airflow","workflow automation","MLOps","DataOps","Dataflow","Pub/Sub"],
    "Equipment Maintenance":["maintenance","patching","updates","system upkeep","database maintenance","lifecycle management"],
    "Troubleshooting":["troubleshoot","debug","diagnose","fix bugs","issue tracking","defect","triage","log analysis","postmortem","RCA"],
    "Repairing":["repair","restore","recovery","disaster recovery","failover","remediation","rollback","hotfix","business continuity"],
    "Quality Control Analysis":["quality control","QA","QC","testing","unit test","integration test","code review","validation","test automation","TDD"],
    "Judgment and Decision Making":["decision making","judgment","prioritize","risk assessment","trade-off","governance","risk management"],
    "Systems Analysis":["systems analysis","enterprise architecture","data architecture","integration design","microservices","distributed systems","scalability"],
    "Systems Evaluation":["benchmark","performance testing","load test","audit","proof of concept","model evaluation","A/B testing","ablation"],
    "Time Management":["time management","deadline","prioritization","scheduling","sprint","agile","scrum","milestone","roadmap","Jira"],
    "Management of Financial Resources":["budget","financial management","ROI","P&L","revenue","forecast","financial planning","cost optimization"],
    "Management of Material Resources":["resource allocation","capacity planning","infrastructure management","cloud cost","compute budget"],
    "Management of Personnel Resources":["people management","team management","hiring","talent management","manage team","lead team","supervise"],
}

ONET_FAMILIES = {
    "Basic Skills":["Reading Comprehension","Active Listening","Writing","Speaking","Mathematics","Science","Critical Thinking","Active Learning","Learning Strategies","Monitoring"],
    "Social Skills":["Social Perceptiveness","Coordination","Persuasion","Negotiation","Instructing","Service Orientation"],
    "Complex Problem Solving":["Complex Problem Solving"],
    "Technical Skills":["Operations Analysis","Technology Design","Equipment Selection","Installation","Programming","Operations Monitoring","Operation and Control","Equipment Maintenance","Troubleshooting","Repairing","Quality Control Analysis"],
    "Systems Skills":["Judgment and Decision Making","Systems Analysis","Systems Evaluation"],
    "Resource Management":["Time Management","Management of Financial Resources","Management of Material Resources","Management of Personnel Resources"],
}
ONET_SKILL_TO_FAMILY = {sk: fam for fam, skills in ONET_FAMILIES.items() for sk in skills}


# ── BQ Schema ─────────────────────────────────────────────────────────────────
BQ_SCHEMA = {
    "fields": [
        {"name": "hash_id",           "type": "STRING",    "mode": "REQUIRED"},
        {"name": "job_title",          "type": "STRING",    "mode": "NULLABLE"},
        {"name": "company",            "type": "STRING",    "mode": "NULLABLE"},
        {"name": "location",           "type": "STRING",    "mode": "NULLABLE"},
        {"name": "domain",             "type": "STRING",    "mode": "NULLABLE"},
        {"name": "era",                "type": "STRING",    "mode": "NULLABLE"},
        {"name": "source",             "type": "STRING",    "mode": "NULLABLE"},
        {"name": "publication_date",   "type": "STRING",    "mode": "NULLABLE"},
        {"name": "application_close",  "type": "STRING",    "mode": "NULLABLE"},
        {"name": "salary",             "type": "STRING",    "mode": "NULLABLE"},
        {"name": "job_type",           "type": "STRING",    "mode": "NULLABLE"},
        {"name": "grade",              "type": "STRING",    "mode": "NULLABLE"},
        {"name": "relevance_score",    "type": "INTEGER",   "mode": "NULLABLE"},
        {"name": "relevance_tier",     "type": "STRING",    "mode": "NULLABLE"},
        {"name": "required_skills",    "type": "STRING",    "mode": "NULLABLE"},
        {"name": "onet_skills",        "type": "STRING",    "mode": "NULLABLE"},
        {"name": "onet_basic",         "type": "INTEGER",   "mode": "NULLABLE"},
        {"name": "onet_social",        "type": "INTEGER",   "mode": "NULLABLE"},
        {"name": "onet_technical",     "type": "INTEGER",   "mode": "NULLABLE"},
        {"name": "onet_systems",       "type": "INTEGER",   "mode": "NULLABLE"},
        {"name": "onet_resource",      "type": "INTEGER",   "mode": "NULLABLE"},
        {"name": "onet_complex",       "type": "INTEGER",   "mode": "NULLABLE"},
        {"name": "tech_skill_count",   "type": "INTEGER",   "mode": "NULLABLE"},
        {"name": "onet_skill_count",   "type": "INTEGER",   "mode": "NULLABLE"},
        {"name": "description",        "type": "STRING",    "mode": "NULLABLE"},
        {"name": "job_url",            "type": "STRING",    "mode": "NULLABLE"},
        {"name": "search_keyword",     "type": "STRING",    "mode": "NULLABLE"},
        {"name": "matched_terms",      "type": "STRING",    "mode": "NULLABLE"},
        {"name": "collected_at",       "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "run_timestamp",      "type": "STRING",    "mode": "NULLABLE"},
    ]
}


# ── Pure helper functions (serialisable for Beam workers) ─────────────────────
def _make_pat(terms):
    esc = [re.escape(t) for t in sorted(terms, key=len, reverse=True)]
    return re.compile(r"\b(" + "|".join(esc) + r")\b", re.IGNORECASE)

PAT_T1 = _make_pat(DOMAIN_TITLE_T1)
PAT_T2 = _make_pat(DOMAIN_TITLE_T2)
PAT_DD = _make_pat(DOMAIN_DESC_TERMS)
PAT_OR = _make_pat(DOMAIN_ORG_NAMES)


def score_relevance(title="", description="", company=""):
    score, matched = 0, []
    tl, dl, cl = str(title).lower(), str(description).lower()[:3000], str(company).lower()
    for pat, pts in [(PAT_T1, 4), (PAT_T2, 2)]:
        h = pat.findall(tl)
        if h:
            score += min(pts, pts * len(set(h)))
            matched += list(set(h))
    h = PAT_DD.findall(dl)
    if h:
        score += min(4, len(set(h)))
        matched += list(set(h))
    h = PAT_OR.findall(cl)
    if h:
        score += 2
        matched += list(set(h))
    tier = "HIGH" if score >= 6 else "MEDIUM" if score >= 4 else "LOW" if score >= 2 else "EXCLUDE"
    return score, list(set(matched)), tier


def extract_skills(title="", description=""):
    corpus = (str(title) + " " + str(description)).lower()
    found_skills = set()
    
    for skill in SKILL_TAXONOMY:
        skill_lower = skill.lower()
        if skill_lower == "r":
            if re.search(r"\b(r programming|r language)\b", corpus):
                found_skills.add(skill)
        elif skill_lower == "go":
            if re.search(r"\b(go language|golang)\b", corpus):
                found_skills.add(skill)
        else:
            pattern = r'\b' + re.escape(skill_lower) + r'\b'
            if re.search(pattern, corpus):
                found_skills.add(skill)
    return sorted(list(found_skills))


def map_onet_skills(title="", description="", existing_skills=None):
    corpus = (str(title) + " " + str(description) + " " + " ".join(existing_skills or [])).lower()
    return [sk for sk, triggers in ONET_SKILLS_MAP.items()
            if any(t.lower() in corpus for t in triggers)]


def onet_family_profile(onet_skills):
    p = {f: 0 for f in ONET_FAMILIES}
    for sk in onet_skills:
        fam = ONET_SKILL_TO_FAMILY.get(sk)
        if fam:
            p[fam] += 1
    return p


def classify_era(date_str):
    if not date_str:
        return "unknown"
    try:
        dt = datetime.fromisoformat(str(date_str)[:10])
        return "current" if dt >= CURRENT_EPOCH else "historical"
    except Exception:
        return "unknown"


def make_hash(title, company, location):
    key = f"{str(title).lower().strip()}|{str(company).lower().strip()}|{str(location).lower().strip()}"
    return hashlib.sha1(key.encode()).hexdigest()[:12]


# ── Beam DoFns ────────────────────────────────────────────────────────────────
class ParseJSON(beam.DoFn):
    """
    Malformed JSON, or a record missing hash_id/job_title, used to be
    silently dropped (`except: pass`) — meaning a bad Pub/Sub message just
    vanished with no trace. Routed to a tagged 'dead_letter' output instead,
    forwarded to a Pub/Sub DLQ topic by build_streaming_pipeline when
    --dlq_topic is provided.
    """
    OUTPUT_DEAD_LETTER = "dead_letter"

    def process(self, element):
        try:
            record = json.loads(element)
            if not (record.get("hash_id") and record.get("job_title")):
                raise ValueError("missing required field 'hash_id' or 'job_title'")
            yield record
        except Exception as exc:  # noqa: BLE001
            logger.warning("Dead-lettering malformed message: %s", exc)
            yield beam.pvalue.TaggedOutput(
                self.OUTPUT_DEAD_LETTER,
                json.dumps({"error": str(exc), "raw": str(element)[:2000]}),
            )


class PublishDeadLetter(beam.DoFn):
    """
    Publishes dead-lettered messages to the DLQ topic via a hand-rolled
    client call wrapped in try/except, instead of the built-in
    beam.io.WriteToPubSub sink.

    RATIONALE: dead-lettering exists specifically to stop one bad message
    from taking down the pipeline — but WriteToPubSub has no built-in error
    isolation. If the DLQ topic write itself hits ANY problem (IAM
    propagation delay right after Step 1.5/setup_geo.sh runs, a malformed
    topic string, a transient Pub/Sub error), Dataflow's default retry-then-
    fail-the-bundle behavior can fail the ENTIRE job over a side branch
    whose whole purpose was to handle failures gracefully. This DoFn
    guarantees a publish failure only ever logs and drops — it can never
    propagate up and fail the main pipeline.
    """

    def setup(self):
        # Lazy client init: google.cloud.pubsub_v1.PublisherClient is not
        # picklable, so it must be created per-worker in setup(), not in
        # __init__ (which runs once on the submitting machine and gets
        # pickled into the pipeline graph).
        from google.cloud import pubsub_v1
        self._publisher = pubsub_v1.PublisherClient()

    def process(self, element, topic):
        try:
            data = element.encode("utf-8") if isinstance(element, str) else element
            self._publisher.publish(topic, data).result(timeout=10)
        except Exception as exc:  # noqa: BLE001
            # Deliberately swallowed: losing a dead-letter record is a
            # (logged) inconvenience; failing the main pipeline because the
            # dead-letter SINK itself hiccuped would be strictly worse.
            logger.error("Dead-letter publish failed (message dropped, main pipeline unaffected): %s", exc)


class EnrichAndFlatten(beam.DoFn):
    """
    Re-score, re-map O*NET, compute family profile, flatten lists to strings.
    Emits one BQ-ready dict per record.
    """

    def process(self, record):
        import json as _json
        from datetime import datetime as _dt

        title = record.get("job_title", "")
        desc  = record.get("description", "")
        comp  = record.get("company", "")

        score, matched, tier = score_relevance(title, desc, comp)
        tech = extract_skills(title, desc)
        onet = map_onet_skills(title, desc, tech)
        fp   = onet_family_profile(onet)
        era  = classify_era(record.get("publication_date", ""))
        
        output_record = {
            "hash_id":          record.get("hash_id", make_hash(title, comp, record.get("location", ""))),
            "job_title":        title,
            "company":          comp,
            "location":         record.get("location", ""),
            "domain":           record.get("domain", ""),
            "era":              era,
            "source":           record.get("source", ""),
            "publication_date": str(record.get("publication_date", ""))[:10],
            "application_close":str(record.get("application_close", ""))[:10],
            "salary":           str(record.get("salary", "")),
            "job_type":         str(record.get("job_type", "")),
            "grade":            str(record.get("grade", "")),
            "relevance_score":  score,
            "relevance_tier":   tier,
            "required_skills":  "|".join(tech),
            "onet_skills":      "|".join(onet),
            "onet_basic":       fp.get("Basic Skills", 0),
            "onet_social":      fp.get("Social Skills", 0),
            "onet_technical":   fp.get("Technical Skills", 0),
            "onet_systems":     fp.get("Systems Skills", 0),
            "onet_resource":    fp.get("Resource Management", 0),
            "onet_complex":     fp.get("Complex Problem Solving", 0),
            "tech_skill_count": len(tech),
            "onet_skill_count": len(onet),
            "description":      str(desc)[:600],
            "job_url":          str(record.get("job_url", "")),
            "search_keyword":   str(record.get("search_keyword", "")),
            "matched_terms":    "|".join(matched),
            "collected_at":     _dt.utcnow().isoformat() + "Z",
            "run_timestamp":    record.get("run_timestamp", ""),
        }

        output_record['tech_skill_count'] = output_record.get('tech_skill_count', 0)
        output_record['onet_skill_count'] = output_record.get('onet_skill_count', 0)

        yield output_record


class FilterByRelevance(beam.DoFn):
    def process(self, record):
        if record.get("relevance_score", 0) >= MIN_SCORE:
            yield record


class DeduplicateByHash(beam.DoFn):
    """Stateful dedup keyed by hash_id within a bundle."""
    def process(self, element):
        yield element  # Beam Distinct() handles cross-bundle dedup


class DecodePubSub(beam.DoFn):
    OUTPUT_DEAD_LETTER = "dead_letter"

    def process(self, element):
        try:
            text = element.decode("utf-8") if isinstance(element, bytes) else element
            yield text
        except Exception as exc:  # noqa: BLE001
            logger.warning("Dead-lettering undecodable message: %s", exc)
            yield beam.pvalue.TaggedOutput(
                self.OUTPUT_DEAD_LETTER,
                json.dumps({"error": str(exc), "raw": repr(element)[:2000]}),
            )


# ── Pipeline builders ─────────────────────────────────────────────────────────
def build_batch_pipeline(known_args, pipeline_args):
    """Builds and runs the batch pipeline."""
    opts = PipelineOptions(pipeline_args)
    opts.view_as(SetupOptions).save_main_session = True
    gcp = opts.view_as(GoogleCloudOptions)
    gcp.project           = known_args.project
    gcp.region            = known_args.region
    gcp.staging_location  = known_args.staging_location
    gcp.temp_location     = known_args.temp_location
    opts.view_as(StandardOptions).runner = known_args.runner

    class ParseJSONArrayFile(beam.DoFn):
        """Each GCS file is a JSON array. Read whole file, emit one record per element."""
        def process(self, readable_file):
            import json as _json
            content = readable_file.read_utf8()
            try:
                records = _json.loads(content)
                if isinstance(records, list):
                    for r in records:
                        if isinstance(r, dict) and r.get("hash_id") and r.get("job_title"):
                            yield r
            except Exception as e:
                import logging
                logging.warning(f"Failed to parse file: {e}")

    with beam.Pipeline(options=opts) as p:
        if known_args.gcs_input_pattern:
            processed = (
                p
                | "MatchFiles"        >> beam_fileio.MatchFiles(known_args.gcs_input_pattern)
                | "ReadFiles"         >> beam_fileio.ReadMatches()
                | "ParseArrays"       >> beam.ParDo(ParseJSONArrayFile())
                | "EnrichAndFlatten"  >> beam.ParDo(EnrichAndFlatten())
                | "FilterRelevance"   >> beam.ParDo(FilterByRelevance())
                | "KeyByHash"         >> beam.Map(lambda r: (r["hash_id"], r))
                | "DeduplicateByKey"  >> beam.combiners.Latest.PerKey()
                | "ExtractValues"     >> beam.Map(lambda kv: kv[1])
            )

            processed | "WriteToBigQuery" >> WriteToBigQuery(
                table=known_args.bq_table,
                schema=BQ_SCHEMA,
                write_disposition=BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            )

            if known_args.gcs_output_path:
                processed | "WriteJSONL" >> beam.io.WriteToText(
                    file_path_prefix=known_args.gcs_output_path,
                    file_name_suffix=".jsonl",
                    shard_name_template="",
                )

    logger.info("Batch pipeline complete.")


def build_streaming_pipeline(known_args, pipeline_args):
    """Builds and runs the streaming pipeline."""
    opts = PipelineOptions(pipeline_args)
    opts.view_as(SetupOptions).save_main_session = True
    opts.view_as(StandardOptions).streaming = True
    opts.view_as(StandardOptions).runner    = "DataflowRunner"
    gcp = opts.view_as(GoogleCloudOptions)
    gcp.project          = known_args.project
    gcp.region           = known_args.region
    gcp.staging_location = known_args.staging_location
    gcp.temp_location    = known_args.temp_location

    with beam.Pipeline(options=opts) as p:
        decoded = (
            p
            | "ReadPubSub" >> beam.io.ReadFromPubSub(subscription=known_args.input_subscription)
            | "Decode" >> beam.ParDo(DecodePubSub()).with_outputs(
                DecodePubSub.OUTPUT_DEAD_LETTER, main="ok"
            )
        )
        parsed = decoded.ok | "ParseJSON" >> beam.ParDo(ParseJSON()).with_outputs(
            ParseJSON.OUTPUT_DEAD_LETTER, main="ok"
        )

        if known_args.dlq_topic:
            (
                (decoded[DecodePubSub.OUTPUT_DEAD_LETTER], parsed[ParseJSON.OUTPUT_DEAD_LETTER])
                | "FlattenDeadLetters" >> beam.Flatten()
                # FIX: was beam.io.WriteToPubSub, which has no error
                # isolation — any hiccup publishing a dead letter (IAM
                # propagation delay, transient error) could fail the whole
                # job over a branch whose entire purpose is failure
                # handling. PublishDeadLetter catches and logs instead.
                | "PublishDeadLetters" >> beam.ParDo(PublishDeadLetter(), topic=known_args.dlq_topic)
            )

        (
            parsed.ok
            | "EnrichAndFlatten" >> beam.ParDo(EnrichAndFlatten())
            | "FilterRelevance"  >> beam.ParDo(FilterByRelevance())
            | "WriteToBigQuery"  >> WriteToBigQuery(
                table=known_args.bq_table,
                schema=BQ_SCHEMA,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )

    logger.info("Streaming pipeline launched.")


def run(argv=None):
    """
    Parse arguments and dispatch to build_batch_pipeline() or
    build_streaming_pipeline(). Extracted out of the old
    `if __name__ == "__main__":` block so this file is cleanly importable
    (e.g. for local testing) as well as directly runnable — which is what
    the Flex Template launcher does via FLEX_TEMPLATE_PYTHON_PY_FILE.
    """
    parser = argparse.ArgumentParser(description="Job Postings Dataflow Pipeline")

    parser.add_argument('--pipeline_mode', choices=['batch', 'streaming'], default='batch', help="Execution mode")
    parser.add_argument("--project",          help="GCP Project ID")
    parser.add_argument("--region",           default="us-central1")
    parser.add_argument("--staging_location", help="GCS staging path")
    parser.add_argument("--temp_location",    help="GCS temp path")
    parser.add_argument("--bq_table",         help="BigQuery table (dataset.table)")
    parser.add_argument("--gcs_input_pattern", help="Batch: GCS input glob")
    parser.add_argument("--gcs_output_path",   help="Batch: GCS output path")
    parser.add_argument("--input_subscription", help="Streaming: Pub/Sub subscription")
    parser.add_argument("--dlq_topic", help="Streaming: full Pub/Sub dead-letter topic path for malformed messages")
    parser.add_argument("--runner",           default="DataflowRunner")

    known_args, pipeline_args = parser.parse_known_args(argv)
    logger.info("known_args: %s", vars(known_args))
    logger.info("pipeline_args (captured by PipelineOptions): %s", pipeline_args)

    required_common = ["project", "staging_location", "temp_location", "bq_table"]
    for arg in required_common:
        if not getattr(known_args, arg):
            parser.error(f"--{arg} is required.")

    if known_args.pipeline_mode == "batch":
        if not known_args.gcs_input_pattern:
            parser.error("--gcs_input_pattern is required for batch mode.")
        if not known_args.gcs_output_path:
            parser.error("--gcs_output_path is required for batch mode.")
        build_batch_pipeline(known_args, pipeline_args)

    elif known_args.pipeline_mode == "streaming":
        if not known_args.input_subscription:
            parser.error("--input_subscription is required for streaming mode.")
        build_streaming_pipeline(known_args, pipeline_args)

    else:
        parser.print_help()


if __name__ == "__main__":
    run()
