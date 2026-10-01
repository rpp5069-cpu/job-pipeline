"""
bq_proxy.py
===========
Lightweight Cloud Run service that serves BigQuery data as JSON
with dynamic date filtering (start_date/end_date) and gzip compression.
"""

import json
import os
import gzip
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs

from google.cloud import bigquery

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "")
BQ_DATASET = os.environ.get("BQ_DATASET", "job_postings")
BQ_TABLE   = os.environ.get("BQ_TABLE",   "feature_postings")
PORT       = int(os.environ.get("PORT", 8080))
MAX_ROWS   = int(os.environ.get("MAX_ROWS", 20000))

client = bigquery.Client(project=PROJECT_ID)

def build_query(start_date=None, end_date=None):
    # Base query
    query = f"""
    SELECT
      ROW_NUMBER() OVER (ORDER BY relevance_score DESC) AS id,
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
      COALESCE(onet_basic, 0)     AS onet_basic,
      COALESCE(onet_social, 0)    AS onet_social,
      COALESCE(onet_technical, 0) AS onet_technical,
      COALESCE(onet_systems, 0)   AS onet_systems,
      COALESCE(onet_resource, 0)  AS onet_resource,
      COALESCE(onet_complex, 0)   AS onet_complex,
      COALESCE(tech_skill_count, 0)  AS tech_skill_count,
      COALESCE(onet_skill_count, 0)  AS onet_skill_count,
      SUBSTR(description, 0, 300) AS description,
      job_url,
      search_keyword,
      matched_terms,
      FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', collected_at) AS collected_at,
      run_timestamp
    FROM `{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}`
    """
    
    # Add dynamic WHERE clauses for date filtering
    where_clauses = []
    if start_date:
        where_clauses.append(f"publication_date >= '{start_date}'")
    if end_date:
        where_clauses.append(f"publication_date <= '{end_date}'")
        
    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)
        
    query += f" ORDER BY relevance_score DESC LIMIT {MAX_ROWS}"
    return query

def fetch_postings(start_date=None, end_date=None):
    query_str = build_query(start_date, end_date)
    rows = list(client.query(query_str).result())
    postings = []
    for row in rows:
        r = dict(row)
        for key in ("required_skills", "onet_skills"):
            val = r.get(key) or ""
            r[key] = [s.strip() for s in val.split("|") if s.strip()] if val else []
        for key in ("relevance_score","onet_basic","onet_social","onet_technical",
                    "onet_systems","onet_resource","onet_complex",
                    "tech_skill_count","onet_skill_count"):
            r[key] = int(r.get(key) or 0)
        postings.append(r)
    return postings

class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {fmt % args}")

    def send_cors(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Accept-Encoding")

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_cors()
        self.end_headers()

    def do_GET(self):
        parsed_url = urlparse(self.path)
        path = parsed_url.path.rstrip("/")
        query_params = parse_qs(parsed_url.query)

        if path in ("", "/health"):
            body = json.dumps({"status": "ok", "project": PROJECT_ID}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_cors()
            self.end_headers()
            self.wfile.write(body)
            return

        if path == "/postings":
            try:
                # Parse start_date and end_date from the request URL parameters
                start_date = query_params.get("start_date", [None])[0]
                end_date = query_params.get("end_date", [None])[0]

                postings = fetch_postings(start_date, end_date)
                payload  = {
                    "metadata": {
                        "title":         "CS/MBA/AI Job Postings — Live BQ Export",
                        "total_records": len(postings),
                        "generated_at":  datetime.utcnow().isoformat() + "Z",
                        "project":       PROJECT_ID,
                        "table":         f"{BQ_DATASET}.{BQ_TABLE}",
                        "filters": {
                            "start_date": start_date,
                            "end_date": end_date
                        }
                    },
                    "postings": postings,
                }
                body = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
                
                accept_encoding = self.headers.get("Accept-Encoding", "")
                use_gzip = "gzip" in accept_encoding

                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_cors()

                if use_gzip:
                    compressed_body = gzip.compress(body)
                    self.send_header("Content-Encoding", "gzip")
                    self.send_header("Content-Length", str(len(compressed_body)))
                    self.end_headers()
                    self.wfile.write(compressed_body)
                    print(f"Served {len(postings)} postings (Gzipped, filtered start_date={start_date}, end_date={end_date})")
                else:
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                    print(f"Served {len(postings)} postings (Uncompressed, filtered start_date={start_date}, end_date={end_date})")

            except Exception as e:
                err = json.dumps({"error": str(e)}).encode()
                self.send_response(500)
                self.send_header("Content-Type", "application/json")
                self.send_cors()
                self.end_headers()
                self.wfile.write(err)
                print(f"ERROR: {e}")
            return

        self.send_response(404)
        self.end_headers()
        self.wfile.write(b'{"error":"not found"}')

if __name__ == "__main__":
    print(f"BQ Proxy starting on :{PORT}")
    HTTPServer(("", PORT), Handler).serve_forever()
