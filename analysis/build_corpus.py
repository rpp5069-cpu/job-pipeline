"""Build the cumulative CoDIT corpus from every weekly MASTER JSON release.

  python build_corpus.py <releases_dir> <out.json.gz>

- Combines all releases, one record per hash_id (title|company|location), keeping the first-seen version.
- Records each source file's SHA-256 so the exact inputs can be re-verified.
- Adds `onet_clean`: skills re-mapped from the stored text with the v4 mapper (onet_map_v4.py): word-boundary
  matching plus trigger fixes (the v3.1 substring matcher matched 'script' inside 'description', 'Go' inside
  'government', 'SME' inside 'assessment', etc.).
- Keeps the pipeline's stored labels as `onet_stored` for the sensitivity analysis.
"""
import json, glob, gzip, hashlib, os, re, sys, collections

REL_DIR, OUT = sys.argv[1], sys.argv[2]
HERE = os.path.dirname(os.path.abspath(__file__))
ns = {}; exec(open(os.path.join(HERE, 'onet_map_v4.py')).read(), ns)   # corrected triggers + word-boundary matcher
onet_match, MAPPER = ns['onet_match'], ns['ONET_MAPPER_VERSION']

def map_clean(title, description, skills):
    return sorted(onet_match(f"{title or ''} {description or ''} {' '.join(skills or [])}"))

files = sorted(f for f in glob.glob(os.path.join(REL_DIR, 'MASTER_*.json')))
manifest, recs, seen_in = [], {}, collections.defaultdict(list)
for f in files:
    raw = open(f, 'rb').read(); tag = os.path.basename(f)[7:20]
    P = json.loads(raw)['postings']
    manifest.append(dict(file=os.path.basename(f), sha256=hashlib.sha256(raw).hexdigest(), postings=len(P),
                         by_source_domain=dict(collections.Counter(f"{p['source']}|{p['domain']}" for p in P))))
    for p in P:
        h = p['hash_id']; seen_in[h].append(tag)
        if h in recs: continue
        recs[h] = dict(hash_id=h, source=p['source'], domain=p['domain'], title=p.get('title', ''), company=p.get('company', ''),
                       location=p.get('location', ''), description=p.get('description', ''), required_skills=p.get('required_skills') or [],
                       salary=p.get('salary', ''), grade=p.get('grade', ''), posted_date=p.get('posted_date', ''), job_url=p.get('job_url', ''),
                       onet_stored=sorted(set(p.get('onet_skills') or [])))
for h, r in recs.items():
    r['onet_clean'] = map_clean(r['title'], r['description'], r['required_skills'])
    r['first_seen'], r['last_seen'], r['weeks_seen'] = seen_in[h][0], seen_in[h][-1], len(seen_in[h])
out = dict(metadata=dict(built_from=len(files), releases=manifest, n=len(recs), dedupe='hash_id = sha1(title|company|location)[:12]; first-seen version kept',
                         label_rule=f'onet_clean = mapper {MAPPER} on stored title + description + extracted skills'),
           postings=list(recs.values()))
with gzip.open(OUT, 'wt') as fh: json.dump(out, fh)
print(f'{len(files)} releases -> {len(recs):,} unique postings -> {OUT} ({os.path.getsize(OUT) / 1e6:.1f} MB)')
