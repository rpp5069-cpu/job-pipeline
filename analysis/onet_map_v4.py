# O*NET 24.2 skill triggers, mapper v4 (2026-11-24).
# Changes from v3.1: "develop" -> software-specific phrases; "Go" -> "Golang"; "train" -> staff-training phrases;
# "SME" and "parse" removed. Matching rule: see onet_match() below.
ONET_SKILLS_MAP = {
"Reading Comprehension": ["documentation", "technical document", "specification", "interpret report", "review document", "written material", "policy document", "literature review", "technical manual"],
"Active Listening": ["active listening", "stakeholder", "requirements gathering", "client meetings", "user interview", "discovery session", "elicit requirements"],
"Writing": ["writing", "written communication", "technical writing", "report", "proposal", "white paper", "draft", "document findings", "publish"],
"Speaking": ["presentation", "verbal communication", "briefing", "communicate ideas", "public speaking", "articulate", "cross-functional communication"],
"Mathematics": ["math", "mathematics", "statistics", "quantitative", "calculus", "algebra", "numerical analysis", "probability", "linear algebra", "regression", "optimization"],
"Science": ["scientific", "research", "experiment", "hypothesis", "empirical", "research methodology", "scientific method", "data collection", "study design"],
"Critical Thinking": ["critical thinking", "analytical", "analysis", "evaluate", "assess", "problem-solving", "logical", "reasoning", "judgment", "root cause", "synthesize", "evidence-based"],
"Active Learning": ["active learning", "continuous learning", "self-directed", "upskill", "reskill", "adapt", "emerging technology", "stay current", "professional development", "evolving landscape"],
"Learning Strategies": ["learning strategies", "training program", "curriculum", "instructional design", "e-learning", "onboarding plan", "coaching", "workshop"],
"Monitoring": ["monitor", "track", "KPI", "metrics", "performance monitoring", "dashboard", "alerting", "observability", "logging", "SLA", "telemetry"],
"Social Perceptiveness": ["social perceptiveness", "emotional intelligence", "empathy", "interpersonal", "team dynamics", "cultural awareness", "relationship building"],
"Coordination": ["coordination", "coordinate", "cross-functional", "collaboration", "teamwork", "liaison", "partner with", "multi-team", "synchronize"],
"Persuasion": ["persuasion", "influence", "sales", "convince", "advocate", "change management", "buy-in", "executive presence", "drive adoption"],
"Negotiation": ["negotiation", "negotiate", "contract", "vendor management", "procurement", "SLA negotiation", "partnership agreement"],
"Instructing": ["instruct", "mentor", "coach", "teach", "train staff", "train employees", "train users", "train new", "provide training", "conduct training", "deliver training", "trainer", "knowledge transfer", "pair programming", "onboard", "technical guidance"],
"Service Orientation": ["customer service", "client-facing", "user support", "help desk", "service delivery", "customer success", "service level", "support ticket"],
"Complex Problem Solving": ["complex problem", "problem solving", "debug", "troubleshoot", "root cause analysis", "issue resolution", "incident response", "triage", "architecture challenge", "unstructured problem", "ambiguity"],
"Operations Analysis": ["operations analysis", "business analysis", "requirements analysis", "workflow analysis", "process improvement", "gap analysis", "cost-benefit", "functional requirements", "business requirements"],
"Technology Design": ["technology design", "system design", "architecture", "solution design", "technical design", "UI/UX", "API design", "data modeling", "schema design", "design pattern", "technical specification"],
"Equipment Selection": ["tool selection", "hardware", "infrastructure", "tool evaluation", "vendor evaluation", "technology selection", "build vs buy"],
"Installation": ["install", "deploy", "configure", "setup", "provisioning", "implementation", "rollout", "release", "CI/CD", "Terraform", "Helm", "ArgoCD"],
"Programming": ["programming", "coding", "software development", "software engineering", "develop software", "software developer", "application development", "develop applications", "develop code", "script", "Python", "Java", "SQL", "C++", "JavaScript", "TypeScript", "Scala", "Golang", "algorithm", "data structure", "object-oriented", "write code"],
"Operations Monitoring": ["system monitoring", "infrastructure monitoring", "network monitoring", "application performance", "Datadog", "Prometheus", "Grafana", "CloudWatch", "PagerDuty", "incident management", "APM"],
"Operation and Control": ["operation and control", "automation", "DevOps", "orchestration", "Kubernetes", "Airflow", "workflow automation", "MLOps", "DataOps", "pipeline orchestration", "job scheduling"],
"Equipment Maintenance": ["maintenance", "patching", "system upkeep", "server maintenance", "database maintenance", "lifecycle management", "vulnerability patching"],
"Troubleshooting": ["troubleshoot", "debug", "diagnose", "fix bugs", "resolve issues", "issue tracking", "defect", "log analysis", "error handling", "postmortem", "RCA"],
"Repairing": ["repair", "restore", "recovery", "disaster recovery", "failover", "remediation", "rollback", "hotfix", "business continuity", "DR plan"],
"Quality Control Analysis": ["quality control", "QA", "QC", "testing", "unit test", "integration test", "code review", "validation", "test automation", "regression", "acceptance testing", "TDD", "pytest", "Selenium", "performance test"],
"Judgment and Decision Making": ["decision making", "judgment", "prioritize", "strategic decision", "risk assessment", "trade-off", "governance", "risk management", "cost-benefit analysis"],
"Systems Analysis": ["systems analysis", "enterprise architecture", "data architecture", "integration design", "microservices architecture", "distributed systems", "scalability", "end-to-end design", "full stack", "platform architecture"],
"Systems Evaluation": ["benchmark", "performance testing", "load test", "audit", "proof of concept", "A/B testing", "model evaluation", "ablation", "retrospective"],
"Time Management": ["time management", "deadline", "prioritization", "scheduling", "project management", "sprint", "agile", "scrum", "milestone", "roadmap", "backlog", "Jira", "Confluence"],
"Management of Financial Resources": ["budget", "financial management", "cost management", "ROI", "P&L", "revenue", "forecast", "financial planning", "cost optimization", "fiscal", "cost savings"],
"Management of Material Resources": ["resource allocation", "inventory", "asset management", "procurement", "capacity planning", "cloud cost", "resource optimization"],
"Management of Personnel Resources": ["people management", "team management", "hiring", "performance review", "talent management", "workforce planning", "manage team", "supervise", "org design", "team building", "recruitment"],
}

import re as _re

def _pattern(t):
    """Start of word always; acronyms and names (any capital letter, or <= 3 chars) must also end at a word boundary."""
    whole = any(c.isupper() for c in t) or len(t) <= 3
    return _re.compile(r"(?<![a-z0-9])" + _re.escape(t.lower()) + (r"(?![a-z0-9])" if whole else ""))

ONET_PATTERNS = {k: [_pattern(t) for t in v] for k, v in ONET_SKILLS_MAP.items()}
ONET_MAPPER_VERSION = "v4 (2026-11-24): word-boundary matching + trigger fixes"

def onet_match(text):
    t = str(text).lower()
    return [k for k, ps in ONET_PATTERNS.items() if any(p.search(t) for p in ps)]
