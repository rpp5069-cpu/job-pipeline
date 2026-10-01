# =============================================================================
# Dockerfile — Dataflow Flex Template (batch + streaming, one image)
# =============================================================================
# One image serves both modes; --pipeline_mode=batch|streaming is a launch
# parameter (see metadata.json), not a build-time choice. This file replaces
# the previous Dockerfile.batch / Dockerfile.stream, which were byte-for-byte
# duplicates of this same image under different names — consolidated here so
# there's one thing to keep correct instead of three.
#
# CRITICAL BASE IMAGE RULE:
#
# The Dataflow Flex Template launcher requires the binary:
#   /opt/google/dataflow/python_template_launcher
#
# This binary exists ONLY in:
#   gcr.io/dataflow-templates-base/python311-template-launcher-base:latest
#
# Common WRONG bases that look reasonable but cause "no such file or directory":
#   FROM python:3.11-slim              <- generic image, no launcher
#   FROM apache/beam_python3.11_sdk    <- has /opt/apache/beam/boot (worker use only)
#
# apache/beam_python3.11_sdk is correct ONLY for custom WORKER containers.
# For the LAUNCHER container (this image), always use python311-template-launcher-base.
# =============================================================================

FROM gcr.io/dataflow-templates-base/python311-template-launcher-base:latest

WORKDIR /template

# REQUIRED: tell the launcher which Python file is the pipeline entry point.
# beam_pipeline.py's `if __name__ == "__main__": run()` block IS the entry
# point — there is no separate launcher script layering on top of it.
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="/template/beam_pipeline.py"

# REQUIRED: tell the launcher where to find the pip requirements for workers
ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE="/template/requirements.txt"

COPY requirements.txt .
COPY beam_pipeline.py .

# Pre-install all runtime dependencies on the launcher image so worker
# startup is fast (the launcher pip-installs these on each worker anyway,
# but having them cached here significantly reduces cold-start time).
#
# apache-beam[gcp] MUST be listed here — do NOT omit it even though the
# base image is beam-related; the template launcher requires it to be
# importable at the module level before delegating to the pipeline.
RUN pip install --no-cache-dir -r requirements.txt

# REQUIRED: Dataflow Flex Template entry point — this is the launcher binary.
# Do NOT change this to /opt/apache/beam/boot (that is for worker containers).
ENTRYPOINT ["/opt/google/dataflow/python_template_launcher"]
