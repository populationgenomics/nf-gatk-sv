# The driver image (NEXTFLOW_PORT_PLAN.md §5.5): runs PLAN, PREPARE_WDL, the Cromwell watchers (STAGE, GSE_CHUNK)
# and the light Python steps, each on its own small google-batch VM. It holds the nf-gatk-sv CLI, whose
# Cromwell client is cpg_utils.cromwell_client.
#
# Not here: Nextflow and Java (the head job only runs Nextflow, in any image that has it), gcloud (the CLI
# talks to GCS, Secret Manager and the metadata server through the Python clients) and Hail.
#
# Outside the Python packages, tasks need: git (PREPARE_WDL bundles the gatk-sv WDL from a clone, and uv
# installs cpg-utils from a git source until a release has cromwell_client), bash and ps
# (Nextflow's task wrapper and its resource metrics).
FROM python:3.14-slim-trixie

RUN apt-get update \
    && apt-get install -y --no-install-recommends git procps ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:0.11 /uv /usr/local/bin/uv

ENV PYTHONDONTWRITEBYTECODE=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    PATH=/opt/venv/bin:$PATH

WORKDIR /nf-gatk-sv

# dependencies first, so a code change doesn't reinstall them
COPY pyproject.toml uv.lock README.md LICENSE ./
RUN uv sync --frozen --no-dev --no-install-project --no-editable

COPY src src
RUN uv sync --frozen --no-dev --no-editable \
    && nf-gatk-sv --help >/dev/null \
    && python -c 'import cpg_utils.cromwell_client'
