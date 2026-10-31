# syntax=docker/dockerfile:1.7
# ============================================================================
# r2g's Studio as a tool of a context-and-governance control plane: the API and
# its one-page UI, served by uvicorn on 8501. The proxy in front strips the
# mount prefix (e.g. /r2g); the page derives its base from its own address.
#
# Needs the control plane's Python SDK as a named build context:
#
#   docker build -f control-plane.Dockerfile \
#     --build-context control_plane_sdk=<control plane repo>/sdk/python .
#
# Runtime: CONTROL_PLANE_URL (the control plane's API), R2G_TENANT and
# R2G_CATALOG_DIR (storage per tenant), and optionally AOE_URL (where the
# project ontology is read from). r2g's own API token is not used in this mode.
# ============================================================================

# Replaced by --build-context control_plane_sdk=...; empty if not given, and the
# install step below then fails with a message rather than building an image
# that cannot verify anyone.
FROM scratch AS control_plane_sdk

FROM python:3.11-slim
WORKDIR /app
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl && \
    rm -rf /var/lib/apt/lists/* && \
    groupadd -r r2g && useradd -r -g r2g -m r2g
COPY pyproject.toml README.md LICENSE NOTICE ./
COPY src/ ./src/
RUN pip install --no-cache-dir ".[ui]"
RUN --mount=type=bind,from=control_plane_sdk,target=/sdk \
    if [ -f /sdk/pyproject.toml ]; then pip install --no-cache-dir /sdk; \
    else echo "missing --build-context control_plane_sdk=<control plane repo>/sdk/python" >&2; exit 1; fi
# The image would refuse to start on an SDK without optional terms (P14.13): fail here.
RUN python -c "import sys; from r2g.control_plane import sdk_problem; sys.exit(sdk_problem())"
RUN mkdir -p /data && chown r2g:r2g /data
ENV R2G_CATALOG_DIR=/data
USER r2g
EXPOSE 8501
CMD ["uvicorn", "r2g.ui.server:create_app", "--factory", "--host", "0.0.0.0", "--port", "8501"]
