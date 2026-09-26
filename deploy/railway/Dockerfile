# Railway builds this file (see deploy/railway/railway.json) so a repo-based
# deploy reuses the published engine image instead of compiling Rust.
# Pin a release tag in production, e.g. ORCH8_VERSION=0.7.1.
ARG ORCH8_VERSION=latest
FROM ghcr.io/orch8-io/engine:${ORCH8_VERSION}
