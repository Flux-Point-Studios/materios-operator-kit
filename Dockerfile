FROM python:3.12-slim

LABEL org.opencontainers.image.source="https://github.com/Flux-Point-Studios/materios-operator-kit"
LABEL org.opencontainers.image.description="Materios attestation cert-daemon for external validators"
# Runtime compat tag — bumped on every spec change that alters the
# canonical cert encoding (currently SCALE-canonical per spec-219).
# Operators identify spec-219-compatible images via either this label
# or the `s219` image-tag suffix in the ghcr.io publish pipeline.
LABEL io.materios.runtime.spec="219"
LABEL io.materios.cert-encoding="scale-v1"

RUN apt-get update && apt-get install -y --no-install-recommends gcc libffi-dev && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY daemon/ ./daemon/

RUN useradd -r -s /bin/false certd \
    && mkdir -p /data/materios-blobs /data/certs \
    && chown -R certd:certd /data
USER certd

EXPOSE 8080
CMD ["python", "-m", "daemon.main"]
