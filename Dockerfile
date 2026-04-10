FROM python:3.12-slim

LABEL org.opencontainers.image.source="https://github.com/Flux-Point-Studios/materios-operator-kit"
LABEL org.opencontainers.image.description="Materios attestation cert-daemon for external validators"

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
