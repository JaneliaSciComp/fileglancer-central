#!/bin/bash

# Set variables for uvicorn
WORKERS="${WORKERS:=1}"
HOST="${HOST:=0.0.0.0}"
PORT="${PORT:=7878}"

set -x
pixi run uvicorn fileglancer_central.app:app --access-log \
    --workers $WORKERS --host $HOST --port $PORT \
    --forwarded-allow-ips='*' --proxy-headers \
    --ssl-keyfile "$KEY_FILE" --ssl-certfile "$CERT_FILE" "$@"
