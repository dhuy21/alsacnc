#!/bin/bash
set -e

MAX_RETRIES=30
RETRY_INTERVAL=10

echo "=== Railway Entrypoint ==="

# Ensure volume directories exist (fresh volume mount is empty)
mkdir -p /opt/crawler/data/experiments /opt/crawler/data/logs

echo "Waiting for dependent services to become available..."

# Wait for PostgreSQL using Python (psql may not be installed)
retries=0
until python -c "
import psycopg2, os
conn = psycopg2.connect(
    host=os.environ['DB_HOST'],
    dbname=os.environ['DB_NAME'],
    user=os.environ['DB_USER'],
    password=os.environ['DB_PASSWORD']
)
conn.close()
" 2>/dev/null; do
    retries=$((retries + 1))
    if [ $retries -ge $MAX_RETRIES ]; then
        echo "ERROR: PostgreSQL at $DB_HOST not available after $MAX_RETRIES attempts. Exiting."
        exit 1
    fi
    echo "Waiting for PostgreSQL at $DB_HOST... (attempt $retries/$MAX_RETRIES)"
    sleep $RETRY_INTERVAL
done
echo "PostgreSQL is available."

# Wait for LibreTranslate
LT_HOST="${LIBRE_TRANSLATE_HOST:-host.docker.internal}"
LT_PORT="${LIBRE_TRANSLATE_PORT:-5000}"
retries=0
until python -c "
import requests
r = requests.get('http://${LT_HOST}:${LT_PORT}/languages', timeout=5)
r.raise_for_status()
" 2>/dev/null; do
    retries=$((retries + 1))
    if [ $retries -ge $MAX_RETRIES ]; then
        echo "WARNING: LibreTranslate at ${LT_HOST}:${LT_PORT} not available after $MAX_RETRIES attempts. Proceeding anyway."
        break
    fi
    echo "Waiting for LibreTranslate at ${LT_HOST}:${LT_PORT}... (attempt $retries/$MAX_RETRIES)"
    sleep $RETRY_INTERVAL
done
echo "LibreTranslate check complete."

# Wait for classifiers (IETC model)
IETC_URL="${IETC_MODEL_URL:-http://host.docker.internal:5001/predict_ietc}"
IETC_BASE=$(echo "$IETC_URL" | sed 's|/predict_ietc.*||')
retries=0
until python -c "
import requests
r = requests.get('${IETC_BASE}/', timeout=5)
" 2>/dev/null; do
    retries=$((retries + 1))
    if [ $retries -ge $MAX_RETRIES ]; then
        echo "WARNING: Classifiers at ${IETC_BASE} not available after $MAX_RETRIES attempts. Proceeding anyway."
        break
    fi
    echo "Waiting for classifiers at ${IETC_BASE}... (attempt $retries/$MAX_RETRIES)"
    sleep $RETRY_INTERVAL
done
echo "Classifiers check complete."

echo "All dependency checks done. Starting crawler..."

CONFIG_PATH="${CRAWLER_CONFIG_PATH:-config/experiment_config.yaml}"
echo "Using config: $CONFIG_PATH"
exec python cookie_crawler/run_crawler.py --config_path "$CONFIG_PATH" "$@"
