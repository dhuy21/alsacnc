#!/bin/bash
set -e

MAX_RETRIES=30
RETRY_INTERVAL=10

echo "=== Railway Entrypoint ==="
echo "Waiting for dependent services to become available..."

# Wait for PostgreSQL
retries=0
until PGPASSWORD="$DB_PASSWORD" psql -h "$DB_HOST" -U "$DB_USER" -d "$DB_NAME" -c '\q' 2>/dev/null; do
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
until curl -sf "http://${LT_HOST}:${LT_PORT}/languages" > /dev/null 2>&1; do
    retries=$((retries + 1))
    if [ $retries -ge $MAX_RETRIES ]; then
        echo "ERROR: LibreTranslate at ${LT_HOST}:${LT_PORT} not available after $MAX_RETRIES attempts. Exiting."
        exit 1
    fi
    echo "Waiting for LibreTranslate at ${LT_HOST}:${LT_PORT}... (attempt $retries/$MAX_RETRIES)"
    sleep $RETRY_INTERVAL
done
echo "LibreTranslate is available."

# Wait for classifiers (IETC model)
IETC_URL="${IETC_MODEL_URL:-http://host.docker.internal:5001/predict_ietc}"
IETC_BASE=$(echo "$IETC_URL" | sed 's|/predict_ietc.*||')
retries=0
until curl -sf "${IETC_BASE}/" > /dev/null 2>&1 || curl -sf -o /dev/null -w "%{http_code}" "${IETC_BASE}/" 2>/dev/null | grep -qE "^[2-4]"; do
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
exec python cookie_crawler/run_crawler.py "$@"
