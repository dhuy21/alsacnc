#!/bin/bash

# Railway deployment runner.
# This script manages the different execution phases on Railway.
#
# Usage (run from the Railway service shell or as the start command):
#   ./run-railway.sh --crawler       # Run the web crawler
#   ./run-railway.sh --predictor     # Run cookie + purpose predictions
#   ./run-railway.sh --summary       # Generate crawl summary
#   ./run-railway.sh --ls            # List experiments

set -e

SCRIPT=""
SCRIPT2=""

case "$1" in
    --crawler)
        SCRIPT="python cookie_crawler/run_crawler.py"
        ;;

    --predictor)
        SCRIPT="python classifiers/predict_cookies.py"
        SCRIPT2="python classifiers/predict_purposes.py"
        ;;

    --summary)
        SCRIPT="python cookie_crawler/crawl_summary.py"
        ;;

    --ls)
        SCRIPT="python database/queries.py --command ls"
        ;;

    *)
        echo "Usage: $0 {--crawler|--predictor|--summary|--ls}"
        echo ""
        echo "  --crawler     Run the cookie banner crawler"
        echo "  --predictor   Run cookie + purpose predictions"
        echo "  --summary     Generate crawl summary report"
        echo "  --ls          List all experiments"
        exit 1
        ;;
esac

shift

echo "=== Running: $SCRIPT $* ==="
$SCRIPT "$@"

if [[ -n "$SCRIPT2" ]]; then
    echo "=== Running: $SCRIPT2 $* ==="
    $SCRIPT2 "$@"
fi

echo "=== Done ==="
