#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

# Default to port 8009
PORT="${PORT:-8009}"

# Check if port is in use
if lsof -Pi :$PORT -sTCP:LISTEN -t >/dev/null 2>&1; then
    echo "Port $PORT is already in use. Stop the existing process first."
    echo "Try: lsof -ti :$PORT | xargs kill"
    exit 1
fi

# Use project venv
if [[ ! -d .venv ]]; then
    echo "Virtual environment not found. Creating..."
    python3.11 -m venv .venv
    .venv/bin/pip install --upgrade pip
    .venv/bin/pip install -r requirements.txt
fi

# Set database path if not already set
if [[ -z "${MKC_INVENTORY_DB:-}" ]]; then
    # Try main app DB location (production)
    if [[ -f /Users/dhogan/invapp_v2/data/mkc_inventory.db ]]; then
        export MKC_INVENTORY_DB="/Users/dhogan/invapp_v2/data/mkc_inventory.db"
    # Try sibling directory (development)
    elif [[ -f ../workspace/data/mkc_inventory.db ]]; then
        export MKC_INVENTORY_DB="../workspace/data/mkc_inventory.db"
    else
        echo "Warning: MKC_INVENTORY_DB not set and default location not found"
        echo "Set MKC_INVENTORY_DB environment variable to the database path"
    fi
fi

# Set photos directory if not already set
if [[ -z "${PRIVATE_PHOTOS_DIR:-}" ]]; then
    if [[ -d /Users/dhogan/invapp_v2/data/private_photos ]]; then
        export PRIVATE_PHOTOS_DIR="/Users/dhogan/invapp_v2/data/private_photos"
    elif [[ -d ../workspace/data/private_photos ]]; then
        export PRIVATE_PHOTOS_DIR="../workspace/data/private_photos"
    fi
fi

echo "========================================="
echo "Starting Photos App"
echo "========================================="
echo "Port: $PORT"
echo "Database: ${MKC_INVENTORY_DB:-not set}"
echo "Photos dir: ${PRIVATE_PHOTOS_DIR:-not set}"
echo "========================================="

exec .venv/bin/uvicorn app:app --host 127.0.0.1 --port $PORT --log-level info
