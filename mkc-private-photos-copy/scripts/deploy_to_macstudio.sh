#!/usr/bin/env bash
set -euo pipefail

# Deployment script for Mac Studio
# Usage: ./scripts/deploy_to_macstudio.sh [--initial|--update]

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# Configuration
DEPLOY_HOST="${DEPLOY_HOST:-macstudio}"
DEPLOY_USER="${DEPLOY_USER:-dhogan}"
DEPLOY_PATH="/Users/${DEPLOY_USER}/photos-app"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}✓${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}⚠${NC} $1"
}

log_error() {
    echo -e "${RED}✗${NC} $1"
}

# Parse arguments
MODE="${1:-update}"

if [[ "$MODE" != "--initial" && "$MODE" != "--update" && "$MODE" != "update" && "$MODE" != "initial" ]]; then
    echo "Usage: $0 [--initial|--update]"
    echo "  --initial: First-time deployment (creates directories, installs deps)"
    echo "  --update:  Update existing deployment (default)"
    exit 1
fi

if [[ "$MODE" == "initial" ]]; then
    MODE="--initial"
elif [[ "$MODE" == "update" ]]; then
    MODE="--update"
fi

echo "========================================="
echo "Photos App Deployment to Mac Studio"
echo "========================================="
echo "Host: $DEPLOY_HOST"
echo "User: $DEPLOY_USER"
echo "Path: $DEPLOY_PATH"
echo "Mode: $MODE"
echo "========================================="

# Check if frontend is built
if [[ ! -f "$PROJECT_DIR/static/dist/index.html" ]]; then
    log_error "Frontend not built!"
    echo "Run: cd frontend && npm install && npm run build"
    exit 1
fi

log_info "Frontend build verified"

# Test SSH connection
echo "Testing SSH connection..."
if ! ssh -o ConnectTimeout=5 "$DEPLOY_HOST" "echo 'SSH OK'" >/dev/null 2>&1; then
    log_error "Cannot connect to $DEPLOY_HOST via SSH"
    echo "Make sure SSH is configured: ssh $DEPLOY_HOST"
    exit 1
fi

log_info "SSH connection successful"

if [[ "$MODE" == "--initial" ]]; then
    echo ""
    echo "========================================="
    echo "INITIAL DEPLOYMENT"
    echo "========================================="
    
    # Create directory
    echo "Creating deployment directory..."
    ssh "$DEPLOY_HOST" "mkdir -p $DEPLOY_PATH" || {
        log_error "Failed to create directory"
        exit 1
    }
    log_info "Directory created: $DEPLOY_PATH"
fi

# Rsync code
echo ""
echo "Deploying code..."
rsync -av --delete \
    --exclude='.venv' \
    --exclude='node_modules' \
    --exclude='.git' \
    --exclude='__pycache__' \
    --exclude='*.pyc' \
    --exclude='.DS_Store' \
    --exclude='frontend/node_modules' \
    "$PROJECT_DIR/" \
    "$DEPLOY_HOST:$DEPLOY_PATH/" || {
    log_error "Rsync failed"
    exit 1
}

log_info "Code deployed"

if [[ "$MODE" == "--initial" ]]; then
    echo ""
    echo "========================================="
    echo "INSTALLING PYTHON DEPENDENCIES"
    echo "========================================="
    
    ssh "$DEPLOY_HOST" "cd $DEPLOY_PATH && python3.11 -m venv .venv && .venv/bin/pip install --upgrade pip && .venv/bin/pip install -r requirements.txt" || {
        log_error "Failed to install dependencies"
        exit 1
    }
    
    log_info "Python dependencies installed"
    
    echo ""
    echo "========================================="
    echo "EXTRACTING PUSHOVER KEYS"
    echo "========================================="
    
    echo "Extracting Pushover keys from existing inventory app..."
    ssh "$DEPLOY_HOST" "sudo grep -A1 'PUSHOVER' /Library/LaunchDaemons/com.dhogan.inventoryApp.plist | grep '<string>' | grep -v '<!--'" > /tmp/pushover_keys.txt || true
    
    if [[ -s /tmp/pushover_keys.txt ]]; then
        log_info "Pushover keys extracted to /tmp/pushover_keys.txt"
        echo "Keys found:"
        cat /tmp/pushover_keys.txt
        echo ""
        log_warn "You'll need to add these to com.dhogan.privatePhotos.plist"
    else
        log_warn "Could not extract Pushover keys automatically"
        echo "Extract manually: ssh $DEPLOY_HOST 'sudo grep -A1 PUSHOVER /Library/LaunchDaemons/com.dhogan.inventoryApp.plist'"
    fi
    
    echo ""
    echo "========================================="
    echo "NEXT STEPS (INITIAL DEPLOYMENT)"
    echo "========================================="
    echo "1. Update launchd plist with Pushover keys:"
    echo "   ssh $DEPLOY_HOST"
    echo "   sudo nano /Library/LaunchDaemons/com.dhogan.privatePhotos.plist"
    echo "   (Replace <!-- REPLACE WITH... --> placeholders with actual keys)"
    echo ""
    echo "2. Copy plist to system location:"
    echo "   sudo cp $DEPLOY_PATH/scripts/com.dhogan.privatePhotos.plist /Library/LaunchDaemons/"
    echo "   sudo chown root:wheel /Library/LaunchDaemons/com.dhogan.privatePhotos.plist"
    echo "   sudo chmod 644 /Library/LaunchDaemons/com.dhogan.privatePhotos.plist"
    echo ""
    echo "3. Load launchd daemon:"
    echo "   sudo launchctl bootstrap system /Library/LaunchDaemons/com.dhogan.privatePhotos.plist"
    echo ""
    echo "4. Verify:"
    echo "   lsof -nP -iTCP:8009 -sTCP:LISTEN"
    echo "   curl http://127.0.0.1:8009/health"
    echo ""
    echo "5. Configure Cloudflare (see DEPLOYMENT.md)"
    
else
    echo ""
    echo "========================================="
    echo "RESTARTING SERVICE"
    echo "========================================="
    
    echo "Restarting photos app..."
    ssh "$DEPLOY_HOST" "sudo launchctl bootout system/com.dhogan.privatePhotos 2>/dev/null || true; sleep 2; sudo launchctl bootstrap system /Library/LaunchDaemons/com.dhogan.privatePhotos.plist" || {
        log_error "Failed to restart service"
        echo "Try manually:"
        echo "  ssh $DEPLOY_HOST"
        echo "  sudo launchctl bootout system/com.dhogan.privatePhotos"
        echo "  sudo launchctl bootstrap system /Library/LaunchDaemons/com.dhogan.privatePhotos.plist"
        exit 1
    }
    
    sleep 3
    
    # Check if running
    echo "Checking if service is running..."
    if ssh "$DEPLOY_HOST" "lsof -nP -iTCP:8009 -sTCP:LISTEN" >/dev/null 2>&1; then
        log_info "Service is running on port 8009"
        
        # Test health endpoint
        echo "Testing health endpoint..."
        HEALTH=$(ssh "$DEPLOY_HOST" "curl -s http://127.0.0.1:8009/health")
        if echo "$HEALTH" | grep -q '"status":"ok"'; then
            log_info "Health check passed"
            echo "$HEALTH"
        else
            log_warn "Health check returned unexpected response"
            echo "$HEALTH"
        fi
    else
        log_error "Service is not running on port 8009"
        echo "Check logs: ssh $DEPLOY_HOST 'tail -50 /Users/$DEPLOY_USER/Library/Logs/privatePhotos.log'"
        exit 1
    fi
fi

echo ""
echo "========================================="
echo "DEPLOYMENT COMPLETE"
echo "========================================="

if [[ "$MODE" == "--initial" ]]; then
    log_info "Initial deployment finished - follow next steps above"
else
    log_info "Update deployment finished"
    echo "View logs: ssh $DEPLOY_HOST 'tail -f /Users/$DEPLOY_USER/Library/Logs/privatePhotos.log'"
fi
