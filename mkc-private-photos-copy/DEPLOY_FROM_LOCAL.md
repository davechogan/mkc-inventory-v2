# Deploy Photos App from Your Local Machine

**This guide is for deploying from a machine with SSH access to the Mac Studio (e.g., your MacBook on the local network).**

---

## Prerequisites

- SSH access to Mac Studio: `ssh dhogan@192.168.50.89` or `ssh macstudio` works
- Git installed on your local machine
- The Mac Studio is reachable at `192.168.50.89` or `Mac-Studio.local`

---

## Step 1: Clone Repository

On your local machine (MacBook):

```bash
cd ~/Projects
git clone git@github.com:davechogan/mkc-private-photos.git
cd mkc-private-photos
```

---

## Step 2: Build Frontend

```bash
cd frontend
npm install
npm run build
cd ..

# Verify
ls -la static/dist/index.html
```

---

## Step 3: Deploy to Mac Studio

### Option A: Automated Deployment (Recommended)

```bash
# Set environment variables (optional, script has defaults)
export DEPLOY_HOST="macstudio"  # or "192.168.50.89" or "Mac-Studio.local"
export DEPLOY_USER="dhogan"

# Initial deployment (first time)
./scripts/deploy_to_macstudio.sh --initial

# This will:
# - Test SSH connection
# - Deploy code via rsync
# - Create virtual environment
# - Install Python dependencies
# - Extract Pushover keys from existing app
# - Show next steps for launchd configuration
```

After initial deployment, follow the on-screen instructions to:
1. Configure launchd plist with Pushover keys
2. Load the daemon
3. Verify service is running

### Option B: Manual Deployment

Follow the step-by-step commands in `DEPLOY_COMMANDS.md`:

```bash
cat DEPLOY_COMMANDS.md
# Copy and paste each section
```

---

## Step 4: Update Deployment (After Code Changes)

```bash
cd ~/Projects/mkc-private-photos

# Pull latest changes
git pull

# Rebuild frontend if changed
cd frontend && npm run build && cd ..

# Deploy update
./scripts/deploy_to_macstudio.sh --update

# This will:
# - Deploy code via rsync
# - Restart the service automatically
# - Test health endpoint
```

---

## Step 5: Configure Cloudflare

After the service is running on Mac Studio, configure Cloudflare routing:

### Add Tunnel Hostname

1. Open https://one.dash.cloudflare.com
2. **Networks** → **Tunnels** → Select Mac Studio tunnel → **Configure**
3. **Public Hostname** → **Add a public hostname**:
   - Subdomain: `photos`
   - Domain: `davechogan.com`
   - Type: `HTTP`
   - URL: `localhost:8009`
4. **Save**

### Create Access Application

1. **Access controls** → **Applications** → **Add an application**
2. **Self-hosted**
3. Configure:
   - Name: `Photos App`
   - Domain: `photos.davechogan.com`
4. Allow policy:
   - Emails: `davechogan@gmail.com`, `natalyashapran1@gmail.com`
5. **Add application**

### Add Redirect Rule (Optional)

1. **Websites** → `davechogan.com` → **Rules** → **Redirect Rules**
2. Create rule to redirect `inventory.davechogan.com/photos` → `photos.davechogan.com`
3. See `DEPLOY_COMMANDS.md` for exact expression syntax

---

## Verification

### Test on Mac Studio

```bash
ssh macstudio

# Check service
lsof -nP -iTCP:8009 -sTCP:LISTEN

# Test health
curl http://127.0.0.1:8009/health

# View logs
tail -50 /Users/dhogan/Library/Logs/privatePhotos.log
```

### Test Public Access

Open in browser:
- `https://photos.davechogan.com` - Should require Cloudflare login
- Upload a photo - Should work and send Pushover notification
- `https://inventory.davechogan.com/photos` - Should redirect to photos subdomain

---

## Troubleshooting

### "SSH connection failed"

```bash
# Test SSH manually
ssh dhogan@192.168.50.89
# or
ssh macstudio

# If connection fails, check:
# - Mac Studio is on and connected to network
# - You're on the same network (192.168.50.x)
# - SSH is enabled on Mac Studio (System Settings → Sharing)
```

### "Frontend not built"

```bash
cd mkc-private-photos/frontend
rm -rf node_modules package-lock.json
npm install
npm run build
```

### "Port 8009 already in use"

```bash
ssh macstudio

# Check what's using it
lsof -i :8009

# If it's the old photos app, restart it
sudo launchctl bootout system/com.dhogan.privatePhotos
sudo launchctl bootstrap system /Library/LaunchDaemons/com.dhogan.privatePhotos.plist
```

### Pushover notifications not working

```bash
ssh macstudio

# Check keys are loaded
sudo launchctl print system/com.dhogan.privatePhotos | grep PUSHOVER

# If empty, edit plist and reload
sudo nano /Library/LaunchDaemons/com.dhogan.privatePhotos.plist
sudo launchctl bootout system/com.dhogan.privatePhotos
sudo launchctl bootstrap system /Library/LaunchDaemons/com.dhogan.privatePhotos.plist
```

---

## Quick Reference

### Deploy Commands

```bash
# Initial deploy
./scripts/deploy_to_macstudio.sh --initial

# Update deploy
./scripts/deploy_to_macstudio.sh --update

# Manual deploy
rsync -av --delete --exclude='.venv' --exclude='node_modules' \
  ./ macstudio:/Users/dhogan/photos-app/
```

### Mac Studio Commands

```bash
# Restart service
ssh macstudio 'sudo launchctl bootout system/com.dhogan.privatePhotos'
ssh macstudio 'sudo launchctl bootstrap system /Library/LaunchDaemons/com.dhogan.privatePhotos.plist'

# View logs
ssh macstudio 'tail -f /Users/dhogan/Library/Logs/privatePhotos.log'

# Check status
ssh macstudio 'lsof -nP -iTCP:8009 -sTCP:LISTEN'
ssh macstudio 'curl -s http://127.0.0.1:8009/health | jq'
```

---

## Files Modified on Mac Studio

These files/directories are created or modified:

```
/Users/dhogan/photos-app/           # Application directory
├── All project files (rsync'd)
├── .venv/                          # Python virtual environment (created on first deploy)
└── static/dist/                    # Built frontend (from your build)

/Library/LaunchDaemons/
└── com.dhogan.privatePhotos.plist  # System daemon config (sudo required)

/Users/dhogan/Library/Logs/
└── privatePhotos.log               # Application logs
```

**Shared with main app (read/write):**
```
/Users/dhogan/invapp_v2/data/
├── mkc_inventory.db                # Shared SQLite database
└── private_photos/                 # Shared photo storage
    ├── originals/
    ├── display/
    └── thumbs/
```

---

## Success Checklist

- [ ] Repository cloned on local machine
- [ ] Frontend built successfully
- [ ] Code deployed to Mac Studio (`/Users/dhogan/photos-app`)
- [ ] Python dependencies installed
- [ ] Pushover keys configured in launchd plist
- [ ] Service running on port 8009
- [ ] Health check returns `{"status":"ok"}`
- [ ] Cloudflare tunnel configured
- [ ] Cloudflare Access application created
- [ ] Public URL works: `https://photos.davechogan.com`
- [ ] Upload photo works
- [ ] Pushover notification received
- [ ] Old URL redirects properly
- [ ] Main app still works at `inventory.davechogan.com/collection`

---

## Next Steps After Deployment

Once the photos app is stable (tested for a few days):

1. **Remove photos code from main app** - Follow `IMPLEMENTATION_STATUS.md` in this repo
2. **Update documentation** - Update `mkc-inventory/docs/private-photos-and-operations.md`
3. **Monitor both apps** - Check logs daily for first week
4. **Test after reboot** - Verify both apps auto-start after Mac Studio reboot

---

## Support

- **Deployment guide**: `DEPLOY_COMMANDS.md`
- **Troubleshooting**: `DEPLOYMENT.md`
- **Architecture**: `README.md`
- **Status**: `IMPLEMENTATION_STATUS.md`
