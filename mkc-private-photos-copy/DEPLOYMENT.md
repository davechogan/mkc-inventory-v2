# Deployment Guide - MKC Private Photos App

## Prerequisites

- Mac Studio with SSH access
- Pushover keys from existing inventory app
- Access to `/Users/dhogan/invapp_v2/` (shared database and photos)

## Step 1: Build Frontend

On your development machine:

```bash
cd mkc-private-photos/frontend
npm install
npm run build
# Output: ../static/dist/index.html and assets
```

Verify build:
```bash
ls -la ../static/dist/
# Should see index.html and assets/ directory
```

## Step 2: Deploy to Mac Studio

### Option A: Direct rsync (if Mac Studio home is mounted)

```bash
# From mkc-private-photos directory
export DEPLOY_PATH="/Volumes/dhogan/photos-app"

rsync -av --exclude='.venv' --exclude='node_modules' \
  --exclude='.git' --exclude='__pycache__' \
  ./ $DEPLOY_PATH/

echo "✓ Code deployed to Mac Studio"
```

### Option B: Remote rsync via SSH

```bash
# From mkc-private-photos directory
rsync -av --exclude='.venv' --exclude='node_modules' \
  --exclude='.git' --exclude='__pycache__' \
  ./ macstudio:/Users/dhogan/photos-app/

echo "✓ Code deployed to Mac Studio"
```

## Step 3: Install Python Dependencies on Mac Studio

```bash
ssh macstudio

cd /Users/dhogan/photos-app

# Create virtual environment
python3.11 -m venv .venv

# Install dependencies
.venv/bin/pip install --upgrade pip
.venv/bin/pip install -r requirements.txt

# Verify installation
.venv/bin/python -c "import fastapi; import pillow_heif; print('✓ Dependencies OK')"
```

## Step 4: Configure launchd Daemon

### Copy Pushover Keys from Existing Inventory App

```bash
ssh macstudio

# Extract Pushover keys from existing app
sudo grep -A1 "PUSHOVER" /Library/LaunchDaemons/com.dhogan.inventoryApp.plist

# Copy the actual key values (not the comment placeholders)
```

### Create launchd Plist

```bash
# Copy template to system location
sudo cp /Users/dhogan/photos-app/scripts/com.dhogan.privatePhotos.plist \
  /Library/LaunchDaemons/com.dhogan.privatePhotos.plist

# Edit the plist to add actual Pushover keys
sudo nano /Library/LaunchDaemons/com.dhogan.privatePhotos.plist
# Replace the <!-- REPLACE WITH ACTUAL... --> placeholders with real keys

# Set ownership and permissions
sudo chown root:wheel /Library/LaunchDaemons/com.dhogan.privatePhotos.plist
sudo chmod 644 /Library/LaunchDaemons/com.dhogan.privatePhotos.plist
```

### Verify Configuration

```bash
# Check environment variables are set
sudo launchctl print system/com.dhogan.inventoryApp | grep PUSHOVER
# Copy those key values into the new plist
```

## Step 5: Start Photos App

```bash
ssh macstudio

# Load the launchd daemon
sudo launchctl bootstrap system /Library/LaunchDaemons/com.dhogan.privatePhotos.plist

# Verify it's running
sudo launchctl print system/com.dhogan.privatePhotos

# Check the process
lsof -nP -iTCP:8009 -sTCP:LISTEN
# Should show uvicorn process

# Test health endpoint
curl -I http://127.0.0.1:8009/health
# Should return 200 OK

curl http://127.0.0.1:8009/health
# Should show JSON with status: ok, db_exists: true, static_built: true
```

## Step 6: Check Logs

```bash
# View logs
tail -f /Users/dhogan/Library/Logs/privatePhotos.log

# Look for:
# - "Application startup complete"
# - "Database path: /Users/dhogan/invapp_v2/data/mkc_inventory.db (exists: True)"
# - "Mounted static files from /Users/dhogan/photos-app/static/dist"

# Check for errors
grep ERROR /Users/dhogan/Library/Logs/privatePhotos.log
# Should be empty
```

## Step 7: Configure Cloudflare Tunnel

### Add Public Hostname

1. Open [Cloudflare Zero Trust](https://one.dash.cloudflare.com)
2. Go to **Networks** → **Tunnels**
3. Select the Mac Studio tunnel
4. Click **Configure** → **Public Hostname** → **Add a public hostname**
5. Configure:
   - Subdomain: `photos`
   - Domain: `davechogan.com`
   - Path: (leave blank)
   - Type: `HTTP`
   - URL: `localhost:8009`
6. Click **Save**

### Verify DNS

1. Go to **Websites** → `davechogan.com` → **DNS**
2. Verify `photos` CNAME exists pointing to tunnel
3. If missing, add manually

## Step 8: Configure Cloudflare Access

### Create New Access Application

1. Go to **Access controls** → **Applications** → **Add an application**
2. Choose **Self-hosted**
3. Configure application:
   - Name: `Photos App`
   - Session duration: `24 hours`
   - Application domain:
     - Subdomain: `photos`
     - Domain: `davechogan.com`
     - Path: (leave blank)
4. Click **Next**

### Configure Allow Policy

1. Policy name: `Photos Users`
2. Action: `Allow`
3. Include:
   - Selector: `Emails`
   - Emails:
     - `davechogan@gmail.com`
     - `natalyashapran1@gmail.com`
4. Click **Next** → **Add application**

## Step 9: Create Redirect Rule (Optional)

To redirect old URL to new subdomain:

1. Go to **Websites** → `davechogan.com` → **Rules** → **Redirect Rules**
2. Click **Create rule**
3. Configure:
   - Name: `Photos page redirect`
   - Expression: `(http.host eq "inventory.davechogan.com" and starts_with(http.request.uri.path, "/photos"))`
   - Type: `Dynamic`
   - Expression: `concat("https://photos.davechogan.com", substring(http.request.uri.path, 7))`
   - Status code: `301`
4. Click **Deploy**

## Step 10: Test Public Access

### Test Logged Out

```bash
curl -I https://photos.davechogan.com
# Should return 302 to Cloudflare login page
```

### Test Logged In (Browser)

1. Open `https://photos.davechogan.com`
2. Sign in with `davechogan@gmail.com` (Google OAuth)
3. Should see Photos page with "Choose from Photos" button
4. Upload a test photo
5. Verify it appears in gallery
6. Check that Pushover notification was received

### Test Chat

1. Send a chat message
2. Verify other user gets Pushover notification
3. Check message appears in chat thread

### Test Old URL Redirect

```bash
curl -I https://inventory.davechogan.com/photos
# Should return 301 to https://photos.davechogan.com
```

## Step 11: Verify Main App Still Works

```bash
# Check main app is still running
lsof -nP -iTCP:8008 -sTCP:LISTEN

# Test main app health
curl -I https://inventory.davechogan.com/collection
# Should return 200
```

Open browser and verify:
- Knife inventory loads: `https://inventory.davechogan.com/collection`
- Master catalog loads: `https://inventory.davechogan.com/master`
- No 404 errors

## Restart Commands

### Restart Photos App

```bash
ssh macstudio

# Stop and restart
sudo launchctl bootout system/com.dhogan.privatePhotos
sudo launchctl bootstrap system /Library/LaunchDaemons/com.dhogan.privatePhotos.plist

# Verify
lsof -nP -iTCP:8009 -sTCP:LISTEN
curl http://127.0.0.1:8009/health
```

### View Logs

```bash
# Real-time
tail -f /Users/dhogan/Library/Logs/privatePhotos.log

# Recent errors
tail -100 /Users/dhogan/Library/Logs/privatePhotos.log | grep ERROR

# Last startup
tail -50 /Users/dhogan/Library/Logs/privatePhotos.log | grep -A5 "startup complete"
```

## Troubleshooting

### Port 8009 Already in Use

```bash
# Find process
lsof -ti :8009

# Kill it
lsof -ti :8009 | xargs kill

# Restart
sudo launchctl bootstrap system /Library/LaunchDaemons/com.dhogan.privatePhotos.plist
```

### Frontend Not Built

```bash
# Check if files exist
ls -la /Users/dhogan/photos-app/static/dist/

# If missing, rebuild on dev machine and redeploy
cd mkc-private-photos/frontend
npm run build
rsync -av ../static/ macstudio:/Users/dhogan/photos-app/static/
```

### Database Not Found

```bash
# Verify shared DB exists
ls -la /Users/dhogan/invapp_v2/data/mkc_inventory.db

# Check permissions (should be readable by dhogan user)
stat /Users/dhogan/invapp_v2/data/mkc_inventory.db
```

### Pushover Notifications Not Working

```bash
# Check env vars are loaded
sudo launchctl print system/com.dhogan.privatePhotos | grep PUSHOVER

# If empty, keys not set in plist
sudo nano /Library/LaunchDaemons/com.dhogan.privatePhotos.plist
# Add actual keys, then restart
```

### Photos Not Loading

```bash
# Check photos directory exists and has correct permissions
ls -la /Users/dhogan/invapp_v2/data/private_photos/
# Should show originals/, display/, thumbs/ with drwx------ (700)

# Check environment variable
sudo launchctl print system/com.dhogan.privatePhotos | grep PRIVATE_PHOTOS_DIR
```

## Rollback

If something goes wrong:

```bash
# Stop photos app
ssh macstudio
sudo launchctl bootout system/com.dhogan.privatePhotos

# Remove Cloudflare routing
# - Delete photos.davechogan.com tunnel ingress
# - Delete photos Access application
# - Delete redirect rule

# Main app still has photos routes (no changes made yet)
# Test: https://inventory.davechogan.com/photos should still work
```

## Success Checklist

- [ ] Photos app running on port 8009
- [ ] Health check returns OK
- [ ] `https://photos.davechogan.com` loads (after login)
- [ ] Upload photo works
- [ ] Gallery displays photos
- [ ] Chat works
- [ ] Pushover notifications received
- [ ] Old URL redirects to new subdomain
- [ ] Main app still functional at `inventory.davechogan.com/collection`
- [ ] No errors in logs

## Next Steps

Once photos app is stable:

1. **Remove Photos from Main App**
   - Edit `mkc-inventory/app.py` (remove photos router)
   - Edit `mkc-inventory/routes/static_pages_routes.py` (remove `/photos` endpoint)
   - Delete photos-related files from main repo
   - Deploy updated main app

2. **Monitor**
   - Check logs for both apps daily for first week
   - Verify both processes auto-restart after Mac Studio reboot
   - Monitor disk space (two apps now)

3. **Document**
   - Update `docs/private-photos-and-operations.md`
   - Update `docs/photos-for-natalya.md` with new URL
   - Add photos app to backup procedures

## References

- Planning docs: `mkc-inventory/artifacts/plans/PRIVATE_PHOTOS_*.md`
- Photos app README: `README.md`
- Main app deploy: `mkc-inventory/scripts/push_release_to_macstudio.sh`
