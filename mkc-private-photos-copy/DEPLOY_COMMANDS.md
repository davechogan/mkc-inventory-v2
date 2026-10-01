# Quick Deploy Commands - Mac Studio

**Copy and paste these commands to deploy the photos app.**

---

## From Development Machine

### 1. Deploy Code to Mac Studio

```bash
cd /workspace/mkc-private-photos

# Deploy code
rsync -av --delete \
  --exclude='.venv' \
  --exclude='node_modules' \
  --exclude='.git' \
  --exclude='__pycache__' \
  ./ macstudio:/Users/dhogan/photos-app/

echo "✓ Code deployed"
```

---

## On Mac Studio (SSH Session)

### 2. SSH to Mac Studio

```bash
ssh macstudio
cd /Users/dhogan/photos-app
```

### 3. Install Python Dependencies (First Time Only)

```bash
# Create virtual environment
python3.11 -m venv .venv

# Install dependencies
.venv/bin/pip install --upgrade pip
.venv/bin/pip install -r requirements.txt

# Verify
.venv/bin/python -c "import fastapi; import pillow_heif; print('✓ Dependencies OK')"
```

### 4. Extract Pushover Keys from Existing App

```bash
# View existing Pushover configuration
sudo grep -A1 "PUSHOVER" /Library/LaunchDaemons/com.dhogan.inventoryApp.plist

# Copy the three key values (between <string> tags):
# - PUSHOVER_USER_KEY
# - PUSHOVER_NATALYA_USER_KEY  
# - PUSHOVER_API_TOKEN
```

### 5. Configure launchd Plist

```bash
# Edit the plist template
nano scripts/com.dhogan.privatePhotos.plist

# Replace these placeholders with actual keys:
# - <!-- REPLACE WITH ACTUAL DAVE KEY FROM EXISTING PLIST -->
# - <!-- REPLACE WITH ACTUAL NATALYA KEY FROM EXISTING PLIST -->
# - <!-- REPLACE WITH ACTUAL TOKEN FROM EXISTING PLIST -->

# Save and exit (Ctrl+X, Y, Enter)
```

### 6. Install launchd Daemon

```bash
# Copy to system location
sudo cp scripts/com.dhogan.privatePhotos.plist /Library/LaunchDaemons/

# Set ownership and permissions
sudo chown root:wheel /Library/LaunchDaemons/com.dhogan.privatePhotos.plist
sudo chmod 644 /Library/LaunchDaemons/com.dhogan.privatePhotos.plist

echo "✓ launchd plist installed"
```

### 7. Start Photos App

```bash
# Load the daemon
sudo launchctl bootstrap system /Library/LaunchDaemons/com.dhogan.privatePhotos.plist

# Verify it's running
lsof -nP -iTCP:8009 -sTCP:LISTEN
# Should show: uvicorn ... (LISTEN)

# Test health endpoint
curl http://127.0.0.1:8009/health
# Should return: {"status":"ok", "db_exists":true, "static_built":true}

echo "✓ Photos app running on port 8009"
```

### 8. Check Logs

```bash
# View recent logs
tail -50 /Users/dhogan/Library/Logs/privatePhotos.log

# Follow logs in real-time
tail -f /Users/dhogan/Library/Logs/privatePhotos.log
# (Ctrl+C to exit)

# Check for errors
grep ERROR /Users/dhogan/Library/Logs/privatePhotos.log
# Should be empty
```

---

## Cloudflare Configuration (Web Browser)

### 9. Add Cloudflare Tunnel

1. Open https://one.dash.cloudflare.com
2. **Networks** → **Tunnels** → Select Mac Studio tunnel → **Configure**
3. **Public Hostname** → **Add a public hostname**:
   - Subdomain: `photos`
   - Domain: `davechogan.com`
   - Path: (leave blank)
   - Type: `HTTP`
   - URL: `localhost:8009`
4. Click **Save**

### 10. Create Cloudflare Access Application

1. **Access controls** → **Applications** → **Add an application**
2. Choose **Self-hosted**
3. Application configuration:
   - Name: `Photos App`
   - Session duration: `24 hours`
   - Subdomain: `photos`
   - Domain: `davechogan.com`
   - Path: (leave blank)
4. Click **Next**
5. Policy configuration:
   - Name: `Photos Users`
   - Action: `Allow`
   - Include → Emails:
     - `davechogan@gmail.com`
     - `natalyashapran1@gmail.com`
6. Click **Next** → **Add application**

### 11. Add Redirect Rule (Optional)

1. **Websites** → `davechogan.com` → **Rules** → **Redirect Rules**
2. **Create rule**:
   - Name: `Photos page redirect`
   - When incoming requests match:
     - Field: `Hostname`
     - Operator: `equals`
     - Value: `inventory.davechogan.com`
     - **AND**
     - Field: `URI Path`
     - Operator: `starts with`
     - Value: `/photos`
   - Then:
     - Type: `Dynamic`
     - Expression: `concat("https://photos.davechogan.com", substring(http.request.uri.path, 7))`
     - Status code: `301`
3. Click **Deploy**

---

## Testing

### 12. Test Local Access (On Mac Studio)

```bash
# Test health
curl http://127.0.0.1:8009/health

# Test page loads
curl -I http://127.0.0.1:8009/
# Should return: 200 OK
```

### 13. Test Public Access (Browser)

**Logged out:**
```
https://photos.davechogan.com
→ Should redirect to Cloudflare login
```

**Logged in (davechogan@gmail.com):**
```
https://photos.davechogan.com
→ Should show Photos page with "Choose from Photos" button
```

**Test upload:**
- Click "Choose from Photos"
- Select a photo
- Click "Send"
- Verify photo appears in gallery
- Check Pushover notification received

**Test redirect:**
```
https://inventory.davechogan.com/photos
→ Should 301 redirect to https://photos.davechogan.com
```

### 14. Verify Main App Still Works

```
https://inventory.davechogan.com/collection
→ Should load knife inventory normally
```

---

## Troubleshooting

### Service won't start

```bash
# Check logs
tail -100 /Users/dhogan/Library/Logs/privatePhotos.log

# Check if port is in use
lsof -i :8009

# Check launchd status
sudo launchctl print system/com.dhogan.privatePhotos
```

### Database not found

```bash
# Verify shared DB exists
ls -la /Users/dhogan/invapp_v2/data/mkc_inventory.db

# Check environment variable
sudo launchctl print system/com.dhogan.privatePhotos | grep MKC_INVENTORY_DB
```

### Pushover not working

```bash
# Check keys are loaded
sudo launchctl print system/com.dhogan.privatePhotos | grep PUSHOVER

# If empty, keys weren't set in plist - go back to step 5
```

### Restart Service

```bash
# Stop
sudo launchctl bootout system/com.dhogan.privatePhotos

# Start
sudo launchctl bootstrap system /Library/LaunchDaemons/com.dhogan.privatePhotos.plist

# Verify
lsof -nP -iTCP:8009 -sTCP:LISTEN
```

---

## Success Checklist

- [ ] Code deployed to `/Users/dhogan/photos-app`
- [ ] Python dependencies installed
- [ ] launchd plist configured with Pushover keys
- [ ] Service running on port 8009
- [ ] Health check returns OK
- [ ] Cloudflare tunnel configured
- [ ] Cloudflare Access application created
- [ ] Redirect rule created (optional)
- [ ] Public URL loads: `https://photos.davechogan.com`
- [ ] Upload works
- [ ] Pushover notifications received
- [ ] Old URL redirects properly
- [ ] Main app still works

---

## Quick Reference

**Restart photos app:**
```bash
sudo launchctl bootout system/com.dhogan.privatePhotos
sudo launchctl bootstrap system /Library/LaunchDaemons/com.dhogan.privatePhotos.plist
```

**View logs:**
```bash
tail -f /Users/dhogan/Library/Logs/privatePhotos.log
```

**Check status:**
```bash
lsof -nP -iTCP:8009 -sTCP:LISTEN
curl http://127.0.0.1:8009/health
```

**Re-deploy after code changes:**
```bash
# From dev machine
cd /workspace/mkc-private-photos
rsync -av --delete --exclude='.venv' --exclude='node_modules' \
  ./ macstudio:/Users/dhogan/photos-app/

# On Mac Studio
ssh macstudio
sudo launchctl bootout system/com.dhogan.privatePhotos
sudo launchctl bootstrap system /Library/LaunchDaemons/com.dhogan.privatePhotos.plist
```
