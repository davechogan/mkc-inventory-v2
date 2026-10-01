# MKC Private Photos & Chat

Standalone application for private photo sharing between allowlisted users.

Extracted from the main MKC inventory application to improve separation of concerns and enable independent deployment.

## Features

- **Photo Upload & Gallery**: HEIC conversion, JPEG derivatives, thumbnail generation
- **Chat**: Simple text messaging between allowlisted users
- **Push Notifications**: Pushover integration for uploads and messages
- **Access Control**: Email-based allowlists (upload, view, admin permissions)
- **Soft Delete**: 30-day retention window for deleted photos
- **Cloudflare Access**: Authentication via Cloudflare Zero Trust

## Development

### Prerequisites

- Python 3.11+
- Access to shared SQLite database (`mkc_inventory.db`)
- Access to shared photo storage directory

### Setup

```bash
# Create virtual environment
python3.11 -m venv .venv

# Install dependencies
.venv/bin/pip install -r requirements.txt
```

### Configuration

Set environment variables:

```bash
# Required: Path to shared database
export MKC_INVENTORY_DB="/path/to/mkc_inventory.db"

# Required: Path to photo storage directory
export PRIVATE_PHOTOS_DIR="/path/to/private_photos"

# Optional: Custom port (default: 8009)
export PORT=8009

# Required for production: Email allowlists
export PRIVATE_PHOTOS_UPLOAD_EMAILS="user1@example.com,user2@example.com"
export PRIVATE_PHOTOS_VIEW_EMAILS="user1@example.com"
export PRIVATE_PHOTOS_ADMIN_EMAILS="admin@example.com"

# Optional: Pushover notifications
export PUSHOVER_USER_KEY="your-user-key"
export PUSHOVER_NATALYA_USER_KEY="other-user-key"
export PUSHOVER_API_TOKEN="your-api-token"
```

### Run Locally

```bash
./scripts/run.sh
# App starts on http://localhost:8009
```

### Run Tests

```bash
# Point to test database
export MKC_INVENTORY_DB="/tmp/test_photos.db"

# Run tests
.venv/bin/pytest tests/ -v
```

## Architecture

```
┌─────────────────────────┐
│   FastAPI Application   │
│      (Port 8009)        │
├─────────────────────────┤
│  Routes: /api/*         │
│  - Photo upload/list    │
│  - Chat messages        │
│  - Delete/restore       │
├─────────────────────────┤
│  Business Logic         │
│  - private_photos.py    │
│  - private_chat.py      │
├─────────────────────────┤
│  Auth Middleware        │
│  - Cloudflare Access    │
│  - Local email cookie   │
└─────────────────────────┘
           │
           ▼
┌─────────────────────────┐
│  Shared SQLite DB       │
│  - private_photos       │
│  - private_chat_*       │
└─────────────────────────┘
           │
           ▼
┌─────────────────────────┐
│  Photo File Storage     │
│  - originals/           │
│  - display/             │
│  - thumbs/              │
└─────────────────────────┘
```

## API Endpoints

All endpoints under `/api` prefix:

### Photos
- `GET /api/access` - Check user permissions
- `GET /api?scope={all|mine|received|deleted}` - List photos
- `POST /api` - Upload photos (multipart/form-data)
- `GET /api/{id}/thumb` - Thumbnail image
- `GET /api/{id}/image` - Full viewer image
- `GET /api/{id}/download` - Download attachment (admin)
- `GET /api/{id}/media` - Video/audio playback
- `PATCH /api/{id}/caption` - Update caption
- `DELETE /api/{id}` - Delete photo
- `POST /api/bulk-delete` - Delete multiple (admin)
- `POST /api/bulk-download` - Download ZIP (admin)
- `POST /api/restore` - Restore deleted photos (admin)

### Chat
- `GET /api/chat` - List recent messages
- `POST /api/chat` - Send message
- `POST /api/chat/read` - Mark thread as read

### Local Access (LAN only)
- `POST /api/local-login` - Email-based local login
- `POST /api/local-logout` - Clear local session

### System
- `GET /health` - Health check

## Deployment (Mac Studio)

### Install

```bash
# Deploy code to /Users/dhogan/photos-app
rsync -av --exclude='.venv' --exclude='node_modules' \
  mkc-private-photos/ macstudio:/Users/dhogan/photos-app/

# Install dependencies on Mac Studio
ssh macstudio
cd /Users/dhogan/photos-app
python3.11 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

### Configure launchd

Create `/Library/LaunchDaemons/com.dhogan.privatePhotos.plist` with environment variables and service configuration. See deployment documentation for full plist template.

### Start/Stop/Restart

```bash
# Start (or restart)
sudo launchctl bootout system/com.dhogan.privatePhotos
sudo launchctl bootstrap system /Library/LaunchDaemons/com.dhogan.privatePhotos.plist

# Check status
lsof -nP -iTCP:8009 -sTCP:LISTEN
curl -I http://127.0.0.1:8009/health
```

### Logs

```bash
tail -f /Users/dhogan/Library/Logs/privatePhotos.log
```

## Cloudflare Configuration

### Tunnel

Add public hostname:
- Subdomain: `photos`
- Domain: `davechogan.com`
- Service: `http://localhost:8009`

### Access Application

Create or update Access application:
- Domain: `photos.davechogan.com`
- Allow policy: emails from `PRIVATE_PHOTOS_*_EMAILS` env vars

### Redirect (Optional)

Redirect old URL to new subdomain:
- From: `inventory.davechogan.com/photos`
- To: `photos.davechogan.com`
- Status: 301

## Database Schema

Tables in shared `mkc_inventory.db`:

### `private_photos`
- `id` (primary key, UUID hex)
- `original_name`, `original_suffix`
- `byte_size`, `width`, `height`
- `taken_at`, `created_at`
- `uploaded_by_email`
- `media_kind` (photo|video|audio)
- `caption`
- `deleted_at`, `deleted_by_email`

### `private_chat_messages`
- `id` (primary key, UUID hex)
- `sender_email`
- `body`
- `created_at`

### `private_chat_reads`
- `email` (primary key)
- `last_read_at`

## Security

- All photo bytes served with `Cache-Control: private, no-store`
- EXIF data stripped from JPEG derivatives
- File permissions: 0700 on storage directories
- Authentication required for all endpoints (except `/health`)
- Email allowlists control upload/view/admin access
- Cloudflare Access protects public URL

## Development Workflow

1. Make changes to code
2. Test locally with `./scripts/run.sh`
3. Run tests with pytest
4. Commit to git
5. Deploy to Mac Studio via rsync
6. Restart launchd service

## Troubleshooting

### Port already in use
```bash
lsof -ti :8009 | xargs kill
```

### Database locked
- Check that WAL mode is enabled: `PRAGMA journal_mode=WAL`
- Verify connection timeout is set (default: 10s)

### Photos not loading
- Check file permissions on `PRIVATE_PHOTOS_DIR` (should be 0700)
- Verify `PRIVATE_PHOTOS_DIR` env var is set correctly
- Check that files exist: `ls -la $PRIVATE_PHOTOS_DIR/{originals,display,thumbs}`

### Pushover not working
- Verify env vars are set: `PUSHOVER_USER_KEY`, `PUSHOVER_API_TOKEN`
- Check logs for "Pushover notification failed" warnings
- Test Pushover API directly: `curl -X POST https://api.pushover.net/1/messages.json ...`

## Related Documentation

- **Planning**: See `mkc-inventory/artifacts/plans/PRIVATE_PHOTOS_*.md`
- **Main App**: See `mkc-inventory` repository
- **User Guide**: See `mkc-inventory/docs/photos-for-natalya.md`

## License

Private repository - not for public distribution.

## Contact

For issues or questions, contact the repository owner.
