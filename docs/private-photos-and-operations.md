# Private photos and running the app

The inventory app runs on the Mac Studio and is reached on the internet at `https://inventory.davechogan.com`. Day-to-day edits happen in the local clone at `/Users/dhogan/Applications/MKC_Inventory` on the MacBook. The live copy is `/Users/dhogan/invapp_v2` on the Mac Studio.

## What was added

A private photo page at `/photos`. It is for sharing pictures between two signed-in people. The pictures are not part of the knife catalog and are not served from `/static`.

Two permissions are separate:

| Permission | Env var | Current value | What that person sees |
|---|---|---|---|
| Upload | `PRIVATE_PHOTOS_UPLOAD_EMAILS` | `natalyashapran1@gmail.com,davechogan@gmail.com` | Choose from Photos, preview, send, and a Sent list with Remove |
| View | `PRIVATE_PHOTOS_VIEW_EMAILS` | `davechogan@gmail.com` | Gallery grouped by month, full-screen viewer, swipe and arrow keys |
| Admin | `PRIVATE_PHOTOS_ADMIN_EMAILS` | `davechogan@gmail.com` | View, plus Download and Delete on the open photo, and bulk select on the gallery. Delete removes the photos for everyone. Admin does not grant upload by itself. |

Someone with both permissions sees the uploader and the gallery. Someone with neither sees “This page is private.” A signed-out visitor is asked to sign in.

iPhone photos are often HEIC. The server keeps the original file and shows a JPEG. Location data stays in the original on disk and is left out of the JPEG the viewer loads. Originals, display JPEGs, and thumbnails live in `data/private_photos/` on the Mac Studio (`0700`, not web-accessible). Deploys exclude that directory and `data/mkc_inventory.db`.

The page is `https://inventory.davechogan.com/photos`. On an iPhone, **Choose from Photos** opens the Photos app picker. There is no `capture` attribute, so it does not force the camera.

Code:

- `private_photos.py` — allowlists, disk storage, JPEG derivatives
- `routes/private_photos_routes.py` — HTTP API
- `frontend/src/pages/Photos.tsx` — upload and viewer UI
- `migrations/migrate_v2.py` — `private_photos` table

API, all under `/api/private-photos`:

- `GET /access` — whether the signed-in email can upload or view
- `GET /?scope=all` — viewer gallery
- `GET /?scope=mine` — photos that email uploaded
- `POST /` — upload (field name `files`)
- `GET /{id}/thumb` — thumbnail
- `GET /{id}/image` — viewer image (view permission only)
- `GET /{id}/download` — admin downloads the viewer JPEG
- `POST /bulk-download` — admin downloads the selected viewer JPEGs as `private-photos.zip` (`{"ids": [...]}`, at most 100)
- `POST /bulk-delete` — admin removes the selected photos
- `DELETE /{id}` — admin removes any photo; an uploader removes only their own

## How the Mac Studio process runs

launchd owns the app. The agent is `com.mkc.inventory`, loaded from:

```text
/Users/dhogan/Library/LaunchAgents/com.mkc.inventory.plist
```

It listens on port 8008, logs to `/tmp/mkc_app.log`, and KeepAlive is on. Killing the process makes launchd start it again. That is why `./scripts/run.sh` on the Mac Studio reports “Port 8008 still in use after 5 seconds.”

`./scripts/run.sh` is for a machine where launchd is not supervising the app. On the Mac Studio, start and stop through launchctl.

### Restart (also how config changes take effect)

SSH to the Studio, or use a Terminal window there:

```bash
launchctl bootout "gui/$(id -u)/com.mkc.inventory"
launchctl bootstrap "gui/$(id -u)" ~/Library/LaunchAgents/com.mkc.inventory.plist
```

`kickstart` reuses the already loaded job and will not see plist edits. `bootout` then `bootstrap` reads the file again.

If `bootstrap` prints `Bootstrap failed: 5: Input/output error`, the service was unloaded and the first load lost a race. Run bootstrap once more:

```bash
launchctl enable "gui/$(id -u)/com.mkc.inventory"
launchctl bootstrap "gui/$(id -u)" ~/Library/LaunchAgents/com.mkc.inventory.plist
```

Confirm:

```bash
lsof -nP -iTCP:8008 -sTCP:LISTEN
curl -s -o /dev/null -w '%{http_code}\n' http://127.0.0.1:8008/photos
```

`200` means the page is up. The photo API still requires a Cloudflare login.

### Stop

```bash
launchctl bootout "gui/$(id -u)/com.mkc.inventory"
```

The agent stays disabled until bootstrap. Rebooting the Mac Studio will load it again because the plist is in `~/Library/LaunchAgents` and `RunAtLoad` is true.

### See that the email lists are actually loaded

```bash
launchctl print "gui/$(id -u)/com.mkc.inventory" | grep PRIVATE_PHOTOS
```

## Configure who can upload and view

Edit the LaunchAgent plist on the Mac Studio, not `invapp_v2/scripts/com.mkc.inventory.plist`. A code deploy overwrites the copy inside the app folder.

Inside `EnvironmentVariables`:

```xml
<key>PRIVATE_PHOTOS_UPLOAD_EMAILS</key>
<string>natalyashapran1@gmail.com,davechogan@gmail.com</string>
<key>PRIVATE_PHOTOS_VIEW_EMAILS</key>
<string>davechogan@gmail.com</string>
<key>PRIVATE_PHOTOS_ADMIN_EMAILS</key>
<string>davechogan@gmail.com</string>
```

Values are comma-separated and matched to the Cloudflare login email, lowercased. After editing, restart with `bootout` and `bootstrap` above.

A photo-only account that has no knife collection is sent to `/photos` instead of the “create a collection” screen.

## Deploy code from the MacBook

The Mac Studio home folder must be mounted on this Mac as `/Volumes/dhogan`. In Finder: Go → Connect to Server → `smb://Mac-Studio.local/dhogan` (or `smb://192.168.50.89/dhogan`). The live tree is then `/Volumes/dhogan/invapp_v2`.

SSH host `macstudio` is `Mac-Studio.local`, user `dhogan` (`~/.ssh/config`).

From `/Users/dhogan/Applications/MKC_Inventory`:

```bash
export DEPLOY_HOST="macstudio"
export DEPLOY_USER="dhogan"
export DEPLOY_PATH="/Volumes/dhogan/invapp_v2"
export DEPLOY_REMOTE_PATH="/Users/dhogan/invapp_v2"

scripts/push_release_to_macstudio.sh --dry-run
scripts/push_release_to_macstudio.sh --release
```

`DEPLOY_PATH` is the shared folder on this Mac. `DEPLOY_REMOTE_PATH` is that same folder as the Studio sees it, which is where SSH restarts the app.

The release run backs up `data/mkc_inventory.db` next to the database, rsyncs the code, installs Python dependencies (including `pillow-heif` for HEIC), and restarts the process on port 8008. It does not copy the database or `data/private_photos/`.

If the shell says `permission denied`, the script’s executable bit is missing. Run `bash scripts/push_release_to_macstudio.sh --release` instead.

rsync warnings that `docs` or `.worktrees` are not empty are leftover directories on the Studio. They do not block the deploy.

The deploy’s restart goes through `./scripts/run.sh` over SSH. launchd may immediately claim port 8008 again. If the site is up afterward, leave it. If a later `run.sh` on the Studio fails with the port still in use, use the launchctl restart above. Email lists live in the LaunchAgent, so a launchctl restart is what keeps them.

## Cloudflare

Access already sits in front of the site through a Cloudflare Tunnel. There is one Access application for `inventory.davechogan.com` (notes name it `mkc-app`). Keep that one application. A second application has its own login session and causes a sign-in loop.

Paths already on the application, matched as prefixes:

- `/collection`
- `/api` (this covers `/api/private-photos`)
- `/master` (catalog and `/master/admin`)
- `/reporting`
- `/identify`

`/` stays public so the landing page does not require login.

### Add the photo page

1. Open [Cloudflare Zero Trust](https://one.dash.cloudflare.com).
2. Go to **Access controls → Applications**.
3. Open the self-hosted application whose hostname is `inventory.davechogan.com`.
4. Add a public hostname:
   - Subdomain: `inventory`
   - Domain: `davechogan.com`
   - Path: `photos`
5. Save. Leave the existing hostnames in place.

No separate hostname is required for the API.

### Allow her to sign in

1. Open the Allow policy already attached to that application.
2. Add an Include selector: **Emails** → `natalyashapran1@gmail.com`, if it is not already listed.
3. Leave Google and the email one-time PIN as the login methods.
4. Save.

The login email and the LaunchAgent email must be the same string. `davechogan@gmail.com` is already on the Allow policy.

### Check

On her iPhone, Safari, open `https://inventory.davechogan.com/photos` and sign in as `natalyashapran1@gmail.com`. She should see **Choose from Photos**. Sign in as `davechogan@gmail.com` and the page shows **Choose from Photos** and the gallery.

If the public knife landing page appears instead of a Cloudflare login, `/photos` was not saved on that application. If Cloudflare says she is not allowed, the Allow policy email does not match the Google account she used.
