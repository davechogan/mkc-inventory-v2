# Private photos and running the app

The inventory app runs on the Mac Studio and is reached on the internet at `https://inventory.davechogan.com`. Day-to-day edits happen in the local clone at `/Users/dhogan/Applications/MKC_Inventory` on the MacBook. The live copy is `/Users/dhogan/invapp_v2` on the Mac Studio.

## What was added

A private photo page at `/photos`. It is for sharing pictures between two signed-in people. The pictures are not part of the knife catalog and are not served from `/static`.

Two permissions are separate:

| Permission | Env var | Current value | What that person sees |
|---|---|---|---|
| Upload | `PRIVATE_PHOTOS_UPLOAD_EMAILS` | `natalyashapran1@gmail.com,davechogan@gmail.com` | Choose from Photos, preview, send, and a Sent list with Remove |
| View | `PRIVATE_PHOTOS_VIEW_EMAILS` | `davechogan@gmail.com` | Gallery grouped by month, full-screen viewer, swipe and arrow keys |
| Admin | `PRIVATE_PHOTOS_ADMIN_EMAILS` | `davechogan@gmail.com` | View, plus Download and Delete on the open photo, bulk select, and a Deleted tab. Delete hides a file for 30 days. Restore puts it back. After 30 days the original, display JPEG, and thumbnail are removed. Admin does not grant upload by itself. |

Someone with both permissions sees the uploader and the gallery. Someone with neither sees “This page is private.” A signed-out visitor is asked to sign in.

iPhone photos are often HEIC. The server keeps the original file and shows a JPEG. Location data stays in the original on disk and is left out of the JPEG the viewer loads. Originals, display JPEGs, and thumbnails live in `data/private_photos/` on the Mac Studio (`0700`, not web-accessible). Deploys exclude that directory and `data/mkc_inventory.db`.

The page is `https://inventory.davechogan.com/photos`. On an iPhone, **Choose from Photos** opens the Photos app picker. There is no `capture` attribute, so it does not force the camera.

Code:

- `private_photos.py` — allowlists, disk storage, JPEG derivatives, Pushover
- `private_chat.py` — the one text thread
- `routes/private_photos_routes.py` — HTTP API
- `frontend/src/pages/Photos.tsx` — upload, viewer, and chat UI
- `migrations/migrate_v2.py` — `private_photos`, `private_chat_messages`, `private_chat_reads`

API, all under `/api/private-photos`:

- `GET /access` — whether the signed-in email can upload or view
- `GET /?scope=all` — viewer gallery
- `GET /?scope=mine` — photos that email uploaded
- `GET /chat` — the thread, newest 200 messages, and the unread count
- `POST /chat` — send `{"body": "..."}` (2000 characters). Notifies the other phone when that user key is set.
- `POST /chat/read` — mark the thread read for the signed-in person
- `POST /` — upload (field name `files`, optional `captions` in the same order)
- `PATCH /{id}/caption` — uploader or admin sets `{"caption": "..."}`; blank clears it
- `GET /{id}/thumb` — thumbnail
- `GET /{id}/image` — viewer image (view permission only)
- `GET /{id}/download` — admin downloads the viewer JPEG
- `POST /bulk-download` — admin downloads the selected viewer JPEGs as `private-photos.zip` (`{"ids": [...]}`, at most 100)
- `GET /?scope=deleted` — admin lists files hidden in the last 30 days
- `POST /bulk-delete` — admin hides the selected photos for 30 days
- `POST /restore` — admin puts the selected hidden photos back (`{"ids": [...]}`)
- `DELETE /{id}` — admin hides any photo; an uploader hides only their own. Files stay on disk for 30 days.

## How the Mac Studio process runs

launchd owns the app. After a reboot the job that starts is the system daemon `com.dhogan.inventoryApp`, loaded from:

```text
/Library/LaunchDaemons/com.dhogan.inventoryApp.plist
```

That file is the one to edit. An older user agent, `~/Library/LaunchAgents/com.mkc.inventory.plist`, is not what comes up at boot. Photo permissions and Pushover keys have to be in the daemon plist or the photos page loads as “This page is private.”

It listens on port 8008, logs to `/Users/dhogan/Library/Logs/inventoryApp.log`, and KeepAlive is on. Killing the process makes launchd start it again. That is why `./scripts/run.sh` on the Mac Studio reports “Port 8008 still in use after 5 seconds.”

`./scripts/run.sh` is for a machine where launchd is not supervising the app. On the Mac Studio, start and stop through launchctl. These commands need sudo because the job is a system daemon.

### Restart (also how config changes take effect)

SSH to the Studio, or use a Terminal window there:

```bash
sudo launchctl bootout system/com.dhogan.inventoryApp
sudo launchctl bootstrap system /Library/LaunchDaemons/com.dhogan.inventoryApp.plist
```

`kickstart` reuses the already loaded job and will not see plist edits. `bootout` then `bootstrap` reads the file again.

Confirm:

```bash
lsof -nP -iTCP:8008 -sTCP:LISTEN
curl -s -o /dev/null -w '%{http_code}\n' http://127.0.0.1:8008/photos
```

`200` means the page is up. The photo API still requires a Cloudflare login.

### Stop

```bash
sudo launchctl bootout system/com.dhogan.inventoryApp
```

The daemon stays unloaded until bootstrap. Rebooting the Mac Studio starts it again because the plist is in `/Library/LaunchDaemons` and `RunAtLoad` is true.

### See that the email lists are actually loaded

```bash
sudo launchctl print system/com.dhogan.inventoryApp | grep PRIVATE_PHOTOS
```

## Configure who can upload and view

Edit `/Library/LaunchDaemons/com.dhogan.inventoryApp.plist` on the Mac Studio. A code deploy does not replace that file. Do not edit `invapp_v2/scripts/com.mkc.inventory.plist` or expect the older user agent to apply after a reboot.

Inside `EnvironmentVariables`:

```xml
<key>PRIVATE_PHOTOS_UPLOAD_EMAILS</key>
<string>natalyashapran1@gmail.com,davechogan@gmail.com</string>
<key>PRIVATE_PHOTOS_VIEW_EMAILS</key>
<string>davechogan@gmail.com</string>
<key>PRIVATE_PHOTOS_ADMIN_EMAILS</key>
<string>davechogan@gmail.com</string>
<key>PUSHOVER_USER_KEY</key>
<string>the user key from the Pushover dashboard</string>
<key>PUSHOVER_NATALYA_USER_KEY</key>
<string></string>
<key>PUSHOVER_API_TOKEN</key>
<string>the application token from pushover.net/apps</string>
```

Values are comma-separated and matched to the Cloudflare login email, lowercased. After editing, restart with `bootout` and `bootstrap` above.

When `PUSHOVER_USER_KEY` and `PUSHOVER_API_TOKEN` are set, each successful upload sends one notice to Dave's phone: who sent it, and how many photos, videos, and voice recordings. The message links to the photos page. `PUSHOVER_NATALYA_USER_KEY` is her Pushover user key for chat notices. Leave it empty until that key exists. A chat message notifies the other person only, and a blank key skips that phone. If a key is missing, or Pushover does not answer, the upload or the chat message still succeeds. Do not put those keys in the git repo.

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
