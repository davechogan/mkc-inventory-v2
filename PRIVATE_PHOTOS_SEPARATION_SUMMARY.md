# Private Photos App Separation - Planning Complete

**Date:** 2026-10-01  
**Status:** Planning documentation created, ready for implementation

## What Was Created

Two comprehensive planning documents have been created in `artifacts/plans/`:

### 1. PRIVATE_PHOTOS_APP_SEPARATION.md
**Main architectural plan covering:**
- Executive summary of the separation goal
- Current state analysis (code, database, deployment)
- Target architecture (standalone app structure)
- 5-phase migration plan:
  1. Code separation (create standalone app)
  2. Frontend build separation
  3. Database migration (optional, deferred)
  4. Deployment & URL routing
  5. Testing & validation
- Rollback procedure
- Risk analysis and mitigations
- Success criteria
- Post-separation benefits
- Future enhancement roadmap

**Key decision points:**
- Subdomain (`photos.davechogan.com`) vs path-based routing
- New repo vs artifacts subdirectory
- Shared vs separate database
- Timeline: 11-16 hours estimated

### 2. PRIVATE_PHOTOS_SEPARATION_CHECKLIST.md
**Detailed implementation checklist with:**
- Step-by-step commands for each phase
- Code snippets for all new files needed
- launchd configuration
- Cloudflare tunnel and Access setup
- Testing procedures
- Rollback steps
- Sign-off checklist

## Files Created

```
artifacts/plans/PRIVATE_PHOTOS_APP_SEPARATION.md (21.5 KB)
artifacts/plans/PRIVATE_PHOTOS_SEPARATION_CHECKLIST.md (25.7 KB)
```

## Next Steps

To commit these documents to the artifacts repository:

```bash
cd artifacts
git add plans/PRIVATE_PHOTOS*.md
git commit -m "[docs] private photos app separation plan and implementation checklist"
git push

cd ..
git add artifacts
git commit -m "[artifacts] update pointer for photos separation plans"
git push
```

## URL Decision Required

**Recommendation: Use subdomain approach**

Change:
- **From:** `https://inventory.davechogan.com/photos`
- **To:** `https://photos.davechogan.com` (with 301 redirect from old URL)

**Benefits:**
- Cleaner separation
- No path prefix conflicts
- Simpler Cloudflare routing
- Easier troubleshooting

**Alternative:** Keep path-based routing (more complex tunnel configuration)

## Key Implementation Highlights

### Phase 1: Standalone App Structure
- Copy 5 core files: `private_photos.py`, `private_chat.py`, routes, tests, frontend
- Create simplified `auth.py` (no tenant logic)
- New `app.py` entry point for FastAPI
- Minimal dependencies: FastAPI, Pillow, pillow-heif, httpx
- Port 8009 (main app stays on 8008)

### Phase 4: Deployment
- New launchd daemon: `com.dhogan.privatePhotos`
- Shared database and photo directory (no data migration needed)
- Cloudflare tunnel ingress for subdomain
- New or updated Cloudflare Access application
- Remove photos code from main app (clean separation)

### Testing
- 14 functional tests specified
- Push notification validation
- Performance checks
- Rollback procedure tested and documented

## Risk Summary

**Low risk:**
- No database changes (read from same DB)
- Photo files unchanged (same directory)
- Rollback is simple (revert main app, disable tunnel)
- Can test fully before removing from main app

**Medium risk:**
- Cloudflare routing misconfiguration (mitigated with staging test)
- launchd daemon conflicts (mitigated with distinct ports/labels)

## Files to Review

1. **`artifacts/plans/PRIVATE_PHOTOS_APP_SEPARATION.md`** - Read this first for full context
2. **`artifacts/plans/PRIVATE_PHOTOS_SEPARATION_CHECKLIST.md`** - Use during implementation

## Approval Required

Before implementation:
- [ ] Review both planning documents
- [ ] Decide: subdomain or path-based URL routing
- [ ] Decide: new repo location for photos app
- [ ] Schedule deployment window
- [ ] Backup Mac Studio database

## Questions?

See "Open Questions" section in PRIVATE_PHOTOS_APP_SEPARATION.md for decision points.
