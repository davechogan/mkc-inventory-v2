/**
 * Private photo share.
 * Upload and view are separate permissions from /api/private-photos/access.
 * The iPhone control is a normal photo picker (Photos app), not the camera.
 */

import { useCallback, useEffect, useRef, useState } from 'react';
import { Sidebar } from '../components/Sidebar';

const SIDEBAR_KEY = 'mkc_sidebar_collapsed';
const PHOTO_BYTES = 40 * 1024 * 1024;
const AUDIO_BYTES = 40 * 1024 * 1024;
const VIDEO_BYTES = 250 * 1024 * 1024;

interface PhotoAccess {
  authenticated: boolean;
  can_upload: boolean;
  can_view: boolean;
  can_admin: boolean;
  local_login: boolean;
  local_session: boolean;
}

type MediaKind = 'photo' | 'video' | 'audio';
type Gallery = 'mine' | 'received';

interface PhotoItem {
  id: string;
  width: number | null;
  height: number | null;
  taken_at: string | null;
  created_at: string;
  uploaded_by_email: string;
  media_kind: MediaKind;
}

interface QueuedFile {
  key: string;
  file: File;
}

function thumbUrl(id: string): string {
  return `/api/private-photos/${id}/thumb`;
}

function imageUrl(id: string): string {
  return `/api/private-photos/${id}/image`;
}

function downloadUrl(id: string): string {
  return `/api/private-photos/${id}/download`;
}

function mediaUrl(id: string): string {
  return `/api/private-photos/${id}/media`;
}

function fileKind(file: File): MediaKind | null {
  const name = file.name.toLowerCase();
  if (file.type.startsWith('video/') || /\.(mp4|m4v|mov|webm)$/.test(name)) return 'video';
  if (file.type.startsWith('audio/') || /\.(m4a|mp3|wav|aac|caf)$/.test(name)) return 'audio';
  if (file.type.startsWith('image/') || /\.(heic|heif|jpe?g|png|webp|gif)$/.test(name)) return 'photo';
  return null;
}

function formatWhen(photo: PhotoItem): string {
  const raw = photo.taken_at || photo.created_at;
  const date = new Date(raw);
  if (Number.isNaN(date.getTime())) return '';
  return date.toLocaleDateString(undefined, { month: 'short', day: 'numeric', year: 'numeric' });
}

function monthKey(photo: PhotoItem): string {
  const raw = photo.taken_at || photo.created_at;
  const date = new Date(raw);
  if (Number.isNaN(date.getTime())) return 'Undated';
  return date.toLocaleDateString(undefined, { month: 'long', year: 'numeric' });
}

function fileLabel(file: File): string {
  const mb = file.size / (1024 * 1024);
  const size = mb >= 0.1 ? `${mb.toFixed(1)} MB` : `${Math.max(1, Math.round(file.size / 1024))} KB`;
  return `${file.name || 'Photo'} · ${size}`;
}

async function readAccess(): Promise<PhotoAccess> {
  const denied: PhotoAccess = {
    authenticated: false,
    can_upload: false,
    can_view: false,
    can_admin: false,
    local_login: false,
    local_session: false,
  };
  try {
    const response = await fetch('/api/private-photos/access');
    const type = response.headers.get('content-type') || '';
    if (!type.includes('application/json')) return denied;
    const data = await response.json();
    return {
      authenticated: Boolean(data.authenticated),
      can_upload: Boolean(data.can_upload),
      can_view: Boolean(data.can_view),
      can_admin: Boolean(data.can_admin),
      local_login: Boolean(data.local_login),
      local_session: Boolean(data.local_session),
    };
  } catch {
    return denied;
  }
}

async function readPhotos(scope: Gallery): Promise<PhotoItem[]> {
  const response = await fetch(`/api/private-photos?scope=${scope}`);
  if (!response.ok) throw new Error('Could not load photos.');
  const data = await response.json();
  return (data.photos ?? []) as PhotoItem[];
}

function postOne(file: File, onProgress: (fraction: number) => void): Promise<void> {
  return new Promise((resolve, reject) => {
    const body = new FormData();
    body.append('files', file, file.name || 'photo.jpg');
    const xhr = new XMLHttpRequest();
    xhr.open('POST', '/api/private-photos');
    xhr.upload.onprogress = (event) => {
      if (event.lengthComputable && event.total > 0) {
        onProgress(event.loaded / event.total);
      }
    };
    xhr.onload = () => {
      if (xhr.status >= 200 && xhr.status < 300) {
        resolve();
        return;
      }
      let detail = 'Upload failed.';
      try {
        const parsed = JSON.parse(xhr.responseText);
        if (typeof parsed.detail === 'string') detail = parsed.detail;
        else if (Array.isArray(parsed.errors) && parsed.errors[0]?.detail) detail = parsed.errors[0].detail;
      } catch {
        /* keep default */
      }
      reject(new Error(detail));
    };
    xhr.onerror = () => reject(new Error('Network error. Check your connection and try again.'));
    xhr.send(body);
  });
}

function LocalPreview({ file }: { file: File }) {
  const [url, setUrl] = useState<string | null>(null);
  const [failed, setFailed] = useState(false);

  useEffect(() => {
    const objectUrl = URL.createObjectURL(file);
    setUrl(objectUrl);
    setFailed(false);
    return () => URL.revokeObjectURL(objectUrl);
  }, [file]);

  if (!url || failed) {
    return (
      <div className="absolute inset-0 flex items-center justify-center bg-card text-muted text-xs px-2 text-center">
        {fileKind(file) === 'audio' ? 'Voice' : 'Preview after send'}
      </div>
    );
  }
  if (fileKind(file) === 'video') {
    return <video src={url} muted playsInline preload="metadata" className="absolute inset-0 w-full h-full object-cover" />;
  }
  if (fileKind(file) === 'audio') {
    return (
      <div className="absolute inset-0 flex items-center justify-center bg-card text-muted text-sm">Voice</div>
    );
  }
  return (
    <img
      src={url}
      alt=""
      className="absolute inset-0 w-full h-full object-cover"
      onError={() => setFailed(true)}
    />
  );
}

function Uploader({ onUploaded }: { onUploaded: () => void }) {
  const [queue, setQueue] = useState<QueuedFile[]>([]);
  const [sending, setSending] = useState(false);
  const [progress, setProgress] = useState<{ index: number; total: number; fraction: number } | null>(null);
  const [note, setNote] = useState<string | null>(null);
  const [problems, setProblems] = useState<string[]>([]);
  const inputRef = useRef<HTMLInputElement>(null);

  const addFiles = (list: FileList | File[]) => {
    const next: QueuedFile[] = [];
    const rejected: string[] = [];
    for (const file of Array.from(list)) {
      const kind = fileKind(file);
      if (!kind) {
        rejected.push(`${file.name || 'A file'} is not a photo, video, or voice recording.`);
        continue;
      }
      const limit = kind === 'video' ? VIDEO_BYTES : kind === 'audio' ? AUDIO_BYTES : PHOTO_BYTES;
      const limitLabel = kind === 'video' ? '250 MB' : '40 MB';
      if (file.size > limit) {
        rejected.push(`${file.name || 'A file'} is over ${limitLabel}.`);
        continue;
      }
      next.push({ key: `${file.name}-${file.size}-${file.lastModified}-${Math.random()}`, file });
    }
    if (next.length) {
      setQueue((current) => [...current, ...next]);
      setNote(null);
    }
    if (rejected.length) setProblems(rejected);
  };

  const send = async () => {
    if (!queue.length || sending) return;
    setSending(true);
    setProblems([]);
    setNote(null);
    const batch = queue;
    const failed: string[] = [];
    let sent = 0;
    for (let i = 0; i < batch.length; i += 1) {
      const item = batch[i];
      setProgress({ index: i + 1, total: batch.length, fraction: 0 });
      try {
        await postOne(item.file, (fraction) => {
          setProgress({ index: i + 1, total: batch.length, fraction });
        });
        sent += 1;
        setQueue((current) => current.filter((entry) => entry.key !== item.key));
      } catch (err) {
        failed.push(`${item.file.name || 'Photo'}: ${err instanceof Error ? err.message : 'Upload failed.'}`);
      }
    }
    setProgress(null);
    setSending(false);
    if (sent) {
      setNote(sent === 1 ? '1 file sent.' : `${sent} files sent.`);
      onUploaded();
    }
    if (failed.length) setProblems(failed);
  };

  return (
    <section className="bg-card border border-border rounded-2xl p-4 md:p-6">
      <h2 className="text-ink text-lg font-semibold">Send</h2>
      <p className="text-muted text-sm mt-1 mb-4">
        Photos, videos, or voice recordings. They go to your “From me” gallery, which the other person can open.
      </p>

      <label
        className="relative flex items-center justify-center min-h-14 w-full rounded-xl bg-gold text-black font-semibold text-base cursor-pointer overflow-hidden active:opacity-80"
      >
        Choose files
        <input
          ref={inputRef}
          type="file"
          accept="image/*,video/*,audio/*,.heic,.heif,.m4a,.mp3,.wav,.aac,.caf,.mp4,.mov,.m4v,.webm"
          multiple
          className="absolute inset-0 opacity-0 cursor-pointer text-base"
          onChange={(event) => {
            if (event.target.files?.length) addFiles(event.target.files);
            event.target.value = '';
          }}
        />
      </label>

      <div
        className="mt-3 rounded-xl border border-dashed border-border px-4 py-6 text-center text-muted text-sm hidden md:block"
        onDragOver={(event) => event.preventDefault()}
        onDrop={(event) => {
          event.preventDefault();
          if (event.dataTransfer.files?.length) addFiles(event.dataTransfer.files);
        }}
      >
        Or drop files here
      </div>

      {queue.length > 0 && (
        <>
          <ul className="grid grid-cols-3 sm:grid-cols-4 gap-2 mt-4">
            {queue.map((item) => (
              <li key={item.key} className="relative aspect-square rounded-lg overflow-hidden bg-surface">
                <LocalPreview file={item.file} />
                <button
                  type="button"
                  aria-label={`Remove ${item.file.name || 'photo'}`}
                  disabled={sending}
                  onClick={() => setQueue((current) => current.filter((entry) => entry.key !== item.key))}
                  className="absolute top-1 right-1 w-7 h-7 rounded-full bg-black/70 text-white text-sm"
                >
                  ×
                </button>
                <div className="absolute bottom-0 inset-x-0 bg-black/55 text-white text-[10px] px-1.5 py-1 truncate">
                  {fileLabel(item.file)}
                </div>
              </li>
            ))}
          </ul>
          <button
            type="button"
            onClick={() => void send()}
            disabled={sending}
            className="mt-4 w-full min-h-12 rounded-xl border border-gold text-gold font-semibold disabled:opacity-50"
          >
            {sending && progress
              ? `Sending ${progress.index} of ${progress.total}… ${Math.round(progress.fraction * 100)}%`
              : `Send ${queue.length} ${queue.length === 1 ? 'file' : 'files'}`}
          </button>
        </>
      )}

      {note && <p className="text-gold text-sm mt-3">{note}</p>}
      {problems.map((problem) => (
        <p key={problem} className="text-red-400 text-sm mt-2">{problem}</p>
      ))}
    </section>
  );
}

function TileMedia({ item }: { item: PhotoItem }) {
  if (item.media_kind === 'video') {
    return <video src={mediaUrl(item.id)} muted playsInline preload="metadata" className="w-full h-full object-cover" />;
  }
  if (item.media_kind === 'audio') {
    return <div className="w-full h-full flex items-center justify-center bg-card text-muted text-xs">Voice</div>;
  }
  return <img src={thumbUrl(item.id)} alt="" className="w-full h-full object-cover" />;
}

function Viewer({ refreshKey, canAdmin, gallery }: { refreshKey: number; canAdmin: boolean; gallery: Gallery }) {
  const [photos, setPhotos] = useState<PhotoItem[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [index, setIndex] = useState<number | null>(null);
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [busy, setBusy] = useState<'download' | 'delete' | null>(null);
  const touchX = useRef<number | null>(null);

  useEffect(() => {
    setLoading(true);
    setIndex(null);
    setSelected(new Set());
    readPhotos(gallery)
      .then((items) => {
        setPhotos(items);
        setError(null);
      })
      .catch(() => setError('Could not load files.'))
      .finally(() => setLoading(false));
  }, [refreshKey, gallery]);

  const close = useCallback(() => setIndex(null), []);
  const prev = useCallback(() => {
    setIndex((current) => (current == null ? current : (current - 1 + photos.length) % photos.length));
  }, [photos.length]);
  const next = useCallback(() => {
    setIndex((current) => (current == null ? current : (current + 1) % photos.length));
  }, [photos.length]);

  useEffect(() => {
    if (index == null) return;
    const onKey = (event: KeyboardEvent) => {
      if (event.key === 'Escape') close();
      if (event.key === 'ArrowLeft') prev();
      if (event.key === 'ArrowRight') next();
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [index, close, prev, next]);

  useEffect(() => {
    if (index == null) return;
    const upcoming = photos[index + 1] ?? photos[0];
    if (!upcoming || photos.length < 2) return;
    const preload = new Image();
    preload.src = imageUrl(upcoming.id);
  }, [index, photos]);

  if (loading) {
    return <div className="grid grid-cols-3 md:grid-cols-5 gap-2 mt-6">{Array.from({ length: 6 }, (_, i) => <div key={i} className="aspect-square skeleton rounded-lg" />)}</div>;
  }
  if (error) return <p className="text-red-400 text-sm mt-4">{error}</p>;
  if (!photos.length) {
    return (
      <p className="text-muted text-sm mt-6">
        {gallery === 'mine' ? 'You have not sent anything yet.' : 'Nothing from them yet.'}
      </p>
    );
  }

  const groups: { label: string; items: { photo: PhotoItem; index: number }[] }[] = [];
  photos.forEach((photo, photoIndex) => {
    const label = monthKey(photo);
    const group = groups.find((entry) => entry.label === label);
    const item = { photo, index: photoIndex };
    if (group) group.items.push(item);
    else groups.push({ label, items: [item] });
  });

  const open = index != null ? photos[index] : null;

  const removeOpen = async () => {
    if (!open || index == null) return;
    if (!window.confirm('Delete this file? It will disappear for everyone.')) return;
    const response = await fetch(`/api/private-photos/${open.id}`, { method: 'DELETE' });
    if (!response.ok) {
      setError('Could not delete that photo.');
      return;
    }
    const next = photos.filter((item) => item.id !== open.id);
    setPhotos(next);
    setSelected((current) => {
      const copy = new Set(current);
      copy.delete(open.id);
      return copy;
    });
    setIndex(next.length === 0 ? null : Math.min(index, next.length - 1));
  };

  const toggleSelected = (id: string) => {
    setSelected((current) => {
      const copy = new Set(current);
      if (copy.has(id)) copy.delete(id);
      else copy.add(id);
      return copy;
    });
  };

  const bulkDownload = async () => {
    const ids = [...selected];
    if (!ids.length || busy) return;
    setBusy('download');
    setError(null);
    try {
      const response = await fetch('/api/private-photos/bulk-download', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ ids }),
      });
      if (!response.ok) {
        setError('Could not download those photos.');
        return;
      }
      const blob = await response.blob();
      const url = URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url;
      link.download = 'private-photos.zip';
      link.click();
      URL.revokeObjectURL(url);
    } catch {
      setError('Could not download those photos.');
    } finally {
      setBusy(null);
    }
  };

  const bulkDelete = async () => {
    const ids = [...selected];
    if (!ids.length || busy) return;
    const label = ids.length === 1 ? 'this photo' : `these ${ids.length} photos`;
    if (!window.confirm(`Delete ${label}? They will disappear for everyone.`)) return;
    setBusy('delete');
    setError(null);
    try {
      const response = await fetch('/api/private-photos/bulk-delete', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ ids }),
      });
      if (!response.ok) {
        setError('Could not delete those photos.');
        return;
      }
      const removed = new Set(ids);
      const next = photos.filter((item) => !removed.has(item.id));
      setPhotos(next);
      setSelected(new Set());
      if (open && removed.has(open.id)) setIndex(null);
    } catch {
      setError('Could not delete those photos.');
    } finally {
      setBusy(null);
    }
  };

  return (
    <section className="mt-2">
      <div className="flex flex-wrap items-center gap-2 mb-4">
        <p className="text-muted text-sm">{photos.length} {photos.length === 1 ? 'file' : 'files'}</p>
        {canAdmin && selected.size > 0 && (
          <>
            <span className="text-ink text-sm">{selected.size} selected</span>
            <button type="button" onClick={() => setSelected(new Set(photos.map((item) => item.id)))} className="min-h-11 px-3 text-sm text-gold">Select all</button>
            <button type="button" onClick={() => setSelected(new Set())} className="min-h-11 px-3 text-sm text-muted">Clear</button>
            <button type="button" disabled={busy != null} onClick={() => void bulkDownload()} className="min-h-11 px-3 rounded-lg border border-border text-sm text-ink disabled:opacity-50">
              {busy === 'download' ? 'Preparing…' : 'Download'}
            </button>
            <button type="button" disabled={busy != null} onClick={() => void bulkDelete()} className="min-h-11 px-3 rounded-lg border border-red-400/50 text-sm text-red-300 disabled:opacity-50">
              {busy === 'delete' ? 'Deleting…' : 'Delete'}
            </button>
          </>
        )}
      </div>
      {error && <p className="text-red-400 text-sm mb-3">{error}</p>}
      {groups.map((group) => (
        <div key={group.label} className="mb-8">
          <h2 className="text-ink text-sm font-semibold mb-2">{group.label}</h2>
          <ul className="grid grid-cols-3 md:grid-cols-4 lg:grid-cols-6 gap-1.5 md:gap-2">
            {group.items.map(({ photo, index: photoIndex }) => (
              <li key={photo.id} className="relative">
                {canAdmin && (
                  <label className="absolute top-1 left-1 z-10 min-w-11 min-h-11 flex items-start justify-start">
                    <input
                      type="checkbox"
                      checked={selected.has(photo.id)}
                      onChange={() => toggleSelected(photo.id)}
                      aria-label="Select photo"
                      className="w-5 h-5 accent-gold"
                    />
                  </label>
                )}
                <button
                  type="button"
                  onClick={() => setIndex(photoIndex)}
                  className={`block w-full aspect-square rounded-md overflow-hidden bg-card focus:outline-none focus:ring-2 focus:ring-gold/70 ${selected.has(photo.id) ? 'ring-2 ring-gold' : ''}`}
                >
                  <TileMedia item={photo} />
                </button>
              </li>
            ))}
          </ul>
        </div>
      ))}

      {open && index != null && (
        <div
          className="fixed inset-0 z-[60] bg-black flex flex-col"
          style={{ paddingBottom: 'env(safe-area-inset-bottom)' }}
          onClick={close}
        >
          <div className="flex items-center justify-between px-4 py-3 text-white" onClick={(event) => event.stopPropagation()}>
            <div className="text-sm">
              <div>{formatWhen(open)}</div>
              <div className="text-white/60 text-xs">{index + 1} / {photos.length}</div>
            </div>
            <div className="flex items-center gap-2">
              {canAdmin && (
                <a
                  href={downloadUrl(open.id)}
                  className="min-h-11 px-3 inline-flex items-center rounded-lg border border-white/30 text-sm"
                >
                  Download
                </a>
              )}
              {(canAdmin || gallery === 'mine') && (
                <button
                  type="button"
                  onClick={() => void removeOpen()}
                  className="min-h-11 px-3 rounded-lg border border-red-400/50 text-red-300 text-sm"
                >
                  Delete
                </button>
              )}
              <button type="button" onClick={close} className="min-w-11 min-h-11 text-2xl" aria-label="Close">×</button>
            </div>
          </div>
          <div
            className="flex-1 relative flex items-center justify-center min-h-0"
            onClick={(event) => event.stopPropagation()}
            onTouchStart={(event) => { touchX.current = event.changedTouches[0].clientX; }}
            onTouchEnd={(event) => {
              if (touchX.current == null) return;
              const delta = event.changedTouches[0].clientX - touchX.current;
              touchX.current = null;
              if (delta > 48) prev();
              else if (delta < -48) next();
            }}
          >
            <button type="button" aria-label="Previous photo" onClick={prev} className="hidden md:flex absolute left-3 w-11 h-11 items-center justify-center rounded-full bg-white/10 text-white text-2xl">‹</button>
            {open.media_kind === 'video' && (
              <video src={mediaUrl(open.id)} controls playsInline className="max-w-full max-h-full" />
            )}
            {open.media_kind === 'audio' && (
              <div className="px-6 w-full max-w-md">
                <p className="text-white text-center mb-4">Voice recording</p>
                <audio src={mediaUrl(open.id)} controls className="w-full" />
              </div>
            )}
            {(open.media_kind === 'photo' || !open.media_kind) && (
              <img
                src={imageUrl(open.id)}
                alt=""
                className="max-w-full max-h-full object-contain select-none"
                draggable={false}
              />
            )}
            <button type="button" aria-label="Next photo" onClick={next} className="hidden md:flex absolute right-3 w-11 h-11 items-center justify-center rounded-full bg-white/10 text-white text-2xl">›</button>
          </div>
          <div className="flex gap-1 overflow-x-auto px-3 py-3" onClick={(event) => event.stopPropagation()}>
            {photos.map((photo, photoIndex) => (
              <button
                key={photo.id}
                type="button"
                onClick={() => setIndex(photoIndex)}
                className={`flex-shrink-0 w-12 h-12 rounded overflow-hidden ${photoIndex === index ? 'ring-2 ring-gold' : 'opacity-70'}`}
              >
                <TileMedia item={photo} />
              </button>
            ))}
          </div>
        </div>
      )}
    </section>
  );
}

export default function Photos() {
  const [sidebarCollapsed, setSidebarCollapsed] = useState<boolean>(
    () => localStorage.getItem(SIDEBAR_KEY) === 'true'
  );
  const [access, setAccess] = useState<PhotoAccess | null>(null);
  const [refreshKey, setRefreshKey] = useState(0);
  const [gallery, setGallery] = useState<Gallery>('received');
  const [signInError, setSignInError] = useState<string | null>(null);

  const signInLocal = async () => {
    const entered = window.prompt('Email address');
    if (entered == null) return;
    setSignInError(null);
    const response = await fetch('/api/private-photos/local-login', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ email: entered }),
    });
    if (!response.ok) {
      setSignInError('That email does not have access.');
      return;
    }
    setAccess(await readAccess());
  };

  const signOutLocal = async () => {
    await fetch('/api/private-photos/local-logout', { method: 'POST' });
    setSignInError(null);
    setAccess(await readAccess());
  };

  useEffect(() => {
    document.title = 'Private Photos';
    let robots = document.querySelector('meta[name="robots"]');
    if (!robots) {
      robots = document.createElement('meta');
      robots.setAttribute('name', 'robots');
      document.head.appendChild(robots);
    }
    robots.setAttribute('content', 'noindex, nofollow');
  }, []);

  useEffect(() => {
    const handler = (event: Event) => {
      const custom = event as CustomEvent<{ collapsed: boolean }>;
      setSidebarCollapsed(custom.detail.collapsed);
    };
    window.addEventListener('mkc-sidebar-toggle', handler);
    return () => window.removeEventListener('mkc-sidebar-toggle', handler);
  }, []);

  useEffect(() => {
    readAccess().then(setAccess);
  }, []);

  const marginClass = sidebarCollapsed ? 'md:ml-16' : 'md:ml-56';

  return (
    <div className="min-h-screen bg-surface">
      <Sidebar />
      <main className={`${marginClass} transition-[margin] duration-200 min-h-screen`}>
        <div className="pl-14 pr-4 md:px-8 pt-5 pb-10 max-w-5xl">
          <div className="flex items-center justify-between gap-3 mb-4">
            <h1 className="text-ink text-xl font-bold">Private Photos</h1>
            {access?.local_session && (
              <button type="button" onClick={() => void signOutLocal()} className="min-h-11 px-3 text-sm text-muted">
                Sign out
              </button>
            )}
          </div>
          {access == null && <div className="h-40 skeleton rounded-2xl" />}
          {access && !access.authenticated && (
            <div className="bg-card border border-border rounded-2xl p-6">
              <p className="text-ink mb-4">Sign in to use this private page.</p>
              {access.local_login && (
                <button
                  type="button"
                  onClick={() => void signInLocal()}
                  className="inline-flex items-center justify-center min-h-12 px-5 rounded-xl bg-gold text-black font-semibold"
                >
                  Sign in
                </button>
              )}
              {signInError && <p className="text-red-400 text-sm mt-3">{signInError}</p>}
            </div>
          )}
          {access && access.authenticated && !access.can_upload && !access.can_view && (
            <p className="text-muted">This page is private.</p>
          )}
          {access?.can_upload && (
            <Uploader onUploaded={() => { setGallery('mine'); setRefreshKey((value) => value + 1); }} />
          )}
          {access?.can_view && (
            <>
              <div className="flex gap-2 mt-6" role="tablist" aria-label="Galleries">
                {([
                  ['mine', 'From me'],
                  ['received', 'From them'],
                ] as const).map(([value, label]) => (
                  <button
                    key={value}
                    type="button"
                    role="tab"
                    aria-selected={gallery === value}
                    onClick={() => setGallery(value)}
                    className={`min-h-11 px-4 rounded-full text-sm font-semibold ${
                      gallery === value ? 'bg-gold text-black' : 'border border-border text-muted'
                    }`}
                  >
                    {label}
                  </button>
                ))}
              </div>
              <Viewer refreshKey={refreshKey} canAdmin={access.can_admin} gallery={gallery} />
            </>
          )}
        </div>
      </main>
    </div>
  );
}
