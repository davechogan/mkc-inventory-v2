/**
 * Tenant context — stores the active tenant and provides fetch wrapper.
 * All API calls should use tenantFetch() instead of fetch() to include X-Tenant-Id.
 */

const TENANT_STORAGE_KEY = 'mkc_active_tenant';

let _activeTenantId: string | null = localStorage.getItem(TENANT_STORAGE_KEY);

export function getActiveTenantId(): string | null {
  return _activeTenantId;
}

export function setActiveTenantId(tenantId: string | null): void {
  _activeTenantId = tenantId;
  if (tenantId) {
    localStorage.setItem(TENANT_STORAGE_KEY, tenantId);
  } else {
    localStorage.removeItem(TENANT_STORAGE_KEY);
  }
}

/**
 * Wrapper around fetch() that adds X-Tenant-Id header.
 * Use this for all API calls that should be tenant-scoped.
 */
export function tenantFetch(input: RequestInfo | URL, init?: RequestInit): Promise<Response> {
  const headers = new Headers(init?.headers);
  if (_activeTenantId && !headers.has('X-Tenant-Id')) {
    headers.set('X-Tenant-Id', _activeTenantId);
  }
  return fetch(input, { ...init, headers });
}
