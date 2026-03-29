import type { InventoryItem, InventoryResponse } from './types';

async function fetchJson<T>(url: string): Promise<T> {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`API ${res.status}: ${res.statusText} (${url})`);
  return res.json() as Promise<T>;
}

export async function getInventory(): Promise<InventoryResponse> {
  // The backend has two separate endpoints — fetch them in parallel.
  const [items, summary] = await Promise.all([
    fetchJson<InventoryItem[]>('/api/v2/inventory'),
    fetchJson<InventoryResponse['summary']>('/api/v2/inventory/summary'),
  ]);
  return { items, summary };
}

export function imageUrl(item: InventoryItem): string | null {
  return item.colorway_image_url ?? null;
}
