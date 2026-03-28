import type { InventoryItem, InventoryResponse } from './types';

export async function getInventory(): Promise<InventoryResponse> {
  const res = await fetch('/api/v2/inventory');
  if (!res.ok) {
    throw new Error(`Failed to fetch inventory: ${res.status} ${res.statusText}`);
  }
  return res.json() as Promise<InventoryResponse>;
}

export function imageUrl(item: InventoryItem): string | null {
  if (item.colorway_image_url) {
    return item.colorway_image_url;
  }
  if (item.has_identifier_image) {
    return `/api/v2/models/${item.knife_model_id}/image`;
  }
  return null;
}
