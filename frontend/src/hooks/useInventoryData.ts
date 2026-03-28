import { useState, useEffect, useCallback } from 'react';
import { getInventory } from '../api';
import type { InventoryItem, Summary } from '../types';

interface UseInventoryDataResult {
  items: InventoryItem[];
  summary: Summary | null;
  loading: boolean;
  error: string | null;
  reload: () => void;
}

export function useInventoryData(): UseInventoryDataResult {
  const [items, setItems] = useState<InventoryItem[]>([]);
  const [summary, setSummary] = useState<Summary | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const data = await getInventory();
      setItems(data.items);
      setSummary(data.summary);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Unknown error');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void load();
  }, [load]);

  return { items, summary, loading, error, reload: load };
}
