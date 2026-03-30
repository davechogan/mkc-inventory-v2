import { useState, useEffect, useCallback, useRef } from 'react';
import { getInventory } from '../api';
import type { InventoryItem, Summary } from '../types';

const POLL_INTERVAL_MS = 30_000; // 30 seconds

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
  const initialLoadDone = useRef(false);

  const load = useCallback(async () => {
    // Only show loading spinner on initial load, not on polls
    if (!initialLoadDone.current) setLoading(true);
    setError(null);
    try {
      const data = await getInventory();
      setItems(data.items);
      setSummary(data.summary);
      initialLoadDone.current = true;
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Unknown error');
    } finally {
      setLoading(false);
    }
  }, []);

  // Initial load
  useEffect(() => {
    void load();
  }, [load]);

  // Silent background poll
  useEffect(() => {
    const id = setInterval(() => { void load(); }, POLL_INTERVAL_MS);
    return () => clearInterval(id);
  }, [load]);

  return { items, summary, loading, error, reload: load };
}
