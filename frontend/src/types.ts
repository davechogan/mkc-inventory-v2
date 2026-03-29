export interface InventoryItem {
  id: number;
  knife_name: string;
  knife_model_id: number;
  knife_type: string | null;
  knife_family: string | null;
  form_name: string | null;
  series_name: string | null;
  catalog_line: string | null;
  handle_color: string | null;
  blade_steel: string | null;
  blade_finish: string | null;
  blade_color: string | null;
  blade_length: number | null;
  condition: string | null;
  location: string | null;
  quantity: number;
  purchase_price: number | null;
  estimated_value: number | null;
  acquired_date: string | null;
  nickname: string | null;
  notes: string | null;
  colorway_image_url: string | null;
  has_identifier_image: boolean;
  is_collab: boolean;
  collaboration_name: string | null;
  mkc_order_number: string | null;
  purchase_source: string | null;
  last_sharpened: string | null;
}

export interface FamilyStat {
  family: string;
  total_quantity: number;
  inventory_rows: number;
}

export interface Summary {
  inventory_rows: number;
  total_quantity: number;
  total_spend: number;
  estimated_value?: number;
  total_estimated_value?: number;
  master_count?: number;
  master_models?: number;
  catalog_total?: number;
  by_family: FamilyStat[];
}

export interface InventoryResponse {
  items: InventoryItem[];
  summary: Summary;
}

export type SortDir = 'asc' | 'desc';
export interface SortState {
  col: string;
  dir: SortDir;
}

export interface FilterState {
  search: string;
  family: string;
  handleColor: string;
  condition: string;
  series: string;
  location: string;
}
