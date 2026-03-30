export interface InventoryItem {
  id: number;
  knife_name: string;
  knife_model_id: number;
  knife_type: string | null;
  knife_family: string | null;
  form_name: string | null;
  series_name: string | null;
  collaborator_name: string | null;
  is_collab: boolean;
  catalog_line: string | null;
  handle_color: string | null;
  blade_color: string | null;
  blade_steel: string | null;
  blade_finish: string | null;
  handle_type: string | null;
  blade_length: number | null;
  colorway_id: number | null;
  colorway_image_url: string | null;
  has_identifier_image: boolean;
  quantity: number;
  purchase_price: number | null;
  acquired_date: string | null;
  mkc_order_number: string | null;
  location: string | null;
  notes: string | null;
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
  series: string;
  location: string;
}
