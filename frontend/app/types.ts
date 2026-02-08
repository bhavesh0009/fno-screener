/**
 * Shared TypeScript type definitions for the frontend application.
 */

// === API Response Types ===

export interface Screen {
    id: string;
    title: string;
    description: string;
}

export interface ColumnDef {
    key: string;
    label: string;
    type: 'symbol' | 'currency' | 'percent' | 'strength' | 'multiplier' | 'date' | string;
}

export interface StockResult {
    symbol: string;
    date: string;
    close: number;
    changePct: number;
    volume: number;
    deliveryPct?: number;
    volumeMult: number;
    strength?: string;
}

export interface ScreenResult {
    screen: string;
    count: number;
    columns: ColumnDef[];
    results: StockResult[];
}

export interface Stock {
    symbol: string;
    companyName: string;
    lotSize: number;
    date: string;
    open: number;
    high: number;
    low: number;
    close: number;
    prevClose: number;
    changePct: number;
    volume: number;
    value: number;
    deliveryVolume: number;
    deliveryPct: number;
    // Enhanced metrics
    ytdPct: number | null;
    pct1Y: number | null;
    pct1M: number | null;
    delta52wHigh: number | null;
    aboveSma20: boolean | null;
    aboveSma50: boolean | null;
    aboveSma200: boolean | null;
    rsRank: number | null;
    sparkline: number[];
}

export interface Stats {
    stockCount: number;
    positiveCount: number;
    lastUpdated: string | null;
}

export interface Pagination {
    page: number;
    limit: number;
    total: number;
    totalPages: number;
}

// === Sorting Types ===

export type SortOrder = 'asc' | 'desc';

export type ScreenSortKey = keyof StockResult | 'strength';

export type StockSortColumn = 'symbol' | 'close' | 'change_pct' | 'volume' | 'date' |
    'delivery_pct' | 'ytd_pct' | 'pct_1y' | 'pct_1m' | 'delta_52w_high' | 'rs_rank';

