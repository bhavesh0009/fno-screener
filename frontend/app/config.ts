/**
 * Shared configuration for the frontend application.
 * Uses environment variables with sensible defaults for local development.
 */

// API Configuration
export const API_BASE = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:5001';
export const API_ENDPOINT = `${API_BASE}/api`;

// Pagination defaults
export const DEFAULT_PAGE_SIZE = 25;
export const DEFAULT_PAGE = 1;

// Feature flags (can be extended as needed)
export const CONFIG = {
    api: {
        base: API_BASE,
        endpoint: API_ENDPOINT,
    },
    pagination: {
        defaultPageSize: DEFAULT_PAGE_SIZE,
        defaultPage: DEFAULT_PAGE,
    },
} as const;
