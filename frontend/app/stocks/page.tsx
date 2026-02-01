'use client';

import { useState, useEffect, useCallback } from 'react';
import { API_BASE, DEFAULT_PAGE_SIZE } from '../config';
import type { Stock, Stats, Pagination, SortOrder, StockSortColumn } from '../types';

// Sparkline component using SVG
const Sparkline = ({ data }: { data: number[] }) => {
    if (!data || data.length < 2) {
        return <div className="sparkline-container">—</div>;
    }

    const width = 80;
    const height = 28;
    const padding = 2;

    const min = Math.min(...data);
    const max = Math.max(...data);
    const range = max - min || 1;

    // Create path
    const points = data.map((value, index) => {
        const x = padding + (index / (data.length - 1)) * (width - padding * 2);
        const y = height - padding - ((value - min) / range) * (height - padding * 2);
        return `${x},${y}`;
    });

    const path = `M ${points.join(' L ')}`;

    // Determine color based on start vs end
    const trend = data[data.length - 1] > data[0] ? 'positive' :
        data[data.length - 1] < data[0] ? 'negative' : 'neutral';

    return (
        <div className="sparkline-container">
            <svg className="sparkline-svg" viewBox={`0 0 ${width} ${height}`} preserveAspectRatio="none">
                <path d={path} className={`sparkline--${trend}`} />
            </svg>
        </div>
    );
};

// SMA Arrow indicator component
const SmaIndicator = ({ above }: { above: boolean | null }) => {
    if (above === null || above === undefined) {
        return <span className="sma-indicator sma-indicator--neutral">—</span>;
    }
    return (
        <span className={`sma-indicator ${above ? 'sma-indicator--above' : 'sma-indicator--below'}`}>
            {above ? '▲' : '▼'}
        </span>
    );
};

// Heatmap cell component
const HeatmapCell = ({ value, suffix = '%' }: { value: number | null; suffix?: string }) => {
    if (value === null || value === undefined) {
        return <span className="heatmap-cell heatmap-cell--neutral">—</span>;
    }

    // Determine intensity level (1-5 based on absolute value)
    const absVal = Math.abs(value);
    let level: number;
    if (absVal < 5) level = 1;
    else if (absVal < 15) level = 2;
    else if (absVal < 30) level = 3;
    else if (absVal < 50) level = 4;
    else level = 5;

    const colorType = value > 0 ? 'positive' : value < 0 ? 'negative' : 'neutral';
    const className = value === 0
        ? 'heatmap-cell heatmap-cell--neutral'
        : `heatmap-cell heatmap-cell--${colorType}-${level}`;

    return (
        <span className={className}>
            {value > 0 ? '+' : ''}{value.toFixed(1)}{suffix}
        </span>
    );
};

// RS Rank cell component
const RsRankCell = ({ value }: { value: number | null }) => {
    if (value === null || value === undefined) {
        return <span className="rs-rank rs-rank--neutral">—</span>;
    }

    const className = value > 5 ? 'rs-rank rs-rank--strong' :
        value < -5 ? 'rs-rank rs-rank--weak' :
            'rs-rank rs-rank--neutral';

    return (
        <span className={className}>
            {value > 0 ? '+' : ''}{value.toFixed(1)}%
        </span>
    );
};

export default function StocksPage() {
    const [stocks, setStocks] = useState<Stock[]>([]);
    const [stats, setStats] = useState<Stats | null>(null);
    const [pagination, setPagination] = useState<Pagination>({
        page: 1,
        limit: DEFAULT_PAGE_SIZE,
        total: 0,
        totalPages: 0,
    });
    const [search, setSearch] = useState('');
    const [sortBy, setSortBy] = useState<StockSortColumn>('symbol');
    const [sortOrder, setSortOrder] = useState<SortOrder>('asc');
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);

    const fetchStats = useCallback(async () => {
        try {
            const response = await fetch(`${API_BASE}/api/stats`);
            if (!response.ok) throw new Error('Failed to fetch stats');
            const data = await response.json();
            setStats(data);
        } catch {
            console.error('Error fetching stats');
        }
    }, []);

    const fetchStocks = useCallback(async () => {
        setLoading(true);
        setError(null);
        try {
            const params = new URLSearchParams({
                page: pagination.page.toString(),
                limit: pagination.limit.toString(),
                search,
                sortBy,
                sortOrder,
            });

            const response = await fetch(`${API_BASE}/api/stocks?${params}`);
            if (!response.ok) throw new Error('Failed to fetch stocks');

            const data = await response.json();
            setStocks(data.stocks);
            setPagination(prev => ({
                ...prev,
                total: data.pagination.total,
                totalPages: data.pagination.totalPages,
            }));
        } catch {
            setError('Unable to connect to API server. Make sure the backend is running.');
        } finally {
            setLoading(false);
        }
    }, [pagination.page, pagination.limit, search, sortBy, sortOrder]);

    useEffect(() => {
        fetchStats();
        fetchStocks();
    }, [fetchStats, fetchStocks]);

    const handleSort = (column: StockSortColumn) => {
        if (sortBy === column) {
            setSortOrder(sortOrder === 'asc' ? 'desc' : 'asc');
        } else {
            setSortBy(column);
            setSortOrder('asc');
        }
        setPagination(prev => ({ ...prev, page: 1 }));
    };

    const handleSearch = (e: React.ChangeEvent<HTMLInputElement>) => {
        setSearch(e.target.value);
        setPagination(prev => ({ ...prev, page: 1 }));
    };

    const formatVolume = (vol: number): string => {
        if (vol >= 10000000) return (vol / 10000000).toFixed(2) + ' Cr';
        if (vol >= 100000) return (vol / 100000).toFixed(2) + ' L';
        if (vol >= 1000) return (vol / 1000).toFixed(2) + ' K';
        return vol.toString();
    };

    const formatCurrency = (val: number): string => {
        return new Intl.NumberFormat('en-IN', {
            style: 'currency',
            currency: 'INR',
            minimumFractionDigits: 2,
        }).format(val);
    };

    const getSortIndicator = (column: StockSortColumn) => {
        if (sortBy !== column) return '';
        return sortOrder === 'asc' ? ' ↑' : ' ↓';
    };

    const getChangeClass = (pct: number | null): string => {
        if (pct === null) return 'change change--neutral';
        if (pct > 0) return 'change change--positive';
        if (pct < 0) return 'change change--negative';
        return 'change change--neutral';
    };

    return (
        <div className="container">
            <header className="header" style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <div>
                    <h1 className="header__title">All Stocks Data</h1>
                    <p className="header__subtitle">Search and filter complete F&O universe with technical indicators</p>
                </div>
                <a href="/" className="btn-home" style={{
                    color: 'var(--text-secondary)',
                    textDecoration: 'none',
                    fontSize: '0.9rem',
                    display: 'flex',
                    alignItems: 'center',
                    gap: '0.5rem'
                }}>
                    ← Back to Scanners
                </a>
            </header>

            {/* Stats Cards */}
            <div className="stats-grid">
                <div className="stat-card">
                    <div className="stat-card__label">Total Stocks</div>
                    <div className="stat-card__value stat-card__value--accent">
                        {stats?.stockCount ?? '—'}
                    </div>
                </div>
                <div className="stat-card">
                    <div className="stat-card__label">Data Points</div>
                    <div className="stat-card__value">
                        {stats?.ohlcvCount?.toLocaleString() ?? '—'}
                    </div>
                </div>
                <div className="stat-card">
                    <div className="stat-card__label">Data Range</div>
                    <div className="stat-card__value" style={{ fontSize: '1rem' }}>
                        {stats?.dateRange?.from ?? '—'} to {stats?.dateRange?.to ?? '—'}
                    </div>
                </div>
            </div>

            {/* Controls */}
            <div className="controls">
                <input
                    type="text"
                    className="search-input"
                    placeholder="Search by symbol or company name..."
                    value={search}
                    onChange={handleSearch}
                />
            </div>

            {/* Stock Table */}
            <div className="table-container">
                {loading ? (
                    <div className="loading">
                        <div className="loading__spinner"></div>
                        Loading stocks...
                    </div>
                ) : error ? (
                    <div className="error">{error}</div>
                ) : (
                    <>
                        <div className="table-scroll-container">
                            <table className="stock-table stock-table--compact stock-table--sticky">
                                <thead>
                                    <tr>
                                        <th
                                            className={`col-symbol ${sortBy === 'symbol' ? 'sorted' : ''}`}
                                            onClick={() => handleSort('symbol')}
                                        >
                                            Symbol{getSortIndicator('symbol')}
                                        </th>
                                        <th
                                            className={`col-price ${sortBy === 'close' ? 'sorted' : ''}`}
                                            onClick={() => handleSort('close')}
                                        >
                                            Price{getSortIndicator('close')}
                                        </th>
                                        <th
                                            className={`col-percent ${sortBy === 'change_pct' ? 'sorted' : ''}`}
                                            onClick={() => handleSort('change_pct')}
                                        >
                                            Day{getSortIndicator('change_pct')}
                                        </th>
                                        <th
                                            className={`col-percent ${sortBy === 'ytd_pct' ? 'sorted' : ''}`}
                                            onClick={() => handleSort('ytd_pct')}
                                        >
                                            YTD{getSortIndicator('ytd_pct')}
                                        </th>
                                        <th className="col-sparkline">
                                            1Y Chart
                                        </th>
                                        <th
                                            className={`col-percent ${sortBy === 'pct_1y' ? 'sorted' : ''}`}
                                            onClick={() => handleSort('pct_1y')}
                                        >
                                            1Y{getSortIndicator('pct_1y')}
                                        </th>
                                        <th
                                            className={`col-percent ${sortBy === 'pct_1m' ? 'sorted' : ''}`}
                                            onClick={() => handleSort('pct_1m')}
                                        >
                                            1M{getSortIndicator('pct_1m')}
                                        </th>
                                        <th
                                            className={`col-percent ${sortBy === 'delta_52w_high' ? 'sorted' : ''}`}
                                            onClick={() => handleSort('delta_52w_high')}
                                        >
                                            Δ52wH{getSortIndicator('delta_52w_high')}
                                        </th>
                                        <th
                                            className={`col-percent ${sortBy === 'rs_rank' ? 'sorted' : ''}`}
                                            onClick={() => handleSort('rs_rank')}
                                        >
                                            RS Rank{getSortIndicator('rs_rank')}
                                        </th>
                                        <th className="col-sma" title="Price vs 20 SMA">20</th>
                                        <th className="col-sma" title="Price vs 50 SMA">50</th>
                                        <th className="col-sma" title="Price vs 200 SMA">200</th>
                                        <th
                                            className={sortBy === 'volume' ? 'sorted' : ''}
                                            onClick={() => handleSort('volume')}
                                        >
                                            Vol{getSortIndicator('volume')}
                                        </th>
                                        <th
                                            className={sortBy === 'delivery_pct' ? 'sorted' : ''}
                                            onClick={() => handleSort('delivery_pct')}
                                        >
                                            Del%{getSortIndicator('delivery_pct')}
                                        </th>
                                    </tr>
                                </thead>
                                <tbody>
                                    {stocks.map((stock) => (
                                        <tr key={stock.symbol}>
                                            <td className="col-symbol">
                                                <div className="symbol">{stock.symbol}</div>
                                                <div className="company-name">{stock.companyName}</div>
                                            </td>
                                            <td className="price col-price">{formatCurrency(stock.close)}</td>
                                            <td className="col-percent">
                                                <span className={getChangeClass(stock.changePct)}>
                                                    {stock.changePct !== null
                                                        ? `${stock.changePct > 0 ? '+' : ''}${stock.changePct?.toFixed(2)}%`
                                                        : '—'}
                                                </span>
                                            </td>
                                            <td className="col-percent">
                                                <HeatmapCell value={stock.ytdPct} />
                                            </td>
                                            <td className="col-sparkline">
                                                <Sparkline data={stock.sparkline} />
                                            </td>
                                            <td className="col-percent">
                                                <HeatmapCell value={stock.pct1Y} />
                                            </td>
                                            <td className="col-percent">
                                                <HeatmapCell value={stock.pct1M} />
                                            </td>
                                            <td className="col-percent">
                                                <HeatmapCell value={stock.delta52wHigh} />
                                            </td>
                                            <td className="col-percent">
                                                <RsRankCell value={stock.rsRank} />
                                            </td>
                                            <td className="col-sma">
                                                <SmaIndicator above={stock.aboveSma20} />
                                            </td>
                                            <td className="col-sma">
                                                <SmaIndicator above={stock.aboveSma50} />
                                            </td>
                                            <td className="col-sma">
                                                <SmaIndicator above={stock.aboveSma200} />
                                            </td>
                                            <td className="volume">{formatVolume(stock.volume)}</td>
                                            <td>{stock.deliveryPct ? Number(stock.deliveryPct).toFixed(1) : '—'}%</td>
                                        </tr>
                                    ))}
                                </tbody>
                            </table>
                        </div>

                        {/* Pagination */}
                        <div className="pagination">
                            <div className="pagination__info">
                                Showing {((pagination.page - 1) * pagination.limit) + 1} to{' '}
                                {Math.min(pagination.page * pagination.limit, pagination.total)} of{' '}
                                {pagination.total} stocks
                            </div>
                            <div className="pagination__controls">
                                <button
                                    className="pagination__btn"
                                    disabled={pagination.page <= 1}
                                    onClick={() => setPagination(prev => ({ ...prev, page: prev.page - 1 }))}
                                >
                                    Previous
                                </button>
                                <button
                                    className="pagination__btn"
                                    disabled={pagination.page >= pagination.totalPages}
                                    onClick={() => setPagination(prev => ({ ...prev, page: prev.page + 1 }))}
                                >
                                    Next
                                </button>
                            </div>
                        </div>
                    </>
                )}
            </div>
        </div>
    );
}
