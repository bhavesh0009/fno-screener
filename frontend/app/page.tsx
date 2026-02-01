"use client";

import { useState, useEffect, useMemo } from 'react';
import { API_ENDPOINT } from './config';
import type { Screen, ColumnDef, StockResult, ScreenResult, SortOrder, ScreenSortKey } from './types';

type SortKey = ScreenSortKey;

export default function ScreensPage() {
  const [screens, setScreens] = useState<Screen[]>([]);
  const [activeScreen, setActiveScreen] = useState<string | null>(null);
  const [results, setResults] = useState<ScreenResult | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [sortConfig, setSortConfig] = useState<{ key: SortKey; direction: SortOrder } | null>(null);

  useEffect(() => {
    fetch(`${API_ENDPOINT}/screens`)
      .then(res => res.json())
      .then(data => setScreens(data))
      .catch(err => setError('Failed to load screens'));
  }, []);

  const runScreen = async (screenId: string) => {
    setLoading(true);
    setActiveScreen(screenId);
    setError(null);
    setResults(null);
    try {
      const res = await fetch(`${API_ENDPOINT}/screens/${screenId}/run`);
      if (!res.ok) throw new Error('Failed to run screen');
      const data = await res.json();
      setResults(data);
    } catch (err: any) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  const handleSort = (key: SortKey) => {
    let direction: SortOrder = 'asc';
    if (sortConfig && sortConfig.key === key && sortConfig.direction === 'asc') {
      direction = 'desc';
    }
    setSortConfig({ key, direction });
  };

  const sortedResults = useMemo(() => {
    if (!results) return [];
    let sortableItems = [...results.results];
    if (sortConfig !== null) {
      sortableItems.sort((a, b) => {
        const aValue = a[sortConfig.key] ?? '';
        const bValue = b[sortConfig.key] ?? '';

        if (aValue < bValue) {
          return sortConfig.direction === 'asc' ? -1 : 1;
        }
        if (aValue > bValue) {
          return sortConfig.direction === 'asc' ? 1 : -1;
        }
        return 0;
      });
    }
    return sortableItems;
  }, [results, sortConfig]);

  const getSortIndicator = (key: SortKey) => {
    if (!sortConfig || sortConfig.key !== key) return '';
    return sortConfig.direction === 'asc' ? ' ↑' : ' ↓';
  };

  const renderCell = (col: ColumnDef, stock: StockResult) => {
    const value = stock[col.key as keyof StockResult];

    switch (col.type) {
      case 'symbol':
        return <span className="symbol">{value}</span>;
      case 'currency':
        return <span className="price">₹{Number(value).toFixed(2)}</span>;
      case 'percent':
        if (col.key === 'changePct') {
          const numVal = Number(value);
          return (
            <span className={numVal >= 0 ? 'change--positive' : 'change--negative'}>
              {numVal > 0 ? '+' : ''}{numVal.toFixed(2)}%
            </span>
          );
        }
        return value ? `${Number(value).toFixed(1)}%` : '—';
      case 'strength':
        if (!value || value === 'N/A') return null;
        return (
          <span className={`strength-badge strength-${String(value).split(' ')[0].toLowerCase()}`}>
            {value}
          </span>
        );
      case 'multiplier':
        return <span style={{ fontWeight: 'bold', color: 'var(--accent-primary)' }}>{value}x</span>;
      case 'date':
        return value;
      default:
        return value ?? '—';
    }
  };

  return (
    <main className="container">
      <header className="header" style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <div>
          <h1 className="header__title">Market Scanners</h1>
          <p className="header__subtitle">Real-time algorithmic screening for price & volume patterns</p>
        </div>
        <a href="/stocks" className="btn-stocks" style={{
          color: 'var(--text-secondary)',
          textDecoration: 'none',
          fontSize: '0.9rem',
          fontWeight: '500',
          border: '1px solid var(--border-color)',
          padding: '0.5rem 1rem',
          borderRadius: 'var(--radius-md)',
          background: 'var(--bg-secondary)'
        }}>
          View All Stocks →
        </a>
      </header>

      {/* Screen Cards Grid */}
      <section className="screens-grid">
        {screens.map(screen => (
          <div
            key={screen.id}
            className={`card screen-card ${activeScreen === screen.id ? 'active' : ''}`}
            onClick={() => runScreen(screen.id)}
          >
            <h3>{screen.title}</h3>
            <p>{screen.description}</p>
            <button className="btn-run" disabled={loading && activeScreen === screen.id}>
              {loading && activeScreen === screen.id ? 'Running...' : 'Run Scan'}
            </button>
          </div>
        ))}
        {screens.length === 0 && !error && <p>Loading screener definitions...</p>}
      </section>

      {/* Error Message */}
      {error && <div className="error-message">{error}</div>}

      {/* Results Table */}
      {results && (
        <section className="results-section">
          <h2>{results.screen} Results <span className="badge">{results.count}</span></h2>

          <div className="table-container">
            <table className="stock-table">
              <thead>
                <tr>
                  {results.columns.map(col => (
                    <th
                      key={col.key}
                      onClick={() => handleSort(col.key as SortKey)}
                      className={`sortable ${sortConfig?.key === col.key ? 'sorted' : ''}`}
                    >
                      {col.label}{getSortIndicator(col.key as SortKey)}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {sortedResults.map((stock) => (
                  <tr key={stock.symbol}>
                    {results.columns.map(col => (
                      <td key={col.key} className={col.type}>
                        {renderCell(col, stock)}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      )}

      <style jsx>{`
        .container {
          max-width: 1200px;
          margin: 0 auto;
          padding: 2rem;
        }
        .header {
          margin-bottom: 3rem;
        }
        .screens-grid {
          display: grid;
          grid-template-columns: repeat(auto-fill, minmax(320px, 1fr));
          gap: 1.5rem;
          margin-bottom: 3rem;
        }
        .screen-card {
          background: var(--bg-card);
          border: 1px solid var(--border-color);
          border-radius: var(--radius-lg);
          padding: 1.5rem;
          cursor: pointer;
          transition: transform 0.2s, border-color 0.2s, box-shadow 0.2s;
        }
        .screen-card:hover {
          transform: translateY(-2px);
          border-color: var(--accent-primary);
          box-shadow: 0 4px 12px var(--shadow-color);
        }
        .screen-card.active {
          border-color: var(--accent-primary);
          background: rgba(0, 217, 255, 0.05);
        }
        .screen-card h3 {
          margin-bottom: 0.5rem;
          color: var(--text-primary);
        }
        .screen-card p {
          color: var(--text-secondary);
          font-size: 0.9rem;
          margin-bottom: 1.5rem;
          line-height: 1.5;
        }
        .btn-run {
          width: 100%;
          background: var(--accent-gradient);
          color: white;
          border: none;
          padding: 0.75rem;
          border-radius: var(--radius-md);
          cursor: pointer;
          font-weight: 600;
          transition: opacity 0.2s;
        }
        .btn-run:disabled {
          opacity: 0.7;
          cursor: not-allowed;
        }
        .badge {
          background: var(--bg-hover);
          padding: 0.2rem 0.8rem;
          border-radius: 20px;
          font-size: 0.7em;
          vertical-align: middle;
          margin-left: 0.8rem;
          color: var(--accent-primary);
        }
        .error-message {
          color: #ff4444;
          background: rgba(255, 68, 68, 0.1);
          padding: 1rem;
          border-radius: 8px;
          margin-bottom: 2rem;
        }
        .results-section h2 {
          margin-bottom: 1rem;
          font-size: 1.5rem;
        }

        th.sortable {
          cursor: pointer;
          user-select: none;
        }
        th.sortable:hover {
          background-color: var(--bg-hover);
        }
        
        /* Reusing global table styles, but forcing some overrides if needed */
        .strength-badge {
          padding: 0.2rem 0.6rem;
          border-radius: 4px;
          font-size: 0.85rem;
          font-weight: 500;
        }
        .strength-full {
          background: rgba(30, 255, 0, 0.15);
          color: #2e8b57;
        }
        .strength-partial {
          background: rgba(255, 166, 0, 0.15);
          color: #d2691e;
        }
        
        .change--positive { color: var(--positive); }
        .change--negative { color: var(--negative); }
      `}</style>
    </main>
  );
}
