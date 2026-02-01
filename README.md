# F&O Stock Screener System

A system to screen Indian F&O stocks using [nselib](https://github.com/RuchiTanmay/nselib) for data, DuckDB for storage, and Next.js for the frontend.

## Prerequisites

- Python 3.8+
- Node.js 18+

## Quick Start

The system consists of two parts containing a Backend (API & Data) and a Frontend (Dashboard). You need to run both.

### 1. Start the Backend (API Server)

This server runs on `http://localhost:5001`.

```bash
cd backend

# Create virtual environment (first time only)
python3 -m venv venv

# Activate virtual environment
# Windows:
# venv\Scripts\activate
# Mac/Linux:
source venv/bin/activate

# Install dependencies (first time only)
pip install -r requirements.txt

# Start the API server
python -m api.server
```

### 2. Start the Frontend (Dashboard)

This runs on `http://localhost:3001` (or 3000 if available).

```bash
cd frontend

# Install dependencies (first time only)
npm install

# Start the development server
npm run dev
```

### 3. Access the Dashboard

Open your browser and navigate to:
**[http://localhost:3001](http://localhost:3001)**

---

## Configuration

### Frontend Environment Variables

For production deployment, set the API URL:

```bash
NEXT_PUBLIC_API_URL=https://your-api-domain.com
```

The frontend will use `http://localhost:5001` as the default for local development.

---

## Data Management

The system stores data in `data/stocks.duckdb`.

### Refresh Data
To fetch the latest data for all F&O stocks (~208 stocks):

```bash
cd backend
source venv/bin/activate
python -m pipeline.main collect-all
```

*Note: This process takes a few minutes as it fetches data for each stock individually.*

### View Statistics
To see database stats from the command line:

```bash
cd backend
source venv/bin/activate
python -m pipeline.main stats
```

### Run Tests
To run the unit tests:

```bash
cd backend
source venv/bin/activate
python -m pytest tests/ -v
```

## Project Structure

```
price-vol-pattern/
├── backend/
│   ├── api/                   # API layer
│   │   ├── server.py          # Flask API server
│   │   └── screens.py         # Screen definitions (SQL queries)
│   ├── pipeline/              # Data pipeline
│   │   ├── main.py            # Data collection CLI
│   │   └── collectors/        # Data collection modules
│   │       └── nse_collector.py
│   ├── common/                # Shared utilities
│   │   ├── config.py          # Backend configuration
│   │   ├── logger.py          # Structured logging
│   │   └── db/                # Database utilities
│   │       └── schema.py
│   ├── tests/                 # Unit tests
│   │   ├── test_collectors.py
│   │   └── test_data_validation.py
│   └── logs/                  # Pipeline logs
├── frontend/
│   └── app/
│       ├── page.tsx           # Main scanners page
│       ├── stocks/            # All stocks page
│       ├── config.ts          # Frontend configuration
│       ├── types.ts           # Shared TypeScript types
│       └── globals.css        # Global styles
└── data/
    └── stocks.duckdb          # DuckDB database file
```
