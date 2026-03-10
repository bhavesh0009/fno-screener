"""Configuration settings for the stock screener."""
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# Project paths
PROJECT_ROOT = Path(__file__).parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data"
LOGS_DIR = Path(__file__).parent.parent / "logs"

# Supabase PostgreSQL connection string
DATABASE_URL = os.environ.get("DATABASE_URL")

# Data collection settings
DATA_PERIOD = "1Y"  # 1 year of historical data (legacy, for nselib period param)
DATA_START_DATE = "01-01-2025"  # Fixed start date (dd-mm-yyyy format for nselib)

# Rate limiting (NSE may block if too many requests)
REQUEST_DELAY_SECONDS = 0.2  # Delay between API calls
