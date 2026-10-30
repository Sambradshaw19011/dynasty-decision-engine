# Dynasty Decision Engine

A Python application for analyzing dynasty fantasy football player value, roster decisions, rookie draft picks, and trades.

## Project status
Initial development scaffold. The database schema, data sources, and MVP requirements will be finalized after reviewing the SRS.

## Getting started

### 1. Requirements
- Python 3.11 or newer
- Git
- VS Code (recommended)

PostgreSQL will be added after the initial environment and data-source checks.

### 2. Create and activate a virtual environment

**Windows PowerShell**
```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
```

**macOS/Linux**
```bash
python3 -m venv .venv
source .venv/bin/activate
```

### 3. Install dependencies
```bash
python -m pip install --upgrade pip
pip install -r requirements.txt
```

### 4. Verify the setup
```bash
python -m src.main
```

## Folder guide
- `data/raw/`: downloaded source data; do not commit large/private datasets
- `data/processed/`: cleaned datasets
- `data/external/`: manually obtained reference data
- `database/`: database connection and SQL scripts
- `src/ingestion/`: source-data retrieval
- `src/processing/`: cleaning and transformation
- `src/features/`: model feature creation
- `src/models/`: training and evaluation
- `src/valuation/`: player and draft-pick valuation
- `src/analysis/`: roster and trade analysis
- `src/api/`: future API layer
- `frontend/`: future user interface
- `notebooks/`: exploratory analysis
- `tests/`: automated tests
- `docs/`: project documentation

## Important
Never commit `.env` files, API keys, passwords, or private credentials. Copy `.env.example` to `.env` when configuration is needed.
