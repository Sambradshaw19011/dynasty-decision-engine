# Dynasty Decision Engine

Dynasty Decision Engine is a fantasy football analytics project for evaluating dynasty player value, roster decisions, rookie draft picks, and trades.

## Data and architecture

- **NFLverse** is the source of truth for NFL players and identifiers, teams, games, schedules, football statistics, usage, and other football data. NFLverse fantasy-point and projection fields are not warehouse fantasy truth.
- **Sleeper** is the source of truth for fantasy-specific player-directory data, fantasy points and statistics where verified, league/scoring settings, rosters, matchups, drafts, and transactions.
- **Supabase/PostgreSQL** is the planned central store for curated, query-serving warehouse data. Raw and high-volume data, especially play-by-play, should remain in local/object storage unless curated for database use.
- **Python** will orchestrate ingestion and validation. **PySpark** is intended for substantial transformations; **Parquet** is the local staging format. These pipeline components are not implemented yet.

See [docs/architecture.md](docs/architecture.md), [docs/data_sources.md](docs/data_sources.md), and [docs/database_schema.md](docs/database_schema.md) for design boundaries and current status.

## Project status

This repository is an early scaffold. The setup entry point and setup test exist, and exploratory NFLverse and Sleeper notebooks contain prior research. Production ingestion clients, transformations, player crosswalk, validation, Supabase connectivity/loaders, migrations, analytics, and valuation logic are not implemented yet. Do not treat the placeholder `database/connection.py` or `database/schema.sql` as operational.

The preserved exploration notebooks are:

- [NFLverse exploration](notebooks/exploratory_analysis/explore_nflverse.ipynb)
- [Sleeper exploration](notebooks/exploratory_analysis/explore_sleeper.ipynb)

## Getting started

### Requirements

- Python 3.11 or newer
- Git
- VS Code (recommended)

### Create and activate a virtual environment

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

### Install dependencies and configure local environment

```bash
python -m pip install --upgrade pip
pip install -r requirements.txt
```

Copy `.env.example` to `.env` and add local Supabase/PostgreSQL connection values when database integration is implemented. The current scaffold does not yet read these values or connect to a database. `.env` is ignored by Git; never commit credentials.

### Verify the setup

```bash
python -m src.main
pytest
```

## Repository guide

- `config/`: application environment settings
- `data/raw/`: source-specific raw data; do not commit downloaded datasets
- `data/processed/`: source-specific processed data
- `data/staging/`: local intermediate data, planned as Parquet
- `database/migrations/`: planned PostgreSQL schema migrations; none are implemented
- `database/`: existing database placeholders and future Supabase integration
- `src/ingestion/`: future source-specific NFLverse and Sleeper ingestion
- `src/processing/`: future data cleaning and transformation
- `src/features/`, `src/models/`, `src/valuation/`: future modeling and dynasty valuation
- `src/analytics/`: future roster, player, usage, and matchup analysis
- `src/validation/`: future data and database validation
- `notebooks/exploratory_analysis/`: preserved source research notebooks
- `tests/`: automated tests
- `docs/`: architecture, sources, schema plan, dictionary, and pipeline status
- `frontend/`: future user interface
