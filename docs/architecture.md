# Architecture

## Intended flow

1. Python ingestion/orchestration retrieves NFLverse football data and Sleeper fantasy data independently.
2. Preserve raw files or API responses in `data/raw/`, including source endpoint, retrieval time, and coverage metadata.
3. Use Parquet in local staging and PySpark for substantial transformations; write source-specific processed outputs to `data/processed/`.
4. Validate player identity and transformed data before loading curated facts, dimensions, and query-serving analytics into Supabase/PostgreSQL.
5. Keep the full high-volume play-by-play dataset in Parquet or object storage by default. Load only selected curated facts or aggregates to the database.
6. Build fantasy analytics and dynasty valuation from the appropriate football and fantasy sources without conflating their ownership.

## Source ownership

- NFLverse owns NFL football data: player biography and NFL identifiers, teams, schedules/games, football statistics, usage, participation, advanced metrics, injuries, depth charts, and play-by-play.
- Sleeper owns fantasy-specific data: its player directory, supported fantasy statistics/points, scoring and league settings, rosters, matchups, drafts, and transactions.
- NFLverse fantasy-point, expected-fantasy-point, ranking, or projection fields are not warehouse fantasy truth.

## Identity

Player names are descriptive attributes, not keys. A future crosswalk will associate GSIS IDs, Sleeper IDs, and other available external IDs while retaining match method, confidence, review state, and unmatched/conflicting candidates. Never assume Sleeper IDs equal GSIS IDs or force ambiguous matches.

## Current implementation

The repository currently has a setup entry point, package scaffolding, dependency list, and source-exploration notebooks. Production ingestion, transformations, validation, crosswalk, database migrations/loaders, analytics, and application models remain planned. See [pipeline_documentation.md](pipeline_documentation.md) for status.
