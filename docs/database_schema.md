# Database schema plan

Supabase/PostgreSQL is the planned central store for curated warehouse facts, dimensions, and query-serving analytics. This document is a design plan, not an implemented schema. There are currently no migrations; do not infer columns or apply changes to a live project.

## Migration sequence

1. `001_create_core_tables.sql`: reviewed core dimensions for players, teams, seasons, and games.
2. `002_create_nflverse_tables.sql`: NFLverse-owned football statistics, usage, participation, and supporting facts.
3. `003_create_sleeper_tables.sql`: Sleeper fantasy data and league/scoring structures.
4. `004_create_player_crosswalk.sql`: identifier mappings, match method/confidence, review status, and unmatched/conflicting records.
5. `005_create_analytics_views.sql`: useful joins and query-serving views/marts.

No migration files should be written until the notebook research and current source/API schemas have been reviewed. Define each table's grain, source ownership, primary/natural keys, temporal meaning, nullability, constraints, and indexes before implementation. Prefer repeatable additive migrations; avoid destructive drops.

## Design boundaries

- Player names are descriptive attributes, not identifiers. Never equate Sleeper IDs with GSIS IDs or force ambiguous matches.
- Store fantasy truth from Sleeper only where the endpoint and coverage are verified; do not load NFLverse fantasy-point/projection columns as fantasy truth.
- Preserve raw/high-volume data in Parquet or object storage. Do not load all raw play-by-play into Supabase by default.
- Load curated facts, aggregates, and query-serving tables needed by the application.

## Next implementation step

Review the two preserved exploration notebooks and verify the current source/API schemas, then write down table grains and key/constraint decisions for migration 001. Implement migrations only after those decisions are grounded in the observed data.
