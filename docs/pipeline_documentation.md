# Pipeline documentation

## Status

No production pipeline is implemented. The repository has setup scaffolding and two exploratory notebooks only. The notebooks are research records; their code should not be treated as scheduled, retry-safe, or validated ingestion.

## Planned stages

1. **Configure:** read local credentials/settings without committing secrets.
2. **Ingest:** retrieve NFLverse football datasets and verified Sleeper endpoints separately.
3. **Capture raw:** preserve source responses/files and record endpoint/dataset, retrieval time, and coverage.
4. **Stage and transform:** use Parquet locally; use Python for orchestration and PySpark for substantial transformations.
5. **Validate and reconcile:** check source-specific schemas and identifiers; retain ambiguous or unmatched player mappings for review.
6. **Load:** write curated facts and dimensions to Supabase/PostgreSQL; leave full high-volume play-by-play in Parquet/object storage by default.
7. **Analyze:** build query-serving views and fantasy/dynasty analyses without merging source ownership.

## Implementation status

| Component | Status |
|---|---|
| Environment/setup entry point | Minimal scaffold |
| NFLverse/Sleeper exploration | Existing notebooks; preserved under `notebooks/exploratory_analysis/` |
| Production source clients and ingestion scripts | Not implemented |
| Transformations and data validation | Not implemented |
| Player crosswalk | Not implemented; design requirements are documented |
| Supabase connection, loaders, and migrations | Not implemented |
| Analytics, projections, and dynasty valuation | Not implemented |

Future ingestion should be idempotent and report errors explicitly. Validate endpoint behavior and historical coverage before promising data availability; no live database operations are part of this restructuring.
