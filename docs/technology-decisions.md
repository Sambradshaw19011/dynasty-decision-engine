# Technology decisions

Record each decision, the alternatives considered, and the reason for the choice. Decisions below describe the intended architecture; planned components are not necessarily implemented.

| Area | Candidate | Status |
|---|---|---|
| Ingestion and orchestration | Python | Selected; production pipeline not implemented |
| Substantial transformations | PySpark | Planned; not yet a dependency |
| Local staging | Parquet | Planned |
| Central warehouse | Supabase/PostgreSQL | Selected; connection and schema not implemented |
| NFL football data | NFLverse | Source of truth |
| Fantasy and league data | Sleeper | Source of truth, subject to endpoint/coverage verification |
| Modeling | pandas / NumPy / scikit-learn | Existing dependencies; project models not implemented |
| Backend and frontend | Undecided | No implementation |
| Version control | Git / GitHub | In use |
