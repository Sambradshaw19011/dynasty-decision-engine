# Local data

Downloaded datasets and generated data are local-only and ignored by Git. Keep raw source extracts separate from processed outputs and intermediate staging files.

- `raw/nflverse/` and `raw/sleeper/`: unmodified source extracts and API responses, with retrieval time, endpoint/source, and coverage metadata.
- `processed/nflverse/` and `processed/sleeper/`: source-owned cleaned/normalized outputs.
- `staging/`: temporary transformation data; Parquet is the planned format.
- `external/`: existing manually obtained reference-data location.

NFLverse is authoritative for football data; Sleeper is authoritative for fantasy-specific data. Do not treat NFLverse fantasy points/projections as fantasy truth. High-volume play-by-play should remain in Parquet/object storage by default rather than being loaded wholesale into Supabase.
