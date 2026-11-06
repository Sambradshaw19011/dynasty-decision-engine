# Data sources

## NFLverse

NFLverse is authoritative for football facts: player biographical/physical attributes where available, GSIS and other NFL identifiers, teams, schedules and game metadata, weekly/seasonal football statistics, snaps/participation, advanced metrics, injuries, depth charts, and play-by-play.

The existing [NFLverse exploration notebook](../notebooks/exploratory_analysis/explore_nflverse.ipynb) is research, not a production client. Before ingestion is implemented, verify each dataset's current schema, coverage, update cadence, and licensing/usage requirements. Exclude NFLverse fantasy-point, expected-fantasy-point, ranking, and projection fields from fantasy-truth tables.

## Sleeper

Sleeper is authoritative for fantasy-specific data, including its player directory and identifiers, league scoring/settings, rosters, matchups, drafts, transactions, and fantasy points/statistics only where a supported source is verified.

The existing [Sleeper exploration notebook](../notebooks/exploratory_analysis/explore_sleeper.ipynb) is research, not a production client. Verify endpoint behavior, historical coverage, rate limits, and response schemas before building ingestion. Do not assume a historical fantasy-stat endpoint is officially supported or available.

## Capture and provenance

Raw API responses and source files should be retained separately by source. Record the source endpoint/dataset, retrieval timestamp, and coverage with each extract. Keep unmatched/ambiguous player identity candidates available for review rather than silently discarding or forcing a mapping.
