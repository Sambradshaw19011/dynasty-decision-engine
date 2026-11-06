# Data dictionary

This is a conceptual dictionary only; no production tables or columns have been defined.

| Concept | Meaning | Authority / grain |
|---|---|---|
| NFL player | A player identity and football attributes, with available NFLverse identifiers | NFLverse; one identity per authoritative player ID, exact source grain to be verified |
| Team | NFL team identity and source identifiers | NFLverse; source grain to be verified |
| Season | NFL season and season-level metadata | NFLverse; season |
| Game | Scheduled or completed NFL game and game metadata | NFLverse; game identifier |
| Football statistic | Football performance measure such as weekly/seasonal statistics, usage, or participation | NFLverse; dataset-specific grain must be documented before schema design |
| Sleeper player | Fantasy platform directory record and Sleeper identifier | Sleeper; one source player record |
| Fantasy statistic | Fantasy-specific statistics/points when supported by a verified endpoint | Sleeper; endpoint-specific player/season/week/league grain to be verified |
| League configuration | League, scoring settings, roster configuration, and season context | Sleeper; league/season or endpoint-specific grain |
| Roster/matchup/draft/transaction | Fantasy league activity and state | Sleeper; endpoint-specific league/season/week/event grain |
| Player crosswalk | Reviewed association between an NFLverse identity and Sleeper/other external IDs | Cross-source mapping; retain method, confidence, review status, and ambiguous/unmatched candidates |

Exact columns, types, keys, and measures remain undecided until current source schemas and endpoint coverage are verified. NFLverse fantasy points and projections are explicitly outside fantasy-statistic ownership.
