# Weekly D/ST refresh (review before enabling)

This pull request adds a separate GitHub Actions workflow scheduled Tuesday and Friday at 14:00 UTC, with manual dispatch. It does not change the existing Thursday player-stat workflow.

## Required GitHub Actions secrets

- `SUPABASE_URL` — project API URL
- `SUPABASE_SERVICE_ROLE_KEY` — service role key (never commit)
- `PBP_INSPECT_SECRET` — secret accepted by deployed `import-pbp-fumbles` Edge Function
- `DST_SCORING_SETTINGS_ID` — UUID of the intended league scoring configuration

## Dependencies and prerequisites

- Existing deployed `import-pbp-fumbles` must accept `mode=audit` and `mode=import`, with `confirm=IMPORT_VERIFIED_EVENTS`.
- Its input must be a **single full-season Parquet object** at `nflverse-data/play_by_play/{season}.parquet`.
- `calculate_team_weekly_scores` must already have the `ON CONFLICT (scoring_settings_id, game_id, team_code) DO UPDATE` change deployed.
- Database unique index `uq_dst_scores_settings_game_team` must exist.
- The NFLverse PBP dataset for the current season must be published and complete for all finalized games; otherwise the workflow deliberately stops before upload.

## Pipeline

1. Refresh schedule results (required for identifying completed games).
2. Refresh the latest five weeks of player stats (existing hash-based loader).
3. Download the current season's full PBP, verify every completed game appears, then replace the current-season Parquet in Storage.
4. Invoke the Edge Function audit and atomic import.
5. Recalculate every completed REG/POST week under the selected scoring configuration.
6. Verify exactly two distinct D/ST rows for every finalized game.

## Operational cautions

- **Review and test via workflow_dispatch before merging.** GitHub scheduled workflows run only from the default branch.
- The PBP Storage upload overwrites the current-season object only after source validation. If the later Edge Function fails, the Parquet may have updated while the fact table retains prior data. The workflow fails visibly; retry after fixing the issue.
- Historical seasons are not refreshed by this workflow.
- Fumble classifier fallback decisions and provisional points-allowed logic remain unchanged.
- A current-season PBP format change, missing completed game, missing secret, or Edge Function timeout causes a failed job; this workflow does not silently claim success.
- Check Supabase storage file limits and Edge Function execution limits for current-season file size.
- The existing Thursday workflow still runs separately. Consider consolidating only after the new workflow is verified.
