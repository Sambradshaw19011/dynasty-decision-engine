-- 002_create_nflverse_weekly_stats.sql
-- Dynasty Decision Engine
-- NFLverse weekly player football statistics.
--
-- Grain: one row per NFLverse player/game/season type.
-- NFLverse is the source of truth for football statistics.
-- Fantasy-point fields are intentionally excluded. Fantasy scoring belongs
-- to the Sleeper-owned fantasy data layer.
--
-- Load order:
--   1. dim_season
--   2. dim_team
--   3. dim_player
--   4. dim_game
--   5. fact_player_weekly_stats
--
-- Rows without a resolved warehouse player_id are retained using
-- source_player_id and can be reconciled through bridge_player_ids later.

begin;

create table if not exists public.fact_player_weekly_stats (
    weekly_stat_id bigint generated always as identity primary key,

    -- Source identity and warehouse relationships
    player_id uuid references public.dim_player(player_id),
    source_player_id text,
    game_id text references public.dim_game(game_id),
    season_id integer not null references public.dim_season(season_id),
    week integer not null,
    season_type text not null,
    team_code text,
    opponent_team_code text,

    -- Passing
    completions integer,
    attempts integer,
    passing_yards numeric(9,2),
    passing_tds integer,
    passing_interceptions integer,
    sacks_suffered integer,
    sack_yards_lost numeric(9,2),
    sack_fumbles integer,
    sack_fumbles_lost integer,
    passing_air_yards numeric(9,2),
    passing_yards_after_catch numeric(9,2),
    passing_first_downs integer,
    passing_epa numeric(10,4),
    passing_cpoe numeric(9,4),
    passing_2pt_conversions integer,
    pacr numeric(9,4),
    passing_10 integer,
    passing_16 integer,
    passing_20 integer,
    passing_40 integer,

    -- Rushing
    carries integer,
    rushing_yards numeric(9,2),
    rushing_tds integer,
    rushing_fumbles integer,
    rushing_fumbles_lost integer,
    rushing_first_downs integer,
    rushing_epa numeric(10,4),
    rushing_2pt_conversions integer,
    rushing_10 integer,
    rushing_12 integer,
    rushing_20 integer,
    rushing_40 integer,

    -- Receiving
    receptions integer,
    targets integer,
    receiving_yards numeric(9,2),
    receiving_tds integer,
    receiving_fumbles integer,
    receiving_fumbles_lost integer,
    receiving_air_yards numeric(9,2),
    receiving_yards_after_catch numeric(9,2),
    receiving_first_downs integer,
    receiving_epa numeric(10,4),
    receiving_2pt_conversions integer,
    receiving_10 integer,
    receiving_16 integer,
    receiving_20 integer,
    receiving_40 integer,
    racr numeric(9,4),
    target_share numeric(9,4),
    air_yards_share numeric(9,4),
    wopr numeric(9,4),

    -- Fumbles and miscellaneous offense
    fumbles_lost integer,
    fumbles int,
    fumble_recovery_own integer,
    fumble_recovery_yards_own numeric(9,2),
    fumble_recovery_opp integer,
    fumble_recovery_yards_opp numeric(9,2),
    fumble_recovery_tds integer,
    misc_yards numeric(9,2),
    penalties integer,
    penalty_yards numeric(9,2),

    -- Defense
    def_tackles_solo integer,
    def_tackles_with_assist integer,
    def_tackle_assists integer,
    def_tackles_for_loss integer,
    def_tackles_for_loss_yards numeric(9,2),
    def_fumbles_forced integer,
    def_sacks numeric(9,2),
    def_sack_yards numeric(9,2),
    def_qb_hits integer,
    def_interceptions integer,
    def_interception_yards numeric(9,2),
    def_pass_defended integer,
    def_tds integer,
    def_fumbles integer,
    def_safeties integer,
    def_punt_blocks integer,
    def_pat_blocks integer,
    def_fg_blocks integer,
    def_2pt_atts integer,
    def_2pt_made integer,

    -- Returns and special teams
    special_teams_tds integer,
    punt_returns integer,
    punt_return_yards numeric(9,2),
    kickoff_returns integer,
    kickoff_return_yards numeric(9,2),
    yards_per_punt_return numeric(9,4),
    yards_per_kickoff_return numeric(9,4),

    -- Kicking
    fg_made integer,
    fg_att integer,
    fg_missed integer,
    fg_blocked integer,
    fg_pct numeric(9,4),
    fg_made_0_19 integer,
    fg_made_20_29 integer,
    fg_made_30_39 integer,
    fg_made_40_49 integer,
    fg_made_50_plus integer,
    fg_missed_0_19 integer,
    fg_missed_20_29 integer,
    fg_missed_30_39 integer,
    fg_missed_40_49 integer,
    fg_missed_50_plus integer,
    pat_made integer,
    pat_att integer,
    pat_missed integer,

    -- Punting
    punts integer,
    punt_yards numeric(9,2),
    punt_avg numeric(9,4),
    punt_net_avg numeric(9,4),
    punts_inside_20 integer,
    punt_touchbacks integer,

    -- Source metadata and lineage
    source_dataset text not null default 'player_stats',
    source_retrieved_at timestamptz,
    source_row_hash text,
    raw_extra_stats jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),

    constraint ck_weekly_stats_week check (week > 0),
    constraint ck_weekly_stats_season_type
        check (season_type in ('REG', 'POST', 'PRE')),
    constraint ck_weekly_stats_player_identity
        check (player_id is not null or source_player_id is not null)
);

-- NFLverse weekly stats are normally unique by source player and game.
-- A partial unique index preserves this natural key while allowing rows with
-- missing source player/game identifiers to be retained for data-quality review.
create unique index if not exists uq_fact_player_weekly_source_game
    on public.fact_player_weekly_stats
       (source_player_id, game_id, season_type)
    where source_player_id is not null and game_id is not null;

create index if not exists ix_fact_player_weekly_player_season
    on public.fact_player_weekly_stats (player_id, season_id, season_type, week);

create index if not exists ix_fact_player_weekly_game
    on public.fact_player_weekly_stats (game_id);

create index if not exists ix_fact_player_weekly_season_week
    on public.fact_player_weekly_stats (season_id, season_type, week);

create index if not exists ix_fact_player_weekly_team
    on public.fact_player_weekly_stats (team_code, season_id, week);

create index if not exists ix_fact_player_weekly_source_player
    on public.fact_player_weekly_stats (source_player_id)
    where source_player_id is not null;

comment on table public.fact_player_weekly_stats is
    'NFLverse weekly player football statistics. Excludes all NFLverse fantasy-point fields; fantasy scoring is managed separately from Sleeper data.';

comment on column public.fact_player_weekly_stats.player_id is
    'Internal warehouse player UUID. Nullable so unmatched source rows can be retained pending identity resolution.';

comment on column public.fact_player_weekly_stats.source_player_id is
    'NFLverse player_id value from the source dataset; retain even when no dim_player match exists.';

comment on column public.fact_player_weekly_stats.raw_extra_stats is
    'Optional JSONB for source fields not yet promoted to typed columns. Do not place NFLverse fantasy-point fields here.';

commit;
