-- 001_create_core_tables.sql
-- Dynasty Decision Engine
-- Core NFLverse identity, NFL dimensions, Sleeper directory identity,
-- cross-source player mapping, and ingestion audit tables.
--
-- Source ownership:
--   NFLverse: football/player/team/game facts and player attributes.
--   Sleeper: fantasy platform player directory and fantasy-specific data.
--   This migration intentionally does NOT create fantasy scoring/stat tables.
--
-- Safe to apply to a new Supabase/PostgreSQL database. Uses CREATE ... IF NOT EXISTS
-- for repeatability. Review before applying to an existing production database.

begin;

create extension if not exists pgcrypto;

-- ---------------------------------------------------------------------------
-- 1. Seasons
-- Grain: one row per NFL season.
-- ---------------------------------------------------------------------------
create table if not exists public.dim_season (
    season_id integer primary key,
    season_type text,
    start_date date,
    end_date date,
    is_current boolean not null default false,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    constraint ck_dim_season_type
        check (season_type is null or season_type in ('REG', 'POST', 'PRE', 'REG+POST'))
);

create index if not exists ix_dim_season_current
    on public.dim_season (is_current) where is_current = true;


-- ---------------------------------------------------------------------------
-- 2. Teams
-- Grain: one row per NFL team/franchise identifier.
-- Team abbreviations are source values and may change over time; team_id is
-- the stable warehouse key. Historical name/abbreviation changes can later
-- be represented with effective-dated records if required.
-- ---------------------------------------------------------------------------
create table if not exists public.dim_team (
    team_id uuid primary key default gen_random_uuid(),
    nflverse_team_code text not null unique,
    team_name text,
    team_nickname text,
    city text,
    conference text,
    division text,
    founded_year integer,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    constraint ck_dim_team_conference
        check (conference is null or conference in ('AFC', 'NFC')),
    constraint ck_dim_team_division
        check (division is null or division in
            ('East', 'North', 'South', 'West'))
);


-- ---------------------------------------------------------------------------
-- 3. Stadiums
-- Grain: one row per stadium/venue identity.
-- ---------------------------------------------------------------------------
create table if not exists public.dim_stadium (
    stadium_id uuid primary key default gen_random_uuid(),
    stadium_name text not null,
    alternate_name text,
    city text,
    state text,
    country text,
    surface text,
    roof_type text,
    capacity integer,
    latitude numeric(9,6),
    longitude numeric(9,6),
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    constraint uq_dim_stadium_identity
        unique (stadium_name, city, state),
    constraint ck_dim_stadium_capacity
        check (capacity is null or capacity > 0),
    constraint ck_dim_stadium_latitude
        check (latitude is null or latitude between -90 and 90),
    constraint ck_dim_stadium_longitude
        check (longitude is null or longitude between -180 and 180)
);


-- ---------------------------------------------------------------------------
-- 4. Canonical NFL player dimension
-- Grain: one row per NFLverse player identity.
--
-- warehouse player_id is an internal UUID and must be used for foreign keys.
-- nflverse_player_id preserves NFLverse's player_id value. gsis_id is stored
-- separately because not every NFLverse identifier is necessarily a GSIS ID.
-- Do not use Sleeper IDs as this table's primary key.
-- ---------------------------------------------------------------------------
create table if not exists public.dim_player (
    player_id uuid primary key default gen_random_uuid(),

    nflverse_player_id text not null unique,
    gsis_id text unique,

    display_name text,
    common_first_name text,
    first_name text,
    last_name text,
    short_name text,
    football_name text,
    suffix text,

    position text,
    position_group text,
    ngs_position text,
    ngs_position_group text,

    birth_date date,
    height integer,
    weight integer,
    college_name text,
    college_conference text,
    headshot_url text,

    jersey_number text,
    rookie_season integer,
    last_season integer,
    latest_team_code text references public.dim_team(nflverse_team_code),
    status text,
    ngs_status text,
    ngs_status_short_description text,
    years_of_experience integer,

    pff_position text,
    pff_status text,
    draft_year integer,
    draft_round integer,
    draft_pick integer,
    draft_team_code text references public.dim_team(nflverse_team_code),

    source_updated_at timestamptz,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),

    constraint ck_dim_player_height check (height is null or height > 0),
    constraint ck_dim_player_weight check (weight is null or weight > 0),
    constraint ck_dim_player_experience
        check (years_of_experience is null or years_of_experience >= 0),
    constraint ck_dim_player_draft_round
        check (draft_round is null or draft_round between 1 and 7),
    constraint ck_dim_player_draft_pick
        check (draft_pick is null or draft_pick > 0)
);

create index if not exists ix_dim_player_name
    on public.dim_player (last_name, first_name);
create index if not exists ix_dim_player_position
    on public.dim_player (position);
create index if not exists ix_dim_player_latest_team
    on public.dim_player (latest_team_code);
create index if not exists ix_dim_player_espn_not_here
    on public.dim_player (nflverse_player_id);


-- ---------------------------------------------------------------------------
-- 5. Games
-- Grain: one row per NFLverse game_id.
-- A game may exist before kickoff; scores and result fields are nullable.
-- ---------------------------------------------------------------------------
create table if not exists public.dim_game (
    game_id text primary key,
    season_id integer not null references public.dim_season(season_id),
    week integer not null,
    season_type text not null,
    game_date date,
    kickoff_at timestamptz,

    home_team_code text references public.dim_team(nflverse_team_code),
    away_team_code text references public.dim_team(nflverse_team_code),
    home_score integer,
    away_score integer,

    stadium_id uuid references public.dim_stadium(stadium_id),
    game_type text,
    game_status text,
    neutral_site boolean,
    roof text,
    surface text,

    source_updated_at timestamptz,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),

    constraint ck_dim_game_week check (week > 0),
    constraint ck_dim_game_season_type
        check (season_type in ('REG', 'POST', 'PRE')),
    constraint ck_dim_game_scores
        check (
            (home_score is null or home_score >= 0)
            and (away_score is null or away_score >= 0)
        ),
    constraint ck_dim_game_distinct_teams
        check (
            home_team_code is null
            or away_team_code is null
            or home_team_code <> away_team_code
        )
);

create index if not exists ix_dim_game_season_week
    on public.dim_game (season_id, season_type, week);
create index if not exists ix_dim_game_home_team
    on public.dim_game (home_team_code, season_id);
create index if not exists ix_dim_game_away_team
    on public.dim_game (away_team_code, season_id);
create index if not exists ix_dim_game_date
    on public.dim_game (game_date);


-- ---------------------------------------------------------------------------
-- 6. Sleeper player directory
-- Grain: one row per Sleeper player_id in the retrieved directory snapshot.
-- This is intentionally separate from dim_player: Sleeper may contain players
-- without an NFLverse match or GSIS ID. Directory snapshots are refreshed via
-- upsert; historical snapshots can be added in a later migration.
-- ---------------------------------------------------------------------------
create table if not exists public.dim_sleeper_player (
    sleeper_player_id text primary key,

    full_name text,
    first_name text,
    last_name text,
    search_full_name text,
    search_first_name text,
    search_last_name text,

    position text,
    fantasy_positions text[],
    team_code text,
    team_abbr text,
    status text,
    active boolean,
    years_experience integer,

    birth_date date,
    age integer,
    height text,
    weight integer,
    college text,
    high_school text,
    jersey_number integer,

    gsis_id text,
    espn_id text,
    yahoo_id text,
    sportradar_id text,
    rotowire_id text,
    stats_id text,
    fantasy_data_id text,

    injury_status text,
    injury_start_date date,
    injury_body_part text,
    injury_notes text,
    practice_participation text,
    practice_description text,

    depth_chart_position text,
    depth_chart_order integer,
    search_rank integer,
    player_shard text,
    hashtag text,

    raw_payload jsonb,
    source_retrieved_at timestamptz,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),

    constraint ck_sleeper_player_age check (age is null or age >= 0),
    constraint ck_sleeper_player_experience
        check (years_experience is null or years_experience >= 0)
);

create index if not exists ix_sleeper_player_gsis
    on public.dim_sleeper_player (gsis_id);
create index if not exists ix_sleeper_player_espn
    on public.dim_sleeper_player (espn_id);
create index if not exists ix_sleeper_player_position
    on public.dim_sleeper_player (position);
create index if not exists ix_sleeper_player_team
    on public.dim_sleeper_player (team_code);
create index if not exists ix_sleeper_player_fantasy_positions
    on public.dim_sleeper_player using gin (fantasy_positions);


-- ---------------------------------------------------------------------------
-- 7. Cross-source player identifier bridge
-- Grain: one row per source-system identifier.
--
-- Rows with player_id NULL represent unresolved/unmatched candidates.
-- Conflicts and ambiguous mappings remain reviewable; do not force them.
-- Multiple source IDs can map to one canonical NFLverse player.
-- ---------------------------------------------------------------------------
create table if not exists public.bridge_player_ids (
    mapping_id uuid primary key default gen_random_uuid(),
    player_id uuid references public.dim_player(player_id) on delete set null,

    source_system text not null,
    source_player_id text not null,
    identifier_type text not null,

    match_method text,
    match_confidence numeric(5,4),
    review_status text not null default 'unreviewed',
    is_primary boolean not null default false,

    candidate_player_ids uuid[] not null default '{}',
    conflict_details jsonb,
    notes text,

    first_seen_at timestamptz not null default now(),
    last_seen_at timestamptz not null default now(),
    reviewed_at timestamptz,
    reviewed_by text,

    constraint uq_bridge_source_identifier
        unique (source_system, identifier_type, source_player_id),
    constraint ck_bridge_source_system
        check (source_system in ('nflverse', 'sleeper', 'espn', 'pfr', 'pff', 'other')),
    constraint ck_bridge_review_status
        check (review_status in
            ('unreviewed', 'matched', 'ambiguous', 'conflict', 'rejected')),
    constraint ck_bridge_confidence
        check (match_confidence is null or match_confidence between 0 and 1),
    constraint ck_bridge_matched_has_player
        check (review_status <> 'matched' or player_id is not null)
);

create index if not exists ix_bridge_player_ids_player
    on public.bridge_player_ids (player_id);
create index if not exists ix_bridge_player_ids_review
    on public.bridge_player_ids (review_status);
create index if not exists ix_bridge_player_ids_source
    on public.bridge_player_ids (source_system, identifier_type);


-- ---------------------------------------------------------------------------
-- 8. Ingestion audit
-- Grain: one row per pipeline execution or source ingestion job.
-- ---------------------------------------------------------------------------
create table if not exists public.pipeline_runs (
    run_id uuid primary key default gen_random_uuid(),
    pipeline_name text not null,
    source_system text not null,
    dataset_name text,
    source_endpoint text,

    season integer,
    season_type text,
    week integer,

    run_status text not null default 'running',
    started_at timestamptz not null default now(),
    completed_at timestamptz,

    records_extracted bigint,
    records_inserted bigint,
    records_updated bigint,
    records_rejected bigint,

    source_retrieved_at timestamptz,
    source_version text,
    error_message text,
    metadata jsonb not null default '{}'::jsonb,

    constraint ck_pipeline_run_status
        check (run_status in ('running', 'succeeded', 'partial', 'failed')),
    constraint ck_pipeline_run_counts
        check (
            (records_extracted is null or records_extracted >= 0)
            and (records_inserted is null or records_inserted >= 0)
            and (records_updated is null or records_updated >= 0)
            and (records_rejected is null or records_rejected >= 0)
        )
);

create index if not exists ix_pipeline_runs_source_started
    on public.pipeline_runs (source_system, started_at desc);
create index if not exists ix_pipeline_runs_status
    on public.pipeline_runs (run_status, started_at desc);


-- ---------------------------------------------------------------------------
-- 9. Documentation comments
-- ---------------------------------------------------------------------------
comment on table public.dim_player is
    'Canonical NFLverse player dimension. Uses internal UUID; NFLverse IDs are external identifiers.';
comment on table public.dim_sleeper_player is
    'Sleeper player directory records. Kept separate so unmatched fantasy players are retained.';
comment on table public.bridge_player_ids is
    'Cross-source identifier bridge. Unmatched and ambiguous identifiers are retained for review.';
comment on table public.pipeline_runs is
    'Audit log for source ingestion and transformation pipeline executions.';
comment on column public.dim_game.game_id is
    'NFLverse game identifier; source game_id is retained as the natural key.';
comment on column public.dim_player.nflverse_player_id is
    'NFLverse player_id from load_players(); not assumed to be a GSIS identifier.';

commit;
