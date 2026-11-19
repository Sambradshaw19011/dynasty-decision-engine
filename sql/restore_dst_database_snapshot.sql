-- Explicitly authorized, atomic D/ST database restore.
-- Install only after the rollback rehearsal has passed.
-- The caller must verify the local SHA256 snapshot before invoking.
create or replace function public.restore_dst_database_snapshot(p_snapshot jsonb, p_confirmation text, p_expected_event_count integer, p_expected_score_count integer)
returns jsonb
language plpgsql
security definer
set search_path = ''
as $$
declare
  v_season integer;
  v_events jsonb;
  v_scores jsonb;
  v_event_count integer;
  v_score_count integer;
  v_restored_events integer;
  v_restored_scores integer;
begin
  if p_confirmation is distinct from 'RESTORE_DST_DATABASE_SNAPSHOT' then
    raise exception 'Explicit restore confirmation required';
  end if;
  if jsonb_typeof(p_snapshot) <> 'object' then
    raise exception 'Snapshot must be a JSON object';
  end if;
  v_season := (p_snapshot ->> 'season')::integer;
  if v_season is null or v_season < 2000 or v_season > 2100 then
    raise exception 'Invalid season';
  end if;
  if (select count(*) from jsonb_object_keys(p_snapshot -> 'tables')) <> 2
     or not (p_snapshot -> 'tables' ? 'fact_team_fumble_events')
     or not (p_snapshot -> 'tables' ? 'fact_calculated_team_weekly_scores') then
    raise exception 'Unexpected snapshot tables';
  end if;
  v_events := p_snapshot #> '{tables,fact_team_fumble_events,rows}';
  v_scores := p_snapshot #> '{tables,fact_calculated_team_weekly_scores,rows}';
  if jsonb_typeof(v_events) <> 'array' or jsonb_typeof(v_scores) <> 'array' then
    raise exception 'Missing snapshot arrays';
  end if;
  v_event_count := jsonb_array_length(v_events);
  v_score_count := jsonb_array_length(v_scores);
  if v_event_count = 0 or v_score_count = 0
     or v_event_count <> (p_snapshot #>> '{tables,fact_team_fumble_events,row_count}')::integer
     or v_score_count <> (p_snapshot #>> '{tables,fact_calculated_team_weekly_scores,row_count}')::integer then
    raise exception 'Snapshot row counts invalid';
  end if;
  if exists(select 1 from jsonb_array_elements(v_events) x where (x ->> 'season_id')::integer is distinct from v_season)
     or exists(select 1 from jsonb_array_elements(v_scores) x where (x ->> 'season_id')::integer is distinct from v_season) then
    raise exception 'Snapshot contains another season';
  end if;

    -- Prevent concurrent restores; refresh writers must also acquire this lock.
    perform pg_catalog.pg_advisory_xact_lock(728401, v_season);
    if (select count(*) from public.fact_team_fumble_events where season_id = v_season)
       is distinct from p_expected_event_count
       or (select count(*) from public.fact_calculated_team_weekly_scores where season_id = v_season)
       is distinct from p_expected_score_count then
      raise exception 'Current production row counts changed; restore refused';
    end if;
    delete from public.fact_calculated_team_weekly_scores where season_id = v_season;
    delete from public.fact_team_fumble_events where season_id = v_season;

    insert into public.fact_team_fumble_events
    overriding system value
    select (pg_catalog.jsonb_populate_record(null::public.fact_team_fumble_events, x)).*
    from jsonb_array_elements(v_events) x;

    insert into public.fact_calculated_team_weekly_scores
    overriding system value
    select (pg_catalog.jsonb_populate_record(null::public.fact_calculated_team_weekly_scores, x)).*
    from jsonb_array_elements(v_scores) x;

    select count(*) into v_restored_events
    from public.fact_team_fumble_events where season_id = v_season;
    select count(*) into v_restored_scores
    from public.fact_calculated_team_weekly_scores where season_id = v_season;
    if v_restored_events <> v_event_count or v_restored_scores <> v_score_count then
      raise exception 'Restored row counts differ from snapshot';
    end if;

    -- Compare complete records as multisets, not just counts or keys.
    if exists (
      (select pg_catalog.to_jsonb(t) as row from public.fact_team_fumble_events t where season_id = v_season
       except all select x from jsonb_array_elements(v_events) x)
      union all
      (select x from jsonb_array_elements(v_events) x
       except all select pg_catalog.to_jsonb(t) from public.fact_team_fumble_events t where season_id = v_season)
    ) then
      raise exception 'Fumble event content differs after restore';
    end if;
    if exists (
      (select pg_catalog.to_jsonb(t) as row from public.fact_calculated_team_weekly_scores t where season_id = v_season
       except all select x from jsonb_array_elements(v_scores) x)
      union all
      (select x from jsonb_array_elements(v_scores) x
       except all select pg_catalog.to_jsonb(t) from public.fact_calculated_team_weekly_scores t where season_id = v_season)
    ) then
      raise exception 'Calculated score content differs after restore';
    end if;

  return pg_catalog.jsonb_build_object(
    'success', true, 'mode', 'authorized_restore',
    'season', v_season, 'fumble_events_verified', v_event_count,
    'dst_scores_verified', v_score_count,
    'changes_committed', true
  );
end;
$$;

revoke all on function public.restore_dst_database_snapshot(jsonb, text, integer, integer) from public;
revoke all on function public.restore_dst_database_snapshot(jsonb, text, integer, integer) from anon;
revoke all on function public.restore_dst_database_snapshot(jsonb, text, integer, integer) from authenticated;
grant execute on function public.restore_dst_database_snapshot(jsonb, text, integer, integer) to service_role;
