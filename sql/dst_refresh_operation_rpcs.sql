-- Deploy AFTER sql/dst_refresh_writer_fence.sql.
-- Transaction-local ownership for the existing D/ST RPCs.
-- Existing calculation SQL and deterministic fumble classifier remain unchanged.
-- NOTE: This protects against accidental conflicting writers, not a malicious
-- holder of the service-role key, which can set custom PostgreSQL GUCs.
create or replace function public.dst_claim_refresh_operation(
  p_operation_id uuid, p_season integer, p_expected_status text
) returns void
language plpgsql security definer set search_path = ''
as $$
declare v_active uuid; v_status text;
begin
  if auth.role() is distinct from 'service_role' then
    raise exception 'Service role required for D/ST refresh claim';
  end if;
  perform pg_catalog.pg_advisory_xact_lock(728401, p_season);
  select operation_id, status into v_active, v_status
  from public.dst_refresh_operations
  where season_id = p_season and status not in ('completed', 'recovered')
  order by created_at desc limit 1;
  if v_active is null then
    if p_operation_id is not null then
      raise exception 'No active D/ST refresh for season %', p_season;
    end if;
    return;
  end if;
  if p_operation_id is distinct from v_active or v_status is distinct from p_expected_status then
    raise exception 'D/ST refresh ownership/status mismatch for season %: expected %', p_season, p_expected_status;
  end if;
  perform pg_catalog.set_config('app.dst_refresh_operation_id', p_operation_id::text, true);
end;
$$;
revoke all on function public.dst_claim_refresh_operation(uuid, integer, text) from public, anon, authenticated;
grant execute on function public.dst_claim_refresh_operation(uuid, integer, text) to service_role;

-- Preserve deployed SQL by renaming its existing implementation.
-- Only perform the rename once, so reapplying the migration is safe.
do $$
begin
  if to_regprocedure('public.replace_fumble_events_season_core(jsonb,integer)') is null then
    alter function public.replace_fumble_events_season(jsonb,integer)
      rename to replace_fumble_events_season_core;
  end if;
  if to_regprocedure('public.calculate_team_weekly_scores_core(uuid,integer,integer,text,text)') is null then
    alter function public.calculate_team_weekly_scores(uuid,integer,integer,text,text)
      rename to calculate_team_weekly_scores_core;
  end if;
  if to_regprocedure('public.restore_dst_database_snapshot_core(jsonb,text,integer,integer)') is null then
    alter function public.restore_dst_database_snapshot(jsonb,text,integer,integer)
      rename to restore_dst_database_snapshot_core;
  end if;
end $$;

create or replace function public.replace_fumble_events_season(
  p_events jsonb, p_season integer, p_operation_id uuid default null
) returns integer language plpgsql security definer set search_path = ''
as $$
begin
  perform public.dst_claim_refresh_operation(p_operation_id, p_season, 'importing');
  return public.replace_fumble_events_season_core(p_events, p_season);
end;
$$;

create or replace function public.calculate_team_weekly_scores(
  p_scoring_settings_id uuid, p_season integer, p_week integer,
  p_season_type text default 'REG', p_engine_version text default 'def-1.4.0',
  p_operation_id uuid default null
) returns uuid language plpgsql security definer set search_path = ''
as $$
begin
  perform public.dst_claim_refresh_operation(p_operation_id, p_season, 'scoring');
  return public.calculate_team_weekly_scores_core(
    p_scoring_settings_id, p_season, p_week, p_season_type, p_engine_version
  );
end;
$$;

create or replace function public.restore_dst_database_snapshot(
  p_snapshot jsonb, p_confirmation text, p_expected_event_count integer,
  p_expected_score_count integer, p_operation_id uuid
) returns jsonb language plpgsql security definer set search_path = ''
as $$
declare v_season integer;
begin
  if p_operation_id is null then raise exception 'Recovery operation ID required'; end if;
  v_season := (p_snapshot ->> 'season')::integer;
  perform public.dst_claim_refresh_operation(p_operation_id, v_season, 'recovering');
  return public.restore_dst_database_snapshot_core(
    p_snapshot, p_confirmation, p_expected_event_count, p_expected_score_count
  );
end;
$$;

-- Disallow direct RPC invocation of unfenced implementations.
-- Internal SECURITY DEFINER wrappers can still call their cores.
revoke all on function public.replace_fumble_events_season_core(jsonb,integer) from public, anon, authenticated, service_role;
revoke all on function public.calculate_team_weekly_scores_core(uuid,integer,integer,text,text) from public, anon, authenticated, service_role;
revoke all on function public.restore_dst_database_snapshot_core(jsonb,text,integer,integer) from public, anon, authenticated, service_role;
revoke all on function public.replace_fumble_events_season(jsonb,integer,uuid) from public, anon, authenticated;
grant execute on function public.replace_fumble_events_season(jsonb,integer,uuid) to service_role;
revoke all on function public.restore_dst_database_snapshot(jsonb,text,integer,integer,uuid) from public, anon, authenticated;
grant execute on function public.restore_dst_database_snapshot(jsonb,text,integer,integer,uuid) to service_role;
revoke all on function public.calculate_team_weekly_scores(uuid,integer,integer,text,text,uuid) from public, anon, authenticated;
grant execute on function public.calculate_team_weekly_scores(uuid,integer,integer,text,text,uuid) to service_role;
