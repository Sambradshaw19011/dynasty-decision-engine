-- D/ST refresh writer fence, phase 1.
-- Deploy together with the operation-aware RPC changes, not by itself.
-- This migration blocks writes to the protected season while a refresh is active.
-- Unrelated seasons remain writable.
create or replace function public.dst_refresh_write_guard()
returns trigger language plpgsql security definer set search_path = ''
as $$
declare
  v_season integer;
  v_owner uuid;
  v_claim text;
begin
  if tg_op = 'DELETE' then
    v_season := old.season_id;
  else
    v_season := new.season_id;
  end if;
  if tg_op = 'UPDATE' and old.season_id is distinct from new.season_id then
    raise exception 'D/ST season cannot be changed by UPDATE';
  end if;
  -- Serialize guarded writes with refresh journal transitions for this season.
  perform pg_catalog.pg_advisory_xact_lock(728401, v_season);
  select operation_id into v_owner
  from public.dst_refresh_operations
  where season_id = v_season
    and status not in ('completed', 'recovered')
  order by created_at desc
  limit 1;
  if v_owner is not null then
    v_claim := pg_catalog.current_setting('app.dst_refresh_operation_id', true);
    if v_claim is distinct from v_owner::text then
      raise exception 'D/ST season % is reserved by refresh operation %', v_season, v_owner;
    end if;
    if auth.role() is distinct from 'service_role' then
      raise exception 'Service role required during D/ST refresh';
    end if;
  end if;
  if tg_op = 'DELETE' then return old; end if;
  return new;
end;
$$;

revoke all on function public.dst_refresh_write_guard() from public, anon, authenticated;

drop trigger if exists dst_refresh_guard_fumbles on public.fact_team_fumble_events;
create trigger dst_refresh_guard_fumbles
before insert or update or delete on public.fact_team_fumble_events
for each row execute function public.dst_refresh_write_guard();

drop trigger if exists dst_refresh_guard_scores on public.fact_calculated_team_weekly_scores;
create trigger dst_refresh_guard_scores
before insert or update or delete on public.fact_calculated_team_weekly_scores
for each row execute function public.dst_refresh_write_guard();

-- IMPORTANT: app.dst_refresh_operation_id is transaction-local. Only
-- operation-aware RPCs should set it with set_config(..., true).
-- The trigger prevents accidental concurrent writes, but service_role is
-- privileged and is not an adversarial security boundary. Additionally,
-- a storage-object overwrite is outside PostgreSQL transaction isolation.
