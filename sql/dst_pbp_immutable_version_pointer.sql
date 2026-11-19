-- Immutable PBP active-version pointer. This migration does NOT switch readers.
-- Apply only after deploying readers that resolve this pointer.
create table if not exists public.dst_pbp_active_versions (
  season_id integer primary key,
  object_path text not null,
  sha256 text not null check (sha256 ~ '^[0-9a-f]{64}$'),
  updated_at timestamptz not null default now(),
  operation_id uuid,
  constraint dst_pbp_active_version_path check (
    object_path = 'play_by_play_versions/' || season_id::text || '/' || sha256 || '.parquet'
  )
);
alter table public.dst_pbp_active_versions enable row level security;
revoke all on public.dst_pbp_active_versions from public, anon, authenticated;
grant select, insert, update on public.dst_pbp_active_versions to service_role;

create or replace function public.dst_switch_pbp_active_version(
  p_season integer,
  p_expected_sha256 text,
  p_new_sha256 text,
  p_operation_id uuid
) returns void
language plpgsql security definer
set search_path = ''
as $$
declare
  v_current text;
  v_status text;
begin
  if auth.role() is distinct from 'service_role' then
    raise exception 'Service role required';
  end if;
  select status into v_status
  from public.dst_refresh_operations
  where operation_id = p_operation_id and season_id = p_season
  for update;
  if v_status is distinct from 'promoting' and v_status is distinct from 'recovering' then
    raise exception 'Operation not in an authorized pointer transition state';
  end if;
  select sha256 into v_current
  from public.dst_pbp_active_versions
  where season_id = p_season for update;
  if v_current is null then
    raise exception 'Active version not initialized for season %', p_season;
  end if;
  if v_current is distinct from p_expected_sha256 then
    raise exception 'Active version changed; expected %, got %', p_expected_sha256, v_current;
  end if;
  if p_new_sha256 !~ '^[0-9a-f]{64}$' then
    raise exception 'Invalid new digest';
  end if;
  update public.dst_pbp_active_versions
  set sha256 = p_new_sha256,
      object_path = 'play_by_play_versions/' || p_season::text || '/' || p_new_sha256 || '.parquet',
      updated_at = now(),
      operation_id = p_operation_id
  where season_id = p_season;
end;
$$;
revoke all on function public.dst_switch_pbp_active_version(integer,text,text,uuid)
  from public, anon, authenticated;
grant execute on function public.dst_switch_pbp_active_version(integer,text,text,uuid)
  to service_role;
