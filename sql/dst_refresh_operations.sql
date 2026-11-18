-- Durable, exclusive D/ST refresh lease and recovery journal.
-- Apply in Supabase SQL Editor before using any production coordinator.
create table if not exists public.dst_refresh_operations (
  operation_id uuid primary key default gen_random_uuid(),
  season_id integer not null references public.dim_season(season_id),
  status text not null check (status in ('prepared','promoting','importing','scoring','verifying','recovering','recovery_failed','recovered','completed')),
  backup_parquet_path text not null,
  backup_parquet_sha256 text not null check (backup_parquet_sha256 ~ '^[a-f0-9]{64}$'),
  backup_database_sha256 text not null check (backup_database_sha256 ~ '^[a-f0-9]{64}$'),
  staged_parquet_path text not null,
  staged_parquet_sha256 text not null check (staged_parquet_sha256 ~ '^[a-f0-9]{64}$'),
  details jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);
create unique index if not exists dst_one_unfinished_operation_per_season
  on public.dst_refresh_operations(season_id)
  where status not in ('recovered','completed');
alter table public.dst_refresh_operations enable row level security;
revoke all on public.dst_refresh_operations from public, anon, authenticated;
grant select, insert, update on public.dst_refresh_operations to service_role;

create or replace function public.start_dst_refresh_operation(
  p_season integer,
  p_backup_parquet_path text,
  p_backup_parquet_sha256 text,
  p_backup_database_sha256 text,
  p_staged_parquet_path text,
  p_staged_parquet_sha256 text
) returns uuid
language plpgsql security definer set search_path = ''
as $$
declare v_id uuid;
begin
  perform pg_catalog.pg_advisory_xact_lock(728401, p_season);
  if exists (
    select 1 from public.dst_refresh_operations
    where season_id = p_season and status not in ('recovered','completed')
  ) then
    raise exception 'Unfinished D/ST refresh exists for season %. Recover it before starting another', p_season;
  end if;
  insert into public.dst_refresh_operations(
    season_id,status,backup_parquet_path,backup_parquet_sha256,
    backup_database_sha256,staged_parquet_path,staged_parquet_sha256
  ) values (
    p_season,'prepared',p_backup_parquet_path,p_backup_parquet_sha256,
    p_backup_database_sha256,p_staged_parquet_path,p_staged_parquet_sha256
  ) returning operation_id into v_id;
  return v_id;
end; $$;

create or replace function public.advance_dst_refresh_operation(
  p_operation_id uuid, p_expected_status text, p_next_status text,
  p_details jsonb default '{}'::jsonb
) returns jsonb
language plpgsql security definer set search_path = ''
as $$
declare v_op public.dst_refresh_operations%rowtype;
begin
  select * into v_op from public.dst_refresh_operations where operation_id = p_operation_id for update;
  if not found or v_op.status is distinct from p_expected_status then
    raise exception 'Operation missing or state changed';
  end if;
  if not (
    (p_expected_status = 'prepared' and p_next_status = 'promoting') or
    (p_expected_status = 'promoting' and p_next_status = 'importing') or
    (p_expected_status = 'importing' and p_next_status = 'scoring') or
    (p_expected_status = 'scoring' and p_next_status = 'verifying') or
    (p_expected_status = 'verifying' and p_next_status = 'completed') or
    (p_expected_status in ('prepared','promoting','importing','scoring','verifying','recovery_failed') and p_next_status = 'recovering') or
    (p_expected_status = 'recovering' and p_next_status in ('recovered','recovery_failed'))
  ) then
    raise exception 'Invalid D/ST operation transition: % -> %', p_expected_status, p_next_status;
  end if;
  update public.dst_refresh_operations
  set status = p_next_status, details = details || coalesce(p_details,'{}'::jsonb),
      updated_at = now()
  where operation_id = p_operation_id;
  return pg_catalog.jsonb_build_object('operation_id',p_operation_id,'status',p_next_status);
end; $$;

revoke all on function public.start_dst_refresh_operation(integer,text,text,text,text,text) from public,anon,authenticated;
revoke all on function public.advance_dst_refresh_operation(uuid,text,text,jsonb) from public,anon,authenticated;
grant execute on function public.start_dst_refresh_operation(integer,text,text,text,text,text) to service_role;
grant execute on function public.advance_dst_refresh_operation(uuid,text,text,jsonb) to service_role;
