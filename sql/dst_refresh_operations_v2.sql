-- Apply after sql/dst_refresh_operations.sql. No existing operations are modified.
alter table public.dst_refresh_operations
  add column if not exists backup_database_path text;

create or replace function public.start_dst_refresh_operation_v2(
  p_season integer,
  p_backup_parquet_path text,
  p_backup_parquet_sha256 text,
  p_backup_database_path text,
  p_backup_database_sha256 text,
  p_staged_parquet_path text,
  p_staged_parquet_sha256 text
) returns uuid
language plpgsql security definer set search_path = ''
as $$
declare v_id uuid;
begin
  if p_backup_database_path is distinct from
     ('dst_database_backups/' || p_season::text || '/' || p_backup_database_sha256 || '.json')
     or p_backup_database_sha256 !~ '^[a-f0-9]{64}$' then
    raise exception 'Invalid durable database backup path or digest';
  end if;
  if p_backup_parquet_path is distinct from
     ('play_by_play_backups/' || p_season::text || '/' || p_backup_parquet_sha256 || '.parquet')
     or p_staged_parquet_path is distinct from
     ('play_by_play_staging/' || p_season::text || '/' || p_staged_parquet_sha256 || '.parquet') then
    raise exception 'Invalid Parquet backup/staging paths';
  end if;
  perform pg_catalog.pg_advisory_xact_lock(728401, p_season);
  if exists (
    select 1 from public.dst_refresh_operations
    where season_id = p_season and status not in ('recovered','completed')
  ) then
    raise exception 'Unfinished D/ST refresh exists for season %', p_season;
  end if;
  insert into public.dst_refresh_operations(
    season_id,status,backup_parquet_path,backup_parquet_sha256,
    backup_database_path,backup_database_sha256,staged_parquet_path,staged_parquet_sha256
  ) values (
    p_season,'prepared',p_backup_parquet_path,p_backup_parquet_sha256,
    p_backup_database_path,p_backup_database_sha256,p_staged_parquet_path,p_staged_parquet_sha256
  ) returning operation_id into v_id;
  return v_id;
end; $$;

revoke all on function public.start_dst_refresh_operation_v2(integer,text,text,text,text,text,text)
  from public,anon,authenticated;
grant execute on function public.start_dst_refresh_operation_v2(integer,text,text,text,text,text,text)
  to service_role;
