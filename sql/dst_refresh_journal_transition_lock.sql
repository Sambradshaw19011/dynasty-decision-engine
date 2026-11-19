-- Deploy before the writer fence and operation-aware RPC wrappers.
-- Preserve transition rules; serialize status changes with season-scoped writes.
create or replace function public.advance_dst_refresh_operation(
  p_operation_id uuid, p_expected_status text, p_next_status text,
  p_details jsonb default '{}'::jsonb
) returns jsonb language plpgsql security definer set search_path = ''
as $$
declare v_season integer; v_op public.dst_refresh_operations%rowtype;
begin
  -- Never acquire the journal row lock before the season advisory lock.
  select season_id into v_season from public.dst_refresh_operations
  where operation_id = p_operation_id;
  if not found then raise exception 'Operation missing or state changed'; end if;
  perform pg_catalog.pg_advisory_xact_lock(728401, v_season);
  select * into v_op from public.dst_refresh_operations
  where operation_id = p_operation_id for update;
  if not found or v_op.season_id is distinct from v_season
     or v_op.status is distinct from p_expected_status then
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
end;
$$;
revoke all on function public.advance_dst_refresh_operation(uuid,text,text,jsonb)
from public, anon, authenticated;
grant execute on function public.advance_dst_refresh_operation(uuid,text,text,jsonb)
to service_role;
