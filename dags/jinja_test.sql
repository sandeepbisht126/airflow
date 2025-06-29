--INSERT INTO public.huge_table_parallel select * from public.huge_table where created_at = CAST({aod} as date) and country = {cc}
--INSERT INTO public.huge_table_parallel select * from public.huge_table where created_at = '{{ macros.ds_add(ds, -1 ) }}':: date limit 10
select * from public.huge_table