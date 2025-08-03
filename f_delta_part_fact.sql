CREATE OR REPLACE FUNCTION ${target_schema}.f_delta_part_fact(p_schema_name text, p_target_name text, p_from_name text, p_partition_key text, start_date timestamp, end_date timestamp)
	RETURNS int
	LANGUAGE plpgsql
	VOLATILE
AS $$

DECLARE 
	v_from_name text;
	v_target_name text;
	v_load_interval interval;
	v_iterDate timestamp;
	v_where text;
	v_prt_table text;
	v_cnt_prt int8;
	v_cnt int;

BEGIN
		
	


END;
$$
EXECUTE ON ANY 
