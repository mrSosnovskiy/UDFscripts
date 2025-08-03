CREATE OR REPLACE FUNCTION ${target_schema}.f_full_load_dict(p_table text, p_file_name text)
	RETURNS int
	LANGUAGE plpgsql
	VOLATILE 
AS $$

DECLARE
	v_ext_table_name text;
	v_sql text;
	v_gpfdist text;
	v_result int;
	v_location text := 'std13_102.f_full_load_dict';
	
BEGIN
	
	PERFORM std13_102.f_write_log(
		p_log_type := 'INFO',
		p_log_message := 'START f_full_lod_dict',
		p_location := v_location);
	
	v_ext_table_name := p_table||'_ext';
	
	EXECUTE 'TRUNCATE TABLE '||p_table;
	
	EXECUTE 'DROP EXTERNAL TABLE IF EXISTS '||v_ext_table_name;

	v_gpfdist = 'gpfdist://172.16.128.74:8080/'||p_file_name||'.CSV';
	
	v_sql = 'CREATE EXTERNAL TABLE '||v_ext_table_name||' (LIKE '||p_table||')
			LOCATION ('''||v_gpfdist||'''
			) ON ALL
			FORMAT ''CSV'' (header delimiter '';'' null ''''	escape ''"'' quote ''"'' )
			encoding ''UTF8''
			segment reject limit 10 rows';
	
	RAISE NOTiCE 'EXTERNAL TABLE SQL: %', v_sql;

	EXECUTE v_sql;

	EXECUTE 'INSERT INTO '||p_table||' SELECT * FROM '||v_ext_table_name;
	
	EXECUTE 'SELECT COUNT(1) FROM '||p_table INTO v_result;

	RETURN v_result;
		
END;
$$
EXECUTE ON ANY
