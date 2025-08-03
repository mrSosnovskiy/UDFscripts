CREATE OR REPLACE FUNCTION std13_102.f_write_log(p_log_type text, p_log_message text, p_location text)
RETURNS void
LANGUAGE plpgsql
VOLATILE
AS $$

DECLARE
    v_log_type text;
    v_log_message text;
    v_sql text;
    v_location text;
    v_res text;
BEGIN
    -- Check message type
    v_log_type := upper(p_log_type);
    v_location := lower(p_location);
    
    IF v_log_type NOT IN ('ERROR', 'INFO') THEN
        RAISE NOTICE 'Illegal log type! Use one of: INFO, ERROR';
    END IF;
    
    RAISE NOTICE '%: %: <%> Location[%]', clock_timestamp(), v_log_type, p_log_message, v_location;
    
    v_log_message := REPLACE(p_log_message, '''', '''''');
    
    v_sql := 'INSERT INTO std13_102.logs(log_id, log_type, log_msg, log_location, is_error, log_timestamp, log_user) '
           || 'VALUES (  nextval(''std13_102.log_seq''), '
           || ''''||v_log_type||''', '
           || COALESCE(''''||v_log_message||'''', '''empty''')||', '
           || COALESCE(''''||v_location||'''', 'null')||', '
           || CASE WHEN v_log_type = 'ERROR' THEN 'TRUE' ELSE 'FALSE' END || ', '
           || 'current_timestamp, current_user);';

    RAISE NOTICE 'INSERT SQL IS: %', v_sql;
--	pg_background_launch(v_sql)
    v_res := dblink('adb_server', v_sql);
END;
$$;


$$
EXECUTE ON ANY;