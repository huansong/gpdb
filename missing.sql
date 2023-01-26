DROP FUNCTION IF EXISTS public.greemplum_check_missing_files();
CREATE FUNCTION public.greemplum_check_missing_files()
    RETURNS TABLE (
    	segment_id int,
	reltablespace oid,
	relid oid,
	relname name,
        relfilenode bigint)
    LANGUAGE plpgsql
    AS $$
DECLARE
    v_function_name text := 'greemplum_check_missing_files';
    v_location int;
    v_sql text;
    v_db_oid text;
    v_gp_tablespace_dir text;
    -- below are per tablespace things 
    r record;
    v_tablespace_oid int;
    v_tablespace_name text;
    v_file_dir text;
BEGIN
    /* 
		The function loops through each tablespace and records what's missing in greenplum_display_missing_files.
		During each loop, we create a table, external table and view
		* The table collects all the relfilenodes found on the database for a given tablespace.
		* The external tables collects all the files that are under the tablespace for the database.
		* The view finds which files are missing.
    */
    DROP TABLE IF EXISTS public.greenplum_display_missing_files;
    CREATE TABLE public.greenplum_display_missing_files (
        segment_id int,
        reltablespace oid,
        relid oid,
        relname name,
        relfilenode bigint);

    FOR r IN
        SELECT *
        FROM pg_tablespace ts
    LOOP 
	    DROP TABLE IF EXISTS public.greenplum_get_refilenodes CASCADE;
	    DROP EXTERNAL WEB TABLE IF EXISTS public.greenplum_get_db_file_ext;

	    -- Set the client min messages to just warning
	    SET client_min_messages TO WARNING;

	    -- Get teh tablespace name 
	    v_tablespace_name := r.spcname;

	    -- Get the GPDB-specific tablespace dir (GP_TABLESPACE_VERSION_DIRECTORY)
	    v_gp_tablespace_dir = (
	    SELECT
		substring(pg_relation_filepath(ns.nspname || '.' || c.relname::text), 'GPDB_[0-9]_[0-9]*')
	    FROM
		pg_class c
		JOIN pg_tablespace t ON c.reltablespace = t.oid
		JOIN pg_namespace ns ON c.relnamespace = ns.oid
	    WHERE
		t.spcname != 'pg_default'
		AND t.spcname != 'pg_global' LIMIT 1);

	    -- Get the database oid
	    v_location := 2000;
	    SELECT d.oid INTO v_db_oid
	    FROM pg_database d
	    WHERE datname = current_database();

	    -- Get the tablespace oid (0 when it's pg_default, not 1663)
	    v_location := 2100;
	    SELECT CASE WHEN v_tablespace_name = 'pg_default' THEN 0 ELSE ts.oid END INTO v_tablespace_oid
	    FROM pg_tablespace ts
	    WHERE spcname = v_tablespace_name;

	    -- Get the file directory path for this tablespace and datbase
	    SELECT CASE 
			WHEN v_tablespace_name = 'pg_default' THEN 'base/' || v_db_oid 
			WHEN v_tablespace_name = 'pg_global' THEN 'global/'
			ELSE 'pg_tblspc/' || v_tablespace_oid::text || '/' || v_gp_tablespace_dir || '/' || v_db_oid
			END
	    INTO v_file_dir;
	    v_location := 2200;
	    EXECUTE v_sql;

	    -- Table to store the relfile records
	    v_location := 4000;
	    v_sql := 'CREATE TABLE public.greenplum_get_refilenodes ('
	    '    segment_id int,'
	    '    reltablespace oid,'
	    '    relid oid,'
	    '    relname name,'
	    '    relfilenode bigint'
	    ')';
	    v_location := 4100;
	    EXECUTE v_sql;
	    -- Store all the data related to the relfilenodes from all
	    -- the segments into the temp table
	    v_location := 5000;
	    v_sql := 'INSERT INTO public.greenplum_get_refilenodes SELECT '
		'  s.gp_segment_id segment_id, '
		'  s.reltablespace, '
		'  s.oid oid, '
		'  s.relname, '
		'  s.relfilenode '
		'FROM '
		'  gp_dist_random(''pg_class'') s ' -- all segment
		'WHERE reltablespace = ' || v_tablespace_oid::text || ' AND relstorage NOT IN (''x'', ''v'', ''f'')';
		v_location := 5100;
	    EXECUTE v_sql;
	   -- Get correct relfilenode for those with pg_class.relfilenode=0
	    v_sql := 'UPDATE public.greenplum_get_refilenodes SET '
		'   relfilenode = pg_relation_filenode(relid) '
		' WHERE '
		'   relfilenode = 0';
	    v_location := 5200;
	    EXECUTE v_sql;
		-- Create a external that runs a shell script to extract all the files 
		-- on the base directory
		v_location := 7000;
	    v_sql := 'CREATE EXTERNAL WEB TABLE public.greenplum_get_db_file_ext ' ||
		    '(segment_id int, relfilenode text, filename text, ' ||
		    'size numeric) ' ||
		    'execute E''ls -l $GP_SEG_DATADIR/' || v_file_dir ||
		    ' | ' ||
		    'grep -v total| ' ||
		    E'awk {''''print ENVIRON["GP_SEGMENT_ID"] "\\t" $9 "\\t" ' ||
		    'ENVIRON["GP_SEG_DATADIR"] "/' || v_file_dir ||
		    E'/" $9 "\\t" $5''''}'' on all ' || 'format ''text''';
	    v_location := 7100;
	    EXECUTE v_sql;
	    -- Drop the view if exists
	    -- Display all the missing files
	    v_location := 9000;
		v_sql := 'CREATE VIEW public.greenplum_display_missing_files_view AS '
		'SELECT '
		'  nodes.segment_id, '
		'  nodes.reltablespace, '
		'  nodes.relid, '
		'  nodes.relname, '
		'  nodes.relfilenode '
		'FROM '
		'  public.greenplum_get_refilenodes nodes LEFT JOIN '
		'  public.greenplum_get_db_file_ext files '
		'  ON nodes.segment_id = files.segment_id AND '
		'  nodes.relfilenode::text = files.relfilenode '
		'WHERE ' 
		'  files.filename IS NULL ';
		v_location := 9100;
	    EXECUTE v_sql;
	    INSERT INTO public.greenplum_display_missing_files
	        SELECT * FROM public.greenplum_display_missing_files_view;
	END LOOP;
    -- Return the data back
    RETURN query (
        SELECT
            *
        FROM public.greenplum_display_missing_files);
    -- Throw the exception whereever it encounters one
    EXCEPTION
        WHEN OTHERS THEN
                RAISE EXCEPTION '(%:%:%)', v_function_name, v_location, sqlerrm;
END;
$$;
SELECT * FROM public.greemplum_check_missing_files();
