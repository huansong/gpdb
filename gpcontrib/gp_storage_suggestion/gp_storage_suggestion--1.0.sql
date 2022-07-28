drop function if exists gp_suggest_table_storage();
drop view if exists gp_suggest_table_storage_rawstats;
create view gp_suggest_table_storage_rawstats as 
	select a.amname as cur_am, '' as suggest_am, '' as reasons, s.relid, s.relname, c.relnatts, c.reltuples, s.seq_tup_read, s.n_tup_ins, s.n_tup_upd, s.n_tup_del, s.n_tup_hot_upd, s.n_live_tup, s.n_dead_tup, sio.heap_blks_read, sio.heap_blks_hit, sio.toast_blks_read, sio.toast_blks_hit
	from pg_class c, pg_am a, pg_stat_user_tables s, pg_statio_user_tables sio
	where c.relam = a.oid and s.relid = c.oid and s.relid = sio.relid;

create or replace function gp_suggest_table_storage()
returns table (
	relname name,
	relid oid,
	cur_am name,
	suggest_am text,
	reasons text,
	suggest_sql text
)
as $$
declare
	heapscore	int := 0;
	aoscore 	int := 0;
	coscore		int := 0;

	heapreasons 	text := '';
	aoreasons 	text := '';
	coreasons 	text := '';

	curreason 	text := '';
	curamscore 	int := 0;

	rrow 		gp_suggest_table_storage_rawstats%rowtype;
begin
	drop table if exists gp_suggest_table_storage_tempres;
	create temp table gp_suggest_table_storage_tempres (relname name, relid oid, cur_am name, suggest_am text, reasons text, suggest_sql text);

	for rrow in select * from gp_suggest_table_storage_rawstats loop
		heapscore 	:= 0;
		aoscore 	:= 0;
		coscore 	:= 0;
		curamscore 	:= 0;
		heapreasons 	:= '';
		aoreasons 	:= '';
		coreasons 	:= '';

		-- The temp table itself shouldn't count
		continue when rrow.relname = 'gp_suggest_table_storage_tempres'; 

		-- Check conditions
		-- 1. Table size
		if pg_relation_size(rrow.relname::text) > 1024 * 1024 then
			curreason	:= 'large table; ';
			aoscore 	:= aoscore + 1;
			aoreasons 	:= curreason || aoreasons;
			coscore 	:= coscore + 1;
			coreasons 	:= curreason || coreasons;
		elsif pg_relation_size(rrow.relname::text) < 1024 * 32 then
			curreason	:= 'small table; ';
			heapscore 	:= heapscore + 1;
			heapreasons 	:= curreason || heapreasons;
		end if;

		-- 2. INSERT vs. SELECT
		if rrow.n_tup_ins > rrow.seq_tup_read * 10 then
			curreason 	:= 'high INSERT/SELECT ratio; ';
			aoscore 	:= aoscore + 1;
			aoreasons 	:= curreason || aoreasons;
			coscore 	:= coscore + 1;
			coreasons 	:= curreason || coreasons;
		elsif rrow.seq_tup_read > rrow.n_tup_ins * 10 then 
			curreason 	:= 'high SELECT/INSERT ratio; ';
			heapscore 	:= heapscore + 1;
			heapreasons 	:= curreason || heapreasons;
		end if;

		-- 3. Frequency of UPDATE/DELETE
		if rrow.n_tup_del > rrow.n_tup_ins / 10 then
			curreason 	:= 'frequent UPDATE/DELETE; ';
			heapscore 	:= heapscore + 1;
			heapreasons 	:= curreason || heapreasons;
		elsif rrow.n_tup_del < rrow.n_tup_ins / 1000 then
			curreason 	:= 'rarely UPDATE/DELETE; ';
			aoscore 	:= aoscore + 1;
			aoreasons 	:= curreason || aoreasons;
			coscore 	:= coscore + 1;
			coreasons 	:= curreason || coreasons;
		end if;

		-- 5. Toast table access (imply large size or large number of columns)
		if rrow.toast_blks_read > rrow.heap_blks_read then
			curreason 	:= 'large rows; ';
			coscore 	:= coscore + 1;
			coreasons 	:= curreason || coreasons;
		end if;

		-- 6. Large row size and small number of columns
		-- FIXME: we actually want to check if there's certain large 
		-- column because only that makes sense to AOCO. For now we 
		-- can only check average.
		if rrow.relnatts < 5 and rrow.reltuples > 0 and pg_relation_size(rrow.relname::text) / rrow.reltuples > 1024 then
			curreason 	:= 'large row size but small number of columns; ';
			coscore 	:= coscore + 1;
			coreasons 	:= curreason || coreasons;
		end if;

		if rrow.cur_am = 'heap' then
			curamscore := heapscore;
		elsif rrow.cur_am = 'ao_row' then
			curamscore := aoscore;
		elsif rrow.cur_am = 'ao_column' then
			curamscore := coscore;
		end if;

		-- Check score, find the winner. 
		if heapscore > curamscore and heapscore >= aoscore and heapscore >= coscore then
			rrow.suggest_am := 'heap';
			rrow.reasons := heapreasons;
		elsif aoscore > curamscore and aoscore >= heapscore and aoscore >= coscore then
			rrow.suggest_am := 'ao_row';
			rrow.reasons := aoreasons;
		elsif coscore > curamscore and coscore >= heapscore and coscore >= aoscore then
			rrow.suggest_am := 'ao_column';
			rrow.reasons := coreasons;
		else
			-- no winner, no suggestion 
		end if;

		if rrow.suggest_am != '' and rrow.cur_am != rrow.suggest_am then
			insert into gp_suggest_table_storage_tempres values(rrow.relname, 
										rrow.relid, 
										rrow.cur_am, 
										rrow.suggest_am, 
										rrow.reasons, 
										'ALTER TABLE ' || rrow.relname || ' SET ACCESS METHOD ' || rrow.suggest_am || ';');
		end if;
	end loop;

	return query select * from gp_suggest_table_storage_tempres order by relname;
end; $$
language 'plpgsql';

