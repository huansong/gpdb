-- Test server crash in case of frozen insert. Make sure that after crash
-- recovery, the frozen insert and index are consistent:
-- 
-- 1. If crash happened before the row is frozen, the row will be invisible;
-- 2. If crash happened after the row is frozen, the row will be visible.
-- 
-- And the above behavior should remain consistent using seqscan or indexscan.
--
-- We test gp_fastsequence here since it does frozen insert and has an index.

-- Case 1. crash after the regular MVCC insert has made to disk, but not
-- the WAL record responsible for updating it to frozen.
-- After crash recovery, the insert will follow regular MVCC and not be seen.
1: create table tab_fi(a int) with (appendoptimized=true) distributed replicated;

-- switch WAL on seg0 to reduce flakiness
1: select gp_segment_id, pg_switch_wal() is not null from gp_dist_random('gp_id') where gp_segment_id = 0;

-- suspend right after the insert into gp_fastsequence during an AO table insert,
-- but before the inplace update that marks it frozen
1: select gp_inject_fault('insert_frozen_before_inplace_update', 'suspend', ''/*DDL*/, ''/*DB*/, 'gp_fastsequence'/*table*/, 1/*start occur*/, 1/*end occur*/, 0/*extra_arg*/, dbid) from gp_segment_configuration where role = 'p' and content = 0;

2>: insert into tab_fi values(1);

1: select gp_wait_until_triggered_fault('insert_frozen_before_inplace_update', 1, dbid) from gp_segment_configuration where role = 'p' and content = 0;

-- switch WAL on seg0, so the new row gets flushed (including its index)
1: select gp_segment_id, pg_switch_wal() is not null from gp_dist_random('gp_id') where gp_segment_id = 0;

-- inject a panic, and resume the insert. The WAL for the corresponding inplace_update is not
-- going to be made to disk (we just flushed WALs), so we won't replay it during restart later.
1: select gp_inject_fault('appendonly_insert', 'panic', ''/*DDL*/, ''/*DB*/, 'tab_fi'/*table*/, 1/*start occur*/, -1/*end occur*/, 0/*extra_arg*/, 2/*db_id*/);
1: select gp_inject_fault('insert_frozen_before_inplace_update', 'reset', dbid) from gp_segment_configuration where role = 'p' and content = 0;

2<:

1q:

-- the GUC setting should be finished after seg0 has started.
-- check the gp_fastsequence content w/ table vs index scan, neither should see the 
-- new inserted row (objmod=1) following MVCC
1: set enable_indexscan = off;
1: set enable_seqscan = on;
1: select distinct f.gp_segment_id, f.objmod, f.last_sequence from gp_dist_random('gp_fastsequence') f left join gp_dist_random('pg_appendonly') a on segrelid = objid and a.gp_segment_id = f.gp_segment_id where a.gp_segment_id = 0 and relid = (select oid from pg_class where relname = 'tab_fi');
1: set enable_indexscan = on;
1: set enable_seqscan = off;
1: select distinct f.gp_segment_id, f.objmod, f.last_sequence from gp_dist_random('gp_fastsequence') f left join gp_dist_random('pg_appendonly') a on segrelid = objid and a.gp_segment_id = f.gp_segment_id where a.gp_segment_id = 0 and relid = (select oid from pg_class where relname = 'tab_fi');
1: reset enable_indexscan;
1: reset enable_seqscan;

1: drop table tab_fi;

-- Case 2. crash after we have flushed the WAL that updates the row to be frozen.
-- After crash recovery, the insert should be seen.
1: create table tab_fi(a int) with (appendoptimized=true) distributed replicated;

-- switch WAL on seg0 to reduce flakiness
1: select gp_segment_id, pg_switch_wal() is not null from gp_dist_random('gp_id') where gp_segment_id = 0;

-- suspend right after the inplace update that marks the gp_fastsequence row frozen
1: select gp_inject_fault('insert_frozen_after_inplace_update', 'suspend', ''/*DDL*/, ''/*DB*/, 'gp_fastsequence'/*table*/, 1/*start occur*/, 1/*end occur*/, 0/*extra_arg*/, dbid) from gp_segment_configuration where role = 'p' and content = 0;

2>: insert into tab_fi values(1);

1: select gp_wait_until_triggered_fault('insert_frozen_after_inplace_update', 1, dbid) from gp_segment_configuration where role = 'p' and content = 0;

-- switch WAL on seg0, so the inplace update record gets flushed
1: select gp_segment_id, pg_switch_wal() is not null from gp_dist_random('gp_id') where gp_segment_id = 0;

-- inject a panic and resume in same way as Case 1. But this time we will be able to replay the frozen insert.
1: select gp_inject_fault('appendonly_insert', 'panic', ''/*DDL*/, ''/*DB*/, 'tab_fi'/*table*/, 1/*start occur*/, -1/*end occur*/, 0/*extra_arg*/, 2/*db_id*/);
1: select gp_inject_fault('insert_frozen_after_inplace_update', 'reset', dbid) from gp_segment_configuration where role = 'p' and content = 0;

2<:

1q:

-- check the gp_fastsequence content w/ table vs index scan, both should see the new inserted row (objmod=1)
1: set enable_indexscan = off;
1: set enable_seqscan = on;
1: select distinct f.gp_segment_id, f.objmod, f.last_sequence from gp_dist_random('gp_fastsequence') f left join gp_dist_random('pg_appendonly') a on segrelid = objid and a.gp_segment_id = f.gp_segment_id where a.gp_segment_id = 0 and relid = (select oid from pg_class where relname = 'tab_fi');
1: set enable_indexscan = on;
1: set enable_seqscan = off;
1: select distinct f.gp_segment_id, f.objmod, f.last_sequence from gp_dist_random('gp_fastsequence') f left join gp_dist_random('pg_appendonly') a on segrelid = objid and a.gp_segment_id = f.gp_segment_id where a.gp_segment_id = 0 and relid = (select oid from pg_class where relname = 'tab_fi');
1: reset enable_indexscan;
1: reset enable_seqscan;

1: drop table tab_fi;

