-- Tests for restore point (RP) based transaction isolation for hot standby

!\retcode gpconfig -c gp_hot_standby_snapshot_mode -v restorepoint;
!\retcode gpstop -ar;

----------------------------------------------------------------
-- Basic transaction isolation test
----------------------------------------------------------------

1: create table hs_rp_basic(a int, b text);

-- in-progress transaction won't be visible
2: begin;
2: insert into hs_rp_basic select i,'in_progress' from generate_series(1,5) i;
-- transactions completed before the RP: all would be visible on standby, including 1PC and 2PC
1: insert into hs_rp_basic select i,'complete_before_rp1_2pc' from generate_series(1,5) i;
1: insert into hs_rp_basic values(1, 'complete_before_rp1_1pc');

-- take the RP
1: select sum(1) from gp_create_restore_point('rp1');

-- transactions after the RP: won't be visible on standby
1: insert into hs_rp_basic select i,'complete_after_rp1_2pc' from generate_series(1,5) i;
1: insert into hs_rp_basic values(1, 'complete_after_rp1_1pc');

-- set RP name on standby
-1S: set gp_hot_standby_snapshot_restore_point_name = 'rp1';
-- see expected result
-1S: select * from hs_rp_basic;

-- more completed transactions, including completing the in-progress one
1: insert into hs_rp_basic select i,'complete_after_rp1' from generate_series(1,5) i;
2: update hs_rp_basic set b = 'in_progress_at_rp1_complete_after_rp1' where b = 'in_progress';
2: end;
-- still won't be seen on the standby
-1S: select * from hs_rp_basic;

-- a new RP is created
1: select sum(1) from gp_create_restore_point('rp2');
1: insert into hs_rp_basic select i,'complete_after_rp2_2pc' from generate_series(1,5) i;
1: insert into hs_rp_basic select i,'complete_after_rp2_1pc' from generate_series(1,5) i;

-- the standby uses it, and sees all that completed before 'rp2'
-1S: set gp_hot_standby_snapshot_restore_point_name = 'rp2';
-1S: select * from hs_rp_basic;

-- using an earlier RP, and sees result according to that
-1S: set gp_hot_standby_snapshot_restore_point_name = 'rp1';
-1S: select * from hs_rp_basic;

----------------------------------------------------------------
-- More testing around the gp_hot_standby_snapshot_restore_point_name GUC
----------------------------------------------------------------

-- standby uses "restorepoint" snapshot mode, but gives no RP name, should use the
-- latest RP which is "rp2". This can be inconsistent in real life too, if not with
-- the "synchronous_commit=remote_apply" setting.
-1S: reset gp_hot_standby_snapshot_restore_point_name;
-1S: select * from hs_rp_basic;

-- standby uses an invalid RP name, should complain
-1S: set gp_hot_standby_snapshot_restore_point_name = 'rp-non-exist';
-1S: select * from hs_rp_basic;

-- primary creates a same-name RP, standby won't create snapshot for it,
-- and still use the old snapshot
1: select sum(1) from gp_create_restore_point('rp1');
-1S: set gp_hot_standby_snapshot_restore_point_name = 'rp1';
-1S: select * from hs_rp_basic;

----------------------------------------------------------------
-- Tests that simulates out-of-sync WAL replays on standby coordinator and segments
----------------------------------------------------------------
-- remote_apply needs to be turned off for these tests
-- XXX: probably need fault injection to make sure no flakiness
1: set synchronous_commit = off;

--
-- Case 1: standby coordinator WAL replay is delayed
--
-1S: select pg_wal_replay_pause();
1: select sum(1) from gp_create_restore_point('rp-qe-only');

-- This RP is only replayed on (some) QEs so far, so the query will fail.
-1S: set gp_hot_standby_snapshot_restore_point_name = 'rp-qe-only';
-1S: select * from hs_rp_basic;
-- And if we use the latest RP, we would be using 'rp2' and see corresponding results.
-1S: reset gp_hot_standby_snapshot_restore_point_name;
-1S: select * from hs_rp_basic;

-- resume replay on QD
-1S: select pg_wal_replay_resume();

--
-- Case 2: standby segment WAL replay is delayed
--
-1S: select pg_wal_replay_pause() from gp_dist_random('gp_id');
1: select sum(1) from gp_create_restore_point('rp-qd-only');

-- This RP is only replayed on QD so far, so the query will fail.
-1S: set gp_hot_standby_snapshot_restore_point_name = 'rp-qd-only';
-1S: select * from hs_rp_basic;
-- If we use the latest RP, it would still be the one above, so same result.
-1S: reset gp_hot_standby_snapshot_restore_point_name;
-1S: select * from hs_rp_basic;
-- Unless we use an RP that we know is consistent
-1S: set gp_hot_standby_snapshot_restore_point_name = 'rp2';
-1S: select * from hs_rp_basic;

--resume
-1S: select pg_wal_replay_resume() from gp_dist_random('gp_id');

-- reset for the rest of tests
1: reset synchronous_commit;

----------------------------------------------------------------
-- Crash recovery test
----------------------------------------------------------------

1: create table hs_rp_crash(a int);

--
-- Case 1: standby coordinator/segment restart, and replay from a checkpoint 
-- that's behind the RP which they are going to use.
--

-- completed tx before RP, will be seen
1: insert into hs_rp_crash select * from generate_series(1,10);
2: begin;
2: insert into hs_rp_crash select * from generate_series(11,20);
1: select sum(1) from gp_create_restore_point('rptest_crash1');

-- completed tx after RP, won't be seen
1: insert into hs_rp_crash select * from generate_series(21,30);

--in-progress tx at the time of RP, won't be seen
2: end;

-- make sure restarted standby would redo *after* the RP
1: checkpoint;
-1S: checkpoint;

-- standby coordinator restarts
-1S: select gp_inject_fault('exec_simple_query_start', 'panic', dbid) from gp_segment_configuration where content=-1 and role='m';
-1S: select 1;
-1Sq:

-- sees expected result corresponding to the RP
-1S: set gp_hot_standby_snapshot_restore_point_name = 'rptest_crash1';
-1S: select count(*) from hs_rp_crash;

-- standby segment restarts, and still can use previous RP too

-- seg0 restarts
-1S: select gp_inject_fault('exec_mpp_query_start', 'panic', dbid) from gp_segment_configuration where content=0 and role='m';
-1S: select count(*) from hs_rp_crash;

-- sees expected result corresponding to the RP
-1S: select count(*) from hs_rp_crash;

--
-- Case 2: standby coordinator/segment restart, and replay from a checkpoint 
-- that's behind the RP which they are going to use.
-- The effect should be the same as Case 1.
--

1: truncate hs_rp_crash;

-- make sure restarted standby would redo *before* the RP
-- completed tx before RP, will be seen
1: insert into hs_rp_crash select * from generate_series(1,10);
2: begin;
2: insert into hs_rp_crash select * from generate_series(11,20);

-- make sure restarted standby would redo *before* the RP
1: checkpoint;
-1S: checkpoint;

1: select sum(1) from gp_create_restore_point('rptest_crash2');

-- completed tx after RP, won't be seen
1: insert into hs_rp_crash select * from generate_series(21,30);

--in-progress tx at the time of RP, won't be seen
2: end;
2q:

-- standby coordinator restarts
-1S: select gp_inject_fault('exec_simple_query_start', 'panic', dbid) from gp_segment_configuration where content=-1 and role='m';
-1S: select 1;
-1Sq:

-- sees expected result corresponding to the RP
-1S: set gp_hot_standby_snapshot_restore_point_name = 'rptest_crash2';
-1S: select count(*) from hs_rp_crash;

-- standby segment restarts, and still can use previous RP too

-- seg0 restarts
-1S: select gp_inject_fault('exec_mpp_query_start', 'panic', dbid) from gp_segment_configuration where content=0 and role='m';
-1S: select count(*) from hs_rp_crash;

-- sees expected result corresponding to the RP
-1S: select count(*) from hs_rp_crash;

----------------------------------------------------------------
-- Snapshot conflict test
----------------------------------------------------------------

1: create table hs_rp_conflict(a int);

-- The primary inserts some rows, creates an RP, then deletes & vacuums all the rows.
-- The standby query, using that RP, will conflict and be cancelled.
1: insert into hs_rp_conflict select * from generate_series(1,10);
1: select sum(1) from gp_create_restore_point('rp_conflict');
-1S: set gp_hot_standby_snapshot_restore_point_name = 'rp_conflict';
1: delete from hs_rp_conflict;
1: vacuum hs_rp_conflict;
1q:

-- The RP is invalidated and the snapshot deleted, the query will fail
-1S: select count(*) from hs_rp_conflict;
-1Sq:

-- Because the VACUUM invalidates the latest RP, it effectively also invalidated all 
-- RPs prior to that. So segments shouldn't have any snapshots left on disk.
-- In order to run the pg_ls_dir, set the snapshot mode to unsync (since all RPs/snapshots are gone).
-- Go back to unsync for any other tests
!\retcode gpconfig -c gp_hot_standby_snapshot_mode -v unsync;
!\retcode gpstop -ar;

-- there shouldn't be any previously exported snapshots left
-1S: select gp_segment_id, pg_ls_dir('pg_snapshots') from gp_dist_random('gp_id');
-1S: select pg_ls_dir('pg_snapshots');
