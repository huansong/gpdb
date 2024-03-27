-- restore-point based transaction isolation

1: create table hs_rp_t1(a int);
1: insert into hs_rp_t1 select * from generate_series(1,10);
1: insert into hs_rp_t1 values(100);
1: select sum(1) from gp_create_restore_point('rp1');
-1S: set gp_restore_point_name_for_hot_standby = 'rp1';
-- should see both 1PC and 2PC
-1S: select count(*) from hs_rp_t1;

-- The primary deletes the result but the standby still uses 'rp1' and see the old result.
1: delete from hs_rp_t1;
-1S: select count(*) from hs_rp_t1;

-- The primary creates a new RP, the standby will use the latest RP, and see the latest result.
1: select sum(1) from gp_create_restore_point('rp2');
-1S: set gp_restore_point_name_for_hot_standby = 'rp2';
-1S: select count(*) from hs_rp_t1;

-- The primary inserts more rows creates a new RP, then deletes & vacuums all the rows.
-- The standby query, using 'rp3', will fail because the snapshot for 'rp3' conflicts with the VACUUM.
1: insert into hs_rp_t1 select * from generate_series(1,10);
1: select sum(1) from gp_create_restore_point('rp3');
1: delete from hs_rp_t1;
1: vacuum hs_rp_t1;
-1S: set gp_restore_point_name_for_hot_standby = 'rp3';
-1S: select count(*) from hs_rp_t1;


--more tests:
-- 1PC before the RP can be seen.

-- standby uses GUC 'gp_restore_point_name_for_hot_standby' to control which RP to use.

-- primary creates same-name RP, what should happen?

-- 1. standby QE is halted and not see an RP, the query would fail
-- 2. standby QD is halted and not see an RP, the query succeeds.

-- auto-generated snapshots should be cleaned up when snapshots are invalidated?

-- 1. QD restarted, and still can use previous RP
-- 2. QE restarted, and still can use previous RP
