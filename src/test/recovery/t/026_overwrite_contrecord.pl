# Copyright (c) 2021, PostgreSQL Global Development Group

# Tests for already-propagated WAL segments ending in incomplete WAL records.

use strict;
use warnings;

use FindBin;
use PostgresNode;
use TestLib;
use Test::More;

plan tests => 2;

# Test: Create a physical replica that's missing the last WAL file,
# then restart the primary to create a divergent WAL file and observe
# that the replica replays the "overwrite contrecord" from that new
# file.

my $node = PostgresNode->get_new_node('primary');
$node->init(allows_streaming => 1);
$node->append_conf('postgresql.conf', 'wal_keep_segments=16');
$node->start;

$node->safe_psql('postgres', 'create table filler (a int, b text)');
$node->safe_psql('postgres', 'create table t (a int, b int[]) WITH (appendonly=true, blocksize=2097152)');

# Now consume all remaining room in the current WAL segment, leaving
# space enough only for the start of a largish record.
$node->safe_psql(
	'postgres', q{
DO $$
DECLARE
    wal_segsize int :=
        (max(setting) filter (where name = 'wal_segment_size'))::int *
        (max(setting) filter (where name = 'wal_block_size'))::int from pg_settings ;
    remain int;
    iters  int := 0;
BEGIN
    LOOP
        INSERT into filler
        select g, repeat(md5(g::text), (random() * 60 + 1)::int)
        from generate_series(1, 10) g;

        remain := wal_segsize - (pg_current_xlog_insert_location() - '0/0') % wal_segsize;
        IF remain < 2 * setting::int from pg_settings where name = 'block_size' THEN
            RAISE log 'exiting after % iterations, % bytes to end of WAL segment', iters, remain;
            EXIT;
        END IF;
        iters := iters + 1;
    END LOOP;
END
$$;
});

$node->safe_psql('postgres', 'checkpoint');

note "start ",
  $node->safe_psql('postgres', 'select pg_current_xlog_insert_location()');
my $initfile = $node->safe_psql('postgres',
	'SELECT pg_xlogfile_name(pg_current_xlog_insert_location())');
$node->safe_psql('postgres',
qq{INSERT INTO t SELECT 1, array_agg(x) from generate_series(1, 24000) x}
);

sleep 1;
my $endfile = $node->safe_psql('postgres',
	'SELECT pg_xlogfile_name(pg_current_xlog_insert_location())');
note "end: ",
  $node->safe_psql('postgres', 'select pg_current_xlog_insert_location()');
ok($initfile != $endfile, "$initfile differs from $endfile");

# Now stop abruptly, to avoid a stop checkpoint.  We can remove the tail file
# afterwards, and on startup the large message should be overwritten with new
# contents
$node->stop('immediate');

unlink $node->basedir . "/pgdata/pg_xlog/$endfile"
  or die "could not unlink "
  . $node->basedir
  . "/pgdata/pg_xlog/$endfile: $!";

# OK, create a standby at this spot.
$node->backup_fs_cold('backup');
my $node_standby = PostgresNode->get_new_node('standby');
$node_standby->init_from_backup($node, 'backup', has_streaming => 1);

$node_standby->start;
$node->start;

$node->safe_psql('postgres',
	qq{create table foo (a text); insert into foo values ('hello')});

# We couldn't poll query on mirror because no hot standby is supported in GPDB
sleep 5;

#my $until_lsn =
#  $node->safe_psql('postgres', "SELECT pg_current_xlog_insert_location()");
#my $caughtup_query =
#  "SELECT '$until_lsn'::pg_lsn <= pg_last_xlog_replay_location()";
#$node_standby->poll_query_until('postgres', $caughtup_query)
#  or die "Timed out while waiting for standby to catch up";
#ok($node_standby->safe_psql('postgres', 'select * from foo') eq 'hello',
#	'standby replays past overwritten contrecord');

# Verify message appears in standby's log
my $log = slurp_file($node_standby->logfile);
like(
	$log,
	qr[sucessfully skipped missing contrecord at],
	"found log line in standby");

$node_standby->stop;
$node->stop;
