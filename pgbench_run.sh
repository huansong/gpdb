#!/bin/bash

# Test configs
db="GPDB7" #GPDB6, GPDB7 or PG
utility_mode=off
GDD=on
orca=on
part="partitioned" # partitioned, normal
low_clients=64
high_clients=128
declare -a am_types=(
"heap"
#"ao"
#"co"
)
declare -a dmls=(
"single-insert"
#"multi-insert"
#"update"
#"delete"
#"copy"
)

if [ $db = "GPDB7" ]; then
  source /home/csteam/workspace/gpdb7/gpdb-env.sh;
  gpconfig_coordinator_opt="--coordinatoronly"
elif [ $db = "GPDB6" ]; then
  source /home/csteam/workspace/gpdb6/gpdb-env.sh;
  gpconfig_coordinator_opt="--masteronly"
fi

# set GPDB config
if [ $db != "PG" ]; then
  gpstop -ai
  gpstart -a
  gpconfig -c optimizer -v $orca
  gpconfig -c max_connections -v 600 $gpconfig_coordinator_opt
  gpconfig -c gp_enable_global_deadlock_detector -v $GDD
  gpstop -ari
fi

out_file=perf/pgbench.out
echo "" > $out_file

for am in "${am_types[@]}"; do
  if [ $db = "PG" ] && [ $am != "heap" ]; then
    continue;
  fi
  for dml in "${dmls[@]}"; do
    # test-specific stuff 
    if [ $part = "partitioned" ]; then
      if [ $dml = "single-insert" ]; then trx_per_c=1000; init_data="no"; script="pgbench-insert-single";
      elif [ $dml = "multi-insert" ]; then trx_per_c=5; init_data="no"; script="pgbench-insert-multi";
      elif [ $dml = "update" ]; then trx_per_c=10; init_data="yes"; script="pgbench-update";
      elif [ $dml = "copy" ]; then trx_per_c=10; init_data="yes"; script="pgbench-copy";
      elif [ $dml = "delete" ]; then trx_per_c=10; init_data="yes"; script="pgbench-delete"; fi
    elif [ $part = "normal" ]; then
      if [ $dml = "single-insert" ]; then trx_per_c=10000; init_data="no"; script="pgbench-insert-single-nonpart";
      elif [ $dml = "multi-insert" ]; then trx_per_c=10; init_data="no"; script="pgbench-insert-multi-nonpart";
      elif [ $dml = "update" ]; then trx_per_c=100000; init_data="yes"; script="pgbench-update-nonpart";
      elif [ $dml = "copy" ]; then trx_per_c=10; init_data="yes"; script="pgbench-copy-nonpart";
      elif [ $dml = "delete" ]; then trx_per_c=1; init_data="yes"; script="pgbench-delete-nonpart"; fi
    fi
 
    echo "---------------------" >> $out_file
    echo "Testing $dml on $am $part table, for $db, ORCA is $orca, GDD=$GDD" >> $out_file
 
    for ((num_clients=$low_clients; num_clients<=$high_clients; num_clients=num_clients*2)); do
      if [ $db = "PG" ]; then
        psql postgres -f perf/upstream_prepare.sql;  #PG
      else
        psql postgres -f perf/prepare-$am.sql ; #GPDB
      fi;
 
      if [ $init_data = "yes" ] && [ $part = "partitioned" ]; then psql perf -f perf/initdata.sql; 
      elif [ $init_data = "yes" ] && [ $part = "normal" ]; then psql perf -f perf/initdata-nonpart.sql; fi
 
      pgbench -c $num_clients -t $trx_per_c -f perf/$script.sql -n perf | grep -E "number of clients|latency|excluding connections|ERROR|WARN" >> $out_file
    done
  done
done

cat perf/pgbench.out
