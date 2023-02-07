-- Fill in some data
copy orders from '/tmp/tpchdata/orders.tbl' (DELIMITER '|');

analyze rootpartition orders;
