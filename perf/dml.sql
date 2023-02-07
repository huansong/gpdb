-- INSERT
select 'INSERT (single-row)';
\timing on
select orders_insert(10000);
\timing off
truncate orders;

select 'INSERT (multi-row)';
\timing on
insert into orders select n, 1, 'a', 1.00, ('1990-01-01'::date + (n%3650)), 'foo', 'bar', 1, 'none' from generate_series(1,100000000) n;
\timing off
truncate orders;

-- COPY
select 'COPY';
\timing on
copy orders from '/tmp/tpchdata/orders.tbl' (DELIMITER '|');
\timing off

-- UPDATE
insert into orders select * from orders;
insert into orders select * from orders;
insert into orders select * from orders;
insert into orders select * from orders;

select 'UDPATE (distribution key)';
\timing on
UPDATE orders SET O_ORDERKEY = O_ORDERKEY+1;
\timing off
vacuum orders;

select 'UDPATE (partition key)';
\timing on
--UPDATE orders SET O_ORDERDATE = O_ORDERDATE + INTERVAL '1day';
UPDATE orders SET O_ORDERDATE = O_ORDERDATE;
\timing off
vacuum orders;

select 'UDPATE (normal column)';
\timing on
UPDATE orders SET O_ORDERSTATUS = 'b';
\timing off
truncate orders;

insert into orders select n, 1, 'a', 1.00, ('1990-01-01'::date + (n%3650)), 'foo', 'bar', 1, 'none' from generate_series(1,100000000) n;
-- DELETE
select 'DELETE';
\timing on
DELETE FROM orders;
\timing off

