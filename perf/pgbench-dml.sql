-- INSERT
select 'INSERT (single-row)';
select orders_insert(10000);
truncate orders;

select 'INSERT (multi-row)';
insert into orders select n, 1, 'a', 1.00, ('1990-01-01'::date + (n%3650)), 'foo', 'bar', 1, 'none' from generate_series(1,100000000) n;
truncate orders;

-- COPY
select 'COPY';
copy orders from '/tmp/tpchdata/orders.tbl' (DELIMITER '|');

-- UPDATE
insert into orders select * from orders;
insert into orders select * from orders;
insert into orders select * from orders;
insert into orders select * from orders;

select 'UDPATE (distribution key)';
UPDATE orders SET O_ORDERKEY = O_ORDERKEY+1;
vacuum orders;

select 'UDPATE (partition key)';
--UPDATE orders SET O_ORDERDATE = O_ORDERDATE + INTERVAL '1day';
UPDATE orders SET O_ORDERDATE = O_ORDERDATE;
vacuum orders;

select 'UDPATE (normal column)';
UPDATE orders SET O_ORDERSTATUS = 'b';
truncate orders;

insert into orders select n, 1, 'a', 1.00, ('1990-01-01'::date + (n%3650)), 'foo', 'bar', 1, 'none' from generate_series(1,100000000) n;
-- DELETE
select 'DELETE';
DELETE FROM orders;

