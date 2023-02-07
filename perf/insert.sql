drop database if exists perf;
create database perf;
\c perf

DROP TABLE IF EXISTS orders;

CREATE TABLE orders
(O_ORDERKEY BIGINT,
O_CUSTKEY INT,
O_ORDERSTATUS CHAR(1),
O_TOTALPRICE DECIMAL(15,2),
O_ORDERDATE DATE,
O_ORDERPRIORITY CHAR(15), 
O_CLERK  CHAR(15), 
O_SHIPPRIORITY INTEGER,
O_COMMENT VARCHAR(79),
dummy text)
--WITH (appendonly=true, orientation=column, compresstype=zlib)
--WITH (appendonly=true, compresstype=zlib)
DISTRIBUTED BY (O_ORDERKEY)
PARTITION BY RANGE (O_ORDERDATE)
--(start('1992-01-01') INCLUSIVE end ('1998-12-31') INCLUSIVE every (30),default partition others
(start('1990-01-01') INCLUSIVE end ('1999-12-31') INCLUSIVE every (30)
);

create or replace function orders_insert(n int)
returns int
as $$
begin
  for i in 1..n
  loop
    INSERT INTO orders values (4, 1, 'a', 1.00, '1993-01-01', 'foo', 'bar', 1, 'none');
  end loop;
  return n;
end;
$$ language plpgsql;

-- INSERT
select 'INSERT (single-row)';
\timing on
select orders_insert(100000);
\timing off
truncate orders;

--select 'INSERT (multi-row)';
--\timing on
--insert into orders select n, 1, 'a', 1.00, ('1990-01-01'::date + (n%3650)), 'foo', 'bar', 1, 'none' from generate_series(1,100000000) n;
--\timing off
--truncate orders;

\c postgres
drop database perf;
