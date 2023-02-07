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
WITH(appendonly=true,orientation=column,compresstype=zlib)
DISTRIBUTED BY (O_ORDERKEY)
PARTITION BY RANGE (O_ORDERDATE)
(start('1992-01-01') INCLUSIVE end ('1998-12-31') INCLUSIVE every (30),default partition others
--(start('1990-01-01') INCLUSIVE end ('1999-12-31') INCLUSIVE every (30)
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

create table foo(a int, b int) with(appendonly, compresstype=zlib);
