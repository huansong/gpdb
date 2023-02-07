\timing on

DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS orders_src;

CREATE EXTERNAL TABLE orders_src
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
LOCATION (:LOCATION)
FORMAT 'TEXT' (DELIMITER '|' NULL AS '' ESCAPE AS E'\\');

CREATE TABLE orders
(O_ORDERKEY BIGINT,
O_CUSTKEY INT,
O_ORDERSTATUS CHAR(1),
O_TOTALPRICE DECIMAL(15,2),
O_ORDERDATE DATE,
O_ORDERPRIORITY CHAR(15), 
O_CLERK  CHAR(15), 
O_SHIPPRIORITY INTEGER,
O_COMMENT VARCHAR(79))
WITH (appendonly=true, orientation=column, compresstype=zlib)
DISTRIBUTED BY (O_ORDERKEY)
PARTITION BY RANGE (O_ORDERDATE)
(start('1992-01-01') INCLUSIVE end ('1998-12-31') INCLUSIVE every (30),
default partition others);

INSERT INTO tpch.orders 
(o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, 
            o_orderpriority, o_clerk, o_shippriority, o_comment)
SELECT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, 
            o_orderpriority, o_clerk, o_shippriority, o_comment 
FROM ext_tpch.orders;
