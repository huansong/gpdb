# Reset optimizer setting for the isolation database

session s1
setup { alter database isolation_regression reset optimizer; }
step dummy { select 1; }
