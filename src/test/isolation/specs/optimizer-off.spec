# Turn optimizer OFF for the isolation database

session s1
setup { alter database isolation_regression set optimizer = false; }
step dummy { select 1; }
