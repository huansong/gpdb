insert into orders select n, 1, 'a', 1.00, ('1990-01-01'::date + (n%3650)), 'foo', 'bar', 1, 'none' from generate_series(1,1000000) n;
