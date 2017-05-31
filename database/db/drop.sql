UPDATE pg_database SET datallowconn = 'false' WHERE datname = :'database';
SELECT pg_terminate_backend(pid) from pg_stat_activity where datname = :'database';
DROP DATABASE IF EXISTS :database;
