DROP TABLE IF EXISTS public.wide_table;
CREATE TABLE public.wide_table(i int, j int, k int, l int, m int, n int, o int);
INSERT INTO public.wide_table SELECT g, g+1, g+2, g+3, g+4, g+5, g+6 FROM generate_series(1, 100000) g;

-- DELETE

select pg_current_wal_insert_lsn() as wal_before, extract(epoch from now()) as time_before
\gset

\! echo "BEGIN; DELETE FROM public.wide_table WHERE i >= 1; ROLLBACK;" > /tmp/dml-pgbench-script.sql

\! pgbench -n -c 1 -t40 -f /tmp/dml-pgbench-script.sql postgres

select pg_current_wal_insert_lsn() as wal_after, extract(epoch from now()) as time_after
\gset
INSERT INTO results (testname, size, walsize, time)
VALUES ('dml, pgbench, DELETE',
        pg_total_relation_size('public.wide_table'),
        :'wal_after'::pg_lsn - :'wal_before',
        :time_after - :time_before);

-- DELETE RETURNING

select pg_current_wal_insert_lsn() as wal_before, extract(epoch from now()) as time_before
\gset

\! echo "BEGIN; DELETE FROM public.wide_table WHERE i >= 1 RETURNING j; ROLLBACK;" > /tmp/dml-pgbench-script.sql

\! pgbench -n -c 1 -t40 -f /tmp/dml-pgbench-script.sql postgres

select pg_current_wal_insert_lsn() as wal_after, extract(epoch from now()) as time_after
\gset
INSERT INTO results (testname, size, walsize, time)
VALUES ('dml, pgbench, DELETE RETURNING',
        pg_total_relation_size('public.wide_table'),
        :'wal_after'::pg_lsn - :'wal_before',
        :time_after - :time_before);

-- UPSERT
CREATE UNIQUE INDEX wide_table_i ON public.wide_table(i);
select pg_current_wal_insert_lsn() as wal_before, extract(epoch from now()) as time_before
\gset

\! echo "BEGIN; INSERT INTO public.wide_table SELECT g FROM generate_series(1, 100000) g ON CONFLICT (i) DO UPDATE SET j=-1, k=-1, l=-1, m=-1, n=-1, o=-1 WHERE public.wide_table.k>=1; ROLLBACK;" > /tmp/dml-pgbench-script.sql

\! pgbench -n -c 1 -t5 -f /tmp/dml-pgbench-script.sql postgres

select pg_current_wal_insert_lsn() as wal_after, extract(epoch from now()) as time_after
\gset
INSERT INTO results (testname, size, walsize, time)
VALUES ('dml, pgbench, UPSERT',
        pg_total_relation_size('public.wide_table'),
        :'wal_after'::pg_lsn - :'wal_before',
        :time_after - :time_before);
DROP INDEX public.wide_table_i;

-- ON CONFLICT DO NOTHING
CREATE UNIQUE INDEX wide_table_i ON public.wide_table(i);
select pg_current_wal_insert_lsn() as wal_before, extract(epoch from now()) as time_before
\gset

\! echo "INSERT INTO public.wide_table SELECT g FROM generate_series(1, 100000) g ON CONFLICT (i) DO NOTHING;" > /tmp/dml-pgbench-script.sql

\! pgbench -n -c 1 -t5 -f /tmp/dml-pgbench-script.sql postgres

select pg_current_wal_insert_lsn() as wal_after, extract(epoch from now()) as time_after
\gset
INSERT INTO results (testname, size, walsize, time)
VALUES ('dml, pgbench, ON CONFLICT DO NOTHING',
        pg_total_relation_size('public.wide_table'),
        :'wal_after'::pg_lsn - :'wal_before',
        :time_after - :time_before);
DROP INDEX public.wide_table_i;

-- Tid scans
select pg_current_wal_insert_lsn() as wal_before, extract(epoch from now()) as time_before
\gset

\! echo "SET enable_seqscan TO off;" > /tmp/dml-pgbench-script.sql

\! echo "EXPLAIN ANALYZE SELECT i FROM wide_table WHERE ctid IN (SELECT ctid from wide_table);" >> /tmp/dml-pgbench-script.sql

\! echo "RESET enable_seqscan;" >> /tmp/dml-pgbench-script.sql

\! pgbench -n -c 1 -t5 -f /tmp/dml-pgbench-script.sql postgres

select pg_current_wal_insert_lsn() as wal_after, extract(epoch from now()) as time_after
\gset
INSERT INTO results (testname, size, walsize, time)
VALUES ('dml, pgbench, TidScan',
        pg_total_relation_size('public.wide_table'),
        :'wal_after'::pg_lsn - :'wal_before',
        :time_after - :time_before);

-- Row level locking

select pg_current_wal_insert_lsn() as wal_before, extract(epoch from now()) as time_before
\gset

\! echo "SELECT i FROM wide_table WHERE j >= 100 FOR UPDATE;" > /tmp/dml-pgbench-script.sql

\! pgbench -n -c 1 -t10 -f /tmp/dml-pgbench-script.sql postgres

select pg_current_wal_insert_lsn() as wal_after, extract(epoch from now()) as time_after
\gset
INSERT INTO results (testname, size, walsize, time)
VALUES ('dml, pgbench, Row level locking',
        pg_total_relation_size('public.wide_table'),
        :'wal_after'::pg_lsn - :'wal_before',
        :time_after - :time_before);
