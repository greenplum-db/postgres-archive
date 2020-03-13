drop view if exists public.redirector;
drop table if exists twocol;

CREATE TABLE twocol (i int4, val int4);

CREATE VIEW public.redirector AS SELECT * FROM twocol;

INSERT INTO twocol SELECT g, 0 FROM generate_series(1, 10) g;

COPY twocol TO '/tmp/twocol.data'; -- dump the data, for COPY test below.
TRUNCATE twocol;
COPY twocol FROM '/tmp/twocol.data';


-- FOR SHARE

select pg_current_wal_insert_lsn() as wal_before, extract(epoch from now()) as time_before
\gset

\! echo "\set i random(1, 10)" > /tmp/lockperf-pgbench-script.sql
\! echo "SELECT i FROM redirector WHERE i = :i FOR SHARE" >> /tmp/lockperf-pgbench-script.sql

\! pgbench -n -t1000 -c 20 -j 4 -f /tmp/lockperf-pgbench-script.sql postgres

select pg_current_wal_insert_lsn() as wal_after, extract(epoch from now()) as time_after
\gset
INSERT INTO results (testname, size, walsize, time)
  VALUES ('lockperf, pgbench, FOR SHARE',
          pg_total_relation_size('twocol'),
	  :'wal_after'::pg_lsn - :'wal_before',
	  :time_after - :time_before);

-- UPDATE

select pg_current_wal_insert_lsn() as wal_before, extract(epoch from now()) as time_before
\gset

\! echo "\set i random(1, 10)" > /tmp/lockperf-pgbench-script.sql
\! echo "UPDATE redirector SET val = val + 1 WHERE i = :i;" >> /tmp/lockperf-pgbench-script.sql

\! pgbench -n -t1000 -c 20 -j 4 -f /tmp/lockperf-pgbench-script.sql postgres

select pg_current_wal_insert_lsn() as wal_after, extract(epoch from now()) as time_after
\gset
INSERT INTO results (testname, size, walsize, time)
  VALUES ('lockperf, pgbench, UPDATE',
          pg_total_relation_size('twocol'),
	  :'wal_after'::pg_lsn - :'wal_before',
	  :time_after - :time_before);
