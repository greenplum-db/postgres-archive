-- Tests with a narrow, single-column table, with some nulls.

CREATE TABLE nullcol (i int4);

-- Populate the table with a bunch of INSERT ... SELECT statements.
-- Measure how long it takes, and the resulting table size.
select pg_current_wal_insert_lsn() as wal_before, extract(epoch from now()) as time_before
\gset

INSERT INTO nullcol SELECT CASE WHEN g % 2 = 0 THEN NULL ELSE g END FROM generate_series(1, 100000) g ;
INSERT INTO nullcol SELECT NULL FROM generate_series(1, 100000) g;
INSERT INTO nullcol SELECT CASE WHEN g % 2 = 0 THEN NULL ELSE g END FROM generate_series(1, 100000) g ;
INSERT INTO nullcol SELECT g FROM generate_series(1, 100000) g;
INSERT INTO nullcol SELECT CASE WHEN g % 2 = 0 THEN NULL ELSE g END FROM generate_series(1, 100000) g ;

select pg_current_wal_insert_lsn() as wal_after, extract(epoch from now()) as time_after
\gset

INSERT INTO results (testname, size, walsize, time)
  VALUES ('nullcol, insert-select',
          pg_total_relation_size('onecol'),
	  :'wal_after'::pg_lsn - :'wal_before',
	  :time_after - :time_before);

COPY nullcol TO '/tmp/nullcol.data'; -- dump the data, for COPY test below.

--
-- Truncate and populate it again with the same data, but this time using COPY.
--
TRUNCATE nullcol;

select pg_current_wal_insert_lsn() as wal_before, extract(epoch from now()) as time_before
\gset

COPY nullcol FROM '/tmp/nullcol.data';

select pg_current_wal_insert_lsn() as wal_after, extract(epoch from now()) as time_after
\gset


INSERT INTO results (testname, size, walsize, time)
  VALUES ('nullcol, COPY',
          pg_total_relation_size('onecol'),
	  :'wal_after'::pg_lsn - :'wal_before',
	  :time_after - :time_before);
