-- Tests with a narrow, single-column table.

CREATE /* UNLOGGED */ TABLE onecol (i int4);

-- Populate the table with a bunch of INSERT ... SELECT statements.
-- Measure how long it takes, and the resulting table size.
select pg_current_wal_insert_lsn() as wal_before, extract(epoch from now()) as time_before
\gset

INSERT INTO onecol SELECT generate_series(1, 100000);
INSERT INTO onecol SELECT generate_series(1, 100000);
INSERT INTO onecol SELECT generate_series(1, 100000);
INSERT INTO onecol SELECT generate_series(1, 100000);
INSERT INTO onecol SELECT generate_series(1, 100000);

select pg_current_wal_insert_lsn() as wal_after, extract(epoch from now()) as time_after
\gset

INSERT INTO results (testname, size, walsize, time)
  VALUES ('onecol, insert-select',
          pg_total_relation_size('onecol'),
	  :'wal_after'::pg_lsn - :'wal_before',
	  :time_after - :time_before);

COPY onecol TO '/tmp/onecol.data'; -- dump the data, for COPY test below.

--
-- Truncate and populate it again with the same data, but this time using COPY.
--
TRUNCATE onecol;

select pg_current_wal_insert_lsn() as wal_before, extract(epoch from now()) as time_before
\gset

COPY onecol FROM '/tmp/onecol.data';

select pg_current_wal_insert_lsn() as wal_after, extract(epoch from now()) as time_after
\gset

INSERT INTO results (testname, size, walsize, time)
  VALUES ('onecol, COPY',
          pg_total_relation_size('onecol'),
	  :'wal_after'::pg_lsn - :'wal_before',
	  :time_after - :time_before);

--
-- SELECT
--

VACUUM FREEZE onecol;

select pg_current_wal_insert_lsn() as wal_before, extract(epoch from now()) as time_before
\gset

SELECT SUM(i) FROM onecol;
SELECT SUM(i) FROM onecol;
SELECT SUM(i) FROM onecol;

select pg_current_wal_insert_lsn() as wal_after, extract(epoch from now()) as time_after
\gset

INSERT INTO results (testname, size, walsize, time)
  VALUES ('onecol, SELECT, seqscan',
          pg_total_relation_size('onecol'),
	  :'wal_after'::pg_lsn - :'wal_before',
	  :time_after - :time_before);

--
-- Bitmap scans
--

CREATE INDEX ON onecol (i);

set enable_seqscan=off;
set enable_indexscan=off;
set enable_bitmapscan=on;

select pg_current_wal_insert_lsn() as wal_before, extract(epoch from now()) as time_before
\gset

SELECT SUM(i) FROM onecol where i < 400000;
SELECT SUM(i) FROM onecol where i < 400000;
SELECT SUM(i) FROM onecol where i < 400000;

select pg_current_wal_insert_lsn() as wal_after, extract(epoch from now()) as time_after
\gset

INSERT INTO results (testname, size, walsize, time)
  VALUES ('onecol, SELECT, bitmap scan',
          pg_total_relation_size('onecol'),
	  :'wal_after'::pg_lsn - :'wal_before',
	  :time_after - :time_before);

reset enable_seqscan;
reset enable_indexscan;
reset enable_bitmapscan;


--
-- Delete half of the rows
--

select pg_current_wal_insert_lsn() as wal_before, extract(epoch from now()) as time_before
\gset

DELETE FROM onecol WHERE i%2 = 0;

select pg_current_wal_insert_lsn() as wal_after, extract(epoch from now()) as time_after
\gset

INSERT INTO results (testname, size, walsize, time)
  VALUES ('onecol, deleted half',
          pg_total_relation_size('onecol'),
	  :'wal_after'::pg_lsn - :'wal_before',
	  :time_after - :time_before);

--
-- And vacuum the deleted rows away
--
select pg_current_wal_insert_lsn() as wal_before, extract(epoch from now()) as time_before
\gset

VACUUM onecol;

select pg_current_wal_insert_lsn() as wal_after, extract(epoch from now()) as time_after
\gset


INSERT INTO results (testname, size, walsize, time)
  VALUES ('onecol, vacuumed',
          pg_total_relation_size('onecol'),
	  :'wal_after'::pg_lsn - :'wal_before',
	  :time_after - :time_before);
