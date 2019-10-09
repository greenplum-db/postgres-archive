-- Tests with a narrow, single-column table.

CREATE /* UNLOGGED */ TABLE toastcol (i text);

-- Populate the table with a bunch of INSERT ... SELECT statements.
-- Measure how long it takes, and the resulting table size.
select pg_current_wal_insert_lsn() as wal_before, extract(epoch from now()) as time_before
\gset

INSERT INTO toastcol SELECT repeat('x', 1000000) FROM generate_series(1, 100);
INSERT INTO toastcol SELECT repeat('x', 1000000) FROM generate_series(1, 100);
INSERT INTO toastcol SELECT repeat('x', 1000000) FROM generate_series(1, 100);
INSERT INTO toastcol SELECT repeat('x', 1000000) FROM generate_series(1, 100);
INSERT INTO toastcol SELECT repeat('x', 1000000) FROM generate_series(1, 100);

select pg_current_wal_insert_lsn() as wal_after, extract(epoch from now()) as time_after
\gset

INSERT INTO results (testname, size, walsize, time)
  VALUES ('toastcol, insert-select',
          pg_total_relation_size('toastcol'),
	  :'wal_after'::pg_lsn - :'wal_before',
	  :time_after - :time_before);

COPY toastcol TO '/tmp/toastcol.data'; -- dump the data, for COPY test below.

--
-- Truncate and populate it again with the same data, but this time using COPY.
--
TRUNCATE toastcol;

select pg_current_wal_insert_lsn() as wal_before, extract(epoch from now()) as time_before
\gset

COPY toastcol FROM '/tmp/toastcol.data';

select pg_current_wal_insert_lsn() as wal_after, extract(epoch from now()) as time_after
\gset

INSERT INTO results (testname, size, walsize, time)
  VALUES ('toastcol, COPY',
          pg_total_relation_size('toastcol'),
	  :'wal_after'::pg_lsn - :'wal_before',
	  :time_after - :time_before);

--
-- SELECT
--

VACUUM FREEZE toastcol;

select pg_current_wal_insert_lsn() as wal_before, extract(epoch from now()) as time_before
\gset

SELECT LENGTH(i) AS len FROM toastcol GROUP BY len;
SELECT LENGTH(i) AS len FROM toastcol GROUP BY len;
SELECT LENGTH(i) AS len FROM toastcol GROUP BY len;

select pg_current_wal_insert_lsn() as wal_after, extract(epoch from now()) as time_after
\gset

INSERT INTO results (testname, size, walsize, time)
  VALUES ('SELECT, seqscan',
          pg_total_relation_size('toastcol'),
	  :'wal_after'::pg_lsn - :'wal_before',
	  :time_after - :time_before);

--
-- Bitmap scans
--

CREATE INDEX ON toastcol (i);

set enable_seqscan=off;
set enable_indexscan=off;
set enable_bitmapscan=on;

select pg_current_wal_insert_lsn() as wal_before, extract(epoch from now()) as time_before
\gset

SELECT LENGTH(i) AS len FROM toastcol where LENGTH(i) < 100000 GROUP BY len;
SELECT LENGTH(i) AS len FROM toastcol where LENGTH(i) < 100000 GROUP BY len;
SELECT LENGTH(i) AS len FROM toastcol where LENGTH(i) < 100000 GROUP BY len;

select pg_current_wal_insert_lsn() as wal_after, extract(epoch from now()) as time_after
\gset

INSERT INTO results (testname, size, walsize, time)
  VALUES ('SELECT, bitmap scan',
          pg_total_relation_size('toastcol'),
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

DELETE FROM toastcol WHERE ctid::zstid::int8 % 2 = 0;

select pg_current_wal_insert_lsn() as wal_after, extract(epoch from now()) as time_after
\gset

INSERT INTO results (testname, size, walsize, time)
  VALUES ('toastcol, deleted half',
          pg_total_relation_size('toastcol'),
	  :'wal_after'::pg_lsn - :'wal_before',
	  :time_after - :time_before);

--
-- And vacuum the deleted rows away
--
select pg_current_wal_insert_lsn() as wal_before, extract(epoch from now()) as time_before
\gset

VACUUM toastcol;

select pg_current_wal_insert_lsn() as wal_after, extract(epoch from now()) as time_after
\gset


INSERT INTO results (testname, size, walsize, time)
  VALUES ('toastcol, vacuumed',
          pg_total_relation_size('toastcol'),
	  :'wal_after'::pg_lsn - :'wal_before',
	  :time_after - :time_before);
