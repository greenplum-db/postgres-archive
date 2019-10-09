-- Tests with a narrow, single-column table.

CREATE /* UNLOGGED */ TABLE inlinecompress (i text);

-- Populate the table with a bunch of INSERT ... SELECT statements.
-- Measure how long it takes, and the resulting table size.
select pg_current_wal_insert_lsn() as wal_before, extract(epoch from now()) as time_before
\gset

INSERT INTO inlinecompress SELECT repeat('x', 5000) FROM generate_series(1, 10000);
INSERT INTO inlinecompress SELECT repeat('x', 5000) FROM generate_series(1, 10000);
INSERT INTO inlinecompress SELECT repeat('x', 5000) FROM generate_series(1, 10000);
INSERT INTO inlinecompress SELECT repeat('x', 5000) FROM generate_series(1, 10000);
INSERT INTO inlinecompress SELECT repeat('x', 5000) FROM generate_series(1, 10000);

select pg_current_wal_insert_lsn() as wal_after, extract(epoch from now()) as time_after
\gset

INSERT INTO results (testname, size, walsize, time)
  VALUES ('inlinecompress, insert-select',
          pg_total_relation_size('inlinecompress'),
	  :'wal_after'::pg_lsn - :'wal_before',
	  :time_after - :time_before);

COPY inlinecompress TO '/tmp/inlinecompress.data'; -- dump the data, for COPY test below.

--
-- Truncate and populate it again with the same data, but this time using COPY.
--
TRUNCATE inlinecompress;

select pg_current_wal_insert_lsn() as wal_before, extract(epoch from now()) as time_before
\gset

COPY inlinecompress FROM '/tmp/inlinecompress.data';

select pg_current_wal_insert_lsn() as wal_after, extract(epoch from now()) as time_after
\gset

INSERT INTO results (testname, size, walsize, time)
  VALUES ('inlinecompress, COPY',
          pg_total_relation_size('inlinecompress'),
	  :'wal_after'::pg_lsn - :'wal_before',
	  :time_after - :time_before);

--
-- SELECT
--

VACUUM FREEZE inlinecompress;

select pg_current_wal_insert_lsn() as wal_before, extract(epoch from now()) as time_before
\gset

SELECT LENGTH(i) AS len FROM inlinecompress GROUP BY len;
SELECT LENGTH(i) AS len FROM inlinecompress GROUP BY len;
SELECT LENGTH(i) AS len FROM inlinecompress GROUP BY len;

select pg_current_wal_insert_lsn() as wal_after, extract(epoch from now()) as time_after
\gset

INSERT INTO results (testname, size, walsize, time)
  VALUES ('SELECT, seqscan',
          pg_total_relation_size('inlinecompress'),
	  :'wal_after'::pg_lsn - :'wal_before',
	  :time_after - :time_before);

--
-- Bitmap scans
--

CREATE INDEX ON inlinecompress (i);

set enable_seqscan=off;
set enable_indexscan=off;
set enable_bitmapscan=on;

select pg_current_wal_insert_lsn() as wal_before, extract(epoch from now()) as time_before
\gset

SELECT LENGTH(i) AS len FROM inlinecompress where LENGTH(i) < 100000 GROUP BY len;
SELECT LENGTH(i) AS len FROM inlinecompress where LENGTH(i) < 100000 GROUP BY len;
SELECT LENGTH(i) AS len FROM inlinecompress where LENGTH(i) < 100000 GROUP BY len;

select pg_current_wal_insert_lsn() as wal_after, extract(epoch from now()) as time_after
\gset

INSERT INTO results (testname, size, walsize, time)
  VALUES ('SELECT, bitmap scan',
          pg_total_relation_size('inlinecompress'),
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

DELETE FROM inlinecompress WHERE ctid::zstid::int8 % 2 = 0;

select pg_current_wal_insert_lsn() as wal_after, extract(epoch from now()) as time_after
\gset

INSERT INTO results (testname, size, walsize, time)
  VALUES ('inlinecompress, deleted half',
          pg_total_relation_size('inlinecompress'),
	  :'wal_after'::pg_lsn - :'wal_before',
	  :time_after - :time_before);

--
-- And vacuum the deleted rows away
--
select pg_current_wal_insert_lsn() as wal_before, extract(epoch from now()) as time_before
\gset

VACUUM inlinecompress;

select pg_current_wal_insert_lsn() as wal_after, extract(epoch from now()) as time_after
\gset


INSERT INTO results (testname, size, walsize, time)
  VALUES ('inlinecompress, vacuumed',
          pg_total_relation_size('inlinecompress'),
	  :'wal_after'::pg_lsn - :'wal_before',
	  :time_after - :time_before);
