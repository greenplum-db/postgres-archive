-- Tests with a narrow, single-column table, with some nulls.

CREATE UNLOGGED TABLE nullcol (i int4);

-- Populate the table with a bunch of INSERT ... SELECT statements.
-- Measure how long it takes, and the resulting table size.
select extract(epoch from now()) as before
\gset

INSERT INTO nullcol SELECT CASE WHEN g % 2 = 0 THEN NULL ELSE g END FROM generate_series(1, 100000) g ;
INSERT INTO nullcol SELECT NULL FROM generate_series(1, 100000) g;
INSERT INTO nullcol SELECT CASE WHEN g % 2 = 0 THEN NULL ELSE g END FROM generate_series(1, 100000) g ;
INSERT INTO nullcol SELECT g FROM generate_series(1, 100000) g;
INSERT INTO nullcol SELECT CASE WHEN g % 2 = 0 THEN NULL ELSE g END FROM generate_series(1, 100000) g ;

select extract(epoch from now()) as after
\gset

INSERT INTO results (testname, val) VALUES ('nullcol, insert-select, size', pg_total_relation_size('nullcol'));
INSERT INTO results (testname, val) VALUES ('nullcol, insert-select, time', :after - :before);

COPY nullcol TO '/tmp/nullcol.data'; -- dump the data, for COPY test below.

--
-- Truncate and populate it again with the same data, but this time using COPY.
--
TRUNCATE nullcol;

select extract(epoch from now()) as before
\gset

COPY nullcol FROM '/tmp/nullcol.data';

select extract(epoch from now()) as after
\gset

INSERT INTO results (testname, val) VALUES ('nullcol, COPY, size', pg_total_relation_size('nullcol'));
INSERT INTO results (testname, val) VALUES ('nullcol, COPY, time', :after - :before);
