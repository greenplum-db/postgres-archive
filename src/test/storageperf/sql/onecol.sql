-- Tests with a narrow, single-column table.

CREATE /* UNLOGGED */ TABLE onecol (i int4);

-- Populate the table with a bunch of INSERT ... SELECT statements.
-- Measure how long it takes, and the resulting table size.
select extract(epoch from now()) as before
\gset

INSERT INTO onecol SELECT generate_series(1, 100000);
INSERT INTO onecol SELECT generate_series(1, 100000);
INSERT INTO onecol SELECT generate_series(1, 100000);
INSERT INTO onecol SELECT generate_series(1, 100000);
INSERT INTO onecol SELECT generate_series(1, 100000);

select extract(epoch from now()) as after
\gset

INSERT INTO results (testname, val) VALUES ('onecol, insert-select, size', pg_total_relation_size('onecol'));
INSERT INTO results (testname, val) VALUES ('onecol, insert-select, time', :after - :before);

COPY onecol TO '/tmp/onecol.data'; -- dump the data, for COPY test below.

--
-- Truncate and populate it again with the same data, but this time using COPY.
--
TRUNCATE onecol;

select extract(epoch from now()) as before
\gset

COPY onecol FROM '/tmp/onecol.data';

select extract(epoch from now()) as after
\gset

INSERT INTO results (testname, val) VALUES ('onecol, COPY, size', pg_total_relation_size('onecol'));
INSERT INTO results (testname, val) VALUES ('onecol, COPY, time', :after - :before);

--
-- SELECT
--

VACUUM FREEZE onecol;

select extract(epoch from now()) as before
\gset

SELECT SUM(i) FROM onecol;
SELECT SUM(i) FROM onecol;
SELECT SUM(i) FROM onecol;

select extract(epoch from now()) as after
\gset

INSERT INTO results (testname, val) VALUES ('SELECT, time', :after - :before);

--
-- Delete half of the rows
--

select extract(epoch from now()) as before
\gset

DELETE FROM onecol WHERE i%2 = 0;

select extract(epoch from now()) as after
\gset

INSERT INTO results (testname, val) VALUES ('onecol, deleted half, size', pg_total_relation_size('onecol'));
INSERT INTO results (testname, val) VALUES ('onecol, deleted half, time', :after - :before);

--
-- And vacuum the deleted rows away
--
select extract(epoch from now()) as before
\gset

VACUUM onecol;

select extract(epoch from now()) as after
\gset

INSERT INTO results (testname, val) VALUES ('onecol, vacuumed, size', pg_total_relation_size('onecol'));
INSERT INTO results (testname, val) VALUES ('onecol, vacuumed, time', :after - :before);
