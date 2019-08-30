--
-- Main script, to run all the tests, and print the results.
--
--

-- Set global variable
\set pg_current_wal_insert_lsn pg_current_xlog_insert_location

-- First run the tests using heap.
DROP SCHEMA IF EXISTS storagetest_heap CASCADE;
CREATE SCHEMA storagetest_heap;
SET search_path='storagetest_heap';

CREATE TABLE results (testname text, size numeric, walsize numeric, time numeric);

checkpoint;
\i tests.sql


-- Repeat with Greenplum AOCO table.

DROP SCHEMA IF EXISTS storagetest_gpdb_aoco CASCADE;
CREATE SCHEMA storagetest_gpdb_aoco;
SET search_path='storagetest_gpdb_aoco';

CREATE TABLE results (testname text, size numeric, walsize numeric, time numeric);

checkpoint;
\i tests_gpdb_aoco.sql


SET search_path='public';

SELECT COALESCE(h.testname, zs.testname) as testname,
       h.time as "heap time",
       h.size as "heap size",
       h.walsize as "heap wal",

       zs.time as "gp time",
       zs.size as "gp size",
       zs.walsize as "gp wal",
       round(zs.time / h.time, 2) as "time ratio",
       round(zs.size / h.size, 2) as "size ratio",
       case when zs.walsize > 0 and h.walsize > 0 then round(zs.walsize / h.walsize, 2) else null end as "wal ratio"
FROM storagetest_heap.results h
FULL OUTER JOIN storagetest_gpdb_aoco.results zs ON (h.testname = zs.testname);
