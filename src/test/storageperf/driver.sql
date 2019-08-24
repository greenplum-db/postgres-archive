--
-- Main script, to run all the tests, and print the results.
--
--

-- First run the tests using heap.
DROP SCHEMA IF EXISTS storagetest_heap CASCADE;
CREATE SCHEMA storagetest_heap;
SET search_path='storagetest_heap';

CREATE TABLE results (testname text, size numeric, walsize numeric, time numeric) USING heap;

SET default_table_access_method=heap;
\i tests.sql


-- Repeat with zedstore

DROP SCHEMA IF EXISTS storagetest_zedstore CASCADE;
CREATE SCHEMA storagetest_zedstore;
SET search_path='storagetest_zedstore';

CREATE TABLE results (testname text, size numeric, walsize numeric, time numeric) USING heap;

SET default_table_access_method=zedstore;
\i tests.sql


SET search_path='public';

SELECT COALESCE(h.testname, zs.testname) as testname,
       h.time as "heap time",
       h.size as "heap size",
       h.walsize as "heap wal",

       zs.time as "ZS time",
       zs.size as "ZS size",
       zs.walsize as "ZS wal",
       round(zs.time / h.time, 2) as "time ratio",
       round(zs.size / h.size, 2) as "size ratio",
       case when zs.walsize > 0 and h.walsize > 0 then round(zs.walsize / h.walsize, 2) else null end as "wal ratio"
FROM storagetest_heap.results h
FULL OUTER JOIN storagetest_zedstore.results zs ON (h.testname = zs.testname);
