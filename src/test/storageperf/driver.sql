--
-- Main script, to run all the tests, and print the results.
--
--

-- First run the tests using heap.
DROP SCHEMA IF EXISTS storagetest_heap CASCADE;
CREATE SCHEMA storagetest_heap;
SET search_path='storagetest_heap';

CREATE TABLE results (testname text, val numeric) USING heap;

SET default_table_access_method=heap;
\i tests.sql


-- Repeat with zedstore

DROP SCHEMA IF EXISTS storagetest_zedstore CASCADE;
CREATE SCHEMA storagetest_zedstore;
SET search_path='storagetest_zedstore';

CREATE TABLE results (testname text, val numeric) USING heap;

SET default_table_access_method=zedstore;
\i tests.sql


SET search_path='public';

SELECT COALESCE(h.testname, zs.testname) as testname,
       h.val as heap,
       zs.val as zedstore,
       round(zs.val / h.val, 2) as "heap / zedstore"
FROM storagetest_heap.results h
FULL OUTER JOIN storagetest_zedstore.results zs ON (h.testname = zs.testname);
