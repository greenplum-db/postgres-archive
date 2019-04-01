-- simple tests to iteratively build the zedstore
-- create and drop works
create table t_zedstore(c1 int, c2 int, c3 int) USING zedstore;
drop table t_zedstore;
-- insert and select works
create table t_zedstore(c1 int, c2 int, c3 int) USING zedstore;
insert into t_zedstore select i,i+1,i+2 from generate_series(1, 10)i;
select * from t_zedstore;
-- selecting only few columns work
select c1, c3 from t_zedstore;
-- only few columns in output and where clause work
select c3 from t_zedstore where c2 > 5;

-- Test abort works
begin;
insert into t_zedstore select i,i+1,i+2 from generate_series(21, 25)i;
abort;
insert into t_zedstore select i,i+1,i+2 from generate_series(31, 35)i;
select * from t_zedstore;

--
-- Test indexing
--
create index on t_zedstore (c1);
set enable_seqscan=off;
set enable_indexscan=on;

-- TODO:Bitmap scans are currently not working
set enable_bitmapscan=off;

-- index scan
select * from t_zedstore where c1 = 5;

-- index-only scan
select c1 from t_zedstore where c1 = 5;


--
-- Test DELETE and UPDATE
--
delete from t_zedstore where c2 = 5;
select * from t_zedstore;
delete from t_zedstore where c2 < 5;
select * from t_zedstore;

update t_zedstore set c2 = 100 where c1 = 8;
select * from t_zedstore;
