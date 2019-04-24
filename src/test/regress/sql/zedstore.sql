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
set enable_bitmapscan=off;

-- index scan
select * from t_zedstore where c1 = 5;

-- index-only scan
select c1 from t_zedstore where c1 = 5;

-- bitmap scan
set enable_indexscan=off;
set enable_bitmapscan=on;
select c1, c2 from t_zedstore where c1 between 5 and 10;

--
-- Test DELETE and UPDATE
--
delete from t_zedstore where c2 = 5;
select * from t_zedstore;
delete from t_zedstore where c2 < 5;
select * from t_zedstore;

update t_zedstore set c2 = 100 where c1 = 8;
select * from t_zedstore;

--
-- Test VACUUM
--
vacuum t_zedstore;
select * from t_zedstore;

--
-- Test toasting
--
create table t_zedtoast(c1 int, t text) USING zedstore;
insert into t_zedtoast select i, repeat('x', 10000) from generate_series(1, 10) i;

select c1, length(t) from t_zedtoast;

--
-- Test NULL values
--
create table t_zednullvalues(c1 int, c2 int) USING zedstore;
insert into t_zednullvalues values(1, NULL), (NULL, 2);
select * from t_zednullvalues;
select c2 from t_zednullvalues;
update t_zednullvalues set c1 = 1, c2 = NULL;
select * from t_zednullvalues;

--
-- Test COPY
--
create table t_zedcopy(a serial, b int, c text not null default 'stuff', d text,e text) USING zedstore;

COPY t_zedcopy (a, b, c, d, e) from stdin;
9999	\N	\\N	\NN	\N
10000	21	31	41	51
\.

COPY t_zedcopy (b, d) from stdin;
1	test_1
\.

COPY t_zedcopy (b, d) from stdin;
2	test_2
3	test_3
4	test_4
5	test_5
\.

COPY t_zedcopy (a, b, c, d, e) from stdin;
10001	22	32	42	52
10002	23	33	43	53
10003	24	34	44	54
10004	25	35	45	55
10005	26	36	46	56
\.

select * from t_zedcopy;

--
-- Also test delete and update on the table that was populated with COPY.
-- This exercises splitting the array item. (A table not populated with
-- COPY only contains single items, at the moment.)
--

delete from t_zedcopy where b = 4;
select * from t_zedcopy;
delete from t_zedcopy where b < 3;
select * from t_zedcopy;

update t_zedcopy set b = 100 where b = 5;
select * from t_zedcopy;

--
-- Test zero column table
--
create table t_zwithzerocols() using zedstore;
insert into t_zwithzerocols select t.* from t_zwithzerocols t right join generate_series(1,1) on true;
select count(*) from t_zwithzerocols;

-- Test for alter table add column
create table t_zaddcol(a int) using zedstore;
insert into t_zaddcol select * from generate_series(1, 3);
-- rewrite case
alter table t_zaddcol add column b int generated always as (a + 1) stored;
select * from t_zaddcol;
-- test alter table add column with no default
create table t_zaddcol_simple(a int) using zedstore;
insert into t_zaddcol_simple values (1);
alter table t_zaddcol_simple add b int;
select * from t_zaddcol_simple;
insert into t_zaddcol_simple values(2,3);
select * from t_zaddcol_simple;
-- fixed length default value stored in catalog
alter table t_zaddcol add column c int default 3;
select * from t_zaddcol;
-- variable length default value stored in catalog
alter table t_zaddcol add column d text default 'abcdefgh';
select d from t_zaddcol;
-- insert after add column
insert into t_zaddcol values (2);
select * from t_zaddcol;
insert into t_zaddcol (a, c, d) values (3,5, 'test_insert');
select b,c,d from t_zaddcol;
