select '1'::zstid;
select '-1'::zstid;
select -'1'::zstid;

-- int2 conversion
select 1::int2::zstid;
select (-1)::int2::zstid;
select -1::int2::zstid;

-- int4 conversion
select 1::zstid;
select (-1)::zstid;
select -1::zstid;

-- int8 conversion
select 1::int8::zstid;
select 1000000000000000::zstid; -- bigger than MaxZSTid
select (-1)::int8::zstid;
select -1::int8::zstid;

create table if not exists zstidscan(a int) using zedstore;

insert into zstidscan values (1), (2), (3);

explain (costs off)
select ctid, ctid::zstid as zstid, a from zstidscan;
select ctid, ctid::zstid as zstid, a from zstidscan;

explain (costs off)
select ctid, ctid::zstid as zstid, a from zstidscan where ctid::zstid = 2;
select ctid, ctid::zstid as zstid, a from zstidscan where ctid::zstid = 2;

explain (costs off)
select ctid, ctid::zstid as zstid, a from zstidscan where ctid::zstid >= 2;
select ctid, ctid::zstid as zstid, a from zstidscan where ctid::zstid >= 2;

explain (costs off)
select ctid, ctid::zstid as zstid, a from zstidscan where ctid::zstid <= 2;
select ctid, ctid::zstid as zstid, a from zstidscan where ctid::zstid <= 2;

explain (costs off)
select ctid, ctid::zstid as zstid, a from zstidscan where ctid::zstid < 2;
select ctid, ctid::zstid as zstid, a from zstidscan where ctid::zstid < 2;

explain (costs off)
select ctid, ctid::zstid as zstid, a from zstidscan where ctid::zstid > 2;
select ctid, ctid::zstid as zstid, a from zstidscan where ctid::zstid > 2;

explain (costs off)
select ctid, ctid::zstid as zstid, a from zstidscan where ctid::zstid <> 2;
select ctid, ctid::zstid as zstid, a from zstidscan where ctid::zstid <> 2;

explain (costs off)
select ctid, ctid::zstid as zstid, a from zstidscan where ctid::zstid in (2,3);
select ctid, ctid::zstid as zstid, a from zstidscan where ctid::zstid in (2,3);

-- TODO: casting to int2 or int4 might be useful
select ctid, ctid::zstid as zstid, a from zstidscan where ctid::zstid::int2 % 3 = 0;
select ctid, ctid::zstid as zstid, a from zstidscan where ctid::zstid::int4 % 3 = 0;

explain (costs off)
select ctid, ctid::zstid as zstid, a from zstidscan where ctid::zstid::int8 % 3 = 0;
select ctid, ctid::zstid as zstid, a from zstidscan where ctid::zstid::int8 % 3 = 0;

-- TODO: Add necessary functions to do these useful aggregates on zstid types
select max(ctid::zstid) from zstidscan;
select min(ctid::zstid) from zstidscan;

drop table zstidscan;
