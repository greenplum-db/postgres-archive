-- Test "schedule". List all the tests you want to run here.

CREATE TABLE onecol (i int4) WITH (appendoptimized=true, orientation=column, compresstype=RLE_TYPE, compresslevel=1);
\i sql/onecol.sql
CREATE TABLE nullcol (i int4) WITH (appendoptimized=true, orientation=column, compresstype=RLE_TYPE, compresslevel=1);
\i sql/nullcol.sql
