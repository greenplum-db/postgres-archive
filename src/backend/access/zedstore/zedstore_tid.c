/*
 * zedstore_tid.c
 *		Functions for the built-in type zstid
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/zedstore/zedstore_tid.c
 */

#include "postgres.h"

#include "access/zedstore_tid.h"
#include "storage/itemptr.h"
#include "utils/fmgrprotos.h"
#include "utils/int8.h"

/* fmgr interface macros */
#ifdef USE_FLOAT8_BYVAL
#define ZSTidGetDatum(X) Int64GetDatum(X)
#define DatumGetZSTid(X) ((zstid) (X))
#else
#define ZSTidGetDatum(X) PointerGetDatum(X)
#define DatumGetZSTid(X) (* ((zstid*) DatumGetPointer(X)))
#endif

#define PG_GETARG_ZSTID(n) DatumGetZSTid(PG_GETARG_DATUM(n))
#define PG_RETURN_ZSTID(x) return ZSTidGetDatum(x)

Datum
tidtozstid(PG_FUNCTION_ARGS)
{
	ItemPointerData *arg = PG_GETARG_ITEMPOINTER(0);
	zstid		tid = ZSTidFromItemPointer(*arg);

	PG_RETURN_ZSTID(tid);
}

Datum
zstidin(PG_FUNCTION_ARGS)
{
	char	   *str = PG_GETARG_CSTRING(0);
	int64		result;

	(void) scanint8(str, false, &result);

	if (result > MaxZSTid || result < MinZSTid)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("value \"%s\" is out of range for type %s",
						str, "zstid")));

	PG_RETURN_INT64(result);
}

Datum
zstidout(PG_FUNCTION_ARGS)
{
	zstid		tid = PG_GETARG_ZSTID(0);
	char		buf[32];

	snprintf(buf, sizeof(buf), "%lu", tid);

	PG_RETURN_CSTRING(pstrdup(buf));
}

Datum
zstidlt(PG_FUNCTION_ARGS)
{
	zstid		tid1 = PG_GETARG_ZSTID(0);
	zstid		tid2 = PG_GETARG_ZSTID(1);

	PG_RETURN_BOOL(tid1 < tid2);
}

Datum
zstidgt(PG_FUNCTION_ARGS)
{
	zstid		tid1 = PG_GETARG_ZSTID(0);
	zstid		tid2 = PG_GETARG_ZSTID(1);

	PG_RETURN_BOOL(tid1 > tid2);
}

Datum
zstideq(PG_FUNCTION_ARGS)
{
	zstid		tid1 = PG_GETARG_ZSTID(0);
	zstid		tid2 = PG_GETARG_ZSTID(1);

	PG_RETURN_BOOL(tid1 == tid2);
}

Datum
zstidle(PG_FUNCTION_ARGS)
{
	zstid		tid1 = PG_GETARG_ZSTID(0);
	zstid		tid2 = PG_GETARG_ZSTID(1);

	PG_RETURN_BOOL(tid1 <= tid2);
}

Datum
zstidge(PG_FUNCTION_ARGS)
{
	zstid		tid1 = PG_GETARG_ZSTID(0);
	zstid		tid2 = PG_GETARG_ZSTID(1);

	PG_RETURN_BOOL(tid1 >= tid2);
}

Datum
zstidne(PG_FUNCTION_ARGS)
{
	zstid		tid1 = PG_GETARG_ZSTID(0);
	zstid		tid2 = PG_GETARG_ZSTID(1);

	PG_RETURN_BOOL(tid1 != tid2);
}

Datum
int2tozstid(PG_FUNCTION_ARGS)
{
	int16		arg = PG_GETARG_INT16(0);

	if (arg < MinZSTid)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("value %d is out of range for type %s",
						arg, "zstid")));

	PG_RETURN_ZSTID((zstid) arg);
}

Datum
int4tozstid(PG_FUNCTION_ARGS)
{
	int32		arg = PG_GETARG_INT32(0);

	if (arg < MinZSTid)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("value %d is out of range for type %s",
						arg, "zstid")));

	PG_RETURN_ZSTID((zstid) arg);
}

Datum
int8tozstid(PG_FUNCTION_ARGS)
{
	int64		arg = PG_GETARG_INT64(0);

	if (arg > MaxZSTid || arg < MinZSTid)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("value " INT64_FORMAT " is out of range for type %s",
						arg, "zstid")));

	PG_RETURN_ZSTID((zstid) arg);
}

Datum
zstidtoint8(PG_FUNCTION_ARGS)
{
	zstid		arg = PG_GETARG_ZSTID(0);

	PG_RETURN_INT64((int64) arg);
}
