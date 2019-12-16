/*
 * zedstore_tid.h
 *		Conversions between ItemPointers and uint64.
 *
 * Copyright (c) 2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/include/access/zedstore_tid.h
 */
#ifndef ZEDSTORE_TID_H
#define ZEDSTORE_TID_H

#include "storage/itemptr.h"

/*
 * Throughout ZedStore, we pass around TIDs as uint64's, rather than ItemPointers,
 * for speed.
 */
typedef uint64	zstid;

#define InvalidZSTid		0
#define MinZSTid			1		/* blk 0, off 1 */
#define MaxZSTid			((uint64) MaxBlockNumber << 16 | 0xffff)
/* note: if this is converted to ItemPointer, it is invalid */
#define MaxPlusOneZSTid		(MaxZSTid + 1)

#define MaxZSTidOffsetNumber	129

#define PG_GETARG_ZSTID(n) DatumGetZSTid(PG_GETARG_DATUM(n))
#define PG_RETURN_ZSTID(x) return ZSTidGetDatum(x)

/* fmgr interface macros */
#ifdef USE_FLOAT8_BYVAL
#define ZSTidGetDatum(X) Int64GetDatum(X)
#define DatumGetZSTid(X) ((zstid) (X))
#else
#define ZSTidGetDatum(X) PointerGetDatum(X)
#define DatumGetZSTid(X) (* ((zstid*) DatumGetPointer(X)))
#endif

static inline zstid
ZSTidFromBlkOff(BlockNumber blk, OffsetNumber off)
{
	Assert(off != 0);

	return (uint64) blk * (MaxZSTidOffsetNumber - 1) + off;
}

static inline zstid
ZSTidFromItemPointer(ItemPointerData iptr)
{
	Assert(ItemPointerIsValid(&iptr));
	return ZSTidFromBlkOff(ItemPointerGetBlockNumber(&iptr),
						   ItemPointerGetOffsetNumber(&iptr));
}

static inline ItemPointerData
ItemPointerFromZSTid(zstid tid)
{
	ItemPointerData iptr;
	BlockNumber blk;
	OffsetNumber off;

	blk = (tid - 1) / (MaxZSTidOffsetNumber - 1);
	off = (tid - 1) % (MaxZSTidOffsetNumber - 1) + 1;

	ItemPointerSet(&iptr, blk, off);
	Assert(ItemPointerIsValid(&iptr));
	return iptr;
}

static inline BlockNumber
ZSTidGetBlockNumber(zstid tid)
{
	return (BlockNumber) ((tid - 1) / (MaxZSTidOffsetNumber - 1));
}

static inline OffsetNumber
ZSTidGetOffsetNumber(zstid tid)
{
	return (OffsetNumber) ((tid - 1) % (MaxZSTidOffsetNumber - 1) + 1);
}

#endif							/* ZEDSTORE_TID_H */
