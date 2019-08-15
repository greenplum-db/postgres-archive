/*
 * zedstore_wal.c
 *		WAL-logging for zedstore.
 *
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/zedstore/zedstore_wal.c
 */
#include "postgres.h"

#include "access/xlogreader.h"
#include "access/zedstore_internal.h"
#include "access/zedstore_wal.h"
#include "lib/stringinfo.h"

void
zedstore_redo(XLogReaderState *record)
{
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	switch (info)
	{
		case WAL_ZEDSTORE_INIT_METAPAGE:
			zsmeta_initmetapage_redo(record);
			break;
		default:
			elog(PANIC, "zedstore_redo: unknown op code %u", info);
	}
}
