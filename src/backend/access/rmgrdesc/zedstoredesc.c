/*
 * zedstoredesc.c
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/zedstore/zedstoredesc.c
 */
#include "postgres.h"
#include "access/xlogreader.h"
#include "access/zedstore_wal.h"
#include "lib/stringinfo.h"

void zedstore_desc(StringInfo buf, XLogReaderState *record)
{
	char	   *rec = XLogRecGetData(record);
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	if (info == WAL_ZEDSTORE_INIT_METAPAGE)
	{
		wal_zedstore_init_metapage *walrec = (wal_zedstore_init_metapage *) rec;
		appendStringInfo(buf, "natts %i", walrec->natts);
	}

}

const char *zedstore_identify(uint8 info)
{
	const char *id = NULL;

	switch (info & ~XLR_INFO_MASK)
	{
		case WAL_ZEDSTORE_INIT_METAPAGE:
			id = "INITMETAPAGE";
			break;
	}
	return id;
}
