/*
 * zedstore_wal.h
 *		internal declarations for ZedStore wal logging
 *
 * Copyright (c) 2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/include/access/zedstore_wal.h
 */
#ifndef ZEDSTORE_WAL_H
#define ZEDSTORE_WAL_H

#include "access/xlogreader.h"
#include "lib/stringinfo.h"

#define WAL_ZEDSTORE_INIT_METAPAGE	0x00

extern void zedstore_redo(XLogReaderState *record);
extern void zedstore_desc(StringInfo buf, XLogReaderState *record);
extern const char *zedstore_identify(uint8 info);

/*
 * WAL record for initializing zedstore metapage (WAL_ZEDSTORE_INIT_METAPAGE)
 *
 * These records always use a full-page image, so this data is really just
 * for debugging purposes.
 */
typedef struct wal_zedstore_init_metapage
{
	int32		natts;		/* number of attributes. */
} wal_zedstore_init_metapage;

#define SizeOfZSWalInitMetapage (offsetof(wal_zedstore_init_metapage, natts) + sizeof(int32))

#endif							/* ZEDSTORE_WAL_H */
