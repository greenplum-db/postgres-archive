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

#include "postgres.h"
#include "access/xlogreader.h"
#include "lib/stringinfo.h"

void zedstore_redo(XLogReaderState *record);
void zedstore_desc(StringInfo buf, XLogReaderState *record);
const char *zedstore_identify(uint8 info);


#endif							/* ZEDSTORE_WAL_H */
