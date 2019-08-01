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

}

const char *zedstore_identify(uint8 info)
{
  return NULL;
}
