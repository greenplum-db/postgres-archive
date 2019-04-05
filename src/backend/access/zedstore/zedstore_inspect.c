/*-------------------------------------------------------------------------
 *
 * zedstoream_inspect.c
 *	  Debugging functions, for viewing ZedStore page contents
 *
 * These should probably be moved to contrib/, but it's handy to have them
 * here during development..
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/zedstore/zedstoream_inspect.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "miscadmin.h"

#include "access/relscan.h"
#include "access/table.h"
#include "access/zedstore_internal.h"
#include "access/zedstore_undo.h"
#include "commands/vacuum.h"
#include "utils/builtins.h"
#include "utils/rel.h"

Datum pg_zs_page_type(PG_FUNCTION_ARGS);

Datum
pg_zs_page_type(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	uint64		pageno = PG_GETARG_INT64(1);
	Relation	rel;
	uint16		zs_page_id;
	Buffer		buf;
	Page		page;
	char	   *result;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 (errmsg("must be superuser to use zedstore inspection functions"))));

	rel = table_open(relid, AccessShareLock);

	/*
	 * Reject attempts to read non-local temporary relations; we would be
	 * likely to get wrong data since we have no visibility into the owning
	 * session's local buffers.
	 */
	if (RELATION_IS_OTHER_TEMP(rel))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot access temporary tables of other sessions")));

	buf = ReadBuffer(rel, pageno);
	LockBuffer(buf, BUFFER_LOCK_SHARE);
	page = BufferGetPage(buf);

	zs_page_id = *((uint16 *) ((char *) page + BLCKSZ - sizeof(uint16)));

	UnlockReleaseBuffer(buf);
				  
	table_close(rel, AccessShareLock);

	switch (zs_page_id)
	{
		case ZS_META_PAGE_ID:
			result = "META";
			break;
		case ZS_BTREE_PAGE_ID:
			result = "BTREE";
			break;
		case ZS_UNDO_PAGE_ID:
			result = "UNDO";
			break;
		case ZS_TOAST_PAGE_ID:
			result = "UNDO";
			break;
		default:
			result = psprintf("UNKNOWN 0x%04x", zs_page_id);
	}

	PG_RETURN_TEXT_P(cstring_to_text(result));
}
