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
#include "funcapi.h"
#include "utils/builtins.h"
#include "utils/rel.h"

Datum pg_zs_page_type(PG_FUNCTION_ARGS);
Datum pg_zs_undo_pages(PG_FUNCTION_ARGS);
Datum pg_zs_btree_pages(PG_FUNCTION_ARGS);

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

/*
 *  blkno int8
 *  nrecords int4
 *  freespace int4
 *  firstrecptr int8
 *  lastrecptr int8
 */
Datum
pg_zs_undo_pages(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	Relation	rel;
	Buffer		metabuf;
	Page		metapage;
	ZSMetaPageOpaque *metaopaque;
	BlockNumber	firstblk;
	BlockNumber	blkno;
	char	   *ptr;
	char	   *endptr;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 (errmsg("must be superuser to use zedstore inspection functions"))));

	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not " \
						"allowed in this context")));

	/* Switch into long-lived context to construct returned data structures */
	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

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

	/*
	 * Get the current oldest undo page from the metapage.
	 */
	metabuf = ReadBuffer(rel, ZS_META_BLK);
	metapage = BufferGetPage(metabuf);
	LockBuffer(metabuf, BUFFER_LOCK_SHARE);
	metaopaque = (ZSMetaPageOpaque *) PageGetSpecialPointer(metapage);

	firstblk = metaopaque->zs_undo_head;

	/*
	 * If we assume that only one process can call TRIM at a time, then we
	 * don't need to hold the metapage locked. Alternatively, if multiple
	 * concurrent trims is possible, we could check after reading the head
	 * page, that it is the page we expect, and re-read the metapage if it's
	 * not.
	 */
	UnlockReleaseBuffer(metabuf);

	/*
	 * Loop through UNDO records, starting from the oldest page, until we
	 * hit a record that we cannot remove.
	 */
	blkno = firstblk;
	while (blkno != InvalidBlockNumber)
	{
		Datum		values[5];
		bool		nulls[5];
		Buffer		buf;
		Page		page;
		ZSUndoPageOpaque *opaque;
		int			nrecords;
		ZSUndoRecPtr firstptr = { 0, 0, 0 };
		ZSUndoRecPtr lastptr = { 0, 0, 0 };

		memset(values, 0, sizeof(values));
		memset(nulls, 0, sizeof(nulls));

		CHECK_FOR_INTERRUPTS();

		/* Read the UNDO page */
		buf = ReadBuffer(rel, blkno);
		page = BufferGetPage(buf);
		LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
		opaque = (ZSUndoPageOpaque *) PageGetSpecialPointer(page);

		if (opaque->zs_page_id != ZS_UNDO_PAGE_ID)
		{
			elog(WARNING, "unexpected page id on UNDO page %u", blkno);
			break;
		}

		/* loop through all records on the page */
		endptr = (char *) page + ((PageHeader) page)->pd_lower;
		ptr = (char *) page + SizeOfPageHeaderData;
		nrecords = 0;
		while (ptr < endptr)
		{
			ZSUndoRec *undorec = (ZSUndoRec *) ptr;

			Assert(undorec->blkno == blkno);

			lastptr = undorec->undorecptr;
			if (nrecords == 0)
				firstptr = lastptr;
			nrecords++;

			ptr += undorec->size;
		}

		values[0] = Int64GetDatum(blkno);
		values[1] = Int32GetDatum(nrecords);
		values[2] = Int32GetDatum(PageGetExactFreeSpace(page));
		values[3] = Int64GetDatum(firstptr.counter);
		values[4] = Int64GetDatum(lastptr.counter);

		blkno = opaque->next;
		UnlockReleaseBuffer(buf);

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}
	tuplestore_donestoring(tupstore);

	table_close(rel, AccessShareLock);

	return (Datum) 0;
}


/*
 *  blkno int8
 *  nextblk int8
 *  attno int4
 *  level int4
 *  
 *  lokey int8
 *  hikey int8

 *  nitems int4
 *  ncompressed int4
 *  totalsz int4
 *  uncompressedsz int4
 *  freespace int4
 */
Datum
pg_zs_btree_pages(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	Relation	rel;
	BlockNumber	blkno;
	BlockNumber	nblocks;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 (errmsg("must be superuser to use zedstore inspection functions"))));

	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not " \
						"allowed in this context")));

	/* Switch into long-lived context to construct returned data structures */
	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

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

	nblocks = RelationGetNumberOfBlocks(rel);

	/* scan all blocks in physical order */
	for (blkno = 1; blkno < nblocks; blkno++)
	{
		Datum		values[11];
		bool		nulls[11];
		OffsetNumber off;
		OffsetNumber maxoff;
		Buffer		buf;
		Page		page;
		ZSBtreePageOpaque *opaque;
		int			nitems;
		int			ncompressed;
		int			totalsz;
		int			uncompressedsz;

		memset(values, 0, sizeof(values));
		memset(nulls, 0, sizeof(nulls));

		CHECK_FOR_INTERRUPTS();

		/* Read the page */
		buf = ReadBuffer(rel, blkno);
		page = BufferGetPage(buf);
		LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

		/*
		 * we're only interested in B-tree pages. (Presumably, most of the
		 * pages in the relation are b-tree pages, so it makes sense to
		 * scan the whole relation in physical order)
		 */
		if (PageGetSpecialSize(page) != MAXALIGN(sizeof(ZSBtreePageOpaque)))
		{
			UnlockReleaseBuffer(buf);
			continue;
		}
		opaque = (ZSBtreePageOpaque *) PageGetSpecialPointer(page);
		if (opaque->zs_page_id != ZS_BTREE_PAGE_ID)
		{
			UnlockReleaseBuffer(buf);
			continue;
		}

		nitems = 0;
		ncompressed = 0;
		totalsz = 0;
		uncompressedsz = 0;
		if (opaque->zs_level == 0)
		{
			/* leaf page */
			maxoff = PageGetMaxOffsetNumber(page);
			for (off = FirstOffsetNumber; off <= maxoff; off++)
			{
				ItemId		iid = PageGetItemId(page, off);
				ZSBtreeItem	*item = (ZSBtreeItem *) PageGetItem(page, iid);

				nitems++;
				totalsz += item->t_size;

				if ((item->t_flags & ZSBT_COMPRESSED) != 0)
				{
					ncompressed++;
					uncompressedsz += item->t_uncompressedsize;
				}
				else
					uncompressedsz += item->t_size;
			}
		}
		else
		{
			/* internal page */
			nitems = ZSBtreeInternalPageGetNumItems(page);
		}
		values[0] = Int64GetDatum(blkno);
		values[1] = Int64GetDatum(opaque->zs_next);
		values[2] = Int32GetDatum(opaque->zs_attno);
		values[3] = Int32GetDatum(opaque->zs_level);
		values[4] = Int64GetDatum(opaque->zs_lokey);
		values[5] = Int64GetDatum(opaque->zs_hikey);
		values[6] = Int32GetDatum(nitems);
		if (opaque->zs_level == 0)
		{
			values[7] = Int32GetDatum(ncompressed);
			values[8] = Int32GetDatum(totalsz);
			values[9] = Int32GetDatum(uncompressedsz);
		}
		else
		{
			nulls[7] = true;
			nulls[8] = true;
			nulls[9] = true;
		}
		values[10] = Int32GetDatum(PageGetExactFreeSpace(page));

		UnlockReleaseBuffer(buf);

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}
	tuplestore_donestoring(tupstore);

	table_close(rel, AccessShareLock);

	return (Datum) 0;
}
