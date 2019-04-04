/*
 * zedstore_undo.c
 *		Temporary UNDO-logging for zedstore.
 *
 * XXX: This is hopefully replaced with an upstream UNDO facility later.
 *
 * TODO:
 * - if there are too many attributes, so that the root block directory
 *   doesn't fit in the metapage, you get segfaults or other nastiness
 *
 * Copyright (c) 2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/access/zedstore/zedstore_undo.c
 */
#include "postgres.h"

#include "access/zedstore_internal.h"
#include "access/zedstore_undo.h"
#include "miscadmin.h"
#include "utils/rel.h"

/*
 * Insert the given UNDO record to the UNDO log.
 */
ZSUndoRecPtr
zsundo_insert(Relation rel, ZSUndoRec *rec)
{
	Buffer		metabuf;
	Page		metapage;
	ZSMetaPageOpaque *metaopaque;
	BlockNumber	tail_blk;
	Buffer		tail_buf = InvalidBuffer;
	Page		tail_pg = NULL;
	ZSUndoPageOpaque *tail_opaque = NULL;
	char	   *dst;
	ZSUndoRecPtr undorecptr;
	int			offset;
	uint64		undo_counter;

	metabuf = ReadBuffer(rel, ZS_META_BLK);
	metapage = BufferGetPage(metabuf);

	/* TODO: get share lock to begin with, for more concurrency */
	LockBuffer(metabuf, BUFFER_LOCK_EXCLUSIVE);
	metaopaque = (ZSMetaPageOpaque *) PageGetSpecialPointer(metapage);

	tail_blk = metaopaque->zs_undo_tail;

	/* Is there space on the tail page? */
	if (tail_blk != InvalidBlockNumber)
	{
		tail_buf = ReadBuffer(rel, tail_blk);
		LockBuffer(tail_buf, BUFFER_LOCK_EXCLUSIVE);
		tail_pg = BufferGetPage(tail_buf);
		tail_opaque = (ZSUndoPageOpaque *) PageGetSpecialPointer(tail_pg);
	}
	if (tail_blk == InvalidBlockNumber || PageGetExactFreeSpace(tail_pg) < rec->size)
	{
		Buffer 		newbuf;
		BlockNumber newblk;
		Page		newpage;
		ZSUndoPageOpaque *newopaque;

		/* new page */
		newbuf = zs_getnewbuf(rel);
		newblk = BufferGetBlockNumber(newbuf);
		newpage = BufferGetPage(newbuf);
		PageInit(newpage, BLCKSZ, sizeof(ZSUndoPageOpaque));
		newopaque = (ZSUndoPageOpaque *) PageGetSpecialPointer(newpage);
		newopaque->next = InvalidBlockNumber;
		newopaque->zs_page_id = ZS_UNDO_PAGE_ID;

		metaopaque->zs_undo_tail = newblk;
		if (tail_blk == InvalidBlockNumber)
			metaopaque->zs_undo_head = newblk;

		MarkBufferDirty(metabuf);

		if (tail_blk != InvalidBlockNumber)
		{
			tail_opaque->next = newblk;
			MarkBufferDirty(tail_buf);
			UnlockReleaseBuffer(tail_buf);
		}

		tail_blk = newblk;
		tail_buf = newbuf;
		tail_pg = newpage;
		tail_opaque = newopaque;
	}

	undo_counter = metaopaque->zs_undo_counter++;
	MarkBufferDirty(metabuf);

	UnlockReleaseBuffer(metabuf);

	/* insert the record to this page */
	offset = ((PageHeader) tail_pg)->pd_lower;

	undorecptr.counter = undo_counter;
	undorecptr.blkno = tail_blk;
	undorecptr.offset = offset;
	rec->undorecptr = undorecptr;
	dst = ((char *) tail_pg) + offset;
	memcpy(dst, rec, rec->size);
	((PageHeader) tail_pg)->pd_lower += rec->size;
	MarkBufferDirty(tail_buf);
	UnlockReleaseBuffer(tail_buf);

	return undorecptr;
}

/*
 * Fetch the UNDO record with the given undo-pointer.
 *
 * The returned record is a palloc'd copy.
 */
ZSUndoRec *
zsundo_fetch(Relation rel, ZSUndoRecPtr undoptr)
{
	Buffer		buf;
	Page		page;
	PageHeader	pagehdr;
	ZSUndoPageOpaque *opaque;
	ZSUndoRec  *undorec;
	ZSUndoRec  *undorec_copy;

	buf = ReadBuffer(rel, undoptr.blkno);
	page = BufferGetPage(buf);
	pagehdr = (PageHeader) page;

	LockBuffer(buf, BUFFER_LOCK_SHARE);
	opaque = (ZSUndoPageOpaque *) PageGetSpecialPointer(page);
	Assert(opaque->zs_page_id == ZS_UNDO_PAGE_ID);

	/* Sanity check that the pointer pointed to a valid place */
	if (undoptr.offset < SizeOfPageHeaderData ||
		undoptr.offset + sizeof(ZSUndoRec) > pagehdr->pd_lower)
		elog(ERROR, "could not find UNDO record " UINT64_FORMAT " at blk %u offset %u",
			 undoptr.counter, undoptr.blkno, undoptr.offset);

	undorec = (ZSUndoRec *) (((char *) page) + undoptr.offset);

	if (memcmp(&undorec->undorecptr, &undoptr, sizeof(ZSUndoRecPtr)) != 0)
		elog(ERROR, "could not find UNDO record");

	undorec_copy = palloc(undorec->size);
	memcpy(undorec_copy, undorec, undorec->size);

	UnlockReleaseBuffer(buf);

	return undorec_copy;
}

void
zsundo_trim(Relation rel, TransactionId OldestXmin)
{
	/* Scan the undo log from oldest to newest */
	Buffer		metabuf;
	Page		metapage;
	ZSMetaPageOpaque *metaopaque;
	BlockNumber	firstblk;
	BlockNumber	lastblk;
	ZSUndoRecPtr oldest_undorecptr;
	char	   *ptr;
	char	   *endptr;

	if (RelationGetNumberOfBlocks(rel) == 0)
		return;		/* empty table */

	/*
	 * Get the current oldest undo page from the metapage.
	 */
	metabuf = ReadBuffer(rel, ZS_META_BLK);
	metapage = BufferGetPage(metabuf);
	LockBuffer(metabuf, BUFFER_LOCK_EXCLUSIVE);
	metaopaque = (ZSMetaPageOpaque *) PageGetSpecialPointer(metapage);

	firstblk = metaopaque->zs_undo_head;

	oldest_undorecptr = metaopaque->zs_undo_oldestptr;

	/*
	 * If we assume that only one process can call TRIM at a time, then we
	 * don't need to hold the metapage locked. Alternatively, if multiple
	 * concurrent trims is possible, we could check after reading the head
	 * page, that it is the page we expect, and re-read the metapage if it's
	 * not.
	 */
	LockBuffer(metabuf, BUFFER_LOCK_UNLOCK);

	/*
	 * Loop through UNDO records, starting from the oldest page, until we
	 * hit a record that we cannot remove.
	 */
	lastblk = firstblk;
	while (lastblk != InvalidBlockNumber)
	{
		Buffer		buf;
		Page		page;
		ZSUndoPageOpaque *opaque;

		CHECK_FOR_INTERRUPTS();

		/* Read the UNDO page */
		buf = ReadBuffer(rel, lastblk);
		page = BufferGetPage(buf);
		LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
		opaque = (ZSUndoPageOpaque *) PageGetSpecialPointer(page);

		if (opaque->zs_page_id != ZS_UNDO_PAGE_ID)
			elog(ERROR, "unexpected page id on UNDO page");

		/* loop through all records on the page */
		endptr = (char *) page + ((PageHeader) page)->pd_lower;
		ptr = (char *) page + SizeOfPageHeaderData;
		while (ptr < endptr)
		{
			ZSUndoRec *undorec = (ZSUndoRec *) ptr;

			oldest_undorecptr = undorec->undorecptr;

			if (undorec->undorecptr.counter >= oldest_undorecptr.counter)
			{
				if (!TransactionIdPrecedes(undorec->xid, OldestXmin))
				{
					/* This is still needed. Bail out */
					break;
				}

				/*
				 * No one thinks this transaction is in-progress anymore. If it
				 * committed, we can just trim away its UNDO record. If it aborted,
				 * we need to apply the UNDO record first.
				 *
				 * TODO: applying UNDO actions has not been implemented, so if we
				 * encounter an aborted record, we just stop dead right there, and
				 * never trim past that point.
				 */
				if (!TransactionIdDidCommit(undorec->xid))
					break;
			}
			ptr += undorec->size;
		}

		if (ptr < endptr)
		{
			UnlockReleaseBuffer(buf);
			break;
		}
		else
		{
			/* We processed all records on the page. Step to the next one, if any. */
			Assert(ptr == endptr);
			lastblk = opaque->next;
			UnlockReleaseBuffer(buf);
		}
	}

	/* TODO: we leak all the removed undo pages */

	/* Update metapage with the oldest value */
	LockBuffer(metabuf, BUFFER_LOCK_EXCLUSIVE);
	metaopaque = (ZSMetaPageOpaque *) PageGetSpecialPointer(metapage);

	//elog(NOTICE, "advancing undo pointer from "UINT64_FORMAT" to "UINT64_FORMAT", blk %u",
	//	 metaopaque->zs_undo_oldestptr.counter, oldest_undorecptr.counter, lastblk);

	metaopaque->zs_undo_oldestptr = oldest_undorecptr;
	if (lastblk == InvalidBlockNumber)
	{
		metaopaque->zs_undo_head = InvalidBlockNumber;
		metaopaque->zs_undo_tail = InvalidBlockNumber;
	}
	else
		metaopaque->zs_undo_head = lastblk;

	/* TODO: WAL-log */

	MarkBufferDirty(metabuf);
	UnlockReleaseBuffer(metabuf);
}
