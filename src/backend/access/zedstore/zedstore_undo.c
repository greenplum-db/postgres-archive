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
