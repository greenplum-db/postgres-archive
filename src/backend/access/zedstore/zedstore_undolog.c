/*
 * zedstore_undolog.c
 *		Temporary UNDO-logging for zedstore.
 *
 * XXX: This is hopefully replaced with an upstream UNDO facility later.
 *
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/zedstore/zedstore_undolog.c
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/xlogreader.h"
#include "access/xlogutils.h"
#include "access/zedstore_internal.h"
#include "access/zedstore_undolog.h"
#include "access/zedstore_wal.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "utils/rel.h"

/*
 * Reserve space in the UNDO log for a new UNDO record.
 *
 * Extends the UNDO log with a new page if needed. Information about the
 * reservation is returned in *reservation_p. reservation_p->undobuf is
 * the buffer containing the reserved space. reservation_p->undorecptr
 * is a pointer that can be use to fetch the record later.
 *
 * This doesn't make any on-disk changes. The buffer is locked, but if
 * the backend aborts later on, before actually writing the record no harm
 * done.
 *
 * The intended usage is to call zs_insert_reserve_space(), then lock any
 * any other pages needed for the operation. Then, write the UNDO record
 * reservation_p->ptr, which points directly to the buffer, in the same
 * critical section as any other page modifications that need to be done
 * atomically. Finally, call zsundo_insert_finish(), to mark the space as
 * used in the undo page header.
 *
 * The caller is responsible for WAL-logging, and replaying the changes, in
 * case of a crash.
 *
 * (If there isn't enough space on the current latest UNDO page, a new page
 * is allocated and appended to the UNDO log. That allocation is WAL-logged
 * separately, the caller doesn't need to care about that.)
 */
void
zsundo_insert_reserve(Relation rel, size_t size, zs_undo_reservation *reservation_p)
{
	Buffer		metabuf;
	Page		metapage;
	ZSMetaPageOpaque *metaopaque;
	BlockNumber	tail_blk;
	Buffer		tail_buf = InvalidBuffer;
	Page		tail_pg = NULL;
	ZSUndoPageOpaque *tail_opaque = NULL;
	uint64		next_counter;
	int			offset;

	Assert(size <= MaxUndoRecordSize);

	metabuf = ReadBuffer(rel, ZS_META_BLK);
	metapage = BufferGetPage(metabuf);

	LockBuffer(metabuf, BUFFER_LOCK_SHARE);
	metaopaque = (ZSMetaPageOpaque *) PageGetSpecialPointer(metapage);

retry_lock_tail:
	tail_blk = metaopaque->zs_undo_tail;

	/*
	 * Is there space on the tail page? If not, allocate a new UNDO page.
	 */
	if (tail_blk != InvalidBlockNumber)
	{
		tail_buf = ReadBuffer(rel, tail_blk);
		LockBuffer(tail_buf, BUFFER_LOCK_EXCLUSIVE);
		tail_pg = BufferGetPage(tail_buf);
		tail_opaque = (ZSUndoPageOpaque *) PageGetSpecialPointer(tail_pg);
		Assert(tail_opaque->first_undorecptr.counter == metaopaque->zs_undo_tail_first_counter);
	}

	if (tail_blk == InvalidBlockNumber || PageGetExactFreeSpace(tail_pg) < size)
	{
		Buffer 		newbuf;
		BlockNumber newblk;
		Page		newpage;
		ZSUndoPageOpaque *newopaque;

		/*
		 * Release the lock on the metapage while we find a new block, because
		 * that could take a while. (And accessing the Free Page Map might lock
		 * the metapage, too, causing self-deadlock.)
		 */
		LockBuffer(metabuf, BUFFER_LOCK_UNLOCK);

		/* new page */
		newbuf = zspage_getnewbuf(rel, metabuf);

		LockBuffer(metabuf, BUFFER_LOCK_EXCLUSIVE);
		if (metaopaque->zs_undo_tail != tail_blk)
		{
			/*
			 * It should not be possible for another backend to extend the UNDO log
			 * while we're holding the tail block locked.
			 */
			if (tail_blk != InvalidBlockNumber)
				elog(ERROR, "UNDO tail block pointer was changed unexpectedly");

			/*
			 * we don't need the new page, after all. (Or maybe we do, if the new
			 * tail block is already full, but we're not smart about it.)
			 */
			zspage_delete_page(rel, newbuf);
			goto retry_lock_tail;
		}

		if (tail_blk == InvalidBlockNumber)
			next_counter = metaopaque->zs_undo_tail_first_counter;
		else
			next_counter = tail_opaque->last_undorecptr.counter + 1;

		START_CRIT_SECTION();

		newblk = BufferGetBlockNumber(newbuf);
		newpage = BufferGetPage(newbuf);
		PageInit(newpage, BLCKSZ, sizeof(ZSUndoPageOpaque));
		newopaque = (ZSUndoPageOpaque *) PageGetSpecialPointer(newpage);
		newopaque->next = InvalidBlockNumber;
		newopaque->first_undorecptr.blkno = newblk;
		newopaque->first_undorecptr.offset = SizeOfPageHeaderData;
		newopaque->first_undorecptr.counter = next_counter;
		newopaque->last_undorecptr = InvalidUndoPtr;
		newopaque->zs_page_id = ZS_UNDO_PAGE_ID;
		MarkBufferDirty(newbuf);

		metaopaque->zs_undo_tail = newblk;
		metaopaque->zs_undo_tail_first_counter = next_counter;
		if (tail_blk == InvalidBlockNumber)
			metaopaque->zs_undo_head = newblk;
		MarkBufferDirty(metabuf);

		if (tail_blk != InvalidBlockNumber)
		{
			tail_opaque->next = newblk;
			MarkBufferDirty(tail_buf);
		}

		if (RelationNeedsWAL(rel))
		{
			wal_zedstore_undo_newpage xlrec;
			XLogRecPtr recptr;

			xlrec.first_counter = next_counter;

			XLogBeginInsert();
			XLogRegisterData((char *) &xlrec, SizeOfZSWalUndoNewPage);

			XLogRegisterBuffer(0, metabuf, REGBUF_STANDARD);
			if (BufferIsValid(tail_buf))
				XLogRegisterBuffer(1, tail_buf, REGBUF_STANDARD);
			XLogRegisterBuffer(2, newbuf, REGBUF_WILL_INIT | REGBUF_STANDARD);

			recptr = XLogInsert(RM_ZEDSTORE_ID, WAL_ZEDSTORE_UNDO_NEWPAGE);

			PageSetLSN(BufferGetPage(metabuf), recptr);
			if (BufferIsValid(tail_buf))
				PageSetLSN(BufferGetPage(tail_buf), recptr);
			PageSetLSN(BufferGetPage(newbuf), recptr);
		}

		if (tail_blk != InvalidBlockNumber)
			UnlockReleaseBuffer(tail_buf);

		END_CRIT_SECTION();

		tail_blk = newblk;
		tail_buf = newbuf;
		tail_pg = newpage;
		tail_opaque = newopaque;
	}
	else
	{
		if (IsZSUndoRecPtrValid(&tail_opaque->last_undorecptr))
		{
			Assert(tail_opaque->last_undorecptr.counter >= metaopaque->zs_undo_tail_first_counter);
			next_counter = tail_opaque->last_undorecptr.counter + 1;
		}
		else
		{
			next_counter = tail_opaque->first_undorecptr.counter;
			Assert(next_counter == metaopaque->zs_undo_tail_first_counter);
		}
	}

	UnlockReleaseBuffer(metabuf);

	/*
	 * All set for writing the record. But since we haven't modified the page
	 * yet, we are free to still turn back and release the lock without writing
	 * anything.
	 */
	offset = ((PageHeader) tail_pg)->pd_lower;

	/* Return the reservation to the caller */
	reservation_p->undobuf = tail_buf;
	reservation_p->undorecptr.counter = next_counter;
	reservation_p->undorecptr.blkno = tail_blk;
	reservation_p->undorecptr.offset = offset;
	reservation_p->length = size;
	reservation_p->ptr = ((char *) tail_pg) + offset;
}

/*
 * Finish the insertion of an UNDO record.
 *
 * See zsundo_insert_reserve().
 */
void
zsundo_insert_finish(zs_undo_reservation *reservation)
{
	Buffer		undobuf = reservation->undobuf;
	Page		undopg = BufferGetPage(undobuf);
	ZSUndoPageOpaque *opaque;

	/*
	 * This should be used as part of a bigger critical section that
	 * writes a WAL record of the change. The caller must've written the
	 * data.
	 */
	Assert(CritSectionCount > 0);

	Assert(((PageHeader) undopg)->pd_lower == reservation->undorecptr.offset);

	undopg = BufferGetPage(undobuf);

	opaque = (ZSUndoPageOpaque *) PageGetSpecialPointer(undopg);
	opaque->last_undorecptr = reservation->undorecptr;

	((PageHeader) undopg)->pd_lower += reservation->length;

	MarkBufferDirty(undobuf);
}

/*
 * Lock page containing the given UNDO record, and return pointer to it
 * within the buffer.
 *
 * If missing_ok is true, it's OK if the UNDO record has been discarded away
 * already. Will return NULL in that case. If missing_ok is false, throws an
 * error if the record cannot be found.
 */
char *
zsundo_fetch(Relation rel, ZSUndoRecPtr undoptr, Buffer *buf_p, int lockmode,
			 bool missing_ok)
{
	Buffer		buf;
	Page		page;
	PageHeader	pagehdr;
	ZSUndoPageOpaque *opaque;
	char	   *ptr;

	buf = ReadBuffer(rel, undoptr.blkno);
	page = BufferGetPage(buf);
	pagehdr = (PageHeader) page;

	/*
	 * FIXME: If the page might've been discarded away, there's a small chance of deadlock if
	 * the buffer now holds an unrelated page, and we or someone else is holding a lock on
	 * it already. We could optimistically try lock the page without blocking first, and
	 * and update oldest undo pointer from the metapage if that fails. And only if the
	 * oldest undo pointer indicates that the record should still be there, wait for the lock.
	 */
	LockBuffer(buf, lockmode);
	if (PageIsNew(page))
		goto record_missing;
	opaque = (ZSUndoPageOpaque *) PageGetSpecialPointer(page);
	if (opaque->zs_page_id != ZS_UNDO_PAGE_ID)
		goto record_missing;

	/* Check that this page contains the given record */
	if (undoptr.counter < opaque->first_undorecptr.counter ||
		!IsZSUndoRecPtrValid(&opaque->last_undorecptr) ||
		undoptr.counter > opaque->last_undorecptr.counter)
		goto record_missing;

	/* FIXME: the callers could do a more thorough check like this,
	 * since they know the record size */
	/* Sanity check that the pointer pointed to a valid place */
	if (undoptr.offset < SizeOfPageHeaderData ||
		undoptr.offset >= pagehdr->pd_lower)
	{
		/*
		 * this should not happen in the case that the page was recycled for
		 * other use, so error even if 'missing_ok' is true
		 */
		elog(ERROR, "could not find UNDO record " UINT64_FORMAT " at blk %u offset %u",
			 undoptr.counter, undoptr.blkno, undoptr.offset);
	}

	ptr = ((char *) page) + undoptr.offset;

#if 0 /* FIXME: move this to the callers? */
	if (memcmp(&undorec->undorecptr, &undoptr, sizeof(ZSUndoRecPtr)) != 0)
	{
		/*
		 * this should not happen in the case that the page was recycled for
		 * other use, so error even if 'fail_ok' is true
		 */
		elog(ERROR, "could not find UNDO record " UINT64_FORMAT " at blk %u offset %u",
			 undoptr.counter, undoptr.blkno, undoptr.offset);
	}
#endif

	*buf_p = buf;
	return ptr;

record_missing:
	UnlockReleaseBuffer(buf);
	*buf_p = InvalidBuffer;

	if (missing_ok)
		return NULL;
	else
		elog(ERROR, "could not find UNDO record " UINT64_FORMAT " at blk %u offset %u; not an UNDO page",
			 undoptr.counter, undoptr.blkno, undoptr.offset);
}

/* Update metapage with the oldest value */
void
zsundo_discard(Relation rel, ZSUndoRecPtr oldest_undorecptr,
			   BlockNumber oldest_undopage, List *unused_pages)
{
	/* Scan the undo log from oldest to newest */
	Buffer		metabuf;
	Page		metapage;
	ZSMetaPageOpaque *metaopaque;
	ListCell   *lc;

	START_CRIT_SECTION();

	metabuf = ReadBuffer(rel, ZS_META_BLK);
	metapage = BufferGetPage(metabuf);
	LockBuffer(metabuf, BUFFER_LOCK_EXCLUSIVE);
	metaopaque = (ZSMetaPageOpaque *) PageGetSpecialPointer(metapage);

	metaopaque->zs_undo_oldestptr = oldest_undorecptr;
	if (oldest_undopage == InvalidBlockNumber)
	{
		metaopaque->zs_undo_head = InvalidBlockNumber;
		metaopaque->zs_undo_tail = InvalidBlockNumber;
		metaopaque->zs_undo_tail_first_counter = oldest_undorecptr.counter;
	}
	else
		metaopaque->zs_undo_head = oldest_undopage;

	if (RelationNeedsWAL(rel))
	{
		wal_zedstore_undo_discard xlrec;
		XLogRecPtr recptr;

		xlrec.oldest_undorecptr = oldest_undorecptr;
		xlrec.oldest_undopage = oldest_undopage;

		XLogBeginInsert();
		XLogRegisterData((char *) &xlrec, SizeOfZSWalUndoDiscard);

		XLogRegisterBuffer(0, metabuf, REGBUF_STANDARD);

		recptr = XLogInsert(RM_ZEDSTORE_ID, WAL_ZEDSTORE_UNDO_DISCARD);

		PageSetLSN(BufferGetPage(metabuf), recptr);
	}

	MarkBufferDirty(metabuf);
	UnlockReleaseBuffer(metabuf);

	END_CRIT_SECTION();

	foreach(lc, unused_pages)
	{
		BlockNumber blk = (BlockNumber) lfirst_int(lc);
		Buffer		buf;
		Page		page;
		ZSUndoPageOpaque *opaque;

		/* check that the page still looks like what we'd expect. */
		buf = ReadBuffer(rel, blk);
		LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
		page = BufferGetPage(buf);
		if (PageIsEmpty(page) ||
			PageGetSpecialSize(page) != MAXALIGN(sizeof(ZSUndoPageOpaque)))
		{
			UnlockReleaseBuffer(buf);
			continue;
		}
		opaque = (ZSUndoPageOpaque *) PageGetSpecialPointer(page);
		if (opaque->zs_page_id != ZS_UNDO_PAGE_ID)
		{
			UnlockReleaseBuffer(buf);
			continue;
		}

		/* FIXME: Also check here that the max UndoRecPtr on the page is less
		 * than the new 'oldest_undorecptr'
		 */

		zspage_delete_page(rel, buf);
		UnlockReleaseBuffer(buf);
	}
}

void
zsundo_discard_redo(XLogReaderState *record)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	wal_zedstore_undo_discard *xlrec = (wal_zedstore_undo_discard *) XLogRecGetData(record);
	ZSUndoRecPtr oldest_undorecptr = xlrec->oldest_undorecptr;
	BlockNumber oldest_undopage = xlrec->oldest_undopage;
	Buffer		metabuf;

	if (XLogReadBufferForRedo(record, 0, &metabuf) == BLK_NEEDS_REDO)
	{
		Page		metapage = BufferGetPage(metabuf);
		ZSMetaPageOpaque *metaopaque;

		metaopaque = (ZSMetaPageOpaque *) PageGetSpecialPointer(metapage);
		metaopaque->zs_undo_oldestptr = oldest_undorecptr;
		if (oldest_undopage == InvalidBlockNumber)
		{
			metaopaque->zs_undo_head = InvalidBlockNumber;
			metaopaque->zs_undo_tail = InvalidBlockNumber;
			metaopaque->zs_undo_tail_first_counter = oldest_undorecptr.counter;
		}
		else
			metaopaque->zs_undo_head = oldest_undopage;

		PageSetLSN(metapage, lsn);
		MarkBufferDirty(metabuf);
	}
	if (BufferIsValid(metabuf))
		UnlockReleaseBuffer(metabuf);
}

void
zsundo_newpage_redo(XLogReaderState *record)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	wal_zedstore_undo_newpage *xlrec = (wal_zedstore_undo_newpage *) XLogRecGetData(record);
	Buffer		metabuf;
	Buffer		prevbuf;
	Buffer		newbuf;
	BlockNumber newblk;
	Page		newpage;
	ZSUndoPageOpaque *newopaque;
	bool		has_prev_block;

	has_prev_block = XLogRecHasBlockRef(record, 1);
	XLogRecGetBlockTag(record, 2, NULL, NULL, &newblk);

	if (XLogReadBufferForRedo(record, 0, &metabuf) == BLK_NEEDS_REDO)
	{
		Page		metapage = BufferGetPage(metabuf);
		ZSMetaPageOpaque *metaopaque;

		metaopaque = (ZSMetaPageOpaque *) PageGetSpecialPointer(metapage);
		metaopaque->zs_undo_tail = newblk;
		metaopaque->zs_undo_tail_first_counter = xlrec->first_counter;
		if (!has_prev_block)
			metaopaque->zs_undo_head = newblk;

		PageSetLSN(metapage, lsn);
		MarkBufferDirty(metabuf);
	}

	if (has_prev_block)
	{
		if (XLogReadBufferForRedo(record, 1, &prevbuf) == BLK_NEEDS_REDO)
		{
			Page		prevpage = BufferGetPage(prevbuf);
			ZSUndoPageOpaque *prev_opaque;

			prev_opaque = (ZSUndoPageOpaque *) PageGetSpecialPointer(prevpage);
			prev_opaque->next = newblk;

			PageSetLSN(prevpage, lsn);
			MarkBufferDirty(prevbuf);
		}
	}
	else
		prevbuf = InvalidBuffer;

	newbuf = XLogInitBufferForRedo(record, 2);
	newblk = BufferGetBlockNumber(newbuf);
	newpage = BufferGetPage(newbuf);
	PageInit(newpage, BLCKSZ, sizeof(ZSUndoPageOpaque));
	newopaque = (ZSUndoPageOpaque *) PageGetSpecialPointer(newpage);
	newopaque->next = InvalidBlockNumber;
	newopaque->first_undorecptr.blkno = newblk;
	newopaque->first_undorecptr.offset = SizeOfPageHeaderData;
	newopaque->first_undorecptr.counter = xlrec->first_counter;
	newopaque->last_undorecptr = InvalidUndoPtr;
	newopaque->zs_page_id = ZS_UNDO_PAGE_ID;

	PageSetLSN(newpage, lsn);
	MarkBufferDirty(newbuf);

	if (BufferIsValid(metabuf))
		UnlockReleaseBuffer(metabuf);
	if (BufferIsValid(prevbuf))
		UnlockReleaseBuffer(prevbuf);
	UnlockReleaseBuffer(newbuf);
}
