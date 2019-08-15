/*-------------------------------------------------------------------------
 *
 * zedstore_freepagemap.c
 *	  ZedStore free space management
 *
 * The Free Page Map keeps track of unused pages in the relation.
 *
 * The FPM is a linked list of pages. Each page contains a pointer to the
 * next free page.

 * Design principles:
 *
 * - it's ok to have a block incorrectly stored in the FPM. Before actually
 *   reusing a page, we must check that it's safe.
 *
 * - a deletable page must be simple to detect just by looking at the page,
 *   and perhaps a few other pages. It should *not* require scanning the
 *   whole table, or even a whole b-tree. For example, if a column is dropped,
 *   we can detect if a b-tree page belongs to the dropped column just by
 *   looking at the information (the attribute number) stored in the page
 *   header.
 *
 * - if a page is deletable, it should become immediately reusable. No
 *   "wait out all possible readers that might be about to follow a link
 *   to it" business. All code that reads pages need to keep pages locked
 *   while following a link, or be prepared to retry if they land on an
 *   unexpected page.
 *
 *
 * TODO:
 *
 * - Avoid fragmentation. If B-tree page is split, try to hand out a page
 *   that's close to the old page. When the relation is extended, allocate
 *   a larger chunk at once.
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/zedstore/zedstore_freepagemap.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "access/zedstore_internal.h"
#include "miscadmin.h"
#include "storage/bufpage.h"
#include "storage/lmgr.h"
#include "utils/rel.h"

typedef struct ZSFreePageOpaque
{
	BlockNumber	zs_next;
	uint16		padding;
	uint16		zs_page_id;		/* ZS_FREE_PAGE_ID */
} ZSFreePageOpaque;

/*
 * zspage_is_recyclable()
 *
 * Is the current page recyclable?
 *
 * It can be:
 *
 * - an empty, all-zeros page,
 * - explicitly marked as deleted,
 * - an UNDO page older than oldest_undo_ptr
 * - a b-tree page belonging to a deleted attribute
 * - a TOAST page belonging to a dead item
 *
 * TODO: currently though, we require that it's always  explicitly marked as empty.
 *
 */
static bool
zspage_is_unused(Buffer buf)
{
	Page		page;
	ZSFreePageOpaque *opaque;

	page = BufferGetPage(buf);
	
	if (PageIsNew(page))
		return false;

	if (PageGetSpecialSize(page) != sizeof(ZSFreePageOpaque))
		return false;
	opaque = (ZSFreePageOpaque *) PageGetSpecialPointer(page);
	if (opaque->zs_page_id != ZS_FREE_PAGE_ID)
		return false;

	return true;
}

/*
 * Allocate a new page.
 *
 * The page is exclusive-locked, but not initialized.
 */
Buffer
zspage_getnewbuf(Relation rel, Buffer metabuf)
{
	bool		release_metabuf;
	Buffer		buf;
	BlockNumber blk;
	Page		metapage;
	ZSMetaPageOpaque *metaopaque;

	if (metabuf == InvalidBuffer)
	{
		metabuf = ReadBuffer(rel, ZS_META_BLK);
		LockBuffer(metabuf, BUFFER_LOCK_EXCLUSIVE);
		release_metabuf = true;
	}
	else
		release_metabuf = false;

	metapage = BufferGetPage(metabuf);
	metaopaque = (ZSMetaPageOpaque *) PageGetSpecialPointer(metapage);

	/* Get a block from the FPM. */
	blk = metaopaque->zs_fpm_head;
	if (blk == 0)
	{
		/* metapage, not expected */
		elog(ERROR, "could not find valid page in FPM");
	}
	if (blk == InvalidBlockNumber)
	{
		/* No free pages. Have to extend the relation. */
		buf = zspage_extendrel_newbuf(rel);
		blk = BufferGetBlockNumber(buf);
	}
	else
	{
		ZSFreePageOpaque *opaque;
		Page		page;

		buf = ReadBuffer(rel, blk);
		LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

		/* Check that the page really is unused. */
		if (!zspage_is_unused(buf))
		{
			UnlockReleaseBuffer(buf);
			elog(ERROR, "unexpected page found in free page list");
		}
		page = BufferGetPage(buf);
		opaque = (ZSFreePageOpaque *) PageGetSpecialPointer(page);
		metaopaque->zs_fpm_head = opaque->zs_next;
	}

	if (release_metabuf)
		UnlockReleaseBuffer(metabuf);
	return buf;
}

/*
 * Extend the relation.
 *
 * Returns the new page, exclusive-locked.
 */
Buffer
zspage_extendrel_newbuf(Relation rel)
{
	Buffer		buf;
	bool		needLock;

	/*
	 * Extend the relation by one page.
	 *
	 * We have to use a lock to ensure no one else is extending the rel at
	 * the same time, else we will both try to initialize the same new
	 * page.  We can skip locking for new or temp relations, however,
	 * since no one else could be accessing them.
	 */
	needLock = !RELATION_IS_LOCAL(rel);

	if (needLock)
		LockRelationForExtension(rel, ExclusiveLock);

	buf = ReadBuffer(rel, P_NEW);

	/* Acquire buffer lock on new page */
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

	/*
	 * Release the file-extension lock; it's now OK for someone else to
	 * extend the relation some more.  Note that we cannot release this
	 * lock before we have buffer lock on the new page, or we risk a race
	 * condition against btvacuumscan --- see comments therein.
	 */
	if (needLock)
		UnlockRelationForExtension(rel, ExclusiveLock);

	return buf;
}


/*
 * Explictly mark a page as deleted and recyclable, and add it to the FPM.
 *
 * The caller must hold an exclusive-lock on the page.
 */
void
zspage_delete_page(Relation rel, Buffer buf)
{
	BlockNumber blk = BufferGetBlockNumber(buf);
	Buffer		metabuf;
	Page		metapage;
	ZSMetaPageOpaque *metaopaque;
	Page		page;
	ZSFreePageOpaque *opaque;

	metabuf = ReadBuffer(rel, ZS_META_BLK);
	LockBuffer(metabuf, BUFFER_LOCK_EXCLUSIVE);
	metapage = BufferGetPage(metabuf);
	metaopaque = (ZSMetaPageOpaque *) PageGetSpecialPointer(metapage);

	page = BufferGetPage(buf);
	PageInit(page, BLCKSZ, sizeof(ZSFreePageOpaque));
	opaque = (ZSFreePageOpaque *) PageGetSpecialPointer(page);
	opaque->zs_page_id = ZS_FREE_PAGE_ID;
	opaque->zs_next = metaopaque->zs_fpm_head;
	metaopaque->zs_fpm_head = blk;

	MarkBufferDirty(metabuf);
	MarkBufferDirty(buf);

	/* FIXME: WAL-logging */
	
	UnlockReleaseBuffer(metabuf);
}
