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

#include "access/xlogutils.h"
#include "access/zedstore_internal.h"
#include "access/zedstore_wal.h"
#include "miscadmin.h"
#include "storage/bufpage.h"
#include "storage/lmgr.h"
#include "utils/rel.h"

/*
 * Deleted pages are initialized as having this structure, in the
 * "special area".
 *
 * zs_next points to the next free block in the FPM chain.
 */
typedef struct ZSFreePageOpaque
{
	BlockNumber	zs_next;
	uint16		padding;
	uint16		zs_page_id;		/* ZS_FREE_PAGE_ID */
} ZSFreePageOpaque;

static Buffer zspage_extendrel_newbuf(Relation rel);

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
 *
 * The head of the FPM chain is kept in the metapage, and thus this
 * function will acquire the lock on the metapage. The caller must
 * not be holding it, or we will self-deadlock!
 *
 * Unlinking the page from the FPM is WAL-logged. Once this function
 * returns, the caller must use the page, and WAL-log its initialization,
 * or give it back by calling zspage_delete_page().
 *
 * NOTE: There is a gap between this function unlinking the page from the
 * FPM, and the caller initializing the page and linking it to somewhere
 * else. If we crash in between, the page will be permanently leaked.
 * That's unfortunate, but hopefully won't happen too often.
 */
Buffer
zspage_getnewbuf(Relation rel, AttrNumber attrNumber)
{
	Buffer		buf;
	BlockNumber blk;
	Buffer		metabuf;
	Page		metapage;
	ZSMetaPage  *metapg;
	ZSMetaPageOpaque *metaopaque;

	metabuf = ReadBuffer(rel, ZS_META_BLK);
	LockBuffer(metabuf, BUFFER_LOCK_EXCLUSIVE);

	metapage   = BufferGetPage(metabuf);
	metaopaque = (ZSMetaPageOpaque *) PageGetSpecialPointer(metapage);
	metapg     = (ZSMetaPage *) PageGetContents(metapage);

	if (attrNumber == ZS_INVALID_ATTRIBUTE_NUM)
		blk = metaopaque->zs_fpm_head;
	else
		blk = metapg->tree_root_dir[attrNumber].fpm_head;

	if (blk == ZS_META_BLK)
	{
		/* metapage, not expected */
		elog(ERROR, "could not find valid page in FPM");
	}
	if (blk != InvalidBlockNumber)
	{
		ZSFreePageOpaque *opaque;
		Page		page;
		BlockNumber next_free_blkno;

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
		next_free_blkno = opaque->zs_next;

		if (attrNumber == ZS_INVALID_ATTRIBUTE_NUM)
			metaopaque->zs_fpm_head = next_free_blkno;
		else
			metapg->tree_root_dir[attrNumber].fpm_head = next_free_blkno;

		if (RelationNeedsWAL(rel))
		{
			wal_zedstore_fpm_reuse_page xlrec;
			XLogRecPtr recptr;

			xlrec.next_free_blkno = next_free_blkno;

			XLogBeginInsert();
			XLogRegisterData((char *) &xlrec, SizeOfZSWalFpmReusePage);
			XLogRegisterBuffer(0, metabuf, REGBUF_STANDARD);

			/*
			 * NOTE: We don't WAL-log the reused page here. It's up to the
			 * caller to WAL-log its initialization. If we crash between here
			 * and the initialization, the page is leaked. That's unfortunate,
			 * but it should be rare enough that we can live with it.
			 */

			recptr = XLogInsert(RM_ZEDSTORE_ID, WAL_ZEDSTORE_FPM_REUSE_PAGE);

			PageSetLSN(BufferGetPage(metabuf), recptr);
		}
		UnlockReleaseBuffer(metabuf);
	}
	else
	{
		/*
		 * No free pages in the FPM. Have to extend the relation.
		 * 1. We extend the relation by zedstore_rel_extension_factor #blocks.
		 * 2. Out of the zedstore_rel_extension_factor #blocks returned by the
		 *    storage manager, we return the first block. The other blocks
		 *    returned are prepended to the attribute level FPM.
		 */
		StdRdOptions *rd_options = (StdRdOptions *)rel->rd_options;
		int extension_factor = rd_options ? rd_options->zedstore_rel_extension_factor : ZEDSTORE_DEFAULT_REL_EXTENSION_FACTOR;

		buf = zspage_extendrel_newbuf(rel);
		blk = BufferGetBlockNumber(buf);

		Buffer *extrabufs = palloc((extension_factor - 1) * sizeof(Buffer));
		for (int i = 0; i < extension_factor - 1; i++) {
			extrabufs[i] = zspage_extendrel_newbuf(rel);
			/*
			 * We unlock the extrabuf here to prevent hitting MAX_SIMUL_LWLOCKS.
			 * It is safe to unlock the extrabuf here as it cannot be referenced
			 * by other backends until it is put on the attribute-level FPM.
			 * We grab the lock again in the following loop before placing the
			 * page on the FPM.
			 */
			LockBuffer(extrabufs[i], BUFFER_LOCK_UNLOCK);
		}

		for (int i = extension_factor - 2; i >=0; i--) {
			LockBuffer(extrabufs[i], BUFFER_LOCK_EXCLUSIVE);
			zspage_delete_page(rel, extrabufs[i], metabuf, attrNumber);
			UnlockReleaseBuffer(extrabufs[i]);
		}
		UnlockReleaseBuffer(metabuf);
	}

	return buf;
}

void
zspage_reuse_page_redo(XLogReaderState *record)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	wal_zedstore_fpm_reuse_page *xlrec = (wal_zedstore_fpm_reuse_page *) XLogRecGetData(record);
	Buffer		metabuf;

	if (XLogReadBufferForRedo(record, 0, &metabuf) == BLK_NEEDS_REDO)
	{
		Page		metapage = BufferGetPage(metabuf);
		ZSMetaPageOpaque *metaopaque;

		metaopaque = (ZSMetaPageOpaque *) PageGetSpecialPointer(metapage);
		metaopaque->zs_fpm_head = xlrec->next_free_blkno;

		PageSetLSN(metapage, lsn);
		MarkBufferDirty(metabuf);
	}

	if (BufferIsValid(metabuf))
		UnlockReleaseBuffer(metabuf);
}

/*
 * Extend the relation.
 *
 * Returns the new page, exclusive-locked.
 */
static Buffer
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
 *
 * This function needs to modify the metapage, to link the page to the
 * FPM chain. If the caller is already holding a lock on the metapage,
 * pass it in the 'metabuf' argument.
 *
 * NOTE: The deletion of the page is WAL-logged. There is a gap between
 * the caller making the page obsolete, and calling this function, and
 * if we crash in between, the page will be leaked. That's unfortunate,
 * but like in zspage_getnewbuf(), we mostly just live with it. However,
 * you can use zspage_mark_page_deleted() to avoid it.
 */
void
zspage_delete_page(Relation rel, Buffer buf, Buffer metabuf, AttrNumber attrNumber)
{
	bool		release_metabuf;
	BlockNumber blk = BufferGetBlockNumber(buf);
	Page		metapage;
	ZSMetaPage  *metapg;
	ZSMetaPageOpaque *metaopaque;
	Page		page;
	BlockNumber next_free_blkno;

	if (metabuf == InvalidBuffer)
	{
		metabuf = ReadBuffer(rel, ZS_META_BLK);
		LockBuffer(metabuf, BUFFER_LOCK_EXCLUSIVE);
		release_metabuf = true;
	}
	else
		release_metabuf = false;

	metapage = BufferGetPage(metabuf);
	metapg = (ZSMetaPage *) PageGetContents(metapage);
	metaopaque = (ZSMetaPageOpaque *) PageGetSpecialPointer(metapage);

	page = BufferGetPage(buf);

	if (attrNumber != ZS_INVALID_ATTRIBUTE_NUM)
	{
		/*
		 * Add the page to the attribute specific free page map.
		 */
		next_free_blkno = metapg->tree_root_dir[attrNumber].fpm_head;
		zspage_mark_page_deleted(page, next_free_blkno);
		metapg->tree_root_dir[attrNumber].fpm_head = blk;
	}
	else
	{
		next_free_blkno = metaopaque->zs_fpm_head;
		zspage_mark_page_deleted(page, next_free_blkno);
		metaopaque->zs_fpm_head = blk;
	}


	MarkBufferDirty(metabuf);
	MarkBufferDirty(buf);

	if (RelationNeedsWAL(rel))
	{
		wal_zedstore_fpm_delete_page xlrec;
		XLogRecPtr recptr;

		xlrec.next_free_blkno = next_free_blkno;

		XLogBeginInsert();
		XLogRegisterData((char *) &xlrec, SizeOfZSWalFpmDeletePage);
		XLogRegisterBuffer(0, metabuf, REGBUF_STANDARD);
		XLogRegisterBuffer(1, buf, REGBUF_WILL_INIT | REGBUF_STANDARD);

		recptr = XLogInsert(RM_ZEDSTORE_ID, WAL_ZEDSTORE_FPM_DELETE_PAGE);

		PageSetLSN(metapage, recptr);
		PageSetLSN(page, recptr);
	}

	if (release_metabuf)
		UnlockReleaseBuffer(metabuf);
}

/*
 * Initialize a page as deleted.
 *
 * This is a low-level function, used by zspage_delete_page(), but it can
 * also be used by callers that are willing to deal with managing the FPM
 * chain and WAL-logging by themselves.
 */
void
zspage_mark_page_deleted(Page page, BlockNumber next_free_blk)
{
	ZSFreePageOpaque *opaque;

	PageInit(page, BLCKSZ, sizeof(ZSFreePageOpaque));
	opaque = (ZSFreePageOpaque *) PageGetSpecialPointer(page);
	opaque->zs_page_id = ZS_FREE_PAGE_ID;
	opaque->zs_next = next_free_blk;
}

void
zspage_delete_page_redo(XLogReaderState *record)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	wal_zedstore_fpm_delete_page *xlrec = (wal_zedstore_fpm_delete_page *) XLogRecGetData(record);
	Buffer		metabuf;
	Buffer		deletedbuf;
	Page		deletedpg;
	BlockNumber deletedblkno;

	deletedbuf = XLogInitBufferForRedo(record, 1);
	deletedpg = BufferGetPage(deletedbuf);
	deletedblkno = BufferGetBlockNumber(deletedbuf);

	zspage_mark_page_deleted(deletedpg, xlrec->next_free_blkno);

	if (XLogReadBufferForRedo(record, 0, &metabuf) == BLK_NEEDS_REDO)
	{
		Page		metapage = BufferGetPage(metabuf);
		ZSMetaPageOpaque *metaopaque;

		metaopaque = (ZSMetaPageOpaque *) PageGetSpecialPointer(metapage);
		metaopaque->zs_fpm_head = deletedblkno;

		PageSetLSN(metapage, lsn);
		MarkBufferDirty(metabuf);
	}

	PageSetLSN(deletedpg, lsn);
	MarkBufferDirty(deletedbuf);

	if (BufferIsValid(metabuf))
		UnlockReleaseBuffer(metabuf);
	UnlockReleaseBuffer(deletedbuf);
}
