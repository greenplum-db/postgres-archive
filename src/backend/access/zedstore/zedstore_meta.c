/*
 * zedstore_meta.c
 *		Routines for handling ZedStore metapage
 *
 * The metapage holds a directory of B-tree root block numbers, one for each
 * column.
 *
 * TODO:
 * - if there are too many attributes, so that the the root block directory
 *   doesn't fit in the metapage, you get segfaults or other nastiness
 *
 * Copyright (c) 2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/access/zedstore/zedstore_meta.c
 */
#include "postgres.h"

#include "access/itup.h"
#include "access/zedstore_internal.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "utils/rel.h"

static void zs_initmetapage(Relation rel, int nattributes);

/*
 * Allocate a new page.
 *
 * Currently, this just extends the relation, but we should have a free space map
 * of some kind.
 */
Buffer
zs_getnewbuf(Relation rel)
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
 * Initialize the metapage for an empty relation.
 */
static void
zs_initmetapage(Relation rel, int nattributes)
{
	Buffer		buf;
	Page		page;
	ZSMetaPage *metapg;
	ZSMetaPageOpaque *opaque;

	buf = ReadBuffer(rel, P_NEW);
	if (BufferGetBlockNumber(buf) != ZS_META_BLK)
		elog(ERROR, "index is not empty");
	page = BufferGetPage(buf);
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

	PageInit(page, BLCKSZ, sizeof(ZSMetaPageOpaque));
	metapg = (ZSMetaPage *) PageGetContents(page);
	metapg->nattributes = nattributes;
	for (int i = 0; i < nattributes; i++)
		metapg->roots[i] = InvalidBlockNumber;

	opaque = (ZSMetaPageOpaque *) PageGetSpecialPointer(page);
	opaque->zs_flags = 0;
	opaque->zs_page_id = ZS_META_PAGE_ID;
	opaque->zs_undo_head = InvalidBlockNumber;
	opaque->zs_undo_tail = InvalidBlockNumber;

	MarkBufferDirty(buf);
	/* TODO: WAL-log */

	UnlockReleaseBuffer(buf);
}

/*
 * Get the block number of the b-tree root for given attribute.
 *
 * If 'forupdate' is true, and the root doesn't exist yet (ie. it's an empty
 * table), a new root is allocated. Otherwise, returns InvalidBlockNumber if
 * the root doesn't exist.
 */
BlockNumber
zsmeta_get_root_for_attribute(Relation rel, AttrNumber attno, bool forupdate)
{
	Buffer		metabuf;
	ZSMetaPage *metapg;
	BlockNumber	rootblk;

	if (RelationGetNumberOfBlocks(rel) == 0)
	{
		if (!forupdate)
			return InvalidBlockNumber;

		zs_initmetapage(rel, RelationGetNumberOfAttributes(rel));
	}

	metabuf = ReadBuffer(rel, ZS_META_BLK);

	/* TODO: get share lock to begin with */
	LockBuffer(metabuf, BUFFER_LOCK_EXCLUSIVE);
	metapg = (ZSMetaPage *) PageGetContents(BufferGetPage(metabuf));

	if (attno <= 0 || attno > metapg->nattributes)
		elog(ERROR, "invalid attribute number %d (table has only %d attributes)", attno, metapg->nattributes);

	rootblk = metapg->roots[attno - 1];

	if (forupdate && rootblk == InvalidBlockNumber)
	{
		/* try to allocate one */
		Buffer		rootbuf;
		Page		rootpage;
		ZSBtreePageOpaque *opaque;

		/* TODO: release lock on metapage while we do I/O */
		rootbuf = zs_getnewbuf(rel);
		rootblk = BufferGetBlockNumber(rootbuf);

		metapg->roots[attno - 1] = rootblk;

		/* initialize the page to look like a root leaf */
		rootpage = BufferGetPage(rootbuf);
		PageInit(rootpage, BLCKSZ, sizeof(ZSBtreePageOpaque));
		opaque = ZSBtreePageGetOpaque(rootpage);
		opaque->zs_next = InvalidBlockNumber;
		ItemPointerSet(&opaque->zs_lokey, 0, 1);
		ItemPointerSet(&opaque->zs_hikey, MaxBlockNumber, 0xFFFF);
		opaque->zs_level = 0;
		opaque->zs_flags = 0;
		opaque->zs_page_id = ZS_BTREE_PAGE_ID;

		MarkBufferDirty(rootbuf);
		MarkBufferDirty(metabuf);
		/* TODO: WAL-log both pages */

		UnlockReleaseBuffer(rootbuf);
	}

	UnlockReleaseBuffer(metabuf);

	return rootblk;
}

/*
 *
 * Caller is responsible for WAL-logging this.
 */
void
zsmeta_update_root_for_attribute(Relation rel, AttrNumber attno, Buffer metabuf, BlockNumber rootblk)
{
	ZSMetaPage *metapg;

	metapg = (ZSMetaPage *) PageGetContents(BufferGetPage(metabuf));

	if (attno <= 0 || attno > metapg->nattributes)
		elog(ERROR, "invalid attribute number %d (table has only %d attributes)", attno, metapg->nattributes);

	metapg->roots[attno - 1] = rootblk;

	MarkBufferDirty(metabuf);
}
