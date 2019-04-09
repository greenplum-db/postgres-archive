/*
 * zedstore_meta.c
 *		Routines for handling ZedStore metapage
 *
 * The metapage holds a directory of B-tree root block numbers, one for each
 * column.
 *
 * TODO:
 * - support ALTER TABLE ADD COLUMN.
 * - extend the root block dir to an overflow page if there are too many
 *   attributes to fit on one page
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
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

/*
 * Allocate a new page.
 *
 * The page is exclusive-locked, but not initialized.
 *
 * Currently, this just extends the relation, but we should have a free space
 * map of some kind.
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
void
zsmeta_initmetapage(Relation rel)
{
	int			natts = RelationGetNumberOfAttributes(rel);
	Buffer		buf;
	Page		page;
	ZSMetaPage *metapg;
	ZSMetaPageOpaque *opaque;
	Size		freespace;
	int			maxatts;

	buf = ReadBuffer(rel, P_NEW);
	if (BufferGetBlockNumber(buf) != ZS_META_BLK)
		elog(ERROR, "index is not empty");
	page = BufferGetPage(buf);
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

	PageInit(page, BLCKSZ, sizeof(ZSMetaPageOpaque));

	/* Initialize the attribute root dir */
	freespace = PageGetExactFreeSpace(page);
	maxatts = freespace / sizeof(BlockNumber);
	if (natts > maxatts)
	{
		/*
		 * The root block directory must fit on the metapage.
		 *
		 * TODO: We could extend this by overflowing to another page.
		 */
		elog(ERROR, "too many attributes for zedstore");
	}

	metapg = (ZSMetaPage *) PageGetContents(page);
	metapg->nattributes = natts;
	for (int i = 0; i < natts; i++)
		metapg->roots[i] = InvalidBlockNumber;
	((PageHeader) page)->pd_lower += natts * sizeof(BlockNumber);

	opaque = (ZSMetaPageOpaque *) PageGetSpecialPointer(page);
	opaque->zs_flags = 0;
	opaque->zs_page_id = ZS_META_PAGE_ID;

	/* UNDO-related fields */
	opaque->zs_undo_counter = 1; /* start at 1, so that 0 is always "old" */
	opaque->zs_undo_head = InvalidBlockNumber;
	opaque->zs_undo_tail = InvalidBlockNumber;
	opaque->zs_undo_oldestptr.counter = 1;

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

		zsmeta_initmetapage(rel);
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
		opaque->zs_attno = attno;
		opaque->zs_next = InvalidBlockNumber;
		opaque->zs_lokey = MinZSTid;
		opaque->zs_hikey = MaxPlusOneZSTid;
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
zsmeta_update_root_for_attribute(Relation rel, AttrNumber attno,
								 Buffer metabuf, BlockNumber rootblk)
{
	ZSMetaPage *metapg;

	metapg = (ZSMetaPage *) PageGetContents(BufferGetPage(metabuf));

	if (attno <= 0 || attno > metapg->nattributes)
		elog(ERROR, "invalid attribute number %d (table has only %d attributes)", attno, metapg->nattributes);

	metapg->roots[attno - 1] = rootblk;

	MarkBufferDirty(metabuf);
}
