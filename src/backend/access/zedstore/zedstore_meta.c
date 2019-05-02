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

static void
zsmeta_add_root_for_attributes(Relation rel, Page page, bool init)
{
	int natts = RelationGetNumberOfAttributes(rel) + 1;
	int cur_natts;
	int maxatts;
	Size freespace;
	ZSMetaPage *metapg;

	/* Initialize the attribute root dir for new attribute */
	freespace = PageGetExactFreeSpace(page);
	maxatts = freespace / sizeof(ZSRootDirItem);
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

	if (init)
		metapg->nattributes = 0;

	for (cur_natts = metapg->nattributes; cur_natts < natts; cur_natts++)
	{
		metapg->tree_root_dir[cur_natts].root = InvalidBlockNumber;
	}

	metapg->nattributes = natts;
	((PageHeader) page)->pd_lower += sizeof(ZSRootDirItem);
}

/*
 * Initialize the metapage for an empty relation.
 */
void
zsmeta_initmetapage(Relation rel)
{
	Buffer		buf;
	Page		page;
	ZSMetaPageOpaque *opaque;

	/*
	 * It's possible that we error out when building the metapage, if there
	 * are too many attribute, so work on a temporary copy first, before actually
	 * allocating the buffer.
	 */
	page = palloc(BLCKSZ);
	PageInit(page, BLCKSZ, sizeof(ZSMetaPageOpaque));
	zsmeta_add_root_for_attributes(rel, page, true);

	opaque = (ZSMetaPageOpaque *) PageGetSpecialPointer(page);
	opaque->zs_flags = 0;
	opaque->zs_page_id = ZS_META_PAGE_ID;

	/* UNDO-related fields */
	opaque->zs_undo_counter = 1; /* start at 1, so that 0 is always "old" */
	opaque->zs_undo_head = InvalidBlockNumber;
	opaque->zs_undo_tail = InvalidBlockNumber;
	opaque->zs_undo_oldestptr.counter = 1;

	opaque->zs_fpm_root = InvalidBlockNumber;

	/* Ok, write it out to disk */
	buf = ReadBuffer(rel, P_NEW);
	if (BufferGetBlockNumber(buf) != ZS_META_BLK)
		elog(ERROR, "index is not empty");
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
	PageRestoreTempPage(page, BufferGetPage(buf));

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
	Page        page;

	if (RelationGetNumberOfBlocks(rel) == 0)
	{
		if (!forupdate)
			return InvalidBlockNumber;

		zsmeta_initmetapage(rel);
	}

	metabuf = ReadBuffer(rel, ZS_META_BLK);

	/* TODO: get share lock to begin with */
	LockBuffer(metabuf, BUFFER_LOCK_EXCLUSIVE);
	page = BufferGetPage(metabuf);
	metapg = (ZSMetaPage *) PageGetContents(page);

	if ((attno != ZS_META_ATTRIBUTE_NUM) && attno <= 0)
		elog(ERROR, "invalid attribute number %d (table has only %d attributes)", attno, metapg->nattributes);

	/*
	 * file has less number of attributes stored compared to catalog. This
	 * happens due to add column default value storing value in catalog and
	 * absent in table. This attribute must be marked with atthasmissing.
	 */
	if (attno >= metapg->nattributes)
	{
		if (forupdate)
			zsmeta_add_root_for_attributes(rel, page, false);
		else
		{
			UnlockReleaseBuffer(metabuf);
			return InvalidBlockNumber;
		}
	}

	rootblk = metapg->tree_root_dir[attno].root;

	if (forupdate && rootblk == InvalidBlockNumber)
	{
		/* try to allocate one */
		Buffer		rootbuf;
		Page		rootpage;
		ZSBtreePageOpaque *opaque;

		/* TODO: release lock on metapage while we do I/O */
		rootbuf = zspage_getnewbuf(rel, metabuf);
		rootblk = BufferGetBlockNumber(rootbuf);

		metapg->tree_root_dir[attno].root = rootblk;

		/* initialize the page to look like a root leaf */
		rootpage = BufferGetPage(rootbuf);
		PageInit(rootpage, BLCKSZ, sizeof(ZSBtreePageOpaque));
		opaque = ZSBtreePageGetOpaque(rootpage);
		opaque->zs_attno = attno;
		opaque->zs_next = InvalidBlockNumber;
		opaque->zs_lokey = MinZSTid;
		opaque->zs_hikey = MaxPlusOneZSTid;
		opaque->zs_level = 0;
		opaque->zs_flags = ZSBT_ROOT;
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

	if ((attno != ZS_META_ATTRIBUTE_NUM) && (attno <= 0 || attno > metapg->nattributes))
		elog(ERROR, "invalid attribute number %d (table \"%s\" has only %d attributes)",
			 attno, RelationGetRelationName(rel), metapg->nattributes);

	metapg->tree_root_dir[attno].root = rootblk;

	MarkBufferDirty(metabuf);
}
