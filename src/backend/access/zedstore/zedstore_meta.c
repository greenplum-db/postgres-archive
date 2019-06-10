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
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "utils/memutils.h"
#include "utils/rel.h"

static ZSMetaCacheData *
zsmeta_populate_cache_from_metapage(Relation rel, Page page)
{
	ZSMetaCacheData *cache;
	ZSMetaPage *metapg;
	int			natts;

	if (rel->rd_smgr->smgr_amcache != NULL)
	{
		pfree(rel->rd_smgr->smgr_amcache);
		rel->rd_smgr->smgr_amcache = NULL;
	}

	metapg = (ZSMetaPage *) PageGetContents(page);

	natts = metapg->nattributes;

	cache =
		MemoryContextAllocZero(TopMemoryContext,
							   offsetof(ZSMetaCacheData, cache_attrs[natts]));
	cache->cache_rel_is_empty = false;
	cache->cache_nattributes = natts;

	for (int i = 0; i < natts; i++)
	{
		cache->cache_attrs[i].root = metapg->tree_root_dir[i].root;
		cache->cache_attrs[i].rightmost = InvalidBlockNumber;
	}

	rel->rd_smgr->smgr_amcache = cache;
	return cache;
}

ZSMetaCacheData *
zsmeta_populate_cache(Relation rel)
{
	ZSMetaCacheData *cache;
	Buffer		metabuf;

	RelationOpenSmgr(rel);

	if (rel->rd_smgr->smgr_amcache != NULL)
	{
		pfree(rel->rd_smgr->smgr_amcache);
		rel->rd_smgr->smgr_amcache = NULL;
	}

	if (RelationGetNumberOfBlocks(rel) == 0)
	{
		cache =
			MemoryContextAllocZero(TopMemoryContext,
								   offsetof(ZSMetaCacheData, cache_attrs));
		cache->cache_rel_is_empty = true;
		cache->cache_nattributes = 0;
		rel->rd_smgr->smgr_amcache = cache;
	}
	else
	{
		metabuf = ReadBuffer(rel, ZS_META_BLK);
		LockBuffer(metabuf, BUFFER_LOCK_SHARE);
		cache = zsmeta_populate_cache_from_metapage(rel, BufferGetPage(metabuf));
		UnlockReleaseBuffer(metabuf);
	}

	return cache;
}

static void
zsmeta_expand_metapage_for_new_attributes(Relation rel)
{
	int			natts = RelationGetNumberOfAttributes(rel) + 1;
	Buffer		metabuf;
	Page		page;
	ZSMetaPage *metapg;

	metabuf = ReadBuffer(rel, ZS_META_BLK);

	LockBuffer(metabuf, BUFFER_LOCK_EXCLUSIVE);
	page = BufferGetPage(metabuf);
	metapg = (ZSMetaPage *) PageGetContents(page);

	if (natts > metapg->nattributes)
	{
		int			new_pd_lower;

		new_pd_lower = (char *) &metapg->tree_root_dir[natts] - (char *) page;
		if (new_pd_lower > ((PageHeader) page)->pd_upper)
		{
			/*
			 * The root block directory must fit on the metapage.
			 *
			 * TODO: We could extend this by overflowing to another page.
			 */
			elog(ERROR, "too many attributes for zedstore");
		}

		START_CRIT_SECTION();

		/* Initialize the new attribute roots to InvalidBlockNumber */
		for (int i = metapg->nattributes; i < natts; i++)
			metapg->tree_root_dir[i].root = InvalidBlockNumber;

		metapg->nattributes = natts;
		((PageHeader) page)->pd_lower = new_pd_lower;

		MarkBufferDirty(metabuf);
		/* TODO: WAL-log */

		END_CRIT_SECTION();
	}
	UnlockReleaseBuffer(metabuf);

	if (rel->rd_smgr->smgr_amcache != NULL)
	{
		pfree(rel->rd_smgr->smgr_amcache);
		rel->rd_smgr->smgr_amcache = NULL;
	}
}

/*
 * Initialize the metapage for an empty relation.
 */
void
zsmeta_initmetapage(Relation rel)
{
	int			natts = RelationGetNumberOfAttributes(rel) + 1;
	Buffer		buf;
	Page		page;
	ZSMetaPageOpaque *opaque;
	ZSMetaPage *metapg;
	int			new_pd_lower;

	/*
	 * It's possible that we error out when building the metapage, if there
	 * are too many attribute, so work on a temporary copy first, before actually
	 * allocating the buffer.
	 */
	page = palloc(BLCKSZ);
	PageInit(page, BLCKSZ, sizeof(ZSMetaPageOpaque));

	opaque = (ZSMetaPageOpaque *) PageGetSpecialPointer(page);
	opaque->zs_flags = 0;
	opaque->zs_page_id = ZS_META_PAGE_ID;

	/* UNDO-related fields */
	opaque->zs_undo_counter = 2; /* start at 2, so that 0 is always "old", and 1 means "dead" */
	opaque->zs_undo_head = InvalidBlockNumber;
	opaque->zs_undo_tail = InvalidBlockNumber;
	opaque->zs_undo_oldestptr.counter = 1;

	opaque->zs_fpm_head = InvalidBlockNumber;

	metapg = (ZSMetaPage *) PageGetContents(page);

	new_pd_lower = (char *) &metapg->tree_root_dir[natts] - (char *) page;
	if (new_pd_lower > ((PageHeader) page)->pd_upper)
	{
		/*
		 * The root block directory must fit on the metapage.
		 *
		 * TODO: We could extend this by overflowing to another page.
		 */
		elog(ERROR, "too many attributes for zedstore");
	}

	metapg->nattributes = natts;
	for (int i = 0; i < natts; i++)
		metapg->tree_root_dir[i].root = InvalidBlockNumber;

	((PageHeader) page)->pd_lower = new_pd_lower;

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
 * If 'readonly' is true, and the root doesn't exist yet (ie. it's an empty
 * table), returns InvalidBlockNumber. Otherwise new root is allocated if
 * the root doesn't exist.
 */
BlockNumber
zsmeta_get_root_for_attribute(Relation rel, AttrNumber attno, bool readonly)
{
	Buffer		metabuf;
	ZSMetaPage *metapg;
	BlockNumber	rootblk;
	ZSMetaCacheData *metacache;

	Assert(attno == ZS_META_ATTRIBUTE_NUM || attno >= 1);

	metacache = zsmeta_get_cache(rel);

	if (metacache->cache_rel_is_empty)
	{
		if (RelationGetNumberOfBlocks(rel) != 0)
			metacache = zsmeta_populate_cache(rel);
		else if (readonly)
			return InvalidBlockNumber;
		else
		{
			LockRelationForExtension(rel, ExclusiveLock);
			/*
			 * Confirm number of blocks is still 0 after taking lock
			 * before initializing a new metapage
			 */
			if (RelationGetNumberOfBlocks(rel) == 0)
				zsmeta_initmetapage(rel);
			UnlockRelationForExtension(rel, ExclusiveLock);
			metacache = zsmeta_populate_cache(rel);
		}
	}

	/*
	 * file has less number of attributes stored compared to catalog. This
	 * happens due to add column default value storing value in catalog and
	 * absent in table. This attribute must be marked with atthasmissing.
	 */
	if (attno >= metacache->cache_nattributes)
	{
		if (readonly)
		{
			/* re-check */
			metacache = zsmeta_populate_cache(rel);
			if (attno >= metacache->cache_nattributes)
				return InvalidBlockNumber;
		}
		else
		{
			zsmeta_expand_metapage_for_new_attributes(rel);
			metacache = zsmeta_populate_cache(rel);
		}
	}

	rootblk = metacache->cache_attrs[attno].root;

	if (!readonly && rootblk == InvalidBlockNumber)
	{
		/* try to allocate one */
		Page		page;

		metabuf = ReadBuffer(rel, ZS_META_BLK);

		LockBuffer(metabuf, BUFFER_LOCK_EXCLUSIVE);
		page = BufferGetPage(metabuf);
		metapg = (ZSMetaPage *) PageGetContents(page);

		/*
		 * Re-check that the root is still invalid, now that we have the
		 * metapage locked.
		 */
		rootblk = metapg->tree_root_dir[attno].root;
		if (rootblk == InvalidBlockNumber)
		{
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
		metacache->cache_attrs[attno].root = rootblk;
		UnlockReleaseBuffer(metabuf);
	}

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
