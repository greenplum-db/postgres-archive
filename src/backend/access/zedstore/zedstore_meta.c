/*
 * zedstore_meta.c
 *		Routines for handling ZedStore metapage
 *
 * The metapage holds a directory of B-tree root block numbers, one for each
 * column.
 *
 * TODO:
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
#include "access/xlogutils.h"
#include "access/zedstore_internal.h"
#include "access/zedstore_wal.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "utils/memutils.h"
#include "utils/rel.h"

static void zsmeta_wal_log_metapage(Buffer buf, int natts);

static ZSMetaCacheData *
zsmeta_populate_cache_from_metapage(Relation rel, Page page)
{
	ZSMetaCacheData *cache;
	ZSMetaPage *metapg;
	int			natts;

	if (rel->rd_amcache != NULL)
	{
		pfree(rel->rd_amcache);
		rel->rd_amcache = NULL;
	}

	metapg = (ZSMetaPage *) PageGetContents(page);

	natts = metapg->nattributes;

	cache =
		MemoryContextAllocZero(CacheMemoryContext,
							   offsetof(ZSMetaCacheData, cache_attrs[natts]));
	cache->cache_nattributes = natts;

	for (int i = 0; i < natts; i++)
	{
		cache->cache_attrs[i].root = metapg->tree_root_dir[i].root;
		cache->cache_attrs[i].rightmost = InvalidBlockNumber;
	}

	rel->rd_amcache = cache;
	return cache;
}

ZSMetaCacheData *
zsmeta_populate_cache(Relation rel)
{
	ZSMetaCacheData *cache;
	Buffer		metabuf;
	BlockNumber nblocks;

	RelationOpenSmgr(rel);

	if (rel->rd_amcache != NULL)
	{
		pfree(rel->rd_amcache);
		rel->rd_amcache = NULL;
	}

	nblocks = RelationGetNumberOfBlocks(rel);
	RelationSetTargetBlock(rel, nblocks);
	if (nblocks == 0)
	{
		cache =
			MemoryContextAllocZero(CacheMemoryContext,
								   offsetof(ZSMetaCacheData, cache_attrs));
		cache->cache_nattributes = 0;
		rel->rd_amcache = cache;
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
		{
			metapg->tree_root_dir[i].root = InvalidBlockNumber;
			metapg->tree_root_dir[i].fpm_head = InvalidBlockNumber;
		}

		metapg->nattributes = natts;
		((PageHeader) page)->pd_lower = new_pd_lower;

		MarkBufferDirty(metabuf);

		if (RelationNeedsWAL(rel))
			zsmeta_wal_log_metapage(metabuf, natts);

		END_CRIT_SECTION();
	}
	UnlockReleaseBuffer(metabuf);

	if (rel->rd_amcache != NULL)
	{
		pfree(rel->rd_amcache);
		rel->rd_amcache = NULL;
	}
}

static Page
zsmeta_initmetapage_internal(int natts)
{
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
	opaque->zs_undo_oldestptr.counter = 2; /* start at 2, so that 0 is always "old", and 1 means "dead" */
	opaque->zs_undo_head = InvalidBlockNumber;
	opaque->zs_undo_tail = InvalidBlockNumber;
	opaque->zs_undo_tail_first_counter = 2;

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
	{
		metapg->tree_root_dir[i].root = InvalidBlockNumber;
		metapg->tree_root_dir[i].fpm_head = InvalidBlockNumber;
	}

	((PageHeader) page)->pd_lower = new_pd_lower;
	return page;
}

/*
 * Initialize the metapage for an empty relation.
 */
void
zsmeta_initmetapage(Relation rel)
{
	Buffer		buf;
	Page		page;
	int			natts = RelationGetNumberOfAttributes(rel) + 1;

	/* Ok, write it out to disk */
	buf = ReadBuffer(rel, P_NEW);
	if (BufferGetBlockNumber(buf) != ZS_META_BLK)
		elog(ERROR, "table is not empty");
	page = zsmeta_initmetapage_internal(natts);
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

	START_CRIT_SECTION();
	PageRestoreTempPage(page, BufferGetPage(buf));

	MarkBufferDirty(buf);

	if (RelationNeedsWAL(rel))
		zsmeta_wal_log_metapage(buf, natts);

	END_CRIT_SECTION();

	UnlockReleaseBuffer(buf);
}

static void
zsmeta_wal_log_metapage(Buffer buf, int natts)
{
	Page		page = BufferGetPage(buf);
	wal_zedstore_init_metapage init_rec;
	XLogRecPtr recptr;

	init_rec.natts = natts;

	XLogBeginInsert();
	XLogRegisterData((char *) &init_rec, SizeOfZSWalInitMetapage);
	XLogRegisterBuffer(0, buf, REGBUF_FORCE_IMAGE | REGBUF_STANDARD);

	recptr = XLogInsert(RM_ZEDSTORE_ID, WAL_ZEDSTORE_INIT_METAPAGE);

	PageSetLSN(page, recptr);
}

static void
zsmeta_wal_log_new_att_root(Buffer metabuf, Buffer rootbuf, AttrNumber attno)
{
	Page		metapage = BufferGetPage(metabuf);
	Page		rootpage = BufferGetPage(rootbuf);
	wal_zedstore_btree_new_root xlrec;
	XLogRecPtr recptr;

	xlrec.attno = attno;

	XLogBeginInsert();
	XLogRegisterData((char *) &xlrec, SizeOfZSWalBtreeNewRoot);
	XLogRegisterBuffer(0, metabuf, REGBUF_STANDARD);
	XLogRegisterBuffer(1, rootbuf, REGBUF_WILL_INIT | REGBUF_STANDARD);

	recptr = XLogInsert(RM_ZEDSTORE_ID, WAL_ZEDSTORE_BTREE_NEW_ROOT);

	PageSetLSN(metapage, recptr);
	PageSetLSN(rootpage, recptr);
}

void
zsmeta_initmetapage_redo(XLogReaderState *record)
{
	Buffer		buf;

	/*
	 * Metapage changes are so rare that we rely on full-page images
	 * for replay.
	 */
	if (XLogReadBufferForRedo(record, 0, &buf) != BLK_RESTORED)
		elog(ERROR, "zedstore metapage init WAL record did not contain a full-page image");

	Assert(BufferGetBlockNumber(buf) == ZS_META_BLK);
	UnlockReleaseBuffer(buf);
}

void
zsmeta_new_btree_root_redo(XLogReaderState *record)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	wal_zedstore_btree_new_root *xlrec =
		(wal_zedstore_btree_new_root *) XLogRecGetData(record);
	AttrNumber	attno = xlrec->attno;
	Buffer		metabuf;
	Buffer		rootbuf;
	Page		rootpage;
	BlockNumber	rootblk;
	ZSBtreePageOpaque *opaque;

	rootbuf = XLogInitBufferForRedo(record, 1);
	rootpage = (Page) BufferGetPage(rootbuf);
	rootblk = BufferGetBlockNumber(rootbuf);
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

	PageSetLSN(rootpage, lsn);
	MarkBufferDirty(rootbuf);

	/* Update the metapage to point to it */
	if (XLogReadBufferForRedo(record, 0, &metabuf) == BLK_NEEDS_REDO)
	{
		Page		metapage = (Page) BufferGetPage(metabuf);
		ZSMetaPage *metapg = (ZSMetaPage *) PageGetContents(metapage);

		Assert(BufferGetBlockNumber(metabuf) == ZS_META_BLK);
		Assert(metapg->tree_root_dir[attno].root == InvalidBlockNumber);

		metapg->tree_root_dir[attno].root = rootblk;

		PageSetLSN(metapage, lsn);
		MarkBufferDirty(metabuf);
	}

	if (BufferIsValid(metabuf))
		UnlockReleaseBuffer(metabuf);
	UnlockReleaseBuffer(rootbuf);
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

	if (RelationGetTargetBlock(rel) == 0 ||
		RelationGetTargetBlock(rel) == InvalidBlockNumber)
	{
		BlockNumber nblocks = RelationGetNumberOfBlocks(rel);

		if (nblocks != 0)
			metacache = zsmeta_populate_cache(rel);
		else if (readonly)
			return InvalidBlockNumber;
		else
		{
			LockRelationForExtension(rel, ExclusiveLock);
			/*
			 * Confirm number of blocks is still 0 after taking lock,
			 * before initializing a new metapage
			 */
			nblocks = RelationGetNumberOfBlocks(rel);
			if (nblocks == 0)
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

	/*
	 * Don't believe a cached result that says that the root is empty.
	 * It's possible that it was created after we populated the cache. If the
	 * root block number is out-of-date, that's OK because the caller will
	 * detect that case, but if the tree is missing altogether, the caller
	 * will have nothing to detect and will incorrectly return an empty result.
	 *
	 * XXX: It's a inefficient to repopulate the cache here, if we just
	 * did so in the zsmeta_get_cache() call above already.
	 */
	if (readonly && rootblk == InvalidBlockNumber)
	{
		zsmeta_invalidate_cache(rel);
		metacache = zsmeta_get_cache(rel);
		rootblk = metacache->cache_attrs[attno].root;
	}

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

			/*
			 * Release the lock on the metapage while we find a new block, because
			 * that could take a while. (And accessing the Free Page Map might lock
			 * the metapage, too, causing self-deadlock.)
			 */
			LockBuffer(metabuf, BUFFER_LOCK_UNLOCK);

			/* TODO: release lock on metapage while we do I/O */
			rootbuf = zspage_getnewbuf(rel, attno);

			LockBuffer(metabuf, BUFFER_LOCK_EXCLUSIVE);
			metapg = (ZSMetaPage *) PageGetContents(page);
			rootblk = metapg->tree_root_dir[attno].root;
			if (rootblk != InvalidBlockNumber)
			{
				/*
				 * Another backend created the root page, while we were busy
				 * finding a free page. We won't need the page we allocated,
				 * after all.
				 */
				zspage_delete_page(rel, rootbuf, metabuf, attno);
			}
			else
			{
				rootblk = BufferGetBlockNumber(rootbuf);

				START_CRIT_SECTION();

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

				if (RelationNeedsWAL(rel))
					zsmeta_wal_log_new_att_root(metabuf, rootbuf, attno);

				END_CRIT_SECTION();
			}

			UnlockReleaseBuffer(rootbuf);
		}
		UnlockReleaseBuffer(metabuf);

		metacache->cache_attrs[attno].root = rootblk;
	}

	return rootblk;
}
