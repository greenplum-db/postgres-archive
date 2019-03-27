/*
handle the metapage.

- directory of roots of each column

 *
 */
#include "postgres.h"

#include "access/itup.h"
#include "access/zedstore_internal.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "utils/rel.h"

/* TODO: move to zedstore_utils.c or zedstore_fsm.c or something */
Buffer
zs_getnewbuf(Relation indrel)
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
	needLock = !RELATION_IS_LOCAL(indrel);

	if (needLock)
		LockRelationForExtension(indrel, ExclusiveLock);

	buf = ReadBuffer(indrel, P_NEW);

	/* Acquire buffer lock on new page */
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

	/*
	 * Release the file-extension lock; it's now OK for someone else to
	 * extend the relation some more.  Note that we cannot release this
	 * lock before we have buffer lock on the new page, or we risk a race
	 * condition against btvacuumscan --- see comments therein.
	 */
	if (needLock)
		UnlockRelationForExtension(indrel, ExclusiveLock);

	return buf;
}


void
zs_initmetapage(Relation indrel, int nattributes)
{
	Buffer		buf;
	Page		page;
	ZSMetaPage *metapg;
	ZSMetaPageOpaque *opaque;

	buf = ReadBuffer(indrel, P_NEW);
	if (BufferGetBlockNumber(buf) != ZS_META_BLK)
		elog(ERROR, "index is not empty");
	page = BufferGetPage(buf);
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

	PageInit(page, BLCKSZ, sizeof(ZSMetaPageOpaque));
	metapg = (ZSMetaPage *) PageGetContents(page);
	metapg->nattributes = nattributes;

	opaque = (ZSMetaPageOpaque *) PageGetSpecialPointer(page);
	opaque->zs_flags = 0;
	opaque->zs_page_id = ZS_META_PAGE_ID;

	MarkBufferDirty(buf);
	/* TODO: WAL-log */

	UnlockReleaseBuffer(buf);
}


BlockNumber
zsmeta_get_root_for_attribute(Relation indrel, AttrNumber attno, bool forupdate)
{
	Buffer		metabuf;
	ZSMetaPage *metapg;
	BlockNumber	rootblk;

	metabuf = ReadBuffer(indrel, ZS_META_BLK);
	LockBuffer(metabuf, BUFFER_LOCK_SHARE);
	metapg = (ZSMetaPage *) PageGetContents(BufferGetPage(metabuf));

	if (attno <= 0 || attno >= metapg->nattributes)
		elog(ERROR, "invalid attribute number %d (table has only %d attributes)", attno, metapg->nattributes);

	rootblk = metapg->roots[attno - 1];

	if (rootblk == InvalidBlockNumber)
	{
		/* try to allocate one */
		Buffer		rootbuf;
		Page		rootpage;
		ZSBtreePageOpaque *opaque;

		/* TODO: release lock on metapage while we do I/O */
		rootbuf = zs_getnewbuf(indrel);
		rootblk = BufferGetBlockNumber(rootbuf);

		metapg->roots[attno - 1] = rootblk;

		rootpage = BufferGetPage(rootbuf);
		PageInit(rootpage, BLCKSZ, sizeof(ZSBtreePageOpaque));
		opaque = ZSBtreePageGetOpaque(rootpage);
		opaque->zs_next = InvalidBlockNumber;
		ItemPointerSetInvalid(&opaque->zs_lokey);
		ItemPointerSetInvalid(&opaque->zs_hikey);
		opaque->zs_level = 0;
		opaque->zs_flags = ZS_BTREE_LEAF;
		opaque->zs_page_id = ZS_BTREE_PAGE_ID;

		/* initialize the page to look like a root leaf */
		

		MarkBufferDirty(rootbuf);
		MarkBufferDirty(metabuf);
		/* TODO: WAL-log both pages */

		UnlockReleaseBuffer(rootbuf);
	}

	UnlockReleaseBuffer(metabuf);

	return rootblk;
}
