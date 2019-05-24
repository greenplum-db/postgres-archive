/*
 * zedstore_btree.c
 *		Common routines for handling TID and attibute B-tree structures
 *
 * A Zedstore table consists of multiple B-trees, one to store TIDs and
 * visibility information of the rows, and one tree for each attribute,
 * to hold the data. The TID and attribute trees differ at the leaf
 * level, but the internal pages have the same layout. This file contains
 * routines to deal with internal pages, and some other common
 * functionality.
 *
 * When dealing with the TID tree, pass ZS_META_ATTRIBUTE_NUM as the
 * attribute number.
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/zedstore/zedstore_btree.c
 */
#include "postgres.h"

#include "access/zedstore_internal.h"
#include "storage/bufmgr.h"
#include "storage/procarray.h"
#include "utils/rel.h"

/* prototypes for local functions */
static zs_split_stack *zsbt_split_internal_page(Relation rel, AttrNumber attno,
												Buffer leftbuf, OffsetNumber newoff, List *downlinks);
static zs_split_stack *zsbt_merge_pages(Relation rel, AttrNumber attno, Buffer leftbuf, Buffer rightbuf, bool target_is_left);

static int zsbt_binsrch_internal(zstid key, ZSBtreeInternalPageItem *arr, int arr_elems);

/*
 * Find the page containing the given key TID at the given level.
 *
 * Level 0 means leaf. The returned buffer is exclusive-locked.
 */
Buffer
zsbt_descend(Relation rel, AttrNumber attno, zstid key, int level, bool readonly)
{
	BlockNumber next;
	Buffer		buf;
	Page		page;
	ZSBtreePageOpaque *opaque;
	ZSBtreeInternalPageItem *items;
	int			nitems;
	int			itemno;
	int			nextlevel;
	BlockNumber failblk = InvalidBlockNumber;
	ZSMetaCacheData *metacache;

	/* Fast path for the very common case that we're looking for the rightmost page */
	metacache = zsmeta_get_cache(rel);
	if (level == 0 &&
		attno < metacache->cache_nattributes &&
		metacache->cache_attrs[attno].rightmost != InvalidBlockNumber &&
		key >= metacache->cache_attrs[attno].rightmost_lokey)
	{
		next = metacache->cache_attrs[attno].rightmost;
		nextlevel = 0;
	}
	else
	{
		/* start from root */
		next = zsmeta_get_root_for_attribute(rel, attno, readonly);
		if (next == InvalidBlockNumber)
		{
			/* completely empty tree */
			return InvalidBuffer;
		}
		nextlevel = -1;
	}
	for (;;)
	{
		/*
		 * If we arrive again to a block that was a dead-end earlier, it seems
		 * that the tree is corrupt.
		 *
		 * XXX: It's theoretically possible that the block was removed, but then
		 * added back at the same location, and removed again. So perhaps retry
		 * a few times?
		 */
		if (next == failblk || next == ZS_META_BLK)
			elog(ERROR, "arrived at incorrect block %u while descending zedstore btree", next);

		buf = ReadBuffer(rel, next);
		LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);		/* TODO: shared */
		page = BufferGetPage(buf);
		if (!zsbt_page_is_expected(rel, attno, key, nextlevel, buf))
		{
			/*
			 * We arrived at an unexpected page. This can happen with concurrent
			 * splits, or page deletions. We could try following the right-link, but
			 * there's no guarantee that's the correct page either, so let's restart
			 * from the root. If we landed here because of concurrent modifications,
			 * the next attempt should land on the correct page. Remember that we
			 * incorrectly ended up on this page, so that if this happens because
			 * the tree is corrupt, rather than concurrent splits, and we land here
			 * again, we won't loop forever.
			 */
			UnlockReleaseBuffer(buf);
			failblk = next;
			nextlevel = -1;
			zsmeta_invalidate_cache(rel);
			next = zsmeta_get_root_for_attribute(rel, attno, readonly);
			if (next == InvalidBlockNumber)
				elog(ERROR, "could not find root for attribute %d", attno);
			continue;
		}
		opaque = ZSBtreePageGetOpaque(page);

		if (nextlevel == -1)
			nextlevel = opaque->zs_level;

		else if (opaque->zs_level != nextlevel)
			elog(ERROR, "unexpected level encountered when descending tree");

		if (opaque->zs_level == level)
			break;

		/* Find the downlink and follow it */
		items = ZSBtreeInternalPageGetItems(page);
		nitems = ZSBtreeInternalPageGetNumItems(page);

		itemno = zsbt_binsrch_internal(key, items, nitems);
		if (itemno < 0)
			elog(ERROR, "could not descend tree for tid (%u, %u)",
				 ZSTidGetBlockNumber(key), ZSTidGetOffsetNumber(key));

		next = items[itemno].childblk;
		nextlevel--;

		UnlockReleaseBuffer(buf);
	}

	if (opaque->zs_level == 0 && opaque->zs_next == InvalidBlockNumber)
	{
		metacache = zsmeta_get_cache(rel);
		if (attno < metacache->cache_nattributes)
		{
			metacache->cache_attrs[attno].rightmost = next;
			metacache->cache_attrs[attno].rightmost_lokey = opaque->zs_lokey;
		}
	}

	return buf;
}

/*
 * Check that a page is a valid B-tree page, and covers the given key.
 *
 * This is used when traversing the tree, to check that e.g. a concurrent page
 * split didn't move pages around, so that the page we were walking to isn't
 * the correct one anymore.
 */
bool
zsbt_page_is_expected(Relation rel, AttrNumber attno, zstid key, int level, Buffer buf)
{
	Page		page = BufferGetPage(buf);
	ZSBtreePageOpaque *opaque;

	/*
	 * The page might have been deleted and even reused as a completely different
	 * kind of a page, so we must be prepared for anything.
	 */
	if (PageIsNew(page))
		return false;

	if (PageGetSpecialSize(page) != MAXALIGN(sizeof(ZSBtreePageOpaque)))
		return false;

	opaque = ZSBtreePageGetOpaque(page);

	if (opaque->zs_page_id != ZS_BTREE_PAGE_ID)
		return false;

	Assert(opaque->zs_next != BufferGetBlockNumber(buf));

	if (opaque->zs_attno != attno)
		return false;

	if (level != -1 && opaque->zs_level != level)
		return false;

	if (opaque->zs_lokey > key || opaque->zs_hikey <= key)
		return false;

	return true;
}

/*
 * Create a new btree root page, containing two downlinks.
 *
 * NOTE: the very first root page of a btree, which is also the leaf, is created
 * in zsmeta_get_root_for_attribute(), not here.
 *
 * XXX: What if there are too many downlinks to fit on a page? Shouldn't happen
 * in practice..
 */
zs_split_stack *
zsbt_newroot(Relation rel, AttrNumber attno, int level, List *downlinks)
{
	Page		metapage;
	ZSMetaPage *metapg;
	Buffer		newrootbuf;
	Page		newrootpage;
	ZSBtreePageOpaque *newrootopaque;
	ZSBtreeInternalPageItem *items;
	Buffer		metabuf;
	zs_split_stack *stack1;
	zs_split_stack *stack2;
	ListCell   *lc;
	int			i;

	metabuf = ReadBuffer(rel, ZS_META_BLK);
	LockBuffer(metabuf, BUFFER_LOCK_EXCLUSIVE);

	/* allocate a new root page */
	newrootbuf = zspage_getnewbuf(rel, metabuf);
	newrootpage = palloc(BLCKSZ);
	PageInit(newrootpage, BLCKSZ, sizeof(ZSBtreePageOpaque));
	newrootopaque = ZSBtreePageGetOpaque(newrootpage);
	newrootopaque->zs_attno = attno;
	newrootopaque->zs_next = InvalidBlockNumber;
	newrootopaque->zs_lokey = MinZSTid;
	newrootopaque->zs_hikey = MaxPlusOneZSTid;
	newrootopaque->zs_level = level;
	newrootopaque->zs_flags = ZSBT_ROOT;
	newrootopaque->zs_page_id = ZS_BTREE_PAGE_ID;

	items = ZSBtreeInternalPageGetItems(newrootpage);

	/* add all the downlinks */
	i = 0;
	foreach (lc, downlinks)
	{
		ZSBtreeInternalPageItem *downlink = (ZSBtreeInternalPageItem *) lfirst(lc);

		items[i++] = *downlink;
	}
	((PageHeader) newrootpage)->pd_lower += i * sizeof(ZSBtreeInternalPageItem);

	/* FIXME: Check that all the downlinks fit on the page. */

	/* update the metapage */
	metapage = PageGetTempPageCopy(BufferGetPage(metabuf));

	metapg = (ZSMetaPage *) PageGetContents(metapage);
	if ((attno != ZS_META_ATTRIBUTE_NUM) && (attno <= 0 || attno > metapg->nattributes))
		elog(ERROR, "invalid attribute number %d (table \"%s\" has only %d attributes)",
			 attno, RelationGetRelationName(rel), metapg->nattributes);

	metapg->tree_root_dir[attno].root = BufferGetBlockNumber(newrootbuf);

	stack1 = zs_new_split_stack_entry(metabuf, metapage);
	stack2 = zs_new_split_stack_entry(newrootbuf, newrootpage);
	stack2->next = stack1;

	return stack2;
}

/*
 * After page split, insert the downlink of 'rightblkno' to the parent.
 *
 * On entry, 'leftbuf' must be pinned exclusive-locked.
 */
zs_split_stack *
zsbt_insert_downlinks(Relation rel, AttrNumber attno,
					  zstid leftlokey, BlockNumber leftblkno, int level,
					  List *downlinks)
{
	int			numdownlinks = list_length(downlinks);
	ZSBtreeInternalPageItem *items;
	int			nitems;
	int			itemno;
	Buffer		parentbuf;
	Page		parentpage;
	zs_split_stack *split_stack;
	ZSBtreeInternalPageItem *firstdownlink;

	/*
	 * re-find parent
	 *
	 * TODO: this is a bit inefficient. Usually, we have just descended the
	 * tree, and if we just remembered the path we descended, we could just
	 * walk back up.
	 */
	parentbuf = zsbt_descend(rel, attno, leftlokey, level, false);
	parentpage = BufferGetPage(parentbuf);

	firstdownlink = (ZSBtreeInternalPageItem *) linitial(downlinks);

	/* Find the position in the parent for the downlink */
	items = ZSBtreeInternalPageGetItems(parentpage);
	nitems = ZSBtreeInternalPageGetNumItems(parentpage);
	itemno = zsbt_binsrch_internal(firstdownlink->tid, items, nitems);

	/* sanity checks */
	if (itemno < 0 || items[itemno].tid != leftlokey ||
		items[itemno].childblk != leftblkno)
	{
		elog(ERROR, "could not find downlink for block %u TID (%u, %u)",
			 leftblkno, ZSTidGetBlockNumber(leftlokey),
			 ZSTidGetOffsetNumber(leftlokey));
	}
	itemno++;

	if (PageGetExactFreeSpace(parentpage) < numdownlinks * sizeof(ZSBtreeInternalPageItem))
	{
		/* split internal page */
		split_stack = zsbt_split_internal_page(rel, attno, parentbuf, itemno, downlinks);
	}
	else
	{
		ZSBtreeInternalPageItem *newitems;
		Page		newpage;
		int			i;
		ListCell   *lc;

		newpage = PageGetTempPageCopySpecial(parentpage);

		split_stack = zs_new_split_stack_entry(parentbuf, newpage);

		/* insert the new downlink for the right page. */
		newitems = ZSBtreeInternalPageGetItems(newpage);
		memcpy(newitems, items, itemno * sizeof(ZSBtreeInternalPageItem));

		i = itemno;
		foreach(lc, downlinks)
		{
			ZSBtreeInternalPageItem *downlink = (ZSBtreeInternalPageItem *) lfirst(lc);

			Assert(downlink->childblk != 0);
			newitems[i++] = *downlink;
		}

		memcpy(&newitems[i], &items[itemno], (nitems - itemno) * sizeof(ZSBtreeInternalPageItem));
		((PageHeader) newpage)->pd_lower += (nitems + numdownlinks) * sizeof(ZSBtreeInternalPageItem);
	}
	return split_stack;
}

/*
 * Split an internal page.
 *
 * The new downlink specified by 'newkey' is inserted to position 'newoff', on 'leftbuf'.
 * The page is split.
 */
static zs_split_stack *
zsbt_split_internal_page(Relation rel, AttrNumber attno, Buffer origbuf,
						 OffsetNumber newoff, List *newitems)
{
	Page		origpage = BufferGetPage(origbuf);
	ZSBtreePageOpaque *origopaque = ZSBtreePageGetOpaque(origpage);
	Buffer		buf;
	Page		page;
	ZSBtreeInternalPageItem *origitems;
	int			orignitems;
	zs_split_stack *stack_first;
	zs_split_stack *stack;
	Size		splitthreshold;
	ListCell   *lc;
	int			origitemno;
	List	   *downlinks = NIL;

	origitems = ZSBtreeInternalPageGetItems(origpage);
	orignitems = ZSBtreeInternalPageGetNumItems(origpage);

	page = PageGetTempPageCopySpecial(origpage);
	buf = origbuf;

	stack = zs_new_split_stack_entry(buf, page);
	stack_first = stack;

	/* XXX: currently, we always do 90/10 splits */
	splitthreshold = PageGetExactFreeSpace(page) * 0.10;

	lc = list_head(newitems);
	origitemno = 0;
	for (;;)
	{
		ZSBtreeInternalPageItem *item;
		ZSBtreeInternalPageItem *p;

		if (origitemno == newoff && lc)
		{
			item = lfirst(lc);
			lc = lnext(lc);
		}
		else
		{
			if (origitemno == orignitems)
				break;
			item = &origitems[origitemno];
			origitemno++;
		}

		if (PageGetExactFreeSpace(page) < splitthreshold)
		{
			/* have to split to another page */
			ZSBtreePageOpaque *prevopaque = ZSBtreePageGetOpaque(page);
			ZSBtreePageOpaque *opaque = ZSBtreePageGetOpaque(page);
			BlockNumber blkno;
			ZSBtreeInternalPageItem *downlink;

			buf = zspage_getnewbuf(rel, InvalidBuffer);
			blkno = BufferGetBlockNumber(buf);
			page = palloc(BLCKSZ);
			PageInit(page, BLCKSZ, sizeof(ZSBtreePageOpaque));

			opaque = ZSBtreePageGetOpaque(page);
			opaque->zs_attno = attno;
			opaque->zs_next = prevopaque->zs_next;
			opaque->zs_lokey = item->tid;
			opaque->zs_hikey = prevopaque->zs_hikey;
			opaque->zs_level = prevopaque->zs_level;
			opaque->zs_flags = 0;
			opaque->zs_page_id = ZS_BTREE_PAGE_ID;

			prevopaque->zs_next = blkno;
			prevopaque->zs_hikey = item->tid;

			stack->next = zs_new_split_stack_entry(buf, page);
			stack = stack->next;

			downlink = palloc(sizeof(ZSBtreeInternalPageItem));
			downlink->tid = item->tid;
			downlink->childblk = blkno;
			downlinks = lappend(downlinks, downlink);
		}

		p = (ZSBtreeInternalPageItem *) ((char *) page + ((PageHeader) page)->pd_lower);
		*p = *item;
		((PageHeader) page)->pd_lower += sizeof(ZSBtreeInternalPageItem);
	}

	/* recurse to insert downlinks, if we had to split. */
	if (downlinks)
	{
		if ((origopaque->zs_flags & ZSBT_ROOT) != 0)
		{
			ZSBtreeInternalPageItem *downlink;

			downlink = palloc(sizeof(ZSBtreeInternalPageItem));
			downlink->tid = MinZSTid;
			downlink->childblk = BufferGetBlockNumber(origbuf);
			downlinks = lcons(downlink, downlinks);

			stack->next = zsbt_newroot(rel, attno, origopaque->zs_level + 1, downlinks);

			/* clear the ZSBT_ROOT flag on the old root page */
			ZSBtreePageGetOpaque(stack_first->page)->zs_flags &= ~ZSBT_ROOT;
		}
		else
		{
			stack->next = zsbt_insert_downlinks(rel, attno,
												origopaque->zs_lokey,
												BufferGetBlockNumber(origbuf),
												origopaque->zs_level + 1,
												downlinks);
		}
	}

	return stack_first;
}


/*
 * Removes the last item from page, and unlinks the page from the tree.
 *
 * NOTE: you cannot remove the only leaf. Returns NULL if the page could not
 * be deleted.
 */
zs_split_stack *
zsbt_unlink_page(Relation rel, AttrNumber attno, Buffer buf, int level)
{
	Page		page = BufferGetPage(buf);
	ZSBtreePageOpaque *opaque = ZSBtreePageGetOpaque(page);
	Buffer		leftbuf;
	Buffer		rightbuf;
	zs_split_stack *stack;

	/* cannot currently remove the only page at its level. */
	if (opaque->zs_lokey == MinZSTid && opaque->zs_hikey == MaxPlusOneZSTid)
	{
		return NULL;
	}

	/*
	 * Find left sibling.
	 * or if this is leftmost page, find right sibling.
	 */
	if (opaque->zs_lokey != MinZSTid)
	{
		rightbuf = buf;
		leftbuf = zsbt_descend(rel, attno, opaque->zs_lokey - 1, level, false);

		stack = zsbt_merge_pages(rel, attno, leftbuf, rightbuf, false);
		if (!stack)
		{
			UnlockReleaseBuffer(leftbuf);
			return NULL;
		}
	}
	else
	{
		rightbuf = zsbt_descend(rel, attno, opaque->zs_hikey, level, false);
		leftbuf = buf;
		stack = zsbt_merge_pages(rel, attno, leftbuf, rightbuf, true);
		if (!stack)
		{
			UnlockReleaseBuffer(rightbuf);
			return NULL;
		}
	}

	return stack;
}

/*
 * Page deletion:
 *
 * Mark page empty, remove downlink. If parent becomes empty, recursively delete it.
 *
 * Unlike in the nbtree index, we don't need to worry about concurrent scans. They
 * will simply retry if they land on an unexpected page.
 */
static zs_split_stack *
zsbt_merge_pages(Relation rel, AttrNumber attno, Buffer leftbuf, Buffer rightbuf, bool target_is_left)
{
	Buffer		parentbuf;
	Page		origleftpage;
	Page		leftpage;
	Page		rightpage;
	ZSBtreePageOpaque *leftopaque;
	ZSBtreePageOpaque *origleftopaque;
	ZSBtreePageOpaque *rightopaque;
	ZSBtreeInternalPageItem *parentitems;
	int			parentnitems;
	Page		parentpage;
	int			itemno;
	zs_split_stack *stack;
	zs_split_stack *stack_head;
	zs_split_stack *stack_tail;

	origleftpage = BufferGetPage(leftbuf);
	origleftopaque = ZSBtreePageGetOpaque(origleftpage);
	rightpage = BufferGetPage(rightbuf);
	rightopaque = ZSBtreePageGetOpaque(rightpage);

	/* find downlink for 'rightbuf' in the parent */
	parentbuf = zsbt_descend(rel, attno, rightopaque->zs_lokey, origleftopaque->zs_level + 1, false);
	parentpage = BufferGetPage(parentbuf);

	parentitems = ZSBtreeInternalPageGetItems(parentpage);
	parentnitems = ZSBtreeInternalPageGetNumItems(parentpage);
	itemno = zsbt_binsrch_internal(rightopaque->zs_lokey, parentitems, parentnitems);
	if (itemno < 0 || parentitems[itemno].childblk != BufferGetBlockNumber(rightbuf))
		elog(ERROR, "could not find downlink to FPM page %u", BufferGetBlockNumber(rightbuf));

	if (parentnitems > 1 && itemno == 0)
	{
		/*
		 * Don't delete the leftmost child of a parent. That would move the
		 * keyspace of the parent, so we'd need to adjust the lo/hikey of
		 * the parent page, and the parent's downlink in the grandparent.
		 * Maybe later...
		 */
		UnlockReleaseBuffer(parentbuf);
		elog(DEBUG1, "deleting leftmost child of a parent not implemented");
		return NULL;
	}

	if (target_is_left)
	{
		/* move all items from right to left before unlinking the right page */
		leftpage = PageGetTempPageCopy(rightpage);
		leftopaque = ZSBtreePageGetOpaque(leftpage);

		memcpy(leftopaque, origleftopaque, sizeof(ZSBtreePageOpaque));
	}
	else
	{
		/* right page is empty. */
		leftpage = PageGetTempPageCopy(origleftpage);
		leftopaque = ZSBtreePageGetOpaque(leftpage);
	}

	/* update left hikey */
	leftopaque->zs_hikey = ZSBtreePageGetOpaque(rightpage)->zs_hikey;
	leftopaque->zs_next = ZSBtreePageGetOpaque(rightpage)->zs_next;

	Assert(ZSBtreePageGetOpaque(leftpage)->zs_level == ZSBtreePageGetOpaque(rightpage)->zs_level);

	stack = zs_new_split_stack_entry(leftbuf, leftpage);
	stack_head = stack_tail = stack;

	/* Mark right page as empty/unused */
	rightpage = palloc0(BLCKSZ);

	stack = zs_new_split_stack_entry(rightbuf, rightpage);
	stack->recycle = true;
	stack_tail->next = stack;
	stack_tail = stack;

	/* remove downlink from parent */
	if (parentnitems > 1)
	{
		Page		newpage = PageGetTempPageCopySpecial(parentpage);
		ZSBtreeInternalPageItem *newitems = ZSBtreeInternalPageGetItems(newpage);

		memcpy(newitems, parentitems, itemno * sizeof(ZSBtreeInternalPageItem));
		memcpy(&newitems[itemno], &parentitems[itemno + 1], (parentnitems - itemno -1) * sizeof(ZSBtreeInternalPageItem));

		((PageHeader) newpage)->pd_lower += (parentnitems - 1) * sizeof(ZSBtreeInternalPageItem);

		stack = zs_new_split_stack_entry(parentbuf, newpage);
		stack_tail->next = stack;
		stack_tail = stack;
	}
	else
	{
		/* the parent becomes empty as well. Recursively remove it. */
		stack_tail->next = zsbt_unlink_page(rel, attno, parentbuf, leftopaque->zs_level + 1);
		if (stack_tail->next == NULL)
		{
			/* oops, couldn't remove the parent. Back out */
			stack = stack_head;
			while (stack)
			{
				zs_split_stack *next = stack->next;

				pfree(stack->page);
				pfree(stack);
				stack = next;
			}
		}
	}

	return stack_head;
}

static int
zsbt_binsrch_internal(zstid key, ZSBtreeInternalPageItem *arr, int arr_elems)
{
	int			low,
		high,
		mid;

	low = 0;
	high = arr_elems;
	while (high > low)
	{
		mid = low + (high - low) / 2;

		if (key >= arr[mid].tid)
			low = mid + 1;
		else
			high = mid;
	}
	return low - 1;
}
