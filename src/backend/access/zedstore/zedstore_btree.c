/*
 * zedstore_btree.c
 *		Routines for handling B-trees structures in ZedStore
 *
 * A Zedstore table consists of multiple B-trees, one for each attribute. The
 * functions in this file deal with one B-tree at a time, it is the caller's
 * responsibility to tie together the scans of each btree.
 *
 * Operations:
 *
 * - Sequential scan in TID order
 *  - must be efficient with scanning multiple trees in sync
 *
 * - random lookups, by TID (for index scan)
 *
 * - range scans by TID (for bitmap index scan)
 *
 * TODO:
 * - internal pages
 * - compression
 *
 * NOTES:
 * - Locking order: child before parent, left before right
 *
 * Copyright (c) 2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/access/zedstore/zedstore_btree.c
 */
#include "postgres.h"

#include "access/itup.h"
#include "access/zedstore_internal.h"
#include "storage/bufmgr.h"
#include "utils/datum.h"
#include "utils/rel.h"

/* This used to pass around context information, when inserting a new tuple */
typedef struct ZSInsertState
{
	Relation	rel;
	AttrNumber	attno;
	Datum		datum;
} ZSInsertState;

/* prototypes for local functions */
static Buffer zsbt_descend(Relation rel, BlockNumber rootblk, ItemPointerData key);
static Buffer zsbt_find_downlink(Relation rel, AttrNumber attno, ItemPointerData key, BlockNumber childblk, int level, int *itemno);
static Buffer zsbt_find_insertion_target(ZSInsertState *state, BlockNumber rootblk);
static ItemPointerData zsbt_insert_to_leaf(Buffer buf, ZSInsertState *state);
static void zsbt_split_leaf(Buffer buf, OffsetNumber lastleftoff, ZSInsertState *state,
				IndexTuple newitem, bool newitemonleft, OffsetNumber newitemoff);
static void zsbt_insert_downlink(Relation rel, AttrNumber attno, Buffer leftbuf, ItemPointerData rightlokey, BlockNumber rightblkno);
static void zsbt_split_internal(Relation rel, AttrNumber attno, Buffer leftbuf, Buffer childbuf,
					OffsetNumber newoff, ItemPointerData newkey, BlockNumber childblk);
static void zsbt_newroot(Relation rel, AttrNumber attno, int level,
			 ItemPointerData key1, BlockNumber blk1,
			 ItemPointerData key2, BlockNumber blk2,
			 Buffer leftchildbuf);
static int zsbt_binsrch_internal(ItemPointerData key, ZSBtreeInternalPageItem *arr, int arr_elems);

/*
 * Insert a new datum to the given attribute's btree.
 *
 * Returns the TID of the new tuple.
 *
 * TODO: When inserting the first attribute of a row, this OK. But subsequent
 * attributes need to be inserted with the same TID. This should take an
 * optional TID argument for that.
 */
ItemPointerData
zsbt_insert(Relation rel, AttrNumber attno, Datum datum)
{
	ZSInsertState state;
	Buffer		buf;
	BlockNumber	rootblk;

	/* TODO: deal with oversized datums that don't fit on a page */

	rootblk = zsmeta_get_root_for_attribute(rel, attno, true);

	state.rel = rel;
	state.attno = attno;
	state.datum = datum;

	buf = zsbt_find_insertion_target(&state, rootblk);

	return zsbt_insert_to_leaf(buf, &state);
}

/*
 * Find the leaf buffer containing the given key TID.
 */
static Buffer
zsbt_descend(Relation rel, BlockNumber rootblk, ItemPointerData key)
{
	BlockNumber next;
	Buffer		buf;
	Page		page;
	ZSBtreePageOpaque *opaque;
	ZSBtreeInternalPageItem *items;
	int			nitems;
	int			itemno;
	int			nextlevel = -1;

	next = rootblk;
	for (;;)
	{
		buf = ReadBuffer(rel, next);
		LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);		/* TODO: shared */
		page = BufferGetPage(buf);
		opaque = ZSBtreePageGetOpaque(page);

		if (nextlevel == -1)
			nextlevel = opaque->zs_level;
		else if (opaque->zs_level != nextlevel)
			elog(ERROR, "unexpected level encountered when descending tree");

		if (opaque->zs_level == 0)
			return buf;

		/*
		 * Do we need to walk right? This could happen if the page was concurrently split.
		 */
		if (ItemPointerCompare(&key, &opaque->zs_hikey) >= 0)
		{
			/* follow the right-link */
			next = opaque->zs_next;
			if (next == InvalidBlockNumber)
				elog(ERROR, "fell off the end of btree");
		}
		else
		{
			/* follow the downlink */
			items = ZSBtreeInternalPageGetItems(page);
			nitems = ZSBtreeInternalPageGetNumItems(page);

			itemno = zsbt_binsrch_internal(key, items, nitems);
			if (itemno < 0)
				elog(ERROR, "could not descend tree for tid (%u, %u)",
					 ItemPointerGetBlockNumberNoCheck(&key),
					 ItemPointerGetOffsetNumberNoCheck(&key));
			next = BlockIdGetBlockNumber(&items[itemno].childblk);
			nextlevel--;
		}
		UnlockReleaseBuffer(buf);
	}
}

/*
 * Re-find the parent page containing downlink for given block.
 * The returned page is exclusive-locked, and *itemno_p is set to the
 * position of the downlink in the parent.
 *
 * If 'childblk' is the root, returns InvalidBuffer.
 */
static Buffer
zsbt_find_downlink(Relation rel, AttrNumber attno,
				   ItemPointerData key, BlockNumber childblk, int level,
				   int *itemno_p)
{
	BlockNumber rootblk;
	BlockNumber next;
	Buffer		buf;
	Page		page;
	ZSBtreePageOpaque *opaque;
	ZSBtreeInternalPageItem *items;
	int			nitems;
	int			itemno;
	int			nextlevel = -1;

	/* start from root */
	rootblk = zsmeta_get_root_for_attribute(rel, attno, true);
	if (rootblk == childblk)
		return InvalidBuffer;

	/* XXX: this is mostly the same as zsbt_descend, but we stop at an internal
	 * page instead of descending all the way down to root */
	next = rootblk;
	for (;;)
	{
		buf = ReadBuffer(rel, next);
		LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
		page = BufferGetPage(buf);
		opaque = ZSBtreePageGetOpaque(page);

		if (nextlevel == -1)
			nextlevel = opaque->zs_level;
		else if (nextlevel != opaque->zs_level)
			elog(ERROR, "unexpected level encountered when descending tree");

		if (opaque->zs_level <= level)
			elog(ERROR, "unexpected page level encountered");

		/*
		 * Do we need to walk right? This could happen if the page was concurrently split.
		 */
		if (ItemPointerCompare(&key, &opaque->zs_hikey) >= 0)
		{
			next = opaque->zs_next;
			if (next == InvalidBlockNumber)
				elog(ERROR, "fell off the end of btree");
		}
		else
		{
			items = ZSBtreeInternalPageGetItems(page);
			nitems = ZSBtreeInternalPageGetNumItems(page);

			itemno = zsbt_binsrch_internal(key, items, nitems);
			if (itemno < 0)
				elog(ERROR, "could not descend tree for tid (%u, %u)",
					 ItemPointerGetBlockNumberNoCheck(&key),
					 ItemPointerGetOffsetNumberNoCheck(&key));

			if (opaque->zs_level == level + 1)
			{
				if (BlockIdGetBlockNumber(&items[itemno].childblk) != childblk)
					elog(ERROR, "could not re-find downlink for block %u", childblk);
				*itemno_p = itemno;
				return buf;
			}

			next = BlockIdGetBlockNumber(&items[itemno].childblk);
			nextlevel--;
		}
		UnlockReleaseBuffer(buf);
	}
}

/*
 * Find a target leaf page to insert new row to.
 *
 * This is used when we're free to pick any TID for the new tuple.
 *
 * TODO: Currently, we just descend to rightmost leaf. Should use a free space
 * map or something to find a suitable target.
 */
static Buffer
zsbt_find_insertion_target(ZSInsertState *state, BlockNumber rootblk)
{
	ItemPointerData rightmostkey;

	ItemPointerSet(&rightmostkey, MaxBlockNumber, 0xfffe);

	return zsbt_descend(state->rel, rootblk, rightmostkey);
}

/*
 * Insert tuple to given leaf page. Return TID of the new item.
 */
static ItemPointerData
zsbt_insert_to_leaf(Buffer buf, ZSInsertState *state)
{
	/* If there's space, add here */
	TupleDesc	desc = RelationGetDescr(state->rel);
	Form_pg_attribute attr = &desc->attrs[state->attno - 1];
	Page		page = BufferGetPage(buf);
	ZSBtreePageOpaque *opaque = ZSBtreePageGetOpaque(page);
	Size		datumsz;
	Size		itemsz;
	IndexTuple	itup;
	char	   *dataptr;
	ItemPointerData tid;
	OffsetNumber maxoff;

	/*
	 * Look at the last item, for its tid.
	 */
	maxoff = PageGetMaxOffsetNumber(page);
	if (maxoff >= FirstOffsetNumber)
	{
		ItemId		iid = PageGetItemId(page, maxoff);
		IndexTuple	hitup = (IndexTuple) PageGetItem(page, iid);

		tid = hitup->t_tid;
		ItemPointerIncrement(&tid);
	}
	else
	{
		tid = opaque->zs_lokey;
	}

	datumsz = datumGetSize(state->datum, attr->attbyval, attr->attlen);
	itemsz = sizeof(IndexTupleData) + datumsz;

	/* TODO: should we detoast or deal with "expanded" datums here? */

	/*
	 * Form an IndexTuple to insert.
	 *
	 * TODO: we probably don't want to use IndexTuples in the future, but it's
	 * handy right now.
	 */
	itup = palloc(itemsz);
	itup->t_tid = tid;
	itup->t_info = 0;
	itup->t_info |= itemsz;

	dataptr = ((char *) itup) + sizeof(IndexTupleData);

	if (attr->attbyval)
		store_att_byval(dataptr, state->datum, attr->attlen);
	else
		memcpy(dataptr, DatumGetPointer(state->datum), datumsz);

	/*
	 * If there's enough space on the page, insert. Otherwise, have to
	 * split the page.
	 */
	if (PageGetFreeSpace(page) >= itemsz)
	{
		if (!PageAddItemExtended(page, (Item) itup, itemsz, maxoff + 1, PAI_OVERWRITE))
			elog(ERROR, "didn't fit, after all?");

		MarkBufferDirty(buf);
		/* TODO: WAL-log */

		UnlockReleaseBuffer(buf);

		return tid;
	}
	else
	{
		maxoff = PageGetMaxOffsetNumber(page);
		zsbt_split_leaf(buf, maxoff, state, itup, false, maxoff + 1);
		return tid;
	}
}

/*
 * Split a leaf page for insertion of 'newitem'.
 */
static void
zsbt_split_leaf(Buffer buf, OffsetNumber lastleftoff, ZSInsertState *state,
				IndexTuple newitem, bool newitemonleft, OffsetNumber newitemoff)
{
	Buffer		leftbuf = buf;
	Buffer		rightbuf;
	BlockNumber rightblkno;
	Page		origpage = BufferGetPage(buf);
	Page		leftpage;
	Page		rightpage;
	ZSBtreePageOpaque *leftopaque;
	ZSBtreePageOpaque *rightopaque;
	ItemPointerData splittid;
	OffsetNumber i,
				maxoff;

	/*
	 * The original page becomes the left half, but we use a temporary copy of it
	 * to operate on. Allocate a new page for the right half.
	 *
	 * TODO: it'd be good to not hold a lock on the original page while we
	 * allocate a new one.
	 */
	leftpage = PageGetTempPageCopySpecial(origpage);
	leftopaque = ZSBtreePageGetOpaque(leftpage);
	Assert(leftopaque->zs_level == 0);
	/* any previous incomplete split must be finished first */
	Assert((leftopaque->zs_flags & ZS_FOLLOW_RIGHT) == 0);

	rightbuf = zs_getnewbuf(state->rel);
	rightpage = BufferGetPage(rightbuf);
	rightblkno = BufferGetBlockNumber(rightbuf);
	PageInit(rightpage, BLCKSZ, sizeof(ZSBtreePageOpaque));
	rightopaque = ZSBtreePageGetOpaque(rightpage);

	/*
	 * Figure out the split point TID.
	 *
	 * TODO: currently, we only append to end, i.e. we only ever split the rightmost leaf.
	 * that makes it easier to figure out the split tid: just take the old page's lokey,
	 * and increment the blocknumber component of it.
	 */
	ItemPointerSet(&splittid, ItemPointerGetBlockNumber(&leftopaque->zs_lokey) + 1, 1);

	/* Set up the page headers */

	rightopaque->zs_next = leftopaque->zs_next;
	rightopaque->zs_lokey = splittid;
	rightopaque->zs_hikey = leftopaque->zs_hikey;
	rightopaque->zs_level = 0;
	rightopaque->zs_flags = 0;
	rightopaque->zs_page_id = ZS_BTREE_PAGE_ID;

	leftopaque->zs_next = rightblkno;
	leftopaque->zs_hikey = splittid;
	leftopaque->zs_flags |= ZS_FOLLOW_RIGHT;

	//elog(NOTICE, "split leaf %u to %u", BufferGetBlockNumber(leftbuf), rightblkno);

	/*
	 * Copy all the tuples
	 */
	maxoff = PageGetMaxOffsetNumber(origpage);
	for (i = FirstOffsetNumber; i <= maxoff; i++)
	{
		ItemId		iid = PageGetItemId(origpage, i);
		IndexTuple	itup = (IndexTuple) PageGetItem(origpage, iid);
		Page		targetpage;

		if (i == newitemoff)
		{
			targetpage = newitemonleft ? leftpage : rightpage;
			if (PageAddItemExtended(targetpage,
									(Item) newitem, IndexTupleSize(newitem),
									PageGetMaxOffsetNumber(targetpage) + 1,
									PAI_OVERWRITE) == InvalidOffsetNumber)
				elog(ERROR, "could not add new item to page on split");
		}

		targetpage = (i <= lastleftoff) ? leftpage : rightpage;
		if (PageAddItemExtended(targetpage,
								(Item) itup, IndexTupleSize(itup),
								PageGetMaxOffsetNumber(targetpage) + 1,
								PAI_OVERWRITE) == InvalidOffsetNumber)
			elog(ERROR, "could not add item to page on split");
	}
	if (i == newitemoff)
	{
		Assert(!newitemonleft);
		if (PageAddItemExtended(rightpage,
								(Item) newitem, IndexTupleSize(newitem),
								FirstOffsetNumber,
								PAI_OVERWRITE) == InvalidOffsetNumber)
			elog(ERROR, "could not add new item to page on split");
	}

	PageRestoreTempPage(leftpage, origpage);

	/* TODO: WAL-log */
	MarkBufferDirty(leftbuf);
	MarkBufferDirty(rightbuf);

	UnlockReleaseBuffer(rightbuf);

	zsbt_insert_downlink(state->rel, state->attno, leftbuf, splittid, rightblkno);
}

/*
 * Create a new btree root page, containing two downlinks.
 *
 * NOTE: the very first root page of a btree, which is also the leaf, is created
 *
 */
static void
zsbt_newroot(Relation rel, AttrNumber attno, int level,
			 ItemPointerData key1, BlockNumber blk1,
			 ItemPointerData key2, BlockNumber blk2,
			 Buffer leftchildbuf)
{
	ZSBtreePageOpaque *opaque;
	ZSBtreePageOpaque *leftchildopaque;
	Buffer		buf;
	Page		page;
	ZSBtreeInternalPageItem *items;
	Buffer		metabuf;

	metabuf = ReadBuffer(rel, ZS_META_BLK);
	LockBuffer(metabuf, BUFFER_LOCK_EXCLUSIVE);

	Assert(ItemPointerCompare(&key1, &key2) < 0);

	buf = zs_getnewbuf(rel);
	page = BufferGetPage(buf);
	PageInit(page, BLCKSZ, sizeof(ZSBtreePageOpaque));
	opaque = ZSBtreePageGetOpaque(page);
	opaque->zs_next = InvalidBlockNumber;
	ItemPointerSet(&opaque->zs_lokey, 0, 1);
	ItemPointerSet(&opaque->zs_hikey, MaxBlockNumber, 0xFFFF);
	opaque->zs_level = level;
	opaque->zs_flags = 0;
	opaque->zs_page_id = ZS_BTREE_PAGE_ID;

	items = ZSBtreeInternalPageGetItems(page);
	items[0].tid = key1;
	BlockIdSet(&items[0].childblk, blk1);
	items[1].tid = key2;
	BlockIdSet(&items[1].childblk, blk2);
	((PageHeader) page)->pd_lower += 2 * sizeof(ZSBtreeInternalPageItem);
	Assert(ZSBtreeInternalPageGetNumItems(page) == 2);

	/* clear the follow-right flag on left child */
	leftchildopaque = ZSBtreePageGetOpaque(BufferGetPage(leftchildbuf));
	leftchildopaque->zs_flags &= ZS_FOLLOW_RIGHT;

	/* TODO: wal-log all, including metapage */

	MarkBufferDirty(buf);
	MarkBufferDirty(leftchildbuf);

	/* Before exiting, update the metapage */
	zsmeta_update_root_for_attribute(rel, attno, metabuf, BufferGetBlockNumber(buf));

	//elog(NOTICE, "new root %u (%u and %u)", BufferGetBlockNumber(buf), blk1, blk2);

	UnlockReleaseBuffer(leftchildbuf);
	UnlockReleaseBuffer(buf);
	UnlockReleaseBuffer(metabuf);
}

/*
 * After page split, insert the downlink of 'rightbuf' to the parent.
 */
static void
zsbt_insert_downlink(Relation rel, AttrNumber attno, Buffer leftbuf,
					 ItemPointerData rightlokey, BlockNumber rightblkno)
{
	BlockNumber	leftblkno = BufferGetBlockNumber(leftbuf);
	Page		leftpage = BufferGetPage(leftbuf);
	ZSBtreePageOpaque *leftopaque = ZSBtreePageGetOpaque(leftpage);
	ItemPointerData leftlokey = leftopaque->zs_lokey;
	ZSBtreeInternalPageItem *items;
	int			nitems;
	int			itemno;
	Buffer		parentbuf;
	Page		parentpage;

	/*
	 * re-find parent
	 *
	 * TODO: this is a bit inefficient. Usually, we have just descended the
	 * tree, and if we just remembered the path we descended, we could just
	 * walk back up.
	 */
	parentbuf = zsbt_find_downlink(rel, attno, leftlokey, leftblkno, leftopaque->zs_level, &itemno);
	if (parentbuf == InvalidBuffer)
	{
		zsbt_newroot(rel, attno, leftopaque->zs_level + 1,
					 leftlokey, BufferGetBlockNumber(leftbuf),
					 rightlokey, rightblkno, leftbuf);
		return;
	}
	parentpage = BufferGetPage(parentbuf);

	/* Find the position in the parent for the downlink */
	items = ZSBtreeInternalPageGetItems(parentpage);
	nitems = ZSBtreeInternalPageGetNumItems(parentpage);
	itemno = zsbt_binsrch_internal(rightlokey, items, nitems);

	/* sanity checks */
	if (itemno < 1 ||
		!ItemPointerEquals(&items[itemno].tid, &leftlokey) ||
		BlockIdGetBlockNumber(&items[itemno].childblk) != leftblkno)
		elog(ERROR, "could not find downlink");
	itemno++;

	if (ZSBtreeInternalPageIsFull(parentpage))
	{
		/* split internal page */
		zsbt_split_internal(rel, attno, parentbuf, leftbuf, itemno, rightlokey, rightblkno);
	}
	else
	{
		/* insert the new downlink for the right page. */
		memmove(&items[itemno + 1],
				&items[itemno],
				(nitems - itemno) * sizeof(ZSBtreeInternalPageItem));
		items[itemno].tid = rightlokey;
		BlockIdSet(&items[itemno].childblk, rightblkno);
		((PageHeader) parentpage)->pd_lower += sizeof(ZSBtreeInternalPageItem);

		leftopaque->zs_flags &= ~ZS_FOLLOW_RIGHT;

		/* TODO: WAL-log */

		MarkBufferDirty(leftbuf);
		MarkBufferDirty(parentbuf);
		UnlockReleaseBuffer(leftbuf);
		UnlockReleaseBuffer(parentbuf);
	}
}

static void
zsbt_split_internal(Relation rel, AttrNumber attno, Buffer leftbuf, Buffer childbuf,
					OffsetNumber newoff, ItemPointerData newkey, BlockNumber childblk)
{
	Buffer		rightbuf;
	Page		origpage = BufferGetPage(leftbuf);
	Page		leftpage;
	Page		rightpage;
	BlockNumber rightblkno;
	ZSBtreePageOpaque *leftopaque;
	ZSBtreePageOpaque *rightopaque;
	ZSBtreeInternalPageItem *origitems;
	ZSBtreeInternalPageItem *leftitems;
	ZSBtreeInternalPageItem *rightitems;
	int			orignitems;
	int			leftnitems;
	int			rightnitems;
	int			splitpoint;
	ItemPointerData splittid;
	bool		newitemonleft;
	int			i;
	ZSBtreeInternalPageItem newitem;

	leftpage = PageGetTempPageCopySpecial(origpage);
	leftopaque = ZSBtreePageGetOpaque(leftpage);
	Assert(leftopaque->zs_level > 0);
	/* any previous incomplete split must be finished first */
	Assert((leftopaque->zs_flags & ZS_FOLLOW_RIGHT) == 0);

	rightbuf = zs_getnewbuf(rel);
	rightpage = BufferGetPage(rightbuf);
	rightblkno = BufferGetBlockNumber(rightbuf);
	PageInit(rightpage, BLCKSZ, sizeof(ZSBtreePageOpaque));
	rightopaque = ZSBtreePageGetOpaque(rightpage);

	/*
	 * Figure out the split point.
	 *
	 * TODO: currently, always do 90/10 split.
	 */
	origitems = ZSBtreeInternalPageGetItems(origpage);
	orignitems = ZSBtreeInternalPageGetNumItems(origpage);
	splitpoint = orignitems * 0.9;
	splittid = origitems[splitpoint].tid;
	newitemonleft = (ItemPointerCompare(&newkey, &splittid) < 0);

	/* Set up the page headers */
	rightopaque->zs_next = leftopaque->zs_next;
	rightopaque->zs_lokey = splittid;
	rightopaque->zs_hikey = leftopaque->zs_hikey;
	rightopaque->zs_level = leftopaque->zs_level;
	rightopaque->zs_flags = 0;
	rightopaque->zs_page_id = ZS_BTREE_PAGE_ID;

	leftopaque->zs_next = rightblkno;
	leftopaque->zs_hikey = splittid;
	leftopaque->zs_flags |= ZS_FOLLOW_RIGHT;

	/* copy the items */
	leftitems = ZSBtreeInternalPageGetItems(leftpage);
	leftnitems = 0;
	rightitems = ZSBtreeInternalPageGetItems(rightpage);
	rightnitems = 0;

	newitem.tid = newkey;
	BlockIdSet(&newitem.childblk, childblk);

	for (i = 0; i < orignitems; i++)
	{
		if (i == newoff)
		{
			if (newitemonleft)
				leftitems[leftnitems++] = newitem;
			else
				rightitems[rightnitems++] = newitem;
		}

		if (i < splitpoint)
			leftitems[leftnitems++] = origitems[i];
		else
			rightitems[rightnitems++] = origitems[i];
	}
	/* cope with possibility that newitem goes at the end */
	if (i <= newoff)
	{
		Assert(!newitemonleft);
		rightitems[rightnitems++] = newitem;
	}
	((PageHeader) leftpage)->pd_lower += leftnitems * sizeof(ZSBtreeInternalPageItem);
	((PageHeader) rightpage)->pd_lower += rightnitems * sizeof(ZSBtreeInternalPageItem);

	Assert(leftnitems + rightnitems == orignitems + 1);

	PageRestoreTempPage(leftpage, origpage);

	//elog(NOTICE, "split internal %u to %u", BufferGetBlockNumber(leftbuf), rightblkno);

	/* TODO: WAL-logging */
	MarkBufferDirty(leftbuf);
	MarkBufferDirty(rightbuf);

	MarkBufferDirty(childbuf);
	ZSBtreePageGetOpaque(BufferGetPage(childbuf))->zs_flags &= ~ZS_FOLLOW_RIGHT;
	UnlockReleaseBuffer(childbuf);

	UnlockReleaseBuffer(rightbuf);

	/* recurse to insert downlink */
	zsbt_insert_downlink(rel, attno, leftbuf, splittid, rightblkno);

	/* Release buffers */
}

/*
 * Begin a scan of the btree.
 */
void
zsbt_begin_scan(Relation rel, AttrNumber attno, ItemPointerData starttid, ZSBtreeScan *scan)
{
	BlockNumber	rootblk;
	Buffer		buf;

	rootblk = zsmeta_get_root_for_attribute(rel, attno, false);

	if (rootblk == InvalidBlockNumber)
	{
		/* completely empty tree */
		scan->rel = NULL;
		scan->attno = InvalidAttrNumber;
		scan->active = false;
		scan->lastbuf = InvalidBuffer;
		scan->lastoff = InvalidOffsetNumber;
		ItemPointerSetInvalid(&scan->nexttid);
		return;
	}

	buf = zsbt_descend(rel, rootblk, starttid);
	LockBuffer(buf, BUFFER_LOCK_UNLOCK);

	scan->rel = rel;
	scan->attno = attno;

	scan->active = true;
	scan->lastbuf = buf;
	scan->lastoff = InvalidOffsetNumber;
	scan->nexttid = starttid;
}

void
zsbt_end_scan(ZSBtreeScan *scan)
{
	if (!scan->active)
		return;

	if (scan->lastbuf)
		ReleaseBuffer(scan->lastbuf);
	scan->active = false;
}

/*
 * Return true if there was another tuple. The datum is returned in *datum,
 * and its TID in *tid. For a pass-by-ref datum, it's a palloc'd copy.
 */
bool
zsbt_scan_next(ZSBtreeScan *scan, Datum *datum, ItemPointerData *tid)
{
	TupleDesc	desc;
	Buffer		buf;
	Page		page;
	ZSBtreePageOpaque *opaque;
	OffsetNumber off;
	OffsetNumber maxoff;
	BlockNumber	next;

	if (!scan->active)
		return false;

	desc = RelationGetDescr(scan->rel);
	for (;;)
	{
		buf = scan->lastbuf;
		page = BufferGetPage(buf);
		opaque = ZSBtreePageGetOpaque(page);

		LockBuffer(buf, BUFFER_LOCK_SHARE);

		/* TODO: check that the page is a valid zs btree page */

		/* TODO: check the last offset first, as an optimization */
		maxoff = PageGetMaxOffsetNumber(page);
		for (off = FirstOffsetNumber; off <= maxoff; off++)
		{
			ItemId		iid = PageGetItemId(page, off);
			IndexTuple	itup = (IndexTuple) PageGetItem(page, iid);
			char	   *ptr;

			if (ItemPointerCompare(&itup->t_tid, &scan->nexttid) >= 0)
			{
				Form_pg_attribute attr = &desc->attrs[scan->attno - 1];

				ptr = ((char *) itup) + sizeof(IndexTupleData);

				*datum = fetchatt(attr, ptr);
				*datum = datumCopy(*datum, attr->attbyval, attr->attlen);
				*tid = itup->t_tid;
				LockBuffer(buf, BUFFER_LOCK_UNLOCK);

				scan->lastbuf = buf;
				scan->lastoff = off;
				scan->nexttid = *tid;
				ItemPointerIncrement(&scan->nexttid);

				return true;
			}
		}

		/* No more items on this page. Walk right, if possible */
		next = opaque->zs_next;
		if (next == BufferGetBlockNumber(buf))
			elog(ERROR, "btree page %u next-pointer points to itself", next);
		LockBuffer(buf, BUFFER_LOCK_UNLOCK);

		if (next == InvalidBlockNumber)
		{
			scan->active = false;
			ReleaseBuffer(scan->lastbuf);
			scan->lastbuf = InvalidBuffer;
			return false;
		}

		scan->lastbuf = ReleaseAndReadBuffer(scan->lastbuf, scan->rel, next);
	}
}

ItemPointerData
zsbt_get_last_tid(Relation rel, AttrNumber attno)
{
	BlockNumber	rootblk;
	ItemPointerData rightmostkey;
	ItemPointerData	tid;
	Buffer		buf;
	Page		page;
	ZSBtreePageOpaque *opaque;
	OffsetNumber maxoff;

	/* Find the rightmost leaf */
	rootblk = zsmeta_get_root_for_attribute(rel, attno, true);
	ItemPointerSet(&rightmostkey, MaxBlockNumber, 0xfffe);
	buf = zsbt_descend(rel, rootblk, rightmostkey);
	page = BufferGetPage(buf);
	opaque = ZSBtreePageGetOpaque(page);

	/*
	 * Look at the last item, for its tid.
	 */
	maxoff = PageGetMaxOffsetNumber(page);
	if (maxoff >= FirstOffsetNumber)
	{
		ItemId		iid = PageGetItemId(page, maxoff);
		IndexTuple	hitup = (IndexTuple) PageGetItem(page, iid);

		tid = hitup->t_tid;
		ItemPointerIncrement(&tid);
	}
	else
	{
		tid = opaque->zs_lokey;
	}
	UnlockReleaseBuffer(buf);
	
	return tid;
}


static int
zsbt_binsrch_internal(ItemPointerData key, ZSBtreeInternalPageItem *arr, int arr_elems)
{
	int			low,
				high,
				mid;
	int			cmp;

	low = 0;
	high = arr_elems;
	while (high > low)
	{
		mid = low + (high - low) / 2;

		cmp = ItemPointerCompare(&key, &arr[mid].tid);
		if (cmp >= 0)
			low = mid + 1;
		else
			high = mid;
	}
	return low - 1;
}
