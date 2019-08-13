/*
 * zedstore_attpage.c
 *		Routines for handling attribute leaf pages.
 *
 * A Zedstore table consists of multiple B-trees, one for each attribute. The
 * functions in this file deal with a scan of one attribute tree.
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
 * NOTES:
 * - Locking order: child before parent, left before right
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/zedstore/zedstore_attpage.c
 */
#include "postgres.h"

#include "access/zedstore_compression.h"
#include "access/zedstore_internal.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "utils/datum.h"
#include "utils/memutils.h"
#include "utils/rel.h"

/* prototypes for local functions */
static void zsbt_attr_repack_replace(Relation rel, AttrNumber attno,
									 Buffer oldbuf, List *items);
static void zsbt_attr_add_items(Relation rel, AttrNumber attno, Buffer buf,
								List *newitems);

/* ----------------------------------------------------------------
 *						 Public interface
 * ----------------------------------------------------------------
 */

/*
 * Begin a scan of an attribute btree.
 *
 * Fills in the scan struct in *scan.
 */
void
zsbt_attr_begin_scan(Relation rel, TupleDesc tdesc, AttrNumber attno, zstid starttid,
					 zstid endtid, ZSAttrTreeScan *scan)
{
	scan->rel = rel;
	scan->attno = attno;
	scan->attdesc = TupleDescAttr(tdesc, attno - 1);

	scan->context = CurrentMemoryContext;
	scan->starttid = starttid;
	scan->endtid = endtid;
	scan->array_datums = MemoryContextAlloc(scan->context, sizeof(Datum));
	scan->array_isnulls = MemoryContextAlloc(scan->context, sizeof(bool) + 7);
	scan->array_tids = MemoryContextAlloc(scan->context, sizeof(zstid));
	scan->array_datums_allocated_size = 1;
	scan->array_num_elements = 0;
	scan->array_curr_idx = -1;

	scan->decompress_buf = NULL;
	scan->decompress_buf_size = 0;
	scan->attr_buf = NULL;
	scan->attr_buf_size = 0;

	scan->active = true;
	scan->lastbuf = InvalidBuffer;
	scan->lastoff = InvalidOffsetNumber;
}

void
zsbt_attr_end_scan(ZSAttrTreeScan *scan)
{
	if (!scan->active)
		return;

	if (scan->lastbuf != InvalidBuffer)
		ReleaseBuffer(scan->lastbuf);

	scan->active = false;
	scan->array_num_elements = 0;
	scan->array_curr_idx = -1;

	if (scan->array_datums)
		pfree(scan->array_datums);
	if (scan->array_isnulls)
		pfree(scan->array_isnulls);
	if (scan->array_tids)
		pfree(scan->array_tids);
	if (scan->decompress_buf)
		pfree(scan->decompress_buf);
	if (scan->attr_buf)
		pfree(scan->attr_buf);
}

/*
 * Fetch the array item whose firsttid-endtid range contains 'nexttid',
 * if any.
 *
 * Return true if an item was found. The Datum/isnull data of are
 * placed into scan->array_* fields. The data is valid until the next
 * call of this function. Note that the item's range contains 'nexttid',
 * but its TID list might not include the exact TID itself. The caller
 * must scan the array to check for that.
 *
 * This is normally not used directly. Use the zsbt_attr_fetch() wrapper,
 * instead.
 */
bool
zsbt_attr_scan_fetch_array(ZSAttrTreeScan *scan, zstid nexttid)
{
	if (!scan->active)
		return InvalidZSTid;

	/*
	 * Advance to the next TID >= nexttid.
	 *
	 * This advances scan->nexttid as it goes.
	 */
	while (nexttid < scan->endtid)
	{
		Buffer		buf;
		Page		page;
		OffsetNumber off;
		OffsetNumber maxoff;

		/*
		 * Find and lock the leaf page containing scan->nexttid.
		 */
		buf = zsbt_find_and_lock_leaf_containing_tid(scan->rel, scan->attno,
													 scan->lastbuf, nexttid,
													 BUFFER_LOCK_SHARE);
		scan->lastbuf = buf;
		if (!BufferIsValid(buf))
		{
			/*
			 * Completely empty tree. This should only happen at the beginning of a
			 * scan - a tree cannot go missing after it's been created - but we don't
			 * currently check for that.
			 */
			break;
		}
		page = BufferGetPage(buf);

		/*
		 * Scan the items on the page, to find the next one that covers
		 * nexttid.
		 */
		/* TODO: check the last offset first, as an optimization */
		maxoff = PageGetMaxOffsetNumber(page);
		for (off = FirstOffsetNumber; off <= maxoff; off++)
		{
			ItemId		iid = PageGetItemId(page, off);
			ZSAttributeArrayItem *item = (ZSAttributeArrayItem *) PageGetItem(page, iid);

			if (item->t_endtid <= nexttid)
				continue;

			if (item->t_firsttid > nexttid)
				break;

			/*
			 * Extract the data into scan->array_* fields.
			 *
			 * NOTE: zsbt_attr_item_extract() always makes a copy of the data,
			 * so we can release the lock on the page after doing this.
			 */
			zsbt_attr_item_extract(scan, item);
			scan->array_curr_idx = -1;

			if (scan->array_num_elements > 0)
			{
				/* Found it! */
				LockBuffer(buf, BUFFER_LOCK_UNLOCK);
				return true;
			}
		}

		/* No matching items. XXX: we should remember the 'next' block, for
		 * the next call. When we're seqscanning, we will almost certainly need
		 * that next.
		 */
		LockBuffer(buf, BUFFER_LOCK_UNLOCK);
		return false;
	}

	/* Reached end of scan. */
	scan->active = false;
	scan->array_num_elements = 0;
	scan->array_curr_idx = -1;
	if (BufferIsValid(scan->lastbuf))
		ReleaseBuffer(scan->lastbuf);
	scan->lastbuf = InvalidBuffer;
	return false;
}

/*
 * Insert a multiple items to the given attribute's btree.
 */
void
zsbt_attr_multi_insert(Relation rel, AttrNumber attno,
					   Datum *datums, bool *isnulls, zstid *tids, int nitems)
{
	Form_pg_attribute attr;
	Buffer		buf;
	zstid		insert_target_key;
	List	   *newitems;

	Assert (attno >= 1);
	attr = &rel->rd_att->attrs[attno - 1];

	/*
	 * Find the right place for the given TID.
	 */
	insert_target_key = tids[0];

	/* Create items to insert. */
	newitems = zsbt_attr_create_items(attr, datums, isnulls, tids, nitems);

	buf = zsbt_descend(rel, attno, insert_target_key, 0, false);

	/*
	 * FIXME: I think it's possible, that the target page has been split by
	 * a concurrent backend, so that it contains only part of the keyspace.
	 * zsbt_attr_add_items() would not handle that correctly.
	 */

	/* recompress and possibly split the page */
	zsbt_attr_add_items(rel, attno, buf, newitems);

	/* zsbt_attr_add_items unlocked 'buf' */
	ReleaseBuffer(buf);
}

/*
 * Remove datums for the given TIDs from the attribute tree.
 */
void
zsbt_attr_remove(Relation rel, AttrNumber attno, IntegerSet *tids)
{
	Form_pg_attribute attr;
	Buffer		buf;
	Page		page;
	ZSBtreePageOpaque *opaque;
	OffsetNumber maxoff;
	OffsetNumber off;
	List	   *newitems = NIL;
	ZSAttributeArrayItem *item;
	ZSExplodedItem *newitem;
	zstid		nexttid;
	MemoryContext oldcontext;
	MemoryContext tmpcontext;

	tmpcontext = AllocSetContextCreate(CurrentMemoryContext,
									   "ZedstoreAMVacuumContext",
									   ALLOCSET_DEFAULT_SIZES);
	oldcontext = MemoryContextSwitchTo(tmpcontext);

	attr = &rel->rd_att->attrs[attno - 1];

	intset_begin_iterate(tids);
	if (!intset_iterate_next(tids, &nexttid))
		nexttid = InvalidZSTid;

	while (nexttid < MaxPlusOneZSTid)
	{
		buf = zsbt_descend(rel, attno, nexttid, 0, false);
		page = BufferGetPage(buf);
		opaque = ZSBtreePageGetOpaque(page);

		newitems = NIL;

		/*
		 * Find the item containing the first tid to remove.
		 */
		maxoff = PageGetMaxOffsetNumber(page);
		off = FirstOffsetNumber;
		for (;;)
		{
			zstid		endtid;
			ItemId		iid;
			int			num_to_remove;
			zstid	   *tids_arr;

			if (off > maxoff)
				break;

			iid = PageGetItemId(page, off);
			item = (ZSAttributeArrayItem *) PageGetItem(page, iid);
			off++;

			/*
			 * If we don't find an item containing the given TID, just skip
			 * over it.
			 *
			 * This can legitimately happen, if e.g. VACUUM is
			 * interrupted, after it has already removed the attribute data for
			 * the dead tuples.
			 */
			while (nexttid < item->t_firsttid)
			{
				if (!intset_iterate_next(tids, &nexttid))
					nexttid = MaxPlusOneZSTid;
			}

			/* If this item doesn't contain any of the items we're removing, keep it as it is. */
			endtid = item->t_endtid;
			if (endtid < nexttid)
			{
				newitems = lappend(newitems, item);
				continue;
			}

			/*
			 * We now have an array item at hand, that contains at least one
			 * of the TIDs we want to remove. Split the array, removing all
			 * the target tids.
			 */
			tids_arr = palloc((item->t_num_elements + 1) * sizeof(zstid));
			num_to_remove = 0;
			while (nexttid < endtid)
			{
				tids_arr[num_to_remove++] = nexttid;
				if (!intset_iterate_next(tids, &nexttid))
					nexttid = MaxPlusOneZSTid;
			}
			tids_arr[num_to_remove++] = MaxPlusOneZSTid;
			newitem = zsbt_attr_remove_from_item(attr, item, tids_arr);
			pfree(tids_arr);
			if (newitem)
				newitems = lappend(newitems, newitem);
		}

		/*
		 * Skip over any remaining TIDs in the dead TID list that would
		 * be on this page, but are missing.
		 */
		while (nexttid < opaque->zs_hikey)
		{
			if (!intset_iterate_next(tids, &nexttid))
				nexttid = MaxPlusOneZSTid;
		}

		/* Now pass the list to the recompressor. */
		IncrBufferRefCount(buf);
		if (newitems)
		{
			zsbt_attr_repack_replace(rel, attno, buf, newitems);
		}
		else
		{
			zs_split_stack *stack;

			stack = zsbt_unlink_page(rel, attno, buf, 0);

			if (!stack)
			{
				/* failed. */
				Page		newpage = PageGetTempPageCopySpecial(BufferGetPage(buf));

				stack = zs_new_split_stack_entry(buf, newpage);
			}

			/* apply the changes */
			zs_apply_split_changes(rel, stack);
		}
		ReleaseBuffer(buf); 	/* zsbt_apply_split_changes unlocked 'buf' */

		/*
		 * We can now free the decompression contexts. The pointers in the 'items' list
		 * point to decompression buffers, so we cannot free them until after writing out
		 * the pages.
		 */
		MemoryContextReset(tmpcontext);
	}
	MemoryContextSwitchTo(oldcontext);
	MemoryContextDelete(tmpcontext);
}

/* ----------------------------------------------------------------
 *						 Internal routines
 * ----------------------------------------------------------------
 */

/*
 * This helper function is used to implement INSERT, UPDATE and DELETE.
 *
 * The items in the 'newitems' list are added to the page, to the correct position.
 *
 * This function handles decompressing and recompressing items, and splitting
 * existing items, or the page, as needed.
 */
static void
zsbt_attr_add_items(Relation rel, AttrNumber attno, Buffer buf, List *newitems)
{
	Form_pg_attribute attr;
	Page		page = BufferGetPage(buf);
	OffsetNumber off;
	OffsetNumber maxoff;
	List	   *items = NIL;
	Size		growth;
	ListCell   *lc;
	ListCell   *nextnewlc;
	zstid		last_existing_tid;
	ZSAttributeArrayItem *olditem;
	ZSAttributeArrayItem *newitem;

	attr = &rel->rd_att->attrs[attno - 1];

	nextnewlc = list_head(newitems);

	Assert(newitems != NIL);

	maxoff = PageGetMaxOffsetNumber(page);

	/*
	 * Quick check if the new items go to the end of the page. This is the
	 * common case, when inserting new rows, since we allocate TIDs in order.
	 */
	if (maxoff == 0)
		last_existing_tid = 0;
	else
	{
		ItemId			iid;
		ZSAttributeArrayItem *lastitem;

		iid = PageGetItemId(page, maxoff);
		lastitem = (ZSAttributeArrayItem *) PageGetItem(page, iid);

		last_existing_tid = lastitem->t_endtid;
	}

	if (((ZSAttributeArrayItem *) lfirst(nextnewlc))->t_firsttid >= last_existing_tid)
	{
		/*
		 * The new items go to the end. Do they fit as is on the page?
		 * */
		growth = 0;
		foreach (lc, newitems)
		{
			ZSAttributeArrayItem *item = (ZSAttributeArrayItem *) lfirst(lc);

			growth += MAXALIGN(item->t_size) + sizeof(ItemId);
		}

		if (growth <= PageGetExactFreeSpace(page))
		{
			/* The new items fit on the page. Add them. */
			START_CRIT_SECTION();

			foreach(lc, newitems)
			{
				ZSAttributeArrayItem *item = (ZSAttributeArrayItem *) lfirst(lc);

				Assert ((item->t_flags & 0x3) == item->t_flags);
				Assert(item->t_size > 5);

				if (PageAddItemExtended(page,
										(Item) item, item->t_size,
										PageGetMaxOffsetNumber(page) + 1,
										PAI_OVERWRITE) == InvalidOffsetNumber)
					elog(ERROR, "could not add item to attribute page");
			}

			MarkBufferDirty(buf);

			/* TODO: WAL-log */

			END_CRIT_SECTION();

			LockBuffer(buf, BUFFER_LOCK_UNLOCK);

			list_free(newitems);

			return;
		}
	}

	/*
	 * Need to recompress and/or split the hard way.
	 *
	 * First, loop through the old and new items in lockstep, to figure out
	 * where the new items go to. If some of the old and new items have
	 * overlapping TID ranges, we will need to split some items to make
	 * them not overlap.
	 */
	off = 1;
	if (off <= maxoff)
	{
		ItemId		iid = PageGetItemId(page, off);
		olditem = (ZSAttributeArrayItem *) PageGetItem(page, iid);
		off++;
	}
	else
		olditem = NULL;

	if (nextnewlc)
	{
		newitem = lfirst(nextnewlc);
		nextnewlc = lnext(newitems, nextnewlc);
	}

	for (;;)
	{
		if (!newitem && !olditem)
			break;

		if (newitem && olditem && newitem->t_firsttid == olditem->t_firsttid)
			elog(ERROR, "duplicate TID on attribute page");

		/*
		 *   NNNNNNNN
		 *             OOOOOOOOO
		 */
		if (newitem && (!olditem || newitem->t_endtid <= olditem->t_firsttid))
		{
			items = lappend(items, newitem);
			if (nextnewlc)
			{
				newitem = lfirst(nextnewlc);
				nextnewlc = lnext(newitems, nextnewlc);
			}
			else
				newitem = NULL;
			continue;
		}

		/*
		 *              NNNNNNNN
		 *   OOOOOOOOO
		 */
		if (olditem && (!newitem || olditem->t_endtid <= newitem->t_firsttid))
		{
			items = lappend(items, olditem);
			if (off <= maxoff)
			{
				ItemId		iid = PageGetItemId(page, off);
				olditem = (ZSAttributeArrayItem *) PageGetItem(page, iid);
				off++;
			}
			else
				olditem = NULL;
			continue;
		}

		/*
		 *   NNNNNNNN
		 *        OOOOOOOOO
		 */
		if (olditem->t_firsttid > newitem->t_firsttid)
		{
			ZSExplodedItem *left_newitem;
			ZSExplodedItem *right_newitem;
			/*
			 * split newitem:
			 *
			 *   NNNNNnnnn
			 *        OOOOOOOOO
			 */
			zsbt_split_item(attr, (ZSExplodedItem *) newitem, olditem->t_firsttid,
							&left_newitem, &right_newitem);
			items = lappend(items, left_newitem);
			newitem = (ZSAttributeArrayItem *) right_newitem;
			continue;
		}

		/*
		 *        NNNNNNNN
		 *   OOOOOOOOO
		 */
		if (olditem->t_firsttid < newitem->t_firsttid)
		{
			ZSExplodedItem *left_olditem;
			ZSExplodedItem *right_olditem;
			/*
			 * split olditem:
			 *
			 *   OOOOOoooo
			 *        NNNNNNNNN
			 */
			zsbt_split_item(attr, (ZSExplodedItem *) olditem, newitem->t_firsttid,
							&left_olditem, &right_olditem);
			items = lappend(items, left_olditem);
			olditem = (ZSAttributeArrayItem *) right_olditem;
			continue;
		}

		elog(ERROR, "shouldn't reach here");
	}

	/* Now pass the list to the repacker, to distribute the items to pages. */
	IncrBufferRefCount(buf);

	/*
	 * Now we have a list of non-overlapping items, containing all the old and
	 * new data. zsbt_attr_repack_replace() takes care of storing them on the
	 * page, splitting the page if needed.
	 */
	zsbt_attr_repack_replace(rel, attno, buf, items);

	list_free(items);
}


/*
 * Repacker routines
 */
typedef struct
{
	Page		currpage;
	int			compressed_items;

	/* first page writes over the old buffer, subsequent pages get newly-allocated buffers */
	zs_split_stack *stack_head;
	zs_split_stack *stack_tail;

	int			total_items;
	int			total_packed_items;

	AttrNumber	attno;
	zstid		hikey;
} zsbt_attr_repack_context;

static void
zsbt_attr_repack_newpage(zsbt_attr_repack_context *cxt, zstid nexttid, int flags)
{
	Page		newpage;
	ZSBtreePageOpaque *newopaque;
	zs_split_stack *stack;

	if (cxt->currpage)
	{
		/* set the last tid on previous page */
		ZSBtreePageOpaque *oldopaque = ZSBtreePageGetOpaque(cxt->currpage);

		oldopaque->zs_hikey = nexttid;
	}

	newpage = (Page) palloc(BLCKSZ);
	PageInit(newpage, BLCKSZ, sizeof(ZSBtreePageOpaque));

	stack = zs_new_split_stack_entry(InvalidBuffer, /* will be assigned later */
									 newpage);
	if (cxt->stack_tail)
		cxt->stack_tail->next = stack;
	else
		cxt->stack_head = stack;
	cxt->stack_tail = stack;

	cxt->currpage = newpage;

	newopaque = ZSBtreePageGetOpaque(newpage);
	newopaque->zs_attno = cxt->attno;
	newopaque->zs_next = InvalidBlockNumber; /* filled in later */
	newopaque->zs_lokey = nexttid;
	newopaque->zs_hikey = cxt->hikey;		/* overwritten later, if this is not last page */
	newopaque->zs_level = 0;
	newopaque->zs_flags = flags;
	newopaque->zs_page_id = ZS_BTREE_PAGE_ID;
}

/*
 * Rewrite a leaf page, with given 'items' as the new content.
 *
 * First, calls zsbt_attr_recompress_items(), which will try to combine
 * short items, and compress uncompressed items. After that, will try to
 * store all the items on the page, replacing old content on the page.
 *
 * The items may contain "exploded" items, as ZSExplodedItem. They will
 * be converted to normal array items suitable for storing on-disk.
 *
 * If the items don't fit on the page, then the page is split. It is
 * entirely possible that they don't fit even on two pages; we split the page
 * into as many pages as needed. Hopefully not more than a few pages, though,
 * because otherwise you might hit limits on the number of buffer pins (with
 * tiny shared_buffers).
 *
 * On entry, 'oldbuf' must be pinned and exclusive-locked. On exit, the lock
 * is released, but it's still pinned.
 */
static void
zsbt_attr_repack_replace(Relation rel, AttrNumber attno, Buffer oldbuf, List *items)
{
	Form_pg_attribute attr = &rel->rd_att->attrs[attno - 1];
	ListCell   *lc;
	zsbt_attr_repack_context cxt;
	ZSBtreePageOpaque *oldopaque = ZSBtreePageGetOpaque(BufferGetPage(oldbuf));
	BlockNumber orignextblk;
	zs_split_stack *stack;
	List	   *downlinks = NIL;
	List	   *recompressed_items;

	/*
	 * Check that the items in the input are in correct order and don't
	 * overlap.
	 */
#ifdef USE_ASSERT_CHECKING
	{
		zstid		prev_endtid = 0;
		ListCell   *lc;

		foreach (lc, items)
		{
			ZSAttributeArrayItem *item = (ZSAttributeArrayItem *) lfirst(lc);
			zstid		item_firsttid;
			zstid		item_endtid;

			if (item->t_size == 0)
			{
				ZSExplodedItem *eitem = (ZSExplodedItem *) item;
				item_firsttid = eitem->tids[0];
				item_endtid = eitem->tids[eitem->t_num_elements - 1] + 1;
			}
			else
			{
				item_firsttid = item->t_firsttid;
				item_endtid = item->t_endtid;;
			}

			Assert(item_firsttid >= prev_endtid);
			Assert(item_endtid > item_firsttid);
			prev_endtid = item_endtid;
		}
	}
#endif

	/*
	 * First, split, merge and compress the items as needed, into
	 * suitable chunks.
	 */
	recompressed_items = zsbt_attr_recompress_items(attr, items);

	/*
	 * Then, store them on the page, creating new pages as needed.
	 */
	orignextblk = oldopaque->zs_next;
	Assert(orignextblk != BufferGetBlockNumber(oldbuf));

	cxt.currpage = NULL;
	cxt.stack_head = cxt.stack_tail = NULL;
	cxt.attno = attno;
	cxt.hikey = oldopaque->zs_hikey;

	cxt.total_items = 0;

	zsbt_attr_repack_newpage(&cxt, oldopaque->zs_lokey, (oldopaque->zs_flags & ZSBT_ROOT));

	foreach(lc, recompressed_items)
	{
		ZSAttributeArrayItem *item = lfirst(lc);

		if (PageGetFreeSpace(cxt.currpage) < MAXALIGN(item->t_size))
			zsbt_attr_repack_newpage(&cxt, item->t_firsttid, 0);

		if (PageAddItemExtended(cxt.currpage,
								(Item) item, item->t_size,
								PageGetMaxOffsetNumber(cxt.currpage) + 1,
								PAI_OVERWRITE) == InvalidOffsetNumber)
			elog(ERROR, "could not add item to page while recompressing");

		cxt.total_items++;
	}

	/*
	 * Ok, we now have a list of pages, to replace the original page, as private
	 * in-memory copies. Allocate buffers for them, and write them out.
	 *
	 * allocate all the pages before entering critical section, so that
	 * out-of-disk-space doesn't lead to PANIC
	 */
	stack = cxt.stack_head;
	Assert(stack->buf == InvalidBuffer);
	stack->buf = oldbuf;
	while (stack->next)
	{
		Page	thispage = stack->page;
		ZSBtreePageOpaque *thisopaque = ZSBtreePageGetOpaque(thispage);
		ZSBtreeInternalPageItem *downlink;
		Buffer	nextbuf;

		Assert(stack->next->buf == InvalidBuffer);

		nextbuf = zspage_getnewbuf(rel, InvalidBuffer);
		stack->next->buf = nextbuf;
		Assert (BufferGetBlockNumber(nextbuf) != orignextblk);

		thisopaque->zs_next = BufferGetBlockNumber(nextbuf);

		downlink = palloc(sizeof(ZSBtreeInternalPageItem));
		downlink->tid = thisopaque->zs_hikey;
		downlink->childblk = BufferGetBlockNumber(nextbuf);
		downlinks = lappend(downlinks, downlink);

		stack = stack->next;
	}
	/* last one in the chain */
	ZSBtreePageGetOpaque(stack->page)->zs_next = orignextblk;

	/* If we had to split, insert downlinks for the new pages. */
	if (cxt.stack_head->next)
	{
		oldopaque = ZSBtreePageGetOpaque(cxt.stack_head->page);

		if ((oldopaque->zs_flags & ZSBT_ROOT) != 0)
		{
			ZSBtreeInternalPageItem *downlink;

			downlink = palloc(sizeof(ZSBtreeInternalPageItem));
			downlink->tid = MinZSTid;
			downlink->childblk = BufferGetBlockNumber(cxt.stack_head->buf);
			downlinks = lcons(downlink, downlinks);

			cxt.stack_tail->next = zsbt_newroot(rel, attno, oldopaque->zs_level + 1, downlinks);

			/* clear the ZSBT_ROOT flag on the old root page */
			oldopaque->zs_flags &= ~ZSBT_ROOT;
		}
		else
		{
			cxt.stack_tail->next = zsbt_insert_downlinks(rel, attno,
														 oldopaque->zs_lokey, BufferGetBlockNumber(oldbuf), oldopaque->zs_level + 1,
														 downlinks);
		}
		/* note: stack_tail is not the real tail anymore */
	}

	/* Finally, overwrite all the pages we had to modify */
	zs_apply_split_changes(rel, cxt.stack_head);
}
