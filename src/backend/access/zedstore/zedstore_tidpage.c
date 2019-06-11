/*
 * zedstore_tidpage.c
 *		Routines for handling the TID tree.
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
 * NOTES:
 * - Locking order: child before parent, left before right
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/zedstore/zedstore_tidpage.c
 */
#include "postgres.h"

#include "access/zedstore_internal.h"
#include "access/zedstore_undo.h"
#include "lib/integerset.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/predicate.h"
#include "utils/rel.h"

/* prototypes for local functions */
static void zsbt_tid_recompress_replace(Relation rel, Buffer oldbuf, List *items);
static OffsetNumber zsbt_tid_fetch(Relation rel, zstid tid,
								   Buffer *buf_p, ZSUndoRecPtr *undo_ptr_p, bool *isdead_p);
static void zsbt_tid_add_items(Relation rel, Buffer buf, List *newitems);
static void zsbt_tid_replace_item(Relation rel, Buffer buf, OffsetNumber off, List *newitems);

static TM_Result zsbt_tid_update_lock_old(Relation rel, zstid otid,
									  TransactionId xid, CommandId cid, bool key_update, Snapshot snapshot,
									  Snapshot crosscheck, bool wait, TM_FailureData *hufd, ZSUndoRecPtr *prevundoptr_p);
static void zsbt_tid_update_insert_new(Relation rel, zstid *newtid,
					   TransactionId xid, CommandId cid, ZSUndoRecPtr prevundoptr);
static void zsbt_tid_mark_old_updated(Relation rel, zstid otid, zstid newtid,
					  TransactionId xid, CommandId cid, bool key_update, Snapshot snapshot);
static OffsetNumber zsbt_binsrch_tidpage(zstid key, Page page);

/* ----------------------------------------------------------------
 *						 Public interface
 * ----------------------------------------------------------------
 */

/*
 * Begin a scan of the btree.
 */
void
zsbt_tid_begin_scan(Relation rel, zstid starttid,
					zstid endtid, Snapshot snapshot, ZSTidTreeScan *scan)
{
	Buffer		buf;

	scan->rel = rel;

	scan->snapshot = snapshot;
	scan->context = CurrentMemoryContext;
	scan->lastoff = InvalidOffsetNumber;
	scan->nexttid = starttid;
	scan->endtid = endtid;
	scan->nonvacuumable_status = ZSNV_NONE;
	memset(&scan->recent_oldest_undo, 0, sizeof(scan->recent_oldest_undo));
	memset(&scan->array_iter, 0, sizeof(scan->array_iter));
	scan->array_iter.context = CurrentMemoryContext;

	buf = zsbt_descend(rel, ZS_META_ATTRIBUTE_NUM, starttid, 0, true);
	if (!BufferIsValid(buf))
	{
		/* completely empty tree */
		scan->active = false;
		scan->lastbuf = InvalidBuffer;
		return;
	}
	LockBuffer(buf, BUFFER_LOCK_UNLOCK);

	scan->active = true;
	scan->lastbuf = buf;

	scan->recent_oldest_undo = zsundo_get_oldest_undo_ptr(rel);
}

/*
 * Reset the 'next' TID in a scan to the given TID.
 */
void
zsbt_tid_reset_scan(ZSTidTreeScan *scan, zstid starttid)
{
	/*
	 * If the new starting position is within the currently processed array,
	 * we can just reposition within the array.
	 */
	if (scan->array_iter.num_tids > 0)
	{
		if (scan->array_iter.tids[0] <= starttid &&
			starttid <= scan->array_iter.tids[scan->array_iter.num_tids - 1])
		{
			/* TODO: could do a binary search here */
			int			i;

			for (i = 0; i < scan->array_iter.num_tids; i++)
			{
				if (scan->array_iter.tids[i] >= starttid)
					break;
			}
			Assert(i < scan->array_iter.num_tids);
			scan->array_iter.next_idx = i;
			scan->nexttid = scan->array_iter.tids[i];
			return;
		}
		else
		{
			scan->array_iter.num_tids = 0;
			scan->array_iter.next_idx = 0;
		}
	}

	if (starttid < scan->nexttid)
	{
		/* have to restart from scratch. */
		/* TODO: if the new starting point lands on the same page, we could
		 * keep 'lastbuf' */
		scan->array_iter.num_tids = 0;
		scan->array_iter.next_idx = 0;
		scan->nexttid = starttid;
		if (scan->lastbuf != InvalidBuffer)
			ReleaseBuffer(scan->lastbuf);
		scan->lastbuf = InvalidBuffer;
	}
	else
	{
		scan->nexttid = starttid;
	}
}

void
zsbt_tid_end_scan(ZSTidTreeScan *scan)
{
	if (!scan->active)
		return;

	if (scan->lastbuf != InvalidBuffer)
		ReleaseBuffer(scan->lastbuf);

	scan->active = false;
	scan->array_iter.num_tids = 0;
	scan->array_iter.next_idx = 0;
}

/*
 * Helper function of zsbt_scan_next(), to extract Datums from the given
 * array item into the scan->array_* fields.
 */
static void
zsbt_tid_scan_extract_array(ZSTidTreeScan *scan, ZSTidArrayItem *aitem)
{
	int			j;
	bool		slots_visible[4];

	zsbt_tid_item_unpack(aitem, &scan->array_iter);

	if (scan->nexttid < scan->array_iter.tids[0])
		scan->nexttid = scan->array_iter.tids[0];

	slots_visible[ZSBT_OLD_UNDO_SLOT] = true;
	slots_visible[ZSBT_DEAD_UNDO_SLOT] = false;

	for (int i = 2; i < aitem->t_num_undo_slots; i++)
	{
		ZSUndoRecPtr undoptr = scan->array_iter.undoslots[i];
		TransactionId obsoleting_xid;

		slots_visible[i] = zs_SatisfiesVisibility(scan, undoptr, &obsoleting_xid, NULL);
		if (!slots_visible[i] && scan->serializable && TransactionIdIsValid(obsoleting_xid))
			CheckForSerializableConflictOut(scan->rel, obsoleting_xid, scan->snapshot);
		scan->array_iter.undoslots[i] = undoptr;
	}

	j = 0;
	for (int i = 0; i < scan->array_iter.num_tids; i++)
	{
		/* skip over elements that we are not interested in */
		if (scan->array_iter.tids[i] < scan->nexttid)
			continue;
		if (scan->array_iter.tids[i] >= scan->endtid)
			break;

		/* Is this item visible? */
		if (slots_visible[scan->array_iter.tid_undoslotnos[i]])
		{
			scan->array_iter.tids[j] = scan->array_iter.tids[i];
			scan->array_iter.tid_undoslotnos[j] = scan->array_iter.tid_undoslotnos[i];
			j++;
		}
	}
	scan->array_iter.num_tids = j;
	scan->array_iter.next_idx = 0;
}

/*
 * Advance scan to next item.
 *
 * Return true if there was another item. The Datum/isnull of the item is
 * placed in scan->array_* fields. For a pass-by-ref datum, it's a palloc'd
 * copy that's valid until the next call.
 *
 * This is normally not used directly. See zsbt_scan_next_tid() and
 * zsbt_scan_next_fetch() wrappers, instead.
 */
zstid
zsbt_tid_scan_next(ZSTidTreeScan *scan)
{
	Buffer		buf;
	bool		buf_is_locked = false;
	Page		page;
	ZSBtreePageOpaque *opaque;
	OffsetNumber maxoff;
	OffsetNumber off;
	BlockNumber	next;
	zstid		result;

	if (!scan->active)
		return InvalidZSTid;

	/*
	 * Process items, until we find something that is visible to the snapshot.
	 *
	 * This advances scan->nexttid as it goes.
	 */
	while (scan->nexttid < scan->endtid)
	{
		/*
		 * If we are still processing an array item, return next element from it.
		 */
		if (scan->array_iter.next_idx < scan->array_iter.num_tids)
			goto have_array;

		/*
		 * Scan the page for the next item.
		 */
		buf = scan->lastbuf;
		if (!buf_is_locked)
		{
			if (BufferIsValid(buf))
			{
				LockBuffer(buf, BUFFER_LOCK_SHARE);
				buf_is_locked = true;

				/*
				 * It's possible that the page was concurrently split or recycled by
				 * another backend (or ourselves). Have to re-check that the page is
				 * still valid.
				 */
				if (!zsbt_page_is_expected(scan->rel, ZS_META_ATTRIBUTE_NUM, scan->nexttid, 0, buf))
				{
					/*
					 * It's not valid for the TID we're looking for, but maybe it was the
					 * right page for the previous TID. In that case, we don't need to
					 * restart from the root, we can follow the right-link instead.
					 */
					if (zsbt_page_is_expected(scan->rel, ZS_META_ATTRIBUTE_NUM, scan->nexttid - 1, 0, buf))
					{
						page = BufferGetPage(buf);
						opaque = ZSBtreePageGetOpaque(page);
						next = opaque->zs_next;
						if (next != InvalidBlockNumber)
						{
							LockBuffer(buf, BUFFER_LOCK_UNLOCK);
							buf_is_locked = false;
							buf = ReleaseAndReadBuffer(buf, scan->rel, next);
							scan->lastbuf = buf;
							continue;
						}
					}

					UnlockReleaseBuffer(buf);
					buf_is_locked = false;
					buf = scan->lastbuf = InvalidBuffer;
				}
			}

			if (!BufferIsValid(buf))
			{
				buf = scan->lastbuf = zsbt_descend(scan->rel, ZS_META_ATTRIBUTE_NUM, scan->nexttid, 0, true);
				buf_is_locked = true;
			}
		}
		page = BufferGetPage(buf);
		opaque = ZSBtreePageGetOpaque(page);
		Assert(opaque->zs_page_id == ZS_BTREE_PAGE_ID);

		/* TODO: check the last offset first, as an optimization */
		maxoff = PageGetMaxOffsetNumber(page);
		for (off = FirstOffsetNumber; off <= maxoff; off++)
		{
			ItemId		iid = PageGetItemId(page, off);
			ZSTidArrayItem *item = (ZSTidArrayItem *) PageGetItem(page, iid);
			zstid		lasttid;

			lasttid = zsbt_tid_item_lasttid(item);

			if (scan->nexttid > lasttid)
				continue;

			if (item->t_firsttid >= scan->endtid)
			{
				scan->nexttid = scan->endtid;
				break;
			}

			zsbt_tid_scan_extract_array(scan, item);

			if (scan->array_iter.next_idx < scan->array_iter.num_tids)
			{
				LockBuffer(scan->lastbuf, BUFFER_LOCK_UNLOCK);
				buf_is_locked = false;
				break;
			}
		}

		if (scan->array_iter.next_idx < scan->array_iter.num_tids)
			continue;

		/* No more items on this page. Walk right, if possible */
		if (scan->nexttid < opaque->zs_hikey)
			scan->nexttid = opaque->zs_hikey;
		next = opaque->zs_next;
		if (next == BufferGetBlockNumber(buf))
			elog(ERROR, "btree page %u next-pointer points to itself", next);
		LockBuffer(buf, BUFFER_LOCK_UNLOCK);
		buf_is_locked = false;

		if (next == InvalidBlockNumber || scan->nexttid >= scan->endtid)
		{
			scan->active = false;
			scan->array_iter.num_tids = 0;
			scan->array_iter.next_idx = 0;
			ReleaseBuffer(scan->lastbuf);
			scan->lastbuf = InvalidBuffer;
			break;
		}

		scan->lastbuf = ReleaseAndReadBuffer(scan->lastbuf, scan->rel, next);
	}

	return InvalidZSTid;

have_array:
	/*
	 * If we are still processing an array item, return next element from it.
	 */
	Assert(scan->array_iter.next_idx < scan->array_iter.num_tids);

	result = scan->array_iter.tids[scan->array_iter.next_idx];
	scan->array_iter.next_idx++;
	scan->nexttid = result + 1;
	return result;
}

/*
 * Get the last tid (plus one) in the tree.
 */
zstid
zsbt_get_last_tid(Relation rel)
{
	zstid		rightmostkey;
	zstid		tid;
	Buffer		buf;
	Page		page;
	ZSBtreePageOpaque *opaque;
	OffsetNumber maxoff;

	/* Find the rightmost leaf */
	rightmostkey = MaxZSTid;
	buf = zsbt_descend(rel, ZS_META_ATTRIBUTE_NUM, rightmostkey, 0, true);
	if (!BufferIsValid(buf))
	{
		return MinZSTid;
	}
	page = BufferGetPage(buf);
	opaque = ZSBtreePageGetOpaque(page);

	/*
	 * Look at the last item, for its tid.
	 */
	maxoff = PageGetMaxOffsetNumber(page);
	if (maxoff >= FirstOffsetNumber)
	{
		ItemId		iid = PageGetItemId(page, maxoff);
		ZSTidArrayItem *lastitem = (ZSTidArrayItem *) PageGetItem(page, iid);

		tid = zsbt_tid_item_lasttid(lastitem) + 1;
	}
	else
	{
		tid = opaque->zs_lokey;
	}
	UnlockReleaseBuffer(buf);

	return tid;
}

/*
 * Insert a multiple TIDs.
 *
 * Populates the TIDs of the new tuples.
 *
 * If 'tid' in list is valid, then that TID is used. It better not be in use already. If
 * it's invalid, then a new TID is allocated, as we see best. (When inserting the
 * first column of the row, pass invalid, and for other columns, pass the TID
 * you got for the first column.)
 */
void
zsbt_tid_multi_insert(Relation rel, zstid *tids, int nitems,
					  TransactionId xid, CommandId cid, uint32 speculative_token, ZSUndoRecPtr prevundoptr)
{
	Buffer		buf;
	Page		page;
	ZSBtreePageOpaque *opaque;
	OffsetNumber maxoff;
	zstid		insert_target_key;
	List	   *newitems;
	ZSUndoRecPtr undorecptr;
	zstid		endtid;
	zstid		tid;

	/*
	 * Insert to the rightmost leaf.
	 *
	 * TODO: use a Free Space Map to find suitable target.
	 */
	insert_target_key = MaxZSTid;
	buf = zsbt_descend(rel, ZS_META_ATTRIBUTE_NUM, insert_target_key, 0, false);
	page = BufferGetPage(buf);
	opaque = ZSBtreePageGetOpaque(page);
	maxoff = PageGetMaxOffsetNumber(page);

	/*
	 * Look at the last item, for its tid.
	 *
	 * assign TIDS for each item.
	 */
	if (maxoff >= FirstOffsetNumber)
	{
		ItemId		iid = PageGetItemId(page, maxoff);
		ZSTidArrayItem *lastitem = (ZSTidArrayItem *) PageGetItem(page, iid);

		endtid = lastitem->t_endtid;
	}
	else
	{
		endtid = opaque->zs_lokey;
	}
	tid = endtid;

	/* Form an undo record */
	if (xid != FrozenTransactionId)
	{
		undorecptr = zsundo_create_for_insert(rel, xid, cid, tid, nitems,
											  speculative_token, prevundoptr);
	}
	else
	{
		undorecptr = InvalidUndoPtr;
	}

	/*
	 * Create a single array item to represent all the TIDs.
	 */
	newitems = zsbt_tid_pack_item(tid, undorecptr, nitems);

	/* recompress and possibly split the page */
	zsbt_tid_add_items(rel, buf, newitems);
	/* zsbt_tid_add_items unlocked 'buf' */
	ReleaseBuffer(buf);

	/* Return the TIDs to the caller */
	for (int i = 0; i < nitems; i++)
		tids[i] = tid + i;
}

TM_Result
zsbt_tid_delete(Relation rel, zstid tid,
				TransactionId xid, CommandId cid,
				Snapshot snapshot, Snapshot crosscheck, bool wait,
				TM_FailureData *hufd, bool changingPart)
{
	ZSUndoRecPtr recent_oldest_undo = zsundo_get_oldest_undo_ptr(rel);
	ZSUndoRecPtr item_undoptr;
	bool		item_isdead;
	TM_Result	result;
	bool		keep_old_undo_ptr = true;
	ZSUndoRecPtr undorecptr;
	OffsetNumber off;
	ZSTidArrayItem *origitem;
	Buffer		buf;
	Page		page;
	zstid		next_tid;
	List	   *newitems = NIL;

	/* Find the item to delete. (It could be compressed) */
	off = zsbt_tid_fetch(rel, tid, &buf, &item_undoptr, &item_isdead);
	if (!OffsetNumberIsValid(off))
	{
		/*
		 * or should this be TM_Invisible? The heapam at least just throws
		 * an error, I think..
		 */
		elog(ERROR, "could not find tuple to delete with TID (%u, %u) in TID tree",
			 ZSTidGetBlockNumber(tid), ZSTidGetOffsetNumber(tid));
	}
	if (item_isdead)
	{
		elog(ERROR, "cannot delete tuple that is already marked DEAD (%u, %u)",
			 ZSTidGetBlockNumber(tid), ZSTidGetOffsetNumber(tid));
	}

	if (snapshot)
	{
		result = zs_SatisfiesUpdate(rel, snapshot, recent_oldest_undo,
									tid, item_undoptr, LockTupleExclusive,
									&keep_old_undo_ptr, hufd, &next_tid);
		if (result != TM_Ok)
		{
			UnlockReleaseBuffer(buf);
			/* FIXME: We should fill TM_FailureData *hufd correctly */
			return result;
		}

		if (crosscheck != InvalidSnapshot && result == TM_Ok)
		{
			/* Perform additional check for transaction-snapshot mode RI updates */
			/* FIXME: dummmy scan */
			ZSTidTreeScan scan;
			TransactionId obsoleting_xid;

			memset(&scan, 0, sizeof(scan));
			scan.rel = rel;
			scan.snapshot = crosscheck;
			scan.recent_oldest_undo = recent_oldest_undo;

			if (!zs_SatisfiesVisibility(&scan, item_undoptr, &obsoleting_xid, NULL))
			{
				UnlockReleaseBuffer(buf);
				/* FIXME: We should fill TM_FailureData *hufd correctly */
				return TM_Updated;
			}
		}
	}

	/* Create UNDO record. */
	undorecptr = zsundo_create_for_delete(rel, xid, cid, tid, changingPart,
										  keep_old_undo_ptr ? item_undoptr : InvalidUndoPtr);

	/* Update the tid with the new UNDO pointer. */
	page = BufferGetPage(buf);
	origitem = (ZSTidArrayItem *) PageGetItem(page, PageGetItemId(page, off));
	newitems = zsbt_tid_item_change_undoptr(origitem, tid, undorecptr,
											recent_oldest_undo);
	zsbt_tid_replace_item(rel, buf, off, newitems);
	list_free_deep(newitems);
	ReleaseBuffer(buf); 	/* zsbt_tid_replace_item unlocked 'buf' */

	return TM_Ok;
}

void
zsbt_find_latest_tid(Relation rel, zstid *tid, Snapshot snapshot)
{
	ZSUndoRecPtr recent_oldest_undo = zsundo_get_oldest_undo_ptr(rel);
	ZSUndoRecPtr item_undoptr;
	bool		item_isdead;
	int			idx;
	Buffer		buf;
	/* Just using meta attribute, we can follow the update chain */
	zstid curr_tid = *tid;

	for(;;)
	{
		zstid next_tid = InvalidZSTid;
		if (curr_tid == InvalidZSTid)
			break;

		/* Find the item */
		idx = zsbt_tid_fetch(rel, curr_tid, &buf, &item_undoptr, &item_isdead);
		if (idx == -1 || item_isdead)
			break;

		if (snapshot)
		{
			/* FIXME: dummmy scan */
			ZSTidTreeScan scan;
			TransactionId obsoleting_xid;

			memset(&scan, 0, sizeof(scan));
			scan.rel = rel;
			scan.snapshot = snapshot;
			scan.recent_oldest_undo = recent_oldest_undo;

			if (zs_SatisfiesVisibility(&scan, item_undoptr,
									   &obsoleting_xid, &next_tid))
			{
				*tid = curr_tid;
			}

			curr_tid = next_tid;
			UnlockReleaseBuffer(buf);
		}
	}
}

/*
 * A new TID is allocated, as we see best and returned to the caller. This
 * function is only called for META attribute btree. Data columns will use the
 * returned tid to insert new items.
 */
TM_Result
zsbt_tid_update(Relation rel, zstid otid,
				TransactionId xid, CommandId cid, bool key_update, Snapshot snapshot,
				Snapshot crosscheck, bool wait, TM_FailureData *hufd,
				zstid *newtid_p)
{
	TM_Result	result;
	ZSUndoRecPtr prevundoptr;

	/*
	 * This is currently only used on the meta-attribute. The other attributes
	 * don't need to carry visibility information, so the caller just inserts
	 * the new values with (multi_)insert() instead. This will change once we
	 * start doing the equivalent of HOT updates, where the TID doesn't change.
	 */
	Assert(*newtid_p == InvalidZSTid);

	/*
	 * Find and lock the old item.
	 *
	 * TODO: If there's free TID space left on the same page, we should keep the
	 * buffer locked, and use the same page for the new tuple.
	 */
	result = zsbt_tid_update_lock_old(rel, otid,
									  xid, cid, key_update, snapshot,
									  crosscheck, wait, hufd, &prevundoptr);

	if (result != TM_Ok)
		return result;

	/* insert new version */
	zsbt_tid_update_insert_new(rel, newtid_p, xid, cid, prevundoptr);

	/* update the old item with the "t_ctid pointer" for the new item */
	zsbt_tid_mark_old_updated(rel, otid, *newtid_p, xid, cid, key_update, snapshot);

	return TM_Ok;
}

/*
 * Subroutine of zsbt_update(): locks the old item for update.
 */
static TM_Result
zsbt_tid_update_lock_old(Relation rel, zstid otid,
					 TransactionId xid, CommandId cid, bool key_update, Snapshot snapshot,
					 Snapshot crosscheck, bool wait, TM_FailureData *hufd, ZSUndoRecPtr *prevundoptr_p)
{
	ZSUndoRecPtr recent_oldest_undo = zsundo_get_oldest_undo_ptr(rel);
	Buffer		buf;
	ZSUndoRecPtr olditem_undoptr;
	bool		olditem_isdead;
	int			idx;
	TM_Result	result;
	bool		keep_old_undo_ptr = true;
	zstid		next_tid;

	/*
	 * Find the item to delete.
	 */
	idx = zsbt_tid_fetch(rel, otid, &buf, &olditem_undoptr, &olditem_isdead);
	if (idx == -1 || olditem_isdead)
	{
		/*
		 * or should this be TM_Invisible? The heapam at least just throws
		 * an error, I think..
		 */
		elog(ERROR, "could not find old tuple to update with TID (%u, %u) in TID tree",
			 ZSTidGetBlockNumber(otid), ZSTidGetOffsetNumber(otid));
	}
	*prevundoptr_p = olditem_undoptr;

	/*
	 * Is it visible to us?
	 */
	result = zs_SatisfiesUpdate(rel, snapshot, recent_oldest_undo,
								otid, olditem_undoptr,
								key_update ? LockTupleExclusive : LockTupleNoKeyExclusive,
								&keep_old_undo_ptr, hufd, &next_tid);
	if (result != TM_Ok)
	{
		UnlockReleaseBuffer(buf);
		/* FIXME: We should fill TM_FailureData *hufd correctly */
		return result;
	}

	if (crosscheck != InvalidSnapshot && result == TM_Ok)
	{
		/* Perform additional check for transaction-snapshot mode RI updates */
		/* FIXME: dummmy scan */
		ZSTidTreeScan scan;
		TransactionId obsoleting_xid;

		memset(&scan, 0, sizeof(scan));
		scan.rel = rel;
		scan.snapshot = crosscheck;
		scan.recent_oldest_undo = recent_oldest_undo;

		if (!zs_SatisfiesVisibility(&scan, olditem_undoptr, &obsoleting_xid, NULL))
		{
			UnlockReleaseBuffer(buf);
			/* FIXME: We should fill TM_FailureData *hufd correctly */
			result = TM_Updated;
		}
	}

	/*
	 * TODO: tuple-locking not implemented. Pray that there is no competing
	 * concurrent update!
	 */

	UnlockReleaseBuffer(buf);

	return TM_Ok;
}

/*
 * Subroutine of zsbt_update(): inserts the new, updated, item.
 */
static void
zsbt_tid_update_insert_new(Relation rel,
					   zstid *newtid,
					   TransactionId xid, CommandId cid, ZSUndoRecPtr prevundoptr)
{
	zsbt_tid_multi_insert(rel, newtid, 1, xid, cid, INVALID_SPECULATIVE_TOKEN, prevundoptr);
}

/*
 * Subroutine of zsbt_update(): mark old item as updated.
 */
static void
zsbt_tid_mark_old_updated(Relation rel, zstid otid, zstid newtid,
					  TransactionId xid, CommandId cid, bool key_update, Snapshot snapshot)
{
	ZSUndoRecPtr recent_oldest_undo = zsundo_get_oldest_undo_ptr(rel);
	Buffer		buf;
	Page		page;
	ZSUndoRecPtr olditem_undoptr;
	bool		olditem_isdead;
	OffsetNumber off;
	TM_Result	result;
	bool		keep_old_undo_ptr = true;
	TM_FailureData tmfd;
	ZSUndoRecPtr undorecptr;
	List	   *newitems;
	ZSTidArrayItem *origitem;
	zstid		next_tid;

	/*
	 * Find the item to delete.  It could be part of a compressed item,
	 * we let zsbt_fetch() handle that.
	 */
	off = zsbt_tid_fetch(rel, otid, &buf, &olditem_undoptr, &olditem_isdead);
	if (!OffsetNumberIsValid(off) || olditem_isdead)
	{
		/*
		 * or should this be TM_Invisible? The heapam at least just throws
		 * an error, I think..
		 */
		elog(ERROR, "could not find old tuple to update with TID (%u, %u) in TID tree",
			 ZSTidGetBlockNumber(otid), ZSTidGetOffsetNumber(otid));
	}

	/*
	 * Is it visible to us?
	 */
	result = zs_SatisfiesUpdate(rel, snapshot, recent_oldest_undo,
								otid, olditem_undoptr,
								key_update ? LockTupleExclusive : LockTupleNoKeyExclusive,
								&keep_old_undo_ptr, &tmfd, &next_tid);
	if (result != TM_Ok)
	{
		UnlockReleaseBuffer(buf);
		elog(ERROR, "tuple concurrently updated - not implemented");
	}

	/* Create UNDO record. */
	{
		ZSUndoRec_Update undorec;

		undorec.rec.size = sizeof(ZSUndoRec_Update);
		undorec.rec.type = ZSUNDO_TYPE_UPDATE;
		undorec.rec.xid = xid;
		undorec.rec.cid = cid;
		undorec.oldtid = otid;
		undorec.newtid = newtid;
		if (keep_old_undo_ptr)
			undorec.rec.prevundorec = olditem_undoptr;
		else
			undorec.rec.prevundorec = InvalidUndoPtr;
		undorec.key_update = key_update;

		undorecptr = zsundo_insert(rel, &undorec.rec);
	}

	/* Replace the ZSBreeItem with one with the updated undo pointer. */
	page = BufferGetPage(buf);
	origitem = (ZSTidArrayItem *) PageGetItem(page, PageGetItemId(page, off));
	newitems = zsbt_tid_item_change_undoptr(origitem, otid, undorecptr,
											recent_oldest_undo);
	zsbt_tid_replace_item(rel, buf, off, newitems);
	list_free_deep(newitems);
	ReleaseBuffer(buf); 	/* zsbt_tid_replace_item unlocked 'buf' */
}

TM_Result
zsbt_tid_lock(Relation rel, zstid tid,
			   TransactionId xid, CommandId cid,
			   LockTupleMode mode, Snapshot snapshot,
			   TM_FailureData *hufd, zstid *next_tid)
{
	ZSUndoRecPtr recent_oldest_undo = zsundo_get_oldest_undo_ptr(rel);
	Buffer		buf;
	Page		page;
	ZSUndoRecPtr item_undoptr;
	bool		item_isdead;
	OffsetNumber off;
	TM_Result	result;
	bool		keep_old_undo_ptr = true;
	ZSUndoRecPtr undorecptr;
	List	   *newitems;
	ZSTidArrayItem *origitem;

	*next_tid = tid;

	/* Find the item to delete. (It could be compressed) */
	off = zsbt_tid_fetch(rel, tid, &buf, &item_undoptr, &item_isdead);
	if (!OffsetNumberIsValid(off) || item_isdead)
	{
		/*
		 * or should this be TM_Invisible? The heapam at least just throws
		 * an error, I think..
		 */
		elog(ERROR, "could not find tuple to lock with TID (%u, %u)",
			 ZSTidGetBlockNumber(tid), ZSTidGetOffsetNumber(tid));
	}
	result = zs_SatisfiesUpdate(rel, snapshot, recent_oldest_undo,
								tid, item_undoptr, mode,
								&keep_old_undo_ptr, hufd, next_tid);
	if (result != TM_Ok)
	{
		UnlockReleaseBuffer(buf);
		return result;
	}

	/* Create UNDO record. */
	{
		ZSUndoRec_TupleLock undorec;

		undorec.rec.size = sizeof(ZSUndoRec_TupleLock);
		undorec.rec.type = ZSUNDO_TYPE_TUPLE_LOCK;
		undorec.rec.xid = xid;
		undorec.rec.cid = cid;
		undorec.tid = tid;
		undorec.lockmode = mode;
		if (keep_old_undo_ptr)
			undorec.rec.prevundorec = item_undoptr;
		else
			undorec.rec.prevundorec = InvalidUndoPtr;

		undorecptr = zsundo_insert(rel, &undorec.rec);
	}

	/* Replace the item with an identical one, but with updated undo pointer. */
	page = BufferGetPage(buf);
	origitem = (ZSTidArrayItem *) PageGetItem(page, PageGetItemId(page, off));
	newitems = zsbt_tid_item_change_undoptr(origitem, tid, undorecptr,
											recent_oldest_undo);
	zsbt_tid_replace_item(rel, buf, off, newitems);
	list_free_deep(newitems);
	ReleaseBuffer(buf); 	/* zsbt_tid_replace_item unlocked 'buf' */

	return TM_Ok;
}

/*
 * Collect all TIDs marked as dead in the TID tree.
 *
 * This is used during VACUUM.
 */
IntegerSet *
zsbt_collect_dead_tids(Relation rel, zstid starttid, zstid *endtid)
{
	Buffer		buf = InvalidBuffer;
	IntegerSet *result;
	ZSBtreePageOpaque *opaque;
	zstid		nexttid;
	BlockNumber	nextblock;
	ZSTidItemIterator iter;

	memset(&iter, 0, sizeof(ZSTidItemIterator));
	iter.context = CurrentMemoryContext;

	result = intset_create();

	nexttid = starttid;
	nextblock = InvalidBlockNumber;
	for (;;)
	{
		Page		page;
		OffsetNumber maxoff;
		OffsetNumber off;

		if (nextblock != InvalidBlockNumber)
		{
			buf = ReleaseAndReadBuffer(buf, rel, nextblock);
			LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
			page = BufferGetPage(buf);

			if (!zsbt_page_is_expected(rel, ZS_META_ATTRIBUTE_NUM, nexttid, 0, buf))
			{
				UnlockReleaseBuffer(buf);
				buf = InvalidBuffer;
			}
		}

		if (!BufferIsValid(buf))
		{
			buf = zsbt_descend(rel, ZS_META_ATTRIBUTE_NUM, nexttid, 0, true);
			if (!BufferIsValid(buf))
				return result;
			page = BufferGetPage(buf);
		}

		maxoff = PageGetMaxOffsetNumber(page);
		for (off = FirstOffsetNumber; off <= maxoff; off++)
		{
			ItemId		iid = PageGetItemId(page, off);
			ZSTidArrayItem *item = (ZSTidArrayItem *) PageGetItem(page, iid);

			zsbt_tid_item_unpack(item, &iter);

			for (int j = 0; j < iter.num_tids; j++)
			{
				if (iter.tid_undoslotnos[j] == 0)
					intset_add_member(result, iter.tids[j]);
			}
		}

		opaque = ZSBtreePageGetOpaque(page);
		nexttid = opaque->zs_hikey;
		nextblock = opaque->zs_next;

		LockBuffer(buf, BUFFER_LOCK_UNLOCK);

		if (nexttid == MaxPlusOneZSTid)
		{
			Assert(nextblock == InvalidBlockNumber);
			break;
		}

		if (intset_memory_usage(result) > (uint64) maintenance_work_mem * 1024)
			break;
	}

	if (BufferIsValid(buf))
		ReleaseBuffer(buf);

	*endtid = nexttid;
	return result;
}

/*
 * Mark item with given TID as dead.
 *
 * This is used when UNDO actions are performed, after a transaction becomes
 * old enough.
 */
void
zsbt_tid_mark_dead(Relation rel, zstid tid, ZSUndoRecPtr recent_oldest_undo)
{
	Buffer		buf;
	Page		page;
	ZSUndoRecPtr item_undoptr;
	OffsetNumber off;
	ZSTidArrayItem *origitem;
	List	   *newitems;
	bool		isdead;

	/* Find the item to delete. (It could be compressed) */
	off = zsbt_tid_fetch(rel, tid, &buf, &item_undoptr, &isdead);
	if (!OffsetNumberIsValid(off))
	{
		elog(WARNING, "could not find tuple to mark dead with TID (%u, %u)",
			 ZSTidGetBlockNumber(tid), ZSTidGetOffsetNumber(tid));
		return;
	}

	/* Replace the ZSBreeItem with a DEAD item. (Unless it's already dead) */
	if (isdead)
	{
		UnlockReleaseBuffer(buf);
		return;
	}

	page = BufferGetPage(buf);
	origitem = (ZSTidArrayItem *) PageGetItem(page, PageGetItemId(page, off));
	newitems = zsbt_tid_item_change_undoptr(origitem, tid, DeadUndoPtr,
											recent_oldest_undo);
	zsbt_tid_replace_item(rel, buf, off, newitems);
	list_free_deep(newitems);
	ReleaseBuffer(buf); 	/* zsbt_tid_replace_item unlocked 'buf' */
}


/*
 * Remove items for the given TIDs from the TID tree.
 *
 * This is used during VACUUM.
 */
void
zsbt_tid_remove(Relation rel, IntegerSet *tids)
{
	ZSUndoRecPtr recent_oldest_undo = zsundo_get_oldest_undo_ptr(rel);
	zstid		nexttid;

	intset_begin_iterate(tids);
	if (!intset_iterate_next(tids, &nexttid))
		nexttid = MaxPlusOneZSTid;

	while (nexttid < MaxPlusOneZSTid)
	{
		Buffer		buf;
		Page		page;
		ZSBtreePageOpaque *opaque;
		List	   *newitems;
		OffsetNumber maxoff;
		OffsetNumber off;

		/*
		 * Find the leaf page containing the next item to remove
		 */
		buf = zsbt_descend(rel, ZS_META_ATTRIBUTE_NUM, nexttid, 0, false);
		page = BufferGetPage(buf);
		opaque = ZSBtreePageGetOpaque(page);

		/*
		 * Rewrite the items on the page, removing all TIDs that need to be
		 * removed from the page.
		 */
		newitems = NIL;
		maxoff = PageGetMaxOffsetNumber(page);
		for (off = FirstOffsetNumber; off <= maxoff; off++)
		{
			ItemId		iid = PageGetItemId(page, off);
			ZSTidArrayItem *item = (ZSTidArrayItem *) PageGetItem(page, iid);

			while (nexttid < item->t_firsttid)
			{
				if (!intset_iterate_next(tids, &nexttid))
					nexttid = MaxPlusOneZSTid;
			}

			if (nexttid < item->t_endtid)
			{
				List		*newitemsx = zsbt_tid_item_remove_tids(item, &nexttid, tids,
																   recent_oldest_undo);

				newitems = list_concat(newitems, newitemsx);
			}
			else
			{
				/* keep this item unmodified */
				newitems = lappend(newitems, item);
			}
		}

		while (nexttid < opaque->zs_hikey)
		{
			if (!intset_iterate_next(tids, &nexttid))
				nexttid = MaxPlusOneZSTid;
		}

		/* Pass the list to the recompressor. */
		IncrBufferRefCount(buf);
		if (newitems)
		{
			zsbt_tid_recompress_replace(rel, buf, newitems);
		}
		else
		{
			zs_split_stack *stack;

			stack = zsbt_unlink_page(rel, ZS_META_ATTRIBUTE_NUM, buf, 0);

			if (!stack)
			{
				/* failed. */
				Page		newpage = PageGetTempPageCopySpecial(BufferGetPage(buf));

				stack = zs_new_split_stack_entry(buf, newpage);
			}

			/* apply the changes */
			zs_apply_split_changes(rel, stack);
		}

		list_free(newitems);

		ReleaseBuffer(buf);
	}
}

/*
 * Clear an item's UNDO pointer.
 *
 * This is used during VACUUM, to clear out aborted deletions.
 */
void
zsbt_tid_undo_deletion(Relation rel, zstid tid, ZSUndoRecPtr undoptr,
					   ZSUndoRecPtr recent_oldest_undo)
{
	Buffer		buf;
	Page		page;
	ZSUndoRecPtr item_undoptr;
	bool		item_isdead;
	OffsetNumber off;

	/* Find the item to delete. (It could be compressed) */
	off = zsbt_tid_fetch(rel, tid, &buf, &item_undoptr, &item_isdead);
	if (!OffsetNumberIsValid(off))
	{
		elog(WARNING, "could not find aborted tuple to remove with TID (%u, %u)",
			 ZSTidGetBlockNumber(tid), ZSTidGetOffsetNumber(tid));
		return;
	}

	if (ZSUndoRecPtrEquals(item_undoptr, undoptr))
	{
		ZSTidArrayItem *origitem;
		List	   *newitems;

		/* FIXME: we're overwriting the undo pointer with 'invalid', meaning the
		 * tuple becomes visible to everyone. That doesn't seem right. Shouldn't
		 * we restore the previous undo pointer, if the insertion was not yet
		 * visible to everyone?
		 */
		page = BufferGetPage(buf);
		origitem = (ZSTidArrayItem *) PageGetItem(page, PageGetItemId(page, off));
		newitems = zsbt_tid_item_change_undoptr(origitem, tid, InvalidUndoPtr,
												recent_oldest_undo);
		zsbt_tid_replace_item(rel, buf, off, newitems);
		list_free_deep(newitems);
		ReleaseBuffer(buf); 	/* zsbt_tid_replace_item unlocked 'buf' */
	}
	else
	{
		Assert(item_isdead ||
			   item_undoptr.counter > undoptr.counter ||
			   !IsZSUndoRecPtrValid(&item_undoptr));
		UnlockReleaseBuffer(buf);
	}
}

/* ----------------------------------------------------------------
 *						 Internal routines
 * ----------------------------------------------------------------
 */

void
zsbt_tid_clear_speculative_token(Relation rel, zstid tid, uint32 spectoken, bool forcomplete)
{
	Buffer		buf;
	ZSUndoRecPtr item_undoptr;
	bool		item_isdead;
	bool		found;

	found = zsbt_tid_fetch(rel, tid, &buf, &item_undoptr, &item_isdead);
	if (!found || item_isdead)
		elog(ERROR, "couldn't find item for meta column for inserted tuple with TID (%u, %u) in rel %s",
			 ZSTidGetBlockNumber(tid), ZSTidGetOffsetNumber(tid), rel->rd_rel->relname.data);

	zsundo_clear_speculative_token(rel, item_undoptr);

	UnlockReleaseBuffer(buf);
}

/*
 * Fetch the item with given TID. The page containing the item is kept locked, and
 * returned to the caller in *buf_p. This is used to locate a tuple for updating
 * or deleting it.
 */
static OffsetNumber
zsbt_tid_fetch(Relation rel, zstid tid, Buffer *buf_p, ZSUndoRecPtr *undoptr_p, bool *isdead_p)
{
	Buffer		buf;
	Page		page;
	OffsetNumber maxoff;
	OffsetNumber off;

	buf = zsbt_descend(rel, ZS_META_ATTRIBUTE_NUM, tid, 0, false);
	if (buf == InvalidBuffer)
	{
		*buf_p = InvalidBuffer;
		*undoptr_p = InvalidUndoPtr;
		return InvalidOffsetNumber;
	}
	page = BufferGetPage(buf);
	maxoff = PageGetMaxOffsetNumber(page);

	/* Find the item on the page that covers the target TID */
	off = zsbt_binsrch_tidpage(tid, page);
	if (off >= FirstOffsetNumber && off <= maxoff)
	{
		ItemId		iid = PageGetItemId(page, off);
		ZSTidArrayItem *item = (ZSTidArrayItem *) PageGetItem(page, iid);

		if (tid < item->t_endtid)
		{
			ZSTidItemIterator iter;

			memset(&iter, 0, sizeof(ZSTidItemIterator));
			iter.context = CurrentMemoryContext;

			zsbt_tid_item_unpack(item, &iter);

			/* TODO: could do binary search here. Better yet, integrate the
			 * unpack function with the callers */
			for (int i = 0; i < iter.num_tids; i++)
			{
				if (iter.tids[i] == tid)
				{
					int			slotno = iter.tid_undoslotnos[i];
					ZSUndoRecPtr undoptr = iter.undoslots[slotno];

					*isdead_p = (slotno == ZSBT_DEAD_UNDO_SLOT);
					*undoptr_p = undoptr;
					*buf_p = buf;

					if (iter.tids)
						pfree(iter.tids);
					if (iter.tid_undoslotnos)
						pfree(iter.tid_undoslotnos);

					return off;
				}
			}

			if (iter.tids)
				pfree(iter.tids);
			if (iter.tid_undoslotnos)
				pfree(iter.tid_undoslotnos);
		}
	}
	return InvalidOffsetNumber;
}

/*
 * This helper function is used to implement INSERT.
 *
 * The items in 'newitems' are added to the page, to the correct position.
 * FIXME: Actually, they're always just added to the end of the page, and that
 * better be the correct position.
 *
 * This function handles splitting the page if needed.
 */
static void
zsbt_tid_add_items(Relation rel, Buffer buf, List *newitems)
{
	Page		page = BufferGetPage(buf);
	OffsetNumber maxoff = PageGetMaxOffsetNumber(page);
	OffsetNumber off;
	Size		newitemsize;
	ListCell   *lc;

	newitemsize = 0;
	foreach(lc, newitems)
	{
		ZSTidArrayItem *item = (ZSTidArrayItem *) lfirst(lc);

		newitemsize += sizeof(ItemIdData) + item->t_size;
	}

	if (newitemsize <= PageGetExactFreeSpace(page))
	{
		/* The new items fit on the page. Add them. */

		START_CRIT_SECTION();

		off = maxoff;
		foreach(lc, newitems)
		{
			ZSTidArrayItem *item = (ZSTidArrayItem *) lfirst(lc);

			off++;
			if (!PageAddItem(page, (Item) item, item->t_size, off, true, false))
				elog(ERROR, "could not add item to TID tree page");
		}

		MarkBufferDirty(buf);

		/* TODO: WAL-log */

		END_CRIT_SECTION();

		LockBuffer(buf, BUFFER_LOCK_UNLOCK);
	}
	else
	{
		List	   *items = NIL;

		/* Collect all the old items on the page to a list */
		for (off = FirstOffsetNumber; off <= maxoff; off++)
		{
			ItemId		iid = PageGetItemId(page, off);
			ZSTidArrayItem *item = (ZSTidArrayItem *) PageGetItem(page, iid);

			/*
			 * Get the next item to process from the page.
			 */
			items = lappend(items, item);
		}

		/* Add any new items to the end */
		if (newitems)
			items = list_concat(items, newitems);

		/* Now pass the list to the recompressor. */
		IncrBufferRefCount(buf);
		if (items)
		{
			zsbt_tid_recompress_replace(rel, buf, items);
		}
		else
		{
			zs_split_stack *stack;

			stack = zsbt_unlink_page(rel, ZS_META_ATTRIBUTE_NUM, buf, 0);

			if (!stack)
			{
				/* failed. */
				Page		newpage = PageGetTempPageCopySpecial(BufferGetPage(buf));

				stack = zs_new_split_stack_entry(buf, newpage);
			}

			/* apply the changes */
			zs_apply_split_changes(rel, stack);
		}

		list_free(items);
	}
}


/*
 * This helper function is used to implement INSERT, UPDATE and DELETE.
 *
 * If 'newitems' is not empty, the items in the list are added to the page,
 * to the correct position. FIXME: Actually, they're always just added to
 * the end of the page, and that better be the correct position.
 *
 * This function handles decompressing and recompressing items, and splitting
 * the page if needed.
 */
static void
zsbt_tid_replace_item(Relation rel, Buffer buf, OffsetNumber targetoff, List *newitems)
{
	Page		page = BufferGetPage(buf);
	ItemId		iid;
	ZSTidArrayItem *olditem;
	ListCell   *lc;
	ssize_t		sizediff;

	/*
	 * Find the item that covers the given tid.
	 */
	if (targetoff < FirstOffsetNumber || targetoff > PageGetMaxOffsetNumber(page))
		elog(ERROR, "could not find item at off %d to replace", targetoff);
	iid = PageGetItemId(page, targetoff);
	olditem = (ZSTidArrayItem *) PageGetItem(page, iid);

	/* Calculate how much free space we'll need */
	sizediff = -(olditem->t_size + sizeof(ItemIdData));
	foreach(lc, newitems)
	{
		ZSTidArrayItem *newitem = (ZSTidArrayItem *) lfirst(lc);

		sizediff += newitem->t_size + sizeof(ItemIdData);
	}

	/* Can we fit them? */
	if (sizediff <= PageGetExactFreeSpace(page))
	{
		ZSTidArrayItem *newitem;
		OffsetNumber off;

		START_CRIT_SECTION();

		/* Remove existing item, and add new ones */
		if (newitems == 0)
			PageIndexTupleDelete(page, targetoff);
		else
		{
			lc = list_head(newitems);
			newitem = (ZSTidArrayItem *) lfirst(lc);
			if (!PageIndexTupleOverwrite(page, targetoff, (Item) newitem, newitem->t_size))
				elog(ERROR, "could not replace item in TID tree page at off %d", targetoff);
			lc = lnext(lc);

			off = targetoff + 1;
			for (; lc != NULL; lc = lnext(lc))
			{
				newitem = (ZSTidArrayItem *) lfirst(lc);
				if (!PageAddItem(page, (Item) newitem, newitem->t_size, off, false, false))
					elog(ERROR, "could not add item in TID tree page at off %d", off);
				off++;
			}
		}

		MarkBufferDirty(buf);
		/* TODO: WAL-log */
		END_CRIT_SECTION();

#ifdef USE_ASSERT_CHECKING
		{
			zstid		lasttid = 0;
			OffsetNumber off;

			for (off = FirstOffsetNumber; off <= PageGetMaxOffsetNumber(page); off++)
			{
				ItemId		iid = PageGetItemId(page, off);
				ZSTidArrayItem *item = (ZSTidArrayItem *) PageGetItem(page, iid);

				Assert(item->t_firsttid >= lasttid);
				lasttid = item->t_endtid;
			}
		}
#endif

		LockBuffer(buf, BUFFER_LOCK_UNLOCK);
	}
	else
	{
		/* Have to split the page. */
		List	   *items = NIL;
		OffsetNumber maxoff = PageGetMaxOffsetNumber(page);
		OffsetNumber off;

		/*
		 * Construct a List that contains all the items in the right order, and
		 * let zsbt_tid_recompress_page() do the heavy lifting to fit them on
		 * pages.
		 */
		for (off = FirstOffsetNumber; off <= maxoff; off++)
		{
			ItemId		iid = PageGetItemId(page, off);
			ZSTidArrayItem *item = (ZSTidArrayItem *) PageGetItem(page, iid);

			if (off == targetoff)
			{
				foreach(lc, newitems)
				{
					items = lappend(items, (ZSTidArrayItem *) lfirst(lc));
				}
			}
			else
				items = lappend(items, item);
		}

#ifdef USE_ASSERT_CHECKING
		{
			zstid endtid = 0;
			ListCell *lc;

			foreach (lc, items)
			{
				ZSTidArrayItem *i = (ZSTidArrayItem *) lfirst(lc);

				Assert(i->t_firsttid >= endtid);
				Assert(i->t_endtid > i->t_firsttid);
				endtid = i->t_endtid;
			}
		}
#endif

		/* Pass the list to the recompressor. */
		IncrBufferRefCount(buf);
		if (items)
		{
			zsbt_tid_recompress_replace(rel, buf, items);
		}
		else
		{
			zs_split_stack *stack;

			stack = zsbt_unlink_page(rel, ZS_META_ATTRIBUTE_NUM, buf, 0);

			if (!stack)
			{
				/* failed. */
				Page		newpage = PageGetTempPageCopySpecial(BufferGetPage(buf));

				stack = zs_new_split_stack_entry(buf, newpage);
			}

			/* apply the changes */
			zs_apply_split_changes(rel, stack);
		}

		list_free(items);
	}
}

/*
 * Recompressor routines
 */
typedef struct
{
	Page		currpage;

	/* first page writes over the old buffer, subsequent pages get newly-allocated buffers */
	zs_split_stack *stack_head;
	zs_split_stack *stack_tail;

	int			num_pages;
	int			free_space_per_page;

	zstid		hikey;
} zsbt_tid_recompress_context;

static void
zsbt_tid_recompress_newpage(zsbt_tid_recompress_context *cxt, zstid nexttid, int flags)
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
	newopaque->zs_attno = ZS_META_ATTRIBUTE_NUM;
	newopaque->zs_next = InvalidBlockNumber; /* filled in later */
	newopaque->zs_lokey = nexttid;
	newopaque->zs_hikey = cxt->hikey;		/* overwritten later, if this is not last page */
	newopaque->zs_level = 0;
	newopaque->zs_flags = flags;
	newopaque->zs_page_id = ZS_BTREE_PAGE_ID;
}

static void
zsbt_tid_recompress_add_to_page(zsbt_tid_recompress_context *cxt, ZSTidArrayItem *item)
{
	OffsetNumber maxoff;
	Size		freespc;

	freespc = PageGetExactFreeSpace(cxt->currpage);
	if (freespc < item->t_size + sizeof(ItemIdData) ||
		freespc < cxt->free_space_per_page)
	{
		zsbt_tid_recompress_newpage(cxt, item->t_firsttid, 0);
	}

	maxoff = PageGetMaxOffsetNumber(cxt->currpage);
	if (!PageAddItem(cxt->currpage, (Item) item, item->t_size, maxoff + 1, true, false))
		elog(ERROR, "could not add item to TID tree page");
}

/*
 * Subroutine of zsbt_tid_recompress_replace.  Compute how much space the
 * items will take, and compute how many pages will be needed for them, and
 * decide how to distribute any free space thats's left over among the
 * pages.
 *
 * Like in B-tree indexes, we aim for 50/50 splits, except for the
 * rightmost page where aim for 90/10, so that most of the free space is
 * left to the end of the index, where it's useful for new inserts. The
 * 90/10 splits ensure that the we don't waste too much space on a table
 * that's loaded at the end, and never updated.
 */
static void
zsbt_tid_recompress_picksplit(zsbt_tid_recompress_context *cxt, List *items)
{
	size_t		total_sz;
	int			num_pages;
	int			space_on_empty_page;
	Size		free_space_per_page;
	ListCell   *lc;

	space_on_empty_page = BLCKSZ - MAXALIGN(SizeOfPageHeaderData) - MAXALIGN(sizeof(ZSBtreePageOpaque));

	/* Compute total space needed for all the items. */
	total_sz = 0;
	foreach(lc, items)
	{
		ZSTidArrayItem *item = lfirst(lc);

		total_sz += sizeof(ItemIdData) + item->t_size;
	}

	/* How many pages will we need for them? */
	num_pages = (total_sz + space_on_empty_page - 1) / space_on_empty_page;

	/* If everything fits on one page, don't split */
	if (num_pages == 1)
	{
		free_space_per_page = 0;
	}
	/* If this is the rightmost page, do a 90/10 split */
	else if (cxt->hikey == MaxPlusOneZSTid)
	{
		/*
		 * What does 90/10 mean if we have to use more than two pages? It means
		 * that 10% of the items go to the last page, and 90% are distributed to
		 * all the others.
		 */
		double		total_free_space;

		total_free_space = space_on_empty_page * num_pages - total_sz;

		free_space_per_page = total_free_space * 0.1 / (num_pages - 1);
	}
	/* Otherwise, aim for an even 50/50 split */
	else
	{
		free_space_per_page = (space_on_empty_page * num_pages - total_sz) / num_pages;
	}

	cxt->num_pages = num_pages;
	cxt->free_space_per_page = free_space_per_page;
}

/*
 * Rewrite a leaf page, with given 'items' as the new content.
 *
 * If there are any uncompressed items in the list, we try to compress them.
 * Any already-compressed items are added as is.
 *
 * If the items no longer fit on the page, then the page is split. It is
 * entirely possible that they don't fit even on two pages; we split the page
 * into as many pages as needed. Hopefully not more than a few pages, though,
 * because otherwise you might hit limits on the number of buffer pins (with
 * tiny shared_buffers).
 *
 * On entry, 'oldbuf' must be pinned and exclusive-locked. On exit, the lock
 * is released, but it's still pinned.
 *
 * TODO: Try to combine single items, and existing array-items, into new array
 * items.
 */
static void
zsbt_tid_recompress_replace(Relation rel, Buffer oldbuf, List *items)
{
	ListCell   *lc;
	zsbt_tid_recompress_context cxt;
	ZSBtreePageOpaque *oldopaque = ZSBtreePageGetOpaque(BufferGetPage(oldbuf));
	BlockNumber orignextblk;
	zs_split_stack *stack;
	List	   *downlinks = NIL;

	orignextblk = oldopaque->zs_next;

	cxt.currpage = NULL;
	cxt.stack_head = cxt.stack_tail = NULL;
	cxt.hikey = oldopaque->zs_hikey;

	zsbt_tid_recompress_picksplit(&cxt, items);
	zsbt_tid_recompress_newpage(&cxt, oldopaque->zs_lokey, (oldopaque->zs_flags & ZSBT_ROOT));

	foreach(lc, items)
	{
		ZSTidArrayItem *item = (ZSTidArrayItem *) lfirst(lc);

		zsbt_tid_recompress_add_to_page(&cxt, item);
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

		thisopaque->zs_next = BufferGetBlockNumber(nextbuf);

		downlink = palloc(sizeof(ZSBtreeInternalPageItem));
		downlink->tid = thisopaque->zs_hikey;
		downlink->childblk = BufferGetBlockNumber(nextbuf);
		downlinks = lappend(downlinks, downlink);

		stack = stack->next;
	}
	/* last one in the chain */
	ZSBtreePageGetOpaque(stack->page)->zs_next = orignextblk;

	/*
	 * zsbt_tid_recompress_picksplit() calculated that we'd need
	 * 'cxt.num_pages' pages. Check that it matches with how many pages we
	 * actually created.
	 */
	Assert(list_length(downlinks) + 1 == cxt.num_pages);

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

			cxt.stack_tail->next = zsbt_newroot(rel, ZS_META_ATTRIBUTE_NUM,
												oldopaque->zs_level + 1, downlinks);

			/* clear the ZSBT_ROOT flag on the old root page */
			oldopaque->zs_flags &= ~ZSBT_ROOT;
		}
		else
		{
			cxt.stack_tail->next = zsbt_insert_downlinks(rel, ZS_META_ATTRIBUTE_NUM,
														 oldopaque->zs_lokey, BufferGetBlockNumber(oldbuf), oldopaque->zs_level + 1,
														 downlinks);
		}
		/* note: stack_tail is not the real tail anymore */
	}

	/* Finally, overwrite all the pages we had to modify */
	zs_apply_split_changes(rel, cxt.stack_head);
}

static OffsetNumber
zsbt_binsrch_tidpage(zstid key, Page page)
{
	OffsetNumber maxoff = PageGetMaxOffsetNumber(page);
	OffsetNumber low,
		high,
		mid;

	low = FirstOffsetNumber;
	high = maxoff + 1;
	while (high > low)
	{
		ItemId		iid;
		ZSTidArrayItem *item;

		mid = low + (high - low) / 2;

		iid = PageGetItemId(page, mid);
		item = (ZSTidArrayItem *) PageGetItem(page, iid);

		if (key >= item->t_firsttid)
			low = mid + 1;
		else
			high = mid;
	}
	return low - 1;
}
