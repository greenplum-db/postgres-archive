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

#include "access/tableam.h"
#include "access/xact.h"
#include "access/zedstore_compression.h"
#include "access/zedstore_internal.h"
#include "access/zedstore_undo.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/predicate.h"
#include "storage/procarray.h"
#include "utils/datum.h"
#include "utils/rel.h"

/* prototypes for local functions */
static void zsbt_tid_recompress_replace(Relation rel, Buffer oldbuf, List *items);
static ZSSingleBtreeItem *zsbt_tid_fetch(Relation rel,
		   ZSUndoRecPtr *recent_oldest_undo, zstid tid, Buffer *buf_p);
static void zsbt_tid_replace_item(Relation rel, Buffer buf,
								  zstid oldtid, ZSBtreeItem *replacementitem,
								  List *newitems);
static ZSBtreeItem *zsbt_tid_create_item(zstid tid, ZSUndoRecPtr undo_ptr, int nelements);

static TM_Result zsbt_tid_update_lock_old(Relation rel, zstid otid,
									  TransactionId xid, CommandId cid, bool key_update, Snapshot snapshot,
									  Snapshot crosscheck, bool wait, TM_FailureData *hufd, ZSUndoRecPtr *prevundoptr_p);
static void zsbt_tid_update_insert_new(Relation rel, zstid *newtid,
					   TransactionId xid, CommandId cid, ZSUndoRecPtr prevundoptr);
static void zsbt_tid_mark_old_updated(Relation rel, zstid otid, zstid newtid,
					  TransactionId xid, CommandId cid, bool key_update, Snapshot snapshot);

/* ----------------------------------------------------------------
 *						 Public interface
 * ----------------------------------------------------------------
 */

/*
 * Begin a scan of the btree.
 */
void
zsbt_tid_begin_scan(Relation rel, zstid starttid,
					zstid endtid, Snapshot snapshot, ZSBtreeScan *scan)
{
	Buffer		buf;

	scan->rel = rel;
	scan->attno = ZS_META_ATTRIBUTE_NUM;
	scan->tupledesc = NULL;

	scan->snapshot = snapshot;
	scan->context = CurrentMemoryContext;
	scan->lastoff = InvalidOffsetNumber;
	scan->has_decompressed = false;
	scan->nexttid = starttid;
	scan->endtid = endtid;
	memset(&scan->recent_oldest_undo, 0, sizeof(scan->recent_oldest_undo));
	memset(&scan->array_undoptr, 0, sizeof(scan->array_undoptr));
	scan->array_datums = palloc(sizeof(Datum));
	scan->array_datums_allocated_size = 1;
	scan->array_elements_left = 0;

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

	zs_decompress_init(&scan->decompressor);
	scan->recent_oldest_undo = zsundo_get_oldest_undo_ptr(rel);
}

/*
 * Reset the 'next' TID in a scan to the given TID.
 */
void
zsbt_tid_reset_scan(ZSBtreeScan *scan, zstid starttid)
{
	if (starttid < scan->nexttid)
	{
		/* have to restart from scratch. */
		scan->array_elements_left = 0;
		scan->nexttid = starttid;
		scan->has_decompressed = false;
		if (scan->lastbuf != InvalidBuffer)
			ReleaseBuffer(scan->lastbuf);
		scan->lastbuf = InvalidBuffer;
	}
	else
		zsbt_scan_skip(scan, starttid);
}

void
zsbt_tid_end_scan(ZSBtreeScan *scan)
{
	if (!scan->active)
		return;

	if (scan->lastbuf != InvalidBuffer)
		ReleaseBuffer(scan->lastbuf);
	zs_decompress_free(&scan->decompressor);

	scan->active = false;
	scan->array_elements_left = 0;
}

/*
 * Helper function of zsbt_scan_next(), to extract Datums from the given
 * array item into the scan->array_* fields.
 */
static void
zsbt_tid_scan_extract_array(ZSBtreeScan *scan, ZSArrayBtreeItem *aitem)
{
	int			nelements = aitem->t_nelements;
	zstid		tid = aitem->t_tid;

	/* skip over elements that we are not interested in */
	while (tid < scan->nexttid && nelements > 0)
	{
		tid++;
		nelements--;
	}

	/* leave out elements that are past end of range */
	if (tid + nelements > scan->endtid)
		nelements = scan->endtid - tid;

	scan->array_undoptr = aitem->t_undo_ptr;
	scan->array_elements_left = nelements;
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
zsbt_tid_scan_next(ZSBtreeScan *scan)
{
	Buffer		buf;
	bool		buf_is_locked = false;
	Page		page;
	ZSBtreePageOpaque *opaque;
	OffsetNumber off;
	OffsetNumber maxoff;
	BlockNumber	next;
	bool		visible;

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
		if (scan->array_elements_left > 0)
			goto have_array;

		/*
		 * If we are still processing a compressed item, process the next item
		 * from the it. If it's an array item, we start iterating the array by
		 * setting the scan->array_* fields, and loop back to top to return the
		 * first element from the array.
		 */
		if (scan->has_decompressed)
		{
			zstid		lasttid;
			ZSBtreeItem *uitem;
			TransactionId obsoleting_xid;

			uitem = zs_decompress_read_item(&scan->decompressor);

			if (uitem == NULL)
			{
				scan->has_decompressed = false;
				continue;
			}

			/* a compressed item cannot contain nested compressed items */
			Assert((uitem->t_flags & ZSBT_COMPRESSED) == 0);

			lasttid = zsbt_item_lasttid(uitem);
			if (lasttid < scan->nexttid)
				continue;

			if (uitem->t_tid >= scan->endtid)
				break;

			visible = zs_SatisfiesVisibility(scan, uitem, &obsoleting_xid, NULL);

			if (scan->serializable && TransactionIdIsValid(obsoleting_xid))
				CheckForSerializableConflictOut(scan->rel, obsoleting_xid, scan->snapshot);

			if (!visible)
			{
				scan->nexttid = lasttid + 1;
				continue;
			}
			if ((uitem->t_flags & ZSBT_ARRAY) != 0)
			{
				/* no need to make a copy, because the uncompressed buffer
				 * is already a copy */
				ZSArrayBtreeItem *aitem = (ZSArrayBtreeItem *) uitem;

				zsbt_tid_scan_extract_array(scan, aitem);
				continue;
			}
			else
			{
				/* single item */
				ZSSingleBtreeItem *sitem = (ZSSingleBtreeItem *) uitem;

				scan->nexttid = sitem->t_tid;
				scan->array_undoptr = sitem->t_undo_ptr;
				scan->array_elements_left = 1;

				if (buf_is_locked)
					LockBuffer(scan->lastbuf, BUFFER_LOCK_UNLOCK);
				buf_is_locked = false;
				goto have_array;
			}
		}

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
				if (!zsbt_page_is_expected(scan->rel, scan->attno, scan->nexttid, 0, buf))
				{
					/*
					 * It's not valid for the TID we're looking for, but maybe it was the
					 * right page for the previous TID. In that case, we don't need to
					 * restart from the root, we can follow the right-link instead.
					 */
					if (zsbt_page_is_expected(scan->rel, scan->attno, scan->nexttid - 1, 0, buf))
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
				buf = scan->lastbuf = zsbt_descend(scan->rel, scan->attno, scan->nexttid, 0, true);
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
			ZSBtreeItem	*item = (ZSBtreeItem *) PageGetItem(page, iid);
			zstid		lasttid;

			lasttid = zsbt_item_lasttid(item);

			if (scan->nexttid > lasttid)
				continue;

			if (item->t_tid >= scan->endtid)
			{
				scan->nexttid = scan->endtid;
				break;
			}

			if ((item->t_flags & ZSBT_COMPRESSED) != 0)
			{
				ZSCompressedBtreeItem *citem = (ZSCompressedBtreeItem *) item;
				MemoryContext oldcxt = MemoryContextSwitchTo(scan->context);

				zs_decompress_chunk(&scan->decompressor, citem);
				MemoryContextSwitchTo(oldcxt);
				scan->has_decompressed = true;
				LockBuffer(buf, BUFFER_LOCK_UNLOCK);
				buf_is_locked = false;
				break;
			}
			else
			{
				TransactionId obsoleting_xid;

				visible = zs_SatisfiesVisibility(scan, item, &obsoleting_xid, NULL);

				if (!visible)
				{
					if (scan->serializable && TransactionIdIsValid(obsoleting_xid))
						CheckForSerializableConflictOut(scan->rel, obsoleting_xid, scan->snapshot);
					scan->nexttid = lasttid + 1;
					continue;
				}

				if ((item->t_flags & ZSBT_ARRAY) != 0)
				{
					/* copy the item, because we can't hold a lock on the page  */
					ZSArrayBtreeItem *aitem;

					aitem = MemoryContextAlloc(scan->context, item->t_size);
					memcpy(aitem, item, item->t_size);

					zsbt_tid_scan_extract_array(scan, aitem);

					if (scan->array_elements_left > 0)
					{
						LockBuffer(scan->lastbuf, BUFFER_LOCK_UNLOCK);
						buf_is_locked = false;
						break;
					}
				}
				else
				{
					/* single item */
					ZSSingleBtreeItem *sitem = (ZSSingleBtreeItem *) item;

					scan->nexttid = sitem->t_tid;
					scan->array_undoptr = sitem->t_undo_ptr;
					scan->array_elements_left = 1;
					LockBuffer(scan->lastbuf, BUFFER_LOCK_UNLOCK);
					buf_is_locked = false;
					goto have_array;
				}
			}
		}

		if (scan->array_elements_left > 0 || scan->has_decompressed)
			continue;

		/* No more items on this page. Walk right, if possible */
		next = opaque->zs_next;
		if (next == BufferGetBlockNumber(buf))
			elog(ERROR, "btree page %u next-pointer points to itself", next);
		LockBuffer(buf, BUFFER_LOCK_UNLOCK);
		buf_is_locked = false;

		if (next == InvalidBlockNumber || scan->nexttid >= scan->endtid)
		{
			scan->active = false;
			scan->array_elements_left = 0;
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
	Assert(scan->array_elements_left > 0);

	scan->array_elements_left--;
	return scan->nexttid++;
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
		ZSBtreeItem	*hitup = (ZSBtreeItem *) PageGetItem(page, iid);

		tid = zsbt_item_lasttid(hitup) + 1;
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
	bool		assign_tids;
	zstid		tid = tids[0];
	Buffer		buf;
	Page		page;
	ZSBtreePageOpaque *opaque;
	OffsetNumber maxoff;
	zstid		insert_target_key;
	ZSUndoRec_Insert undorec;
	int			i;
	List	   *newitems;
	ZSUndoRecPtr undorecptr;

	/*
	 * If TID was given, find the right place for it. Otherwise, insert to
	 * the rightmost leaf.
	 *
	 * TODO: use a Free Space Map to find suitable target.
	 */
	assign_tids = (tid == InvalidZSTid);

	if (!assign_tids)
		insert_target_key = tid;
	else
		insert_target_key = MaxZSTid;

	buf = zsbt_descend(rel, ZS_META_ATTRIBUTE_NUM, insert_target_key, 0, false);
	page = BufferGetPage(buf);
	opaque = ZSBtreePageGetOpaque(page);
	maxoff = PageGetMaxOffsetNumber(page);

	/*
	 * Look at the last item, for its tid.
	 *
	 * assign TIDS for each item, if needed.
	 */
	if (assign_tids)
	{
		zstid		lasttid;

		if (maxoff >= FirstOffsetNumber)
		{
			ItemId		iid = PageGetItemId(page, maxoff);
			ZSBtreeItem	*hitup = (ZSBtreeItem *) PageGetItem(page, iid);

			lasttid = zsbt_item_lasttid(hitup);
			tid = lasttid + 1;
		}
		else
		{
			lasttid = opaque->zs_lokey;
			tid = lasttid;
		}

		for (i = 0; i < nitems; i++)
		{
			tids[i] = tid;
			tid++;
		}
	}

	/* Form an undo record */
	if (xid != FrozenTransactionId)
	{
		undorec.rec.size = sizeof(ZSUndoRec_Insert);
		undorec.rec.type = ZSUNDO_TYPE_INSERT;
		undorec.rec.xid = xid;
		undorec.rec.cid = cid;
		undorec.rec.tid = tids[0];
		undorec.rec.speculative_token = speculative_token;
		undorec.rec.prevundorec = prevundoptr;
		undorec.endtid = tids[nitems - 1];

		undorecptr = zsundo_insert(rel, &undorec.rec);
	}
	else
	{
		ZSUndoRecPtrInitialize(&undorecptr);
	}

	/* Create items to insert. */
	newitems = NIL;
	i = 0;
	while (i < nitems)
	{
		int			j;
		ZSBtreeItem *newitem;

		/*
		 * Try to collapse as many items as possible into an Array item.
		 * The first item in the array is now at tids[i]/datums[i]/isnulls[i].
		 * Items can be stored in the same array as long as the TIDs are
		 * consecutive, they all have the same isnull flag, and the array
		 * isn't too large to be stored on a single leaf page. Scan the
		 * arrays, checking those conditions.
		 *
		 * FIXME: this math is bogus for TIDs
		 */
		for (j = i + 1; j < nitems; j++)
		{
			if (tids[j] != tids[j - 1] + 1)
				break;
		}

		/*
		 * 'i' is now the first entry to store in the array, and 'j' is the
		 * last + 1 elemnt to store. If j == i + 1, then there is only one
		 * element and zsbt_create_item() will create a 'single' item rather
		 * than an array.
		 */
		newitem = zsbt_tid_create_item(tids[i], undorecptr, j - i);

		newitems = lappend(newitems, newitem);
		i = j;
	}

	/* recompress and possibly split the page */
	zsbt_tid_replace_item(rel, buf,
						  InvalidZSTid, NULL,
						  newitems);
	/* zsbt_replace_item unlocked 'buf' */
	ReleaseBuffer(buf);
}

TM_Result
zsbt_tid_delete(Relation rel, zstid tid,
			TransactionId xid, CommandId cid,
			Snapshot snapshot, Snapshot crosscheck, bool wait,
			TM_FailureData *hufd, bool changingPart)
{
	ZSUndoRecPtr recent_oldest_undo = zsundo_get_oldest_undo_ptr(rel);
	ZSSingleBtreeItem *item;
	TM_Result	result;
	bool		keep_old_undo_ptr = true;
	ZSUndoRecPtr undorecptr;
	ZSSingleBtreeItem *deleteditem;
	Buffer		buf;
	zstid		next_tid;

	/* Find the item to delete. (It could be compressed) */
	item = zsbt_tid_fetch(rel, &recent_oldest_undo, tid, &buf);
	if (item == NULL)
	{
		/*
		 * or should this be TM_Invisible? The heapam at least just throws
		 * an error, I think..
		 */
		elog(ERROR, "could not find tuple to delete with TID (%u, %u) in TID tree",
			 ZSTidGetBlockNumber(tid), ZSTidGetOffsetNumber(tid));
	}

	if (snapshot)
	{
		result = zs_SatisfiesUpdate(rel, snapshot, recent_oldest_undo,
									(ZSBtreeItem *) item, LockTupleExclusive,
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
			ZSBtreeScan scan;
			TransactionId obsoleting_xid;

			memset(&scan, 0, sizeof(scan));
			scan.rel = rel;
			scan.snapshot = crosscheck;
			scan.recent_oldest_undo = recent_oldest_undo;

			if (!zs_SatisfiesVisibility(&scan, (ZSBtreeItem *) item, &obsoleting_xid, NULL))
			{
				UnlockReleaseBuffer(buf);
				/* FIXME: We should fill TM_FailureData *hufd correctly */
				result = TM_Updated;
			}
		}
	}

	/* Create UNDO record. */
	{
		ZSUndoRec_Delete undorec;

		undorec.rec.size = sizeof(ZSUndoRec_Delete);
		undorec.rec.type = ZSUNDO_TYPE_DELETE;
		undorec.rec.xid = xid;
		undorec.rec.cid = cid;
		undorec.rec.tid = tid;
		undorec.changedPart = changingPart;

		if (keep_old_undo_ptr)
			undorec.rec.prevundorec = item->t_undo_ptr;
		else
			ZSUndoRecPtrInitialize(&undorec.rec.prevundorec);

		undorecptr = zsundo_insert(rel, &undorec.rec);
	}

	/* Replace the ZSBreeItem with one with the new UNDO pointer. */
	deleteditem = palloc(item->t_size);
	memcpy(deleteditem, item, item->t_size);
	deleteditem->t_undo_ptr = undorecptr;

	zsbt_tid_replace_item(rel, buf,
						  item->t_tid, (ZSBtreeItem *) deleteditem,
						  NIL);
	ReleaseBuffer(buf);	/* zsbt_replace_item unlocked */

	pfree(deleteditem);

	return TM_Ok;
}

void
zsbt_find_latest_tid(Relation rel, zstid *tid, Snapshot snapshot)
{
	ZSUndoRecPtr recent_oldest_undo = zsundo_get_oldest_undo_ptr(rel);
	ZSSingleBtreeItem *item;
	Buffer		buf;
	/* Just using meta attribute, we can follow the update chain */
	zstid curr_tid = *tid;

	for(;;)
	{
		zstid next_tid = InvalidZSTid;
		if (curr_tid == InvalidZSTid)
			break;

		/* Find the item */
		item = zsbt_tid_fetch(rel, &recent_oldest_undo, curr_tid, &buf);
		if (item == NULL)
			break;

		if (snapshot)
		{
			/* FIXME: dummmy scan */
			ZSBtreeScan scan;
			TransactionId obsoleting_xid;

			memset(&scan, 0, sizeof(scan));
			scan.rel = rel;
			scan.snapshot = snapshot;
			scan.recent_oldest_undo = recent_oldest_undo;

			if (zs_SatisfiesVisibility(&scan, (ZSBtreeItem *) item,
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
	ZSSingleBtreeItem *olditem;
	TM_Result	result;
	bool		keep_old_undo_ptr = true;
	zstid		next_tid;

	/*
	 * Find the item to delete.
	 */
	olditem = zsbt_tid_fetch(rel, &recent_oldest_undo, otid, &buf);
	if (olditem == NULL)
	{
		/*
		 * or should this be TM_Invisible? The heapam at least just throws
		 * an error, I think..
		 */
		elog(ERROR, "could not find old tuple to update with TID (%u, %u) in TID tree",
			 ZSTidGetBlockNumber(otid), ZSTidGetOffsetNumber(otid));
	}
	*prevundoptr_p = olditem->t_undo_ptr;

	/*
	 * Is it visible to us?
	 */
	result = zs_SatisfiesUpdate(rel, snapshot, recent_oldest_undo,
								(ZSBtreeItem *) olditem,
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
		ZSBtreeScan scan;
		TransactionId obsoleting_xid;

		memset(&scan, 0, sizeof(scan));
		scan.rel = rel;
		scan.snapshot = crosscheck;
		scan.recent_oldest_undo = recent_oldest_undo;

		if (!zs_SatisfiesVisibility(&scan, (ZSBtreeItem *) olditem, &obsoleting_xid, NULL))
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
	ZSSingleBtreeItem *olditem;
	TM_Result	result;
	bool		keep_old_undo_ptr = true;
	TM_FailureData tmfd;
	ZSUndoRecPtr undorecptr;
	ZSSingleBtreeItem *deleteditem;
	zstid		next_tid;

	/*
	 * Find the item to delete.  It could be part of a compressed item,
	 * we let zsbt_fetch() handle that.
	 */
	olditem = zsbt_tid_fetch(rel, &recent_oldest_undo, otid, &buf);
	if (olditem == NULL)
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
								(ZSBtreeItem *) olditem,
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
		undorec.rec.tid = otid;
		if (keep_old_undo_ptr)
			undorec.rec.prevundorec = olditem->t_undo_ptr;
		else
			ZSUndoRecPtrInitialize(&undorec.rec.prevundorec);
		undorec.newtid = newtid;
		undorec.key_update = key_update;

		undorecptr = zsundo_insert(rel, &undorec.rec);
	}

	/* Replace the ZSBreeItem with one with the updated undo pointer. */
	deleteditem = palloc(olditem->t_size);
	memcpy(deleteditem, olditem, olditem->t_size);
	deleteditem->t_undo_ptr = undorecptr;

	zsbt_tid_replace_item(rel, buf,
						  otid, (ZSBtreeItem *) deleteditem,
						  NIL);
	ReleaseBuffer(buf);		/* zsbt_recompress_replace released */

	pfree(deleteditem);
}

TM_Result
zsbt_tid_lock(Relation rel, zstid tid,
			   TransactionId xid, CommandId cid,
			   LockTupleMode mode, Snapshot snapshot,
			   TM_FailureData *hufd, zstid *next_tid)
{
	ZSUndoRecPtr recent_oldest_undo = zsundo_get_oldest_undo_ptr(rel);
	Buffer		buf;
	ZSSingleBtreeItem *item;
	TM_Result	result;
	bool		keep_old_undo_ptr = true;
	ZSUndoRecPtr undorecptr;
	ZSSingleBtreeItem *newitem;

	*next_tid = tid;

	/* Find the item to delete. (It could be compressed) */
	item = zsbt_tid_fetch(rel, &recent_oldest_undo, tid, &buf);
	if (item == NULL)
	{
		/*
		 * or should this be TM_Invisible? The heapam at least just throws
		 * an error, I think..
		 */
		elog(ERROR, "could not find tuple to lock with TID (%u, %u)",
			 ZSTidGetBlockNumber(tid), ZSTidGetOffsetNumber(tid));
	}
	result = zs_SatisfiesUpdate(rel, snapshot, recent_oldest_undo,
								   (ZSBtreeItem *) item, mode,
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
		undorec.rec.tid = tid;
		undorec.lockmode = mode;
		if (keep_old_undo_ptr)
			undorec.rec.prevundorec = item->t_undo_ptr;
		else
			ZSUndoRecPtrInitialize(&undorec.rec.prevundorec);

		undorecptr = zsundo_insert(rel, &undorec.rec);
	}

	/* Replace the item with an identical one, but with updated undo pointer. */
	newitem = palloc(item->t_size);
	memcpy(newitem, item, item->t_size);
	newitem->t_undo_ptr = undorecptr;

	zsbt_tid_replace_item(rel, buf,
						  item->t_tid, (ZSBtreeItem *) newitem,
						  NIL);
	ReleaseBuffer(buf);		/* zsbt_replace_item unlocked */

	pfree(newitem);

	return TM_Ok;
}

/*
 * Mark item with given TID as dead.
 *
 * This is used during VACUUM.
 */
void
zsbt_tid_mark_dead(Relation rel, zstid tid, ZSUndoRecPtr undoptr)
{
	Buffer		buf;
	ZSSingleBtreeItem *item;
	ZSSingleBtreeItem deaditem;

	/* Find the item to delete. (It could be compressed) */
	item = zsbt_tid_fetch(rel, NULL, tid, &buf);
	if (item == NULL)
	{
		elog(WARNING, "could not find tuple to mark dead with TID (%u, %u)",
			 ZSTidGetBlockNumber(tid), ZSTidGetOffsetNumber(tid));
		return;
	}

	/* Replace the ZSBreeItem with a DEAD item. (Unless it's already dead) */
	if ((item->t_flags & ZSBT_DEAD) != 0)
	{
		UnlockReleaseBuffer(buf);
		return;
	}

	memset(&deaditem, 0, offsetof(ZSSingleBtreeItem, t_payload));
	deaditem.t_tid = tid;
	deaditem.t_size = sizeof(ZSSingleBtreeItem);
	deaditem.t_flags = ZSBT_DEAD;
	deaditem.t_undo_ptr = undoptr;

	zsbt_tid_replace_item(rel, buf,
						  tid, (ZSBtreeItem *) &deaditem,
						  NIL);
	ReleaseBuffer(buf); 	/* zsbt_replace_item released */
}

/*
 * Clear an item's UNDO pointer.
 *
 * This is used during VACUUM, to clear out aborted deletions.
 */
void
zsbt_tid_undo_deletion(Relation rel, zstid tid, ZSUndoRecPtr undoptr)
{
	Buffer		buf;
	ZSSingleBtreeItem *item;
	ZSSingleBtreeItem *copy;

	/* Find the item to delete. (It could be compressed) */
	item = zsbt_tid_fetch(rel, NULL, tid, &buf);
	if (item == NULL)
	{
		elog(WARNING, "could not find aborted tuple to remove with TID (%u, %u)",
			 ZSTidGetBlockNumber(tid), ZSTidGetOffsetNumber(tid));
		return;
	}

	if (ZSUndoRecPtrEquals(item->t_undo_ptr, undoptr))
	{
		copy = palloc(item->t_size);
		memcpy(copy, item, item->t_size);
		ZSUndoRecPtrInitialize(&copy->t_undo_ptr);
		zsbt_tid_replace_item(rel, buf,
							  tid, (ZSBtreeItem *) copy,
							  NIL);
		ReleaseBuffer(buf); 	/* zsbt_replace_item unlocked */
	}
	else
	{
		Assert(item->t_undo_ptr.counter > undoptr.counter ||
			   !IsZSUndoRecPtrValid(&item->t_undo_ptr));
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
	ZSSingleBtreeItem *item = NULL;
	ZSUndoRecPtr recent_oldest_undo;

	item = zsbt_tid_fetch(rel, &recent_oldest_undo, tid, &buf);

	if (item == NULL)
		elog(ERROR, "couldn't find item for meta column for inserted tuple with TID (%u, %u) in rel %s",
			 ZSTidGetBlockNumber(tid), ZSTidGetOffsetNumber(tid), rel->rd_rel->relname.data);
	Assert(item->t_tid == tid);

	zsundo_clear_speculative_token(rel, item->t_undo_ptr);

	UnlockReleaseBuffer(buf);
}

/*
 * Fetch the item with given TID. The page containing the item is kept locked, and
 * returned to the caller in *buf_p. This is used to locate a tuple for updating
 * or deleting it.
 */
static ZSSingleBtreeItem *
zsbt_tid_fetch(Relation rel, ZSUndoRecPtr *recent_oldest_undo,
		   zstid tid, Buffer *buf_p)
{
	Buffer		buf;
	Page		page;
	ZSBtreeItem *item = NULL;
	bool		found = false;
	OffsetNumber maxoff;
	OffsetNumber off;

	buf = zsbt_descend(rel, ZS_META_ATTRIBUTE_NUM, tid, 0, false);
	if (buf == InvalidBuffer)
	{
		*buf_p = InvalidBuffer;
		return NULL;
	}
	page = BufferGetPage(buf);

	/* Find the item on the page that covers the target TID */
	maxoff = PageGetMaxOffsetNumber(page);
	for (off = FirstOffsetNumber; off <= maxoff; off++)
	{
		ItemId		iid = PageGetItemId(page, off);
		item = (ZSBtreeItem *) PageGetItem(page, iid);

		if ((item->t_flags & ZSBT_COMPRESSED) != 0)
		{
			ZSCompressedBtreeItem *citem = (ZSCompressedBtreeItem *) item;
			ZSDecompressContext decompressor;

			zs_decompress_init(&decompressor);
			zs_decompress_chunk(&decompressor, citem);

			while ((item = zs_decompress_read_item(&decompressor)) != NULL)
			{
				zstid		lasttid = zsbt_item_lasttid(item);

				if (item->t_tid <= tid && lasttid >= tid)
				{
					found = true;
					break;
				}
			}
			if (found)
			{
				/* FIXME: decompressor is leaked. Can't free it yet, because we still
				 * need to access the item below
				 */
				break;
			}
			zs_decompress_free(&decompressor);
		}
		else
		{
			zstid		lasttid = zsbt_item_lasttid(item);

			if (item->t_tid <= tid && lasttid >= tid)
			{
				found = true;
				break;
			}
		}
	}

	if (found)
	{
		ZSSingleBtreeItem *result;

		if ((item->t_flags & ZSBT_ARRAY) != 0)
		{
			ZSArrayBtreeItem *aitem = (ZSArrayBtreeItem *) item;
			int			elemno = tid - aitem->t_tid;
			int			resultsize;

			Assert(elemno < aitem->t_nelements);

			resultsize = offsetof(ZSSingleBtreeItem, t_payload);
			result = palloc(resultsize);
			memset(result, 0, offsetof(ZSSingleBtreeItem, t_payload)); /* zero padding */
			result->t_tid = tid;
			result->t_flags = item->t_flags & ~ZSBT_ARRAY;
			result->t_size = resultsize;
			result->t_undo_ptr = aitem->t_undo_ptr;
		}
		else
		{
			/* single item */
			result = (ZSSingleBtreeItem *) item;
		}

		*buf_p = buf;
		return result;
	}
	else
	{
		UnlockReleaseBuffer(buf);
		*buf_p = InvalidBuffer;
		return NULL;
	}
}

/*
 * Form a ZSBtreeItem out of the given datums, or data that's already in on-disk
 * array format, for insertion.
 *
 * If there's more than one element, an array item is created. Otherwise, a single
 * item.
 */
static ZSBtreeItem *
zsbt_tid_create_item(zstid tid, ZSUndoRecPtr undo_ptr,
				 int nelements)
{
	ZSBtreeItem *result;
	Size		itemsz;

	Assert(nelements > 0);

	if (nelements > 1)
	{
		ZSArrayBtreeItem *newitem;

		itemsz = offsetof(ZSArrayBtreeItem, t_payload);

		newitem = palloc(itemsz);
		memset(newitem, 0, offsetof(ZSArrayBtreeItem, t_payload)); /* zero padding */
		newitem->t_tid = tid;
		newitem->t_size = itemsz;
		newitem->t_flags = ZSBT_ARRAY;
		newitem->t_nelements = nelements;
		newitem->t_undo_ptr = undo_ptr;

		result = (ZSBtreeItem *) newitem;
	}
	else
	{
		ZSSingleBtreeItem *newitem;

		itemsz = offsetof(ZSSingleBtreeItem, t_payload);

		newitem = palloc(itemsz);
		memset(newitem, 0, offsetof(ZSSingleBtreeItem, t_payload)); /* zero padding */
		newitem->t_tid = tid;
		newitem->t_flags = 0;
		newitem->t_size = itemsz;
		newitem->t_undo_ptr = undo_ptr;

		result = (ZSBtreeItem *) newitem;
	}

	return result;
}

/*
 * This helper function is used to implement INSERT, UPDATE and DELETE.
 *
 * If 'olditem' is not NULL, then 'olditem' on the page is replaced with
 * 'replacementitem'. 'replacementitem' can be NULL, to remove an old item.
 *
 * If 'newitems' is not empty, the items in the list are added to the page,
 * to the correct position. FIXME: Actually, they're always just added to
 * the end of the page, and that better be the correct position.
 *
 * This function handles decompressing and recompressing items, and splitting
 * the page if needed.
 */
static void
zsbt_tid_replace_item(Relation rel, Buffer buf,
					  zstid oldtid,
					  ZSBtreeItem *replacementitem,
					  List       *newitems)
{
	Page		page = BufferGetPage(buf);
	OffsetNumber off;
	OffsetNumber maxoff;
	List	   *items;
	bool		found_old_item = false;
	/* We might need to decompress up to two previously compressed items */
	ZSDecompressContext decompressor;
	bool		decompressor_used = false;
	bool		decompressing;

	if (replacementitem)
		Assert(replacementitem->t_tid == oldtid);

	/*
	 * TODO: It would be good to have a fast path, for the common case that we're
	 * just adding items to the end.
	 */

	/* Loop through all old items on the page */
	items = NIL;
	maxoff = PageGetMaxOffsetNumber(page);
	decompressing = false;
	off = 1;
	for (;;)
	{
		ZSBtreeItem *item;

		/*
		 * Get the next item to process. If we're decompressing, get the next
		 * tuple from the decompressor, otherwise get the next item from the page.
		 */
		if (decompressing)
		{
			item = zs_decompress_read_item(&decompressor);
			if (!item)
			{
				decompressing = false;
				continue;
			}
		}
		else if (off <= maxoff)
		{
			ItemId		iid = PageGetItemId(page, off);

			item = (ZSBtreeItem *) PageGetItem(page, iid);
			off++;

		}
		else
		{
			/* out of items */
			break;
		}

		/* we now have an item to process, either straight from the page or from
		 * the decompressor */
		if ((item->t_flags & ZSBT_COMPRESSED) != 0)
		{
			zstid		item_lasttid = zsbt_item_lasttid(item);

			/* there shouldn't nested compressed items */
			if (decompressing)
				elog(ERROR, "nested compressed items on zedstore page not supported");

			if (oldtid != InvalidZSTid && item->t_tid <= oldtid && oldtid <= item_lasttid)
			{
				ZSCompressedBtreeItem *citem = (ZSCompressedBtreeItem *) item;

				/* Found it, this compressed item covers the target or the new TID. */
				/* We have to decompress it, and recompress */
				Assert(!decompressor_used);

				zs_decompress_init(&decompressor);
				zs_decompress_chunk(&decompressor, citem);
				decompressor_used = true;
				decompressing = true;
				continue;
			}
			else
			{
				/* keep this compressed item as it is */
				items = lappend(items, item);
			}
		}
		else if ((item->t_flags & ZSBT_ARRAY) != 0)
		{
			/* array item */
			ZSArrayBtreeItem *aitem = (ZSArrayBtreeItem *) item;
			zstid		item_lasttid = zsbt_item_lasttid(item);

			if (oldtid != InvalidZSTid && item->t_tid <= oldtid && oldtid <= item_lasttid)
			{
				/*
				 * The target TID is currently part of an array item. We have to split
				 * the array item into two, and put the replacement item in the middle.
				 */
				int			cutoff;
				int			nelements = aitem->t_nelements;

				cutoff = oldtid - item->t_tid;

				/* Array slice before the target TID */
				if (cutoff > 0)
				{
					ZSBtreeItem *item1;

					item1 = zsbt_tid_create_item(aitem->t_tid, aitem->t_undo_ptr,
												 cutoff);
					items = lappend(items, item1);
				}

				/*
				 * Skip over the target element, and store the replacement
				 * item, if any, in its place
				 */
				if (replacementitem)
					items = lappend(items, replacementitem);

				/* Array slice after the target */
				if (cutoff + 1 < nelements)
				{
					ZSBtreeItem *item2;

					item2 = zsbt_tid_create_item(oldtid + 1, aitem->t_undo_ptr,
												 nelements - (cutoff + 1));
					items = lappend(items, item2);
				}

				found_old_item = true;
			}
			else
				items = lappend(items, item);
		}
		else
		{
			/* single item */
			if (oldtid != InvalidZSTid && item->t_tid == oldtid)
			{
				Assert(!found_old_item);
				found_old_item = true;
				if (replacementitem)
					items = lappend(items, replacementitem);
			}
			else
				items = lappend(items, item);
		}
	}

	if (oldtid != InvalidZSTid && !found_old_item)
		elog(ERROR, "could not find old item to replace");

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

	/*
	 * We can now free the decompression contexts. The pointers in the 'items' list
	 * point to decompression buffers, so we cannot free them until after writing out
	 * the pages.
	 */
	if (decompressor_used)
		zs_decompress_free(&decompressor);
	list_free(items);
}

/*
 * Recompressor routines
 */
typedef struct
{
	Page		currpage;
	ZSCompressContext compressor;
	int			compressed_items;

	/* first page writes over the old buffer, subsequent pages get newly-allocated buffers */
	zs_split_stack *stack_head;
	zs_split_stack *stack_tail;

	int			total_items;
	int			total_compressed_items;
	int			total_already_compressed_items;

	zstid		hikey;
} zsbt_tid_recompress_context;

static void
zsbt_recompress_newpage(zsbt_tid_recompress_context *cxt, zstid nexttid, int flags)
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
zsbt_recompress_add_to_page(zsbt_tid_recompress_context *cxt, ZSBtreeItem *item)
{
	if (PageGetFreeSpace(cxt->currpage) < MAXALIGN(item->t_size))
		zsbt_recompress_newpage(cxt, item->t_tid, 0);

	if (PageAddItemExtended(cxt->currpage,
							(Item) item, item->t_size,
							PageGetMaxOffsetNumber(cxt->currpage) + 1,
							PAI_OVERWRITE) == InvalidOffsetNumber)
		elog(ERROR, "could not add item to page while recompressing");

	cxt->total_items++;
}

static bool
zsbt_recompress_add_to_compressor(zsbt_tid_recompress_context *cxt, ZSBtreeItem *item)
{
	bool		result;

	if (cxt->compressed_items == 0)
		zs_compress_begin(&cxt->compressor, PageGetFreeSpace(cxt->currpage));

	result = zs_compress_add(&cxt->compressor, item);
	if (result)
	{
		cxt->compressed_items++;

		cxt->total_compressed_items++;
	}

	return result;
}

static void
zsbt_recompress_flush(zsbt_tid_recompress_context *cxt)
{
	ZSCompressedBtreeItem *citem;

	if (cxt->compressed_items == 0)
		return;

	citem = zs_compress_finish(&cxt->compressor);

	if (citem)
		zsbt_recompress_add_to_page(cxt, (ZSBtreeItem *) citem);
	else
	{
		uint16 size = 0;
		/*
		 * compression failed hence add items uncompressed. We should maybe
		 * note that these items/pattern are not compressible and skip future
		 * attempts to compress but its possible this clubbed with some other
		 * future items may compress. So, better avoid recording such info and
		 * try compression again later if required.
		 */
		for (int i = 0; i < cxt->compressor.nitems; i++)
		{
			citem = (ZSCompressedBtreeItem *) (cxt->compressor.uncompressedbuffer + size);
			zsbt_recompress_add_to_page(cxt, (ZSBtreeItem *) citem);

			size += MAXALIGN(citem->t_size);
		}
	}

	cxt->compressed_items = 0;
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
	ZSUndoRecPtr recent_oldest_undo = { 0 };
	BlockNumber orignextblk;
	zs_split_stack *stack;
	List	   *downlinks = NIL;

	orignextblk = oldopaque->zs_next;

	cxt.currpage = NULL;
	zs_compress_init(&cxt.compressor);
	cxt.compressed_items = 0;
	cxt.stack_head = cxt.stack_tail = NULL;
	cxt.hikey = oldopaque->zs_hikey;

	cxt.total_items = 0;
	cxt.total_compressed_items = 0;
	cxt.total_already_compressed_items = 0;

	zsbt_recompress_newpage(&cxt, oldopaque->zs_lokey, (oldopaque->zs_flags & ZSBT_ROOT));

	foreach(lc, items)
	{
		ZSBtreeItem *item = (ZSBtreeItem *) lfirst(lc);

		/* We can leave out any old-enough DEAD items */
		if ((item->t_flags & ZSBT_DEAD) != 0)
		{
			ZSBtreeItem *uitem = (ZSBtreeItem *) item;

			if (recent_oldest_undo.counter == 0)
				recent_oldest_undo = zsundo_get_oldest_undo_ptr(rel);

			if (zsbt_item_undoptr(uitem).counter <= recent_oldest_undo.counter)
				continue;
		}

		if ((item->t_flags & ZSBT_COMPRESSED) != 0)
		{
			/* already compressed, add as it is. */
			zsbt_recompress_flush(&cxt);
			cxt.total_already_compressed_items++;
			zsbt_recompress_add_to_page(&cxt, item);
		}
		else
		{
			/* try to add this item to the compressor */
			if (!zsbt_recompress_add_to_compressor(&cxt, item))
			{
				if (cxt.compressed_items > 0)
				{
					/* flush, and retry */
					zsbt_recompress_flush(&cxt);

					if (!zsbt_recompress_add_to_compressor(&cxt, item))
					{
						/* could not compress, even on its own. Store it uncompressed, then */
						zsbt_recompress_add_to_page(&cxt, item);
					}
				}
				else
				{
					/* could not compress, even on its own. Store it uncompressed, then */
					zsbt_recompress_add_to_page(&cxt, item);
				}
			}
		}
	}

	/* flush the last one, if any */
	zsbt_recompress_flush(&cxt);

	zs_compress_free(&cxt.compressor);

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
