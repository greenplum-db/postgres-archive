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
 * NOTES:
 * - Locking order: child before parent, left before right
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/zedstore/zedstore_btree.c
 */
#include "postgres.h"

#include "access/tableam.h"
#include "access/xact.h"
#include "access/zedstore_compression.h"
#include "access/zedstore_internal.h"
#include "access/zedstore_undo.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/procarray.h"
#include "utils/datum.h"
#include "utils/rel.h"

/* prototypes for local functions */
static Buffer zsbt_descend(Relation rel, AttrNumber attno, zstid key, int level);
static void zsbt_recompress_replace(Relation rel, AttrNumber attno,
									Buffer oldbuf, List *items);
static zs_split_stack *zsbt_insert_downlinks(Relation rel, AttrNumber attno,
					  zstid leftlokey, BlockNumber leftblkno, int level,
					  List *downlinks);
static zs_split_stack *zsbt_split_internal_page(Relation rel, AttrNumber attno,
												Buffer leftbuf, OffsetNumber newoff, List *downlinks);
static zs_split_stack *zsbt_newroot(Relation rel, AttrNumber attno, int level,
									List *downlinks);
static zs_split_stack *zsbt_unlink_page(Relation rel, AttrNumber attno, Buffer buf, int level);
static zs_split_stack *zsbt_merge_pages(Relation rel, AttrNumber attno, Buffer leftbuf, Buffer rightbuf, bool target_is_left);
static ZSSingleBtreeItem *zsbt_fetch(Relation rel, AttrNumber attno, Snapshot snapshot,
		   ZSUndoRecPtr *recent_oldest_undo, zstid tid, Buffer *buf_p);
static void zsbt_replace_item(Relation rel, AttrNumber attno, Buffer buf,
							  zstid oldtid, ZSBtreeItem *replacementitem,
							  List *newitems);
static Size zsbt_compute_data_size(Form_pg_attribute atti, Datum val, bool isnull);
static ZSBtreeItem *zsbt_create_item(Form_pg_attribute att, zstid tid, ZSUndoRecPtr undo_ptr,
				 int nelements, Datum *datums,
				 char *dataptr, Size datasz, bool isnull);

static int zsbt_binsrch_internal(zstid key, ZSBtreeInternalPageItem *arr, int arr_elems);

static TM_Result zsbt_update_lock_old(Relation rel, AttrNumber attno, zstid otid,
					 TransactionId xid, CommandId cid, Snapshot snapshot,
					 Snapshot crosscheck, bool wait, TM_FailureData *hufd);
static void zsbt_update_insert_new(Relation rel, AttrNumber attno,
					   Datum newdatum, bool newisnull, zstid *newtid,
					   TransactionId xid, CommandId cid);
static void zsbt_mark_old_updated(Relation rel, AttrNumber attno, zstid otid, zstid newtid,
					  TransactionId xid, CommandId cid, Snapshot snapshot);

/* ----------------------------------------------------------------
 *						 Public interface
 * ----------------------------------------------------------------
 */

/*
 * Begin a scan of the btree.
 */
void
zsbt_begin_scan(Relation rel, AttrNumber attno, zstid starttid, zstid endtid, Snapshot snapshot, ZSBtreeScan *scan)
{
	int16		attlen;
	bool		attbyval;
	Buffer		buf;

	/*
	 * Fetch attlen/attbyval.
	 *
	 * XXX: This is duplicative with the zsmeta_get_root_for_attribute()
	 * call in zsbt_descend.
	 */
	(void) zsmeta_get_root_for_attribute(rel, attno, false, &attlen, &attbyval);

	scan->rel = rel;
	scan->attno = attno;
	scan->attlen = attlen;
	scan->attbyval = attbyval;
	if (attno == ZS_META_ATTRIBUTE_NUM)
		scan->atthasmissing = false;
	else
		scan->atthasmissing = rel->rd_att->attrs[attno - 1].atthasmissing;

	/*
	 * FIXME: should we store attalign in the metapage, too? Or can we remove
	 * attlen/attbyval from there as well?
	 */
	scan->attalign = rel->rd_att->attrs[attno - 1].attalign;

	scan->snapshot = snapshot;
	scan->context = CurrentMemoryContext;
	scan->lastbuf_is_locked = false;
	scan->lastoff = InvalidOffsetNumber;
	scan->has_decompressed = false;
	scan->nexttid = starttid;
	scan->endtid = endtid;
	memset(&scan->recent_oldest_undo, 0, sizeof(scan->recent_oldest_undo));
	scan->array_datums = palloc(sizeof(Datum));
	scan->array_datums_allocated_size = 1;
	scan->array_elements_left = 0;

	buf = zsbt_descend(rel, attno, starttid, 0);
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

void
zsbt_end_scan(ZSBtreeScan *scan)
{
	if (!scan->active)
		return;

	if (scan->lastbuf != InvalidBuffer)
	{
		if (scan->lastbuf_is_locked)
			LockBuffer(scan->lastbuf, BUFFER_LOCK_UNLOCK);
		ReleaseBuffer(scan->lastbuf);
	}
	zs_decompress_free(&scan->decompressor);

	scan->active = false;
	scan->array_elements_left = 0;
}

/*
 * Helper function of zsbt_scan_next(), to extract Datums from the given
 * array item into the scan->array_* fields.
 */
static void
zsbt_scan_extract_array(ZSBtreeScan *scan, ZSArrayBtreeItem *aitem)
{
	int			nelements = aitem->t_nelements;
	zstid		tid = aitem->t_tid;
	bool		isnull = (aitem->t_flags & ZSBT_NULL) != 0;
	char	   *p = aitem->t_payload;

	/* skip over elements that we are not interested in */
	while (tid < scan->nexttid && nelements > 0)
	{
		if (!isnull)
		{
			if (scan->attlen > 0)
			{
				p += att_align_nominal(scan->attlen, scan->attalign);
			}
			else
			{
				p = (Pointer) att_align_pointer(p, scan->attalign, scan->attlen, p);
				p = att_addlength_pointer(p, scan->attlen, p);
			}
		}
		tid++;
		nelements--;
	}

	/* leave out elements that are past end of range */
	if (tid + nelements > scan->endtid)
		nelements = scan->endtid - tid;

	scan->array_isnull = isnull;

	if (nelements > scan->array_datums_allocated_size)
	{
		if (scan->array_datums)
			pfree(scan->array_datums);
		scan->array_datums = palloc(nelements * sizeof(Datum));
		scan->array_datums_allocated_size = nelements;
	}

	if (isnull)
	{
		/*
		 * For NULLs, clear the Datum array. Not strictly necessary, I think,
		 * but less confusing when debugging.
		 */
		memset(scan->array_datums, 0, nelements * sizeof(Datum));
	}
	else
	{
		/*
		 * Expand the packed array data into an array of Datums.
		 *
		 * It would perhaps be more natural to loop through the elements with
		 * datumGetSize() and fetch_att(), but this is a pretty hot loop, so it's
		 * better to avoid checking attlen/attbyval in the loop.
		 *
		 * TODO: a different on-disk representation might make this better still,
		 * for varlenas (this is pretty optimal for fixed-lengths already).
		 * For example, storing an array of sizes or an array of offsets, followed
		 * by the data itself, might incur fewer pipeline stalls in the CPU.
		 */
		int16		attlen = scan->attlen;

		if (scan->attbyval)
		{
			if (attlen == sizeof(Datum))
			{
				memcpy(scan->array_datums, p, nelements * sizeof(Datum));
			}
			else if (attlen == sizeof(int32))
			{
				for (int i = 0; i < nelements; i++)
				{
					scan->array_datums[i] = fetch_att(p, true, sizeof(int32));
					p += sizeof(int32);
				}
			}
			else if (attlen == sizeof(int16))
			{
				for (int i = 0; i < nelements; i++)
				{
					scan->array_datums[i] = fetch_att(p, true, sizeof(int16));
					p += sizeof(int16);
				}
			}
			else if (attlen == 1)
			{
				for (int i = 0; i < nelements; i++)
				{
					scan->array_datums[i] = fetch_att(p, true, 1);
					p += 1;
				}
			}
			else
				Assert(false);
		}
		else if (attlen > 0)
		{
			for (int i = 0; i < nelements; i++)
			{
				scan->array_datums[i] = PointerGetDatum(p);
				p += att_align_nominal(scan->attlen, scan->attalign);
			}
		}
		else if (attlen == -1)
		{
			for (int i = 0; i < nelements; i++)
			{
				p = (Pointer) att_align_pointer(p, scan->attalign, scan->attlen, p);
				scan->array_datums[i] = PointerGetDatum(p);
				p = att_addlength_pointer(p, scan->attlen, p);
			}
		}
		else
		{
			/* TODO: convert cstrings to varlenas before we get here? */
			elog(ERROR, "cstrings not supported");
		}
	}
	scan->array_next_datum = &scan->array_datums[0];
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
bool
zsbt_scan_next(ZSBtreeScan *scan)
{
	Buffer		buf;
	bool		buf_is_locked = false;
	Page		page;
	ZSBtreePageOpaque *opaque;
	OffsetNumber off;
	OffsetNumber maxoff;
	BlockNumber	next;

	if (!scan->active)
		return false;

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
		{
			return true;
		}

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

			if (!zs_SatisfiesVisibility(scan, uitem))
			{
				scan->nexttid = lasttid + 1;
				continue;
			}
			if ((uitem->t_flags & ZSBT_ARRAY) != 0)
			{
				/* no need to make a copy, because the uncompressed buffer
				 * is already a copy */
				ZSArrayBtreeItem *aitem = (ZSArrayBtreeItem *) uitem;

				zsbt_scan_extract_array(scan, aitem);
				continue;
			}
			else
			{
				/* single item */
				ZSSingleBtreeItem *sitem = (ZSSingleBtreeItem *) uitem;

				scan->nexttid = sitem->t_tid;
				scan->array_elements_left = 1;
				scan->array_next_datum = &scan->array_datums[0];
				if (sitem->t_flags & ZSBT_NULL)
					scan->array_isnull = true;
				else
				{
					scan->array_isnull = false;
					scan->array_datums[0] = fetch_att(sitem->t_payload, scan->attbyval, scan->attlen);
					/* no need to copy, because the uncompression buffer is a copy already */
					/* FIXME: do we need to copy anyway, to make sure it's aligned correctly? */
				}

				if (buf_is_locked)
					LockBuffer(scan->lastbuf, BUFFER_LOCK_UNLOCK);
				buf_is_locked = false;
				return true;
			}
		}

		/*
		 * Scan the page for the next item.
		 */
		buf = scan->lastbuf;
		if (!buf_is_locked)
		{
			LockBuffer(buf, BUFFER_LOCK_SHARE);
			buf_is_locked = true;
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
				if (!zs_SatisfiesVisibility(scan, item))
				{
					scan->nexttid = lasttid + 1;
					continue;
				}

				if ((item->t_flags & ZSBT_ARRAY) != 0)
				{
					/* copy the item, because we can't hold a lock on the page  */
					ZSArrayBtreeItem *aitem;

					aitem = MemoryContextAlloc(scan->context, item->t_size);
					memcpy(aitem, item, item->t_size);

					zsbt_scan_extract_array(scan, aitem);

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
					scan->array_elements_left = 1;
					scan->array_next_datum = &scan->array_datums[0];
					if (item->t_flags & ZSBT_NULL)
						scan->array_isnull = true;
					else
					{
						scan->array_isnull = false;
						scan->array_datums[0] = fetch_att(sitem->t_payload, scan->attbyval, scan->attlen);
						scan->array_datums[0] = zs_datumCopy(scan->array_datums[0], scan->attbyval, scan->attlen);
					}
					LockBuffer(scan->lastbuf, BUFFER_LOCK_UNLOCK);
					buf_is_locked = false;
					return true;
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

	return false;
}

/*
 * Get the last tid (plus one) in the tree.
 */
zstid
zsbt_get_last_tid(Relation rel, AttrNumber attno)
{
	zstid		rightmostkey;
	zstid		tid;
	Buffer		buf;
	Page		page;
	ZSBtreePageOpaque *opaque;
	OffsetNumber maxoff;

	/* Find the rightmost leaf */
	rightmostkey = MaxZSTid;
	buf = zsbt_descend(rel, attno, rightmostkey, 0);
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
 * Insert a multiple items to the given attribute's btree.
 *
 * Populates the TIDs of the new tuples.
 *
 * If 'tid' in list is valid, then that TID is used. It better not be in use already. If
 * it's invalid, then a new TID is allocated, as we see best. (When inserting the
 * first column of the row, pass invalid, and for other columns, pass the TID
 * you got for the first column.)
 */
void
zsbt_multi_insert(Relation rel, AttrNumber attno,
				  Datum *datums, bool *isnulls, zstid *tids, int nitems,
				  TransactionId xid, CommandId cid, ZSUndoRecPtr *undorecptr_p)
{
	Form_pg_attribute attr;
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

	if (attno == ZS_META_ATTRIBUTE_NUM)
		attr = NULL;
	else
		attr = &rel->rd_att->attrs[attno - 1];

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

	buf = zsbt_descend(rel, attno, insert_target_key, 0);
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
	if (undorecptr_p)
	{
		Assert(attno == ZS_META_ATTRIBUTE_NUM);

		undorec.rec.size = sizeof(ZSUndoRec_Insert);
		undorec.rec.type = ZSUNDO_TYPE_INSERT;
		undorec.rec.xid = xid;
		undorec.rec.cid = cid;
		undorec.rec.tid = tids[0];
		undorec.endtid = tids[nitems - 1];

		undorecptr = zsundo_insert(rel, &undorec.rec);
		*undorecptr_p = undorecptr;
	}
	else
	{
		ZSUndoRecPtrInitialize(&undorecptr);
	}

	/* Create items to insert */
	newitems = NIL;
	i = 0;
	while (i < nitems)
	{
		Size		datasz;
		int			j;
		ZSBtreeItem *newitem;

		datasz = zsbt_compute_data_size(attr, datums[i], isnulls[i]);
		for (j = i + 1; j < nitems; j++)
		{
			if (isnulls[j] != isnulls[i])
				break;

			if (tids[j] != tids[j - 1] + 1)
				break;

			if (!isnulls[i])
			{
				Datum		val = datums[j];
				Size		datum_sz;

				datum_sz = zsbt_compute_data_size(attr, val, false);
				if (datasz + datum_sz < MaxZedStoreDatumSize / 4)
					break;
				datasz += datum_sz;
			}
		}

		newitem = zsbt_create_item(attr, tids[i], undorecptr,
								   j - i, &datums[i], NULL, datasz, isnulls[i]);

		newitems = lappend(newitems, newitem);
		i = j;
	}

	/* recompress and possibly split the page */
	zsbt_replace_item(rel, attno, buf,
					  InvalidZSTid, NULL,
					  newitems);
	/* zsbt_replace_item unlocked 'buf' */
	ReleaseBuffer(buf);
}

TM_Result
zsbt_delete(Relation rel, AttrNumber attno, zstid tid,
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

	/*
	 * This is currently only used on the meta-attribute. Other attributes don't
	 * carry visibility information.
	 */
	Assert(attno == ZS_META_ATTRIBUTE_NUM);

	/* Find the item to delete. (It could be compressed) */
	item = zsbt_fetch(rel, attno, snapshot, &recent_oldest_undo, tid, &buf);
	if (item == NULL)
	{
		/*
		 * or should this be TM_Invisible? The heapam at least just throws
		 * an error, I think..
		 */
		elog(ERROR, "could not find tuple to delete with TID (%u, %u) for attribute %d",
			 ZSTidGetBlockNumber(tid), ZSTidGetOffsetNumber(tid), attno);
	}
	result = zs_SatisfiesUpdate(rel, snapshot, recent_oldest_undo,
								(ZSBtreeItem *) item, &keep_old_undo_ptr, hufd);
	if (result != TM_Ok)
	{
		UnlockReleaseBuffer(buf);
		/* FIXME: We should fill TM_FailureData *hufd correctly */
		return result;
	}

	/* Create UNDO record. */
	{
		ZSUndoRec_Delete undorec;

		undorec.rec.size = sizeof(ZSUndoRec_Delete);
		undorec.rec.type = ZSUNDO_TYPE_DELETE;
		undorec.rec.xid = xid;
		undorec.rec.cid = cid;
		undorec.rec.tid = tid;

		if (keep_old_undo_ptr)
			undorec.prevundorec = item->t_undo_ptr;
		else
			ZSUndoRecPtrInitialize(&undorec.prevundorec);

		undorecptr = zsundo_insert(rel, &undorec.rec);
	}

	/* Replace the ZSBreeItem with a DELETED item. */
	deleteditem = palloc(item->t_size);
	memcpy(deleteditem, item, item->t_size);
	deleteditem->t_flags |= ZSBT_DELETED;
	deleteditem->t_undo_ptr = undorecptr;

	zsbt_replace_item(rel, attno, buf,
					  item->t_tid, (ZSBtreeItem *) deleteditem,
					  NIL);
	ReleaseBuffer(buf);	/* zsbt_replace_item unlocked */

	pfree(deleteditem);

	return TM_Ok;
}

/*
 * A new TID is allocated, as we see best and returned to the caller. This
 * function is only called for META attribute btree. Data columns will use the
 * returned tid to insert new items.
 */
TM_Result
zsbt_update(Relation rel, AttrNumber attno, zstid otid, Datum newdatum,
			bool newisnull, TransactionId xid, CommandId cid, Snapshot snapshot,
			Snapshot crosscheck, bool wait, TM_FailureData *hufd,
			zstid *newtid_p)
{
	TM_Result	result;

	/*
	 * This is currently only used on the meta-attribute. The other attributes
	 * don't need to carry visibility information, so the caller just inserts
	 * the new values with (multi_)insert() instead. This will change once we
	 * start doing the equivalent of HOT updates, where the TID doesn't change.
	 */
	Assert(attno == ZS_META_ATTRIBUTE_NUM);
	Assert(*newtid_p == InvalidZSTid);

	/*
	 * Find and lock the old item.
	 *
	 * TODO: If there's free TID space left on the same page, we should keep the
	 * buffer locked, and use the same page for the new tuple.
	 */
	result = zsbt_update_lock_old(rel, attno, otid,
								  xid, cid, snapshot,
								  crosscheck, wait, hufd);

	if (result != TM_Ok)
		return result;

	/* insert new version */
	zsbt_update_insert_new(rel, attno, newdatum, newisnull, newtid_p, xid, cid);

	/* update the old item with the "t_ctid pointer" for the new item */
	zsbt_mark_old_updated(rel, attno, otid, *newtid_p, xid, cid, snapshot);

	return TM_Ok;
}

/*
 * Subroutine of zsbt_update(): locks the old item for update.
 */
static TM_Result
zsbt_update_lock_old(Relation rel, AttrNumber attno, zstid otid,
					 TransactionId xid, CommandId cid, Snapshot snapshot,
					 Snapshot crosscheck, bool wait, TM_FailureData *hufd)
{
	ZSUndoRecPtr recent_oldest_undo = zsundo_get_oldest_undo_ptr(rel);
	Buffer		buf;
	ZSSingleBtreeItem *olditem;
	TM_Result	result;
	bool		keep_old_undo_ptr = true;

	/*
	 * Find the item to delete.
	 */
	olditem = zsbt_fetch(rel, attno, snapshot, &recent_oldest_undo, otid, &buf);
	if (olditem == NULL)
	{
		/*
		 * or should this be TM_Invisible? The heapam at least just throws
		 * an error, I think..
		 */
		elog(ERROR, "could not find old tuple to update with TID (%u, %u) for attribute %d",
			 ZSTidGetBlockNumber(otid), ZSTidGetOffsetNumber(otid), attno);
	}

	/*
	 * Is it visible to us?
	 */
	result = zs_SatisfiesUpdate(rel, snapshot, recent_oldest_undo,
								(ZSBtreeItem *) olditem, &keep_old_undo_ptr, hufd);
	if (result != TM_Ok)
	{
		UnlockReleaseBuffer(buf);
		/* FIXME: We should fill TM_FailureData *hufd correctly */
		return result;
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
zsbt_update_insert_new(Relation rel, AttrNumber attno,
					   Datum newdatum, bool newisnull, zstid *newtid,
					   TransactionId xid, CommandId cid)
{
	ZSUndoRecPtr undorecptr;

	ZSUndoRecPtrInitialize(&undorecptr);
	zsbt_multi_insert(rel, attno, &newdatum, &newisnull, newtid, 1,
					  xid, cid, attno == ZS_META_ATTRIBUTE_NUM ? &undorecptr : NULL);
}

/*
 * Subroutine of zsbt_update(): mark old item as updated.
 */
static void
zsbt_mark_old_updated(Relation rel, AttrNumber attno, zstid otid, zstid newtid,
					  TransactionId xid, CommandId cid, Snapshot snapshot)
{
	ZSUndoRecPtr recent_oldest_undo = zsundo_get_oldest_undo_ptr(rel);
	Buffer		buf;
	ZSSingleBtreeItem *olditem;
	TM_Result	result;
	bool		keep_old_undo_ptr = true;
	TM_FailureData tmfd;
	ZSUndoRecPtr undorecptr;
	ZSSingleBtreeItem *deleteditem;

	/*
	 * Find the item to delete.  It could be part of a compressed item,
	 * we let zsbt_fetch() handle that.
	 */
	olditem = zsbt_fetch(rel, attno, snapshot, &recent_oldest_undo, otid, &buf);
	if (olditem == NULL)
	{
		/*
		 * or should this be TM_Invisible? The heapam at least just throws
		 * an error, I think..
		 */
		elog(ERROR, "could not find old tuple to update with TID (%u, %u) for attribute %d",
			 ZSTidGetBlockNumber(otid), ZSTidGetOffsetNumber(otid), attno);
	}

	/*
	 * Is it visible to us?
	 */
	result = zs_SatisfiesUpdate(rel, snapshot, recent_oldest_undo,
								(ZSBtreeItem *) olditem, &keep_old_undo_ptr, &tmfd);
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
			undorec.prevundorec = olditem->t_undo_ptr;
		else
			ZSUndoRecPtrInitialize(&undorec.prevundorec);
		undorec.newtid = newtid;

		undorecptr = zsundo_insert(rel, &undorec.rec);
	}

	/* Replace the ZSBreeItem with an UPDATED item. */
	deleteditem = palloc(olditem->t_size);
	memcpy(deleteditem, olditem, olditem->t_size);
	deleteditem->t_flags |= ZSBT_UPDATED;
	deleteditem->t_undo_ptr = undorecptr;

	zsbt_replace_item(rel, attno, buf,
					  otid, (ZSBtreeItem *) deleteditem,
					  NIL);
	ReleaseBuffer(buf);		/* zsbt_recompress_replace released */

	pfree(deleteditem);
}

TM_Result
zsbt_lock_item(Relation rel, AttrNumber attno, zstid tid,
			   TransactionId xid, CommandId cid, Snapshot snapshot,
			   LockTupleMode lockmode, LockWaitPolicy wait_policy,
			   TM_FailureData *hufd)
{
	ZSUndoRecPtr recent_oldest_undo = zsundo_get_oldest_undo_ptr(rel);
	Buffer		buf;
	ZSSingleBtreeItem *item;
	TM_Result	result;
	bool		keep_old_undo_ptr = true;
	ZSUndoRecPtr undorecptr;
	ZSSingleBtreeItem *newitem;

	/*
	 * This is currently only used on the meta-attribute. The other attributes
	 * currently don't carry visibility information. This might need to change,
	 * if we start supporting locking individual (key) columns.
	 */
	Assert(attno == ZS_META_ATTRIBUTE_NUM);

	/* Find the item to delete. (It could be compressed) */
	item = zsbt_fetch(rel, attno, snapshot, &recent_oldest_undo, tid, &buf);
	if (item == NULL)
	{
		/*
		 * or should this be TM_Invisible? The heapam at least just throws
		 * an error, I think..
		 */
		elog(ERROR, "could not find tuple to delete with TID (%u, %u) for attribute %d",
			 ZSTidGetBlockNumber(tid), ZSTidGetOffsetNumber(tid), attno);
	}
	result = zs_SatisfiesUpdate(rel, snapshot, recent_oldest_undo,
								(ZSBtreeItem *) item, &keep_old_undo_ptr, hufd);
	if (result != TM_Ok)
	{
		UnlockReleaseBuffer(buf);
		/* FIXME: We should fill TM_FailureData *hufd correctly */
		return result;
	}

	if ((item->t_flags & ZSBT_DELETED) != 0)
		elog(ERROR, "cannot lock deleted tuple");

	if ((item->t_flags & ZSBT_UPDATED) != 0)
		elog(ERROR, "cannot lock updated tuple");

	/* Create UNDO record. */
	{
		ZSUndoRec_TupleLock undorec;

		undorec.rec.size = sizeof(ZSUndoRec_TupleLock);
		undorec.rec.type = ZSUNDO_TYPE_TUPLE_LOCK;
		undorec.rec.xid = xid;
		undorec.rec.cid = cid;
		undorec.rec.tid = tid;
		undorec.lockmode = lockmode;
		if (keep_old_undo_ptr)
			undorec.prevundorec = item->t_undo_ptr;
		else
			ZSUndoRecPtrInitialize(&undorec.prevundorec);

		undorecptr = zsundo_insert(rel, &undorec.rec);
	}

	/* Replace the item with an identical one, but with updated undo pointer. */
	newitem = palloc(item->t_size);
	memcpy(newitem, item, item->t_size);
	newitem->t_undo_ptr = undorecptr;

	zsbt_replace_item(rel, attno, buf,
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
zsbt_mark_item_dead(Relation rel, AttrNumber attno, zstid tid, ZSUndoRecPtr undoptr)
{
	Buffer		buf;
	ZSSingleBtreeItem *item;
	ZSSingleBtreeItem deaditem;

	/*
	 * This is currently only used on the meta-attribute. Other attributes can
	 * be deleted immediately.
	 */
	Assert(attno == ZS_META_ATTRIBUTE_NUM);

	/* Find the item to delete. (It could be compressed) */
	item = zsbt_fetch(rel, attno, NULL, NULL, tid, &buf);
	if (item == NULL)
	{
		elog(WARNING, "could not find tuple to mark dead with TID (%u, %u) for attribute %d",
			 ZSTidGetBlockNumber(tid), ZSTidGetOffsetNumber(tid), attno);
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

	zsbt_replace_item(rel, attno, buf,
					  tid, (ZSBtreeItem *) &deaditem,
					  NIL);
	ReleaseBuffer(buf); 	/* zsbt_replace_item released */
}

void
zsbt_remove_item(Relation rel, AttrNumber attno, zstid tid)
{
	Buffer		buf;
	ZSSingleBtreeItem *item;

	/* Find the item to delete. (It could be compressed) */
	item = zsbt_fetch(rel, attno, NULL, NULL, tid, &buf);
	if (item == NULL)
	{
		elog(WARNING, "could not find tuple to remove with TID (%u, %u) for attribute %d",
			 ZSTidGetBlockNumber(tid), ZSTidGetOffsetNumber(tid), attno);
		return;
	}

	/* remove it */
	zsbt_replace_item(rel, attno, buf,
					  tid, NULL,
					  NIL);
	ReleaseBuffer(buf); 	/* zsbt_replace_item released */
}

/*
 * Clear an item's UNDO pointer.
 *
 * This is used during VACUUM, to clear out aborted deletions.
 */
void
zsbt_undo_item_deletion(Relation rel, AttrNumber attno, zstid tid, ZSUndoRecPtr undoptr)
{
	Buffer		buf;
	ZSSingleBtreeItem *item;
	ZSSingleBtreeItem *copy;

	/*
	 * This is currently only used on the meta-attribute. Other attributes don't
	 * carry visibility information.
	 */
	Assert(attno == ZS_META_ATTRIBUTE_NUM);

	/* Find the item to delete. (It could be compressed) */
	item = zsbt_fetch(rel, attno, NULL, NULL, tid, &buf);
	if (item == NULL)
	{
		elog(WARNING, "could not find aborted tuple to remove with TID (%u, %u) for attribute %d",
			 ZSTidGetBlockNumber(tid), ZSTidGetOffsetNumber(tid), attno);
		return;
	}

	if (ZSUndoRecPtrEquals(item->t_undo_ptr, undoptr))
	{
		copy = palloc(item->t_size);
		memcpy(copy, item, item->t_size);
		copy->t_flags &= ~(ZSBT_DELETED | ZSBT_UPDATED);
		ZSUndoRecPtrInitialize(&copy->t_undo_ptr);
		zsbt_replace_item(rel, attno, buf,
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

/*
 * Find the leaf page containing the given key TID.
 */
static Buffer
zsbt_descend(Relation rel, AttrNumber attno, zstid key, int level)
{
	BlockNumber next;
	Buffer		buf;
	Page		page;
	ZSBtreePageOpaque *opaque;
	ZSBtreeInternalPageItem *items;
	int			nitems;
	int			itemno;
	BlockNumber rootblk;
	int			nextlevel = -1;
	BlockNumber failblk = InvalidBlockNumber;
	int16		attlen;
	bool		attbyval;

	/* start from root */
restart:
	rootblk = zsmeta_get_root_for_attribute(rel, attno, true, &attlen, &attbyval);

	if (rootblk == InvalidBlockNumber)
	{
		/* completely empty tree */
		return InvalidBuffer;
	}

	next = rootblk;
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
		opaque = ZSBtreePageGetOpaque(page);

		if (nextlevel == -1)
			nextlevel = opaque->zs_level;
		else if (opaque->zs_level != nextlevel)
			elog(ERROR, "unexpected level encountered when descending tree");

		/*
		 * Do we need to walk right? This could happen if the page was concurrently split.
		 */
		if (key >= opaque->zs_hikey)
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
			failblk = next;
			goto restart;
		}
		else
		{
			if (opaque->zs_level == level)
				return buf;

			/* Find the downlink and follow it */
			items = ZSBtreeInternalPageGetItems(page);
			nitems = ZSBtreeInternalPageGetNumItems(page);

			itemno = zsbt_binsrch_internal(key, items, nitems);
			if (itemno < 0)
				elog(ERROR, "could not descend tree for tid (%u, %u)",
					 ZSTidGetBlockNumber(key), ZSTidGetOffsetNumber(key));

			next = items[itemno].childblk;
			nextlevel--;
		}
		UnlockReleaseBuffer(buf);
	}
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
static zs_split_stack *
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
static zs_split_stack *
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
	parentbuf = zsbt_descend(rel, attno, leftlokey, level);
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
	splitthreshold = PageGetExactFreeSpace(page) * 0.90;

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
			origopaque->zs_flags &= ~ZSBT_ROOT;
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
 *
 * NOTE: you cannot remove the only leaf.
 */
static zs_split_stack *
zsbt_unlink_page(Relation rel, AttrNumber attno, Buffer buf, int level)
{
	Page		page = BufferGetPage(buf);
	ZSBtreePageOpaque *opaque = ZSBtreePageGetOpaque(page);
	Buffer		leftbuf;
	Buffer		rightbuf;
	bool		target_is_left;

	/* cannot currently remove the only page at its level. Just empty it. */
	if (opaque->zs_lokey == MinZSTid && opaque->zs_hikey == MaxPlusOneZSTid)
	{
		Page		newpage = PageGetTempPageCopySpecial(page);

		return zs_new_split_stack_entry(buf, newpage);
	}

	/*
	 * Find left sibling.
	 * or if this is leftmost page, find right sibling.
	 */
	if (opaque->zs_lokey != MinZSTid)
	{
		rightbuf = buf;
		leftbuf = zsbt_descend(rel, attno, opaque->zs_lokey - 1, level);
		target_is_left = false;
	}
	else
	{
		rightbuf = zsbt_descend(rel, attno, opaque->zs_hikey, level);
		leftbuf = buf;
		target_is_left = true;
	}

	return zsbt_merge_pages(rel, attno, leftbuf, rightbuf, target_is_left);
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

	Assert(ZSBtreePageGetOpaque(leftpage)->zs_level == ZSBtreePageGetOpaque(rightpage)->zs_level);

	stack = zs_new_split_stack_entry(leftbuf, leftpage);
	stack_head = stack_tail = stack;

	/* Mark right page as empty/unused */
	rightpage = palloc0(BLCKSZ);

	stack = zs_new_split_stack_entry(rightbuf, rightpage);
	stack->recycle = true;
	stack_tail->next = stack;
	stack_tail = stack;

	/* find downlink for 'rightbuf' in the parent */
	parentbuf = zsbt_descend(rel, attno, rightopaque->zs_lokey, leftopaque->zs_level + 1);
	parentpage = BufferGetPage(parentbuf);

	parentitems = ZSBtreeInternalPageGetItems(parentpage);
	parentnitems = ZSBtreeInternalPageGetNumItems(parentpage);
	itemno = zsbt_binsrch_internal(rightopaque->zs_lokey, parentitems, parentnitems);
	if (itemno < 0 || parentitems[itemno].childblk != BufferGetBlockNumber(rightbuf))
		elog(ERROR, "could not find downlink to FPM page %u", BufferGetBlockNumber(rightbuf));

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
	}

	return stack_head;
}

static ZSSingleBtreeItem *
zsbt_fetch(Relation rel, AttrNumber attno, Snapshot snapshot, ZSUndoRecPtr *recent_oldest_undo,
		   zstid tid, Buffer *buf_p)
{
	int16		attlen;
	bool		attbyval;
	Buffer		buf;
	Page		page;
	ZSBtreeItem *item = NULL;
	bool		found = false;
	OffsetNumber maxoff;
	OffsetNumber off;

	/*
	 * Fetch attlen/attbyval. We might need them to extract the single item
	 * from an array item.
	 *
	 * XXX: This is duplicative with the zsmeta_get_root_for_attribute()
	 * call in zsbt_descend.
	 */
	(void) zsmeta_get_root_for_attribute(rel, attno, true, &attlen, &attbyval);

	buf = zsbt_descend(rel, attno, tid, 0);
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

	if (found && snapshot)
	{
		/*
		 * Ok, we have the item that covers the target TID now, in 'item'. Check
		 * if it's visible.
		 */
		/* FIXME: dummmy scan */
		ZSBtreeScan scan;
		memset(&scan, 0, sizeof(scan));
		scan.rel = rel;
		scan.snapshot = snapshot;
		scan.recent_oldest_undo = *recent_oldest_undo;

		if (!zs_SatisfiesVisibility(&scan, item))
			found = false;
	}

	if (found)
	{
		ZSSingleBtreeItem *result;

		if ((item->t_flags & ZSBT_ARRAY) != 0)
		{
			ZSArrayBtreeItem *aitem = (ZSArrayBtreeItem *) item;
			int			elemno = tid - aitem->t_tid;
			char	   *dataptr = NULL;
			int			datasz;
			int			resultsize;

			Assert(elemno < aitem->t_nelements);

			if ((item->t_flags & ZSBT_NULL) == 0)
			{
				if (attlen > 0)
				{
					dataptr = aitem->t_payload + elemno * attlen;
					datasz = attlen;
				}
				else
				{
					dataptr = aitem->t_payload;
					for (int i = 0; i < elemno; i++)
					{
						dataptr += zs_datumGetSize(PointerGetDatum(dataptr), attbyval, attlen);
					}
					datasz = zs_datumGetSize(PointerGetDatum(dataptr), attbyval, attlen);
				}
			}
			else
				datasz = 0;

			resultsize = offsetof(ZSSingleBtreeItem, t_payload) + datasz;
			result = palloc(resultsize);
			memset(result, 0, offsetof(ZSSingleBtreeItem, t_payload)); /* zero padding */
			result->t_tid = tid;
			result->t_flags = item->t_flags & ~ZSBT_ARRAY;
			result->t_size = resultsize;
			result->t_undo_ptr = aitem->t_undo_ptr;
			if (datasz > 0)
				memcpy(result->t_payload, dataptr, datasz);
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
 * Compute the size of a slice of an array, from an array item. 'dataptr'
 * points to the packed on-disk representation of the array item's data.
 * The elements are stored one after each other.
 */
static Size
zsbt_get_array_slice_len(int16 attlen, bool attbyval, bool isnull,
						 char *dataptr, int nelements)
{
	Size		datasz;

	if (isnull)
		datasz = 0;
	else
	{
		/*
		 * For a fixed-width type, we can just multiply. For variable-length,
		 * we have to walk through the elements, looking at the length of each
		 * element.
		 */
		if (attlen > 0)
		{
			datasz = attlen * nelements;
		}
		else
		{
			char	   *p = dataptr;

			datasz = 0;
			for (int i = 0; i < nelements; i++)
			{
				Size		datumsz;

				datumsz = zs_datumGetSize(PointerGetDatum(p), attbyval, attlen);

				/*
				 * The array should already use short varlen representation whenever
				 * possible.
				 */
				Assert(!VARATT_CAN_MAKE_SHORT(DatumGetPointer(p)));

				datasz += datumsz;
				p += datumsz;
			}
		}
	}
	return datasz;
}


/* Does att's datatype allow packing into the 1-byte-header varlena format? */
#define ATT_IS_PACKABLE(att) \
	((att)->attlen == -1 && (att)->attstorage != 'p')
/* Use this if it's already known varlena */
#define VARLENA_ATT_IS_PACKABLE(att) \
	((att)->attstorage != 'p')

/*
 * This is very similar to heap_compute_data_size()
 */
static Size
zsbt_compute_data_size(Form_pg_attribute atti, Datum val, bool isnull)
{
	Size		data_length = 0;

	if (isnull)
		return 0;

	if (ATT_IS_PACKABLE(atti) &&
		VARATT_CAN_MAKE_SHORT(DatumGetPointer(val)))
	{
		/*
		 * we're anticipating converting to a short varlena header, so
		 * adjust length and don't count any alignment
		 */
		data_length += VARATT_CONVERTED_SHORT_SIZE(DatumGetPointer(val));
	}
	else if (atti->attlen == -1 &&
			 VARATT_IS_EXTERNAL_EXPANDED(DatumGetPointer(val)))
	{
		/*
		 * we want to flatten the expanded value so that the constructed
		 * tuple doesn't depend on it
		 */
		data_length = att_align_nominal(data_length, atti->attalign);
		data_length += EOH_get_flat_size(DatumGetEOHP(val));
	}
	else if (atti->attlen == -1 &&
			 VARATT_IS_EXTERNAL(val) && VARTAG_EXTERNAL(val) == VARTAG_ZEDSTORE)
	{
		data_length += sizeof(varatt_zs_toastptr);
	}
	else
	{
		data_length = att_align_datum(data_length, atti->attalign,
									  atti->attlen, val);
		data_length = att_addlength_datum(data_length, atti->attlen,
										  val);
	}

	return data_length;
}

/*
 * Form a ZSBtreeItem out of the given datums, or data that's already in on-disk
 * array format, for insertion.
 *
 * If there's more than one element, an array item is created. Otherwise, a single
 * item.
 */
static ZSBtreeItem *
zsbt_create_item(Form_pg_attribute att, zstid tid, ZSUndoRecPtr undo_ptr,
				 int nelements, Datum *datums,
				 char *datasrc, Size datasz, bool isnull)
{
	ZSBtreeItem *result;
	Size		itemsz;
	char	   *databegin;

	Assert(nelements > 0);

	if (nelements > 1)
	{
		ZSArrayBtreeItem *newitem;

		itemsz = offsetof(ZSArrayBtreeItem, t_payload) + datasz;

		newitem = palloc(itemsz);
		memset(newitem, 0, offsetof(ZSArrayBtreeItem, t_payload)); /* zero padding */
		newitem->t_tid = tid;
		newitem->t_size = itemsz;
		newitem->t_flags = ZSBT_ARRAY;
		if (isnull)
			newitem->t_flags |= ZSBT_NULL;
		newitem->t_nelements = nelements;
		newitem->t_undo_ptr = undo_ptr;

		databegin = newitem->t_payload;

		result = (ZSBtreeItem *) newitem;
	}
	else
	{
		ZSSingleBtreeItem *newitem;

		itemsz = offsetof(ZSSingleBtreeItem, t_payload) + datasz;

		newitem = palloc(itemsz);
		memset(newitem, 0, offsetof(ZSSingleBtreeItem, t_payload)); /* zero padding */
		newitem->t_tid = tid;
		newitem->t_flags = 0;
		if (isnull)
			newitem->t_flags |= ZSBT_NULL;
		newitem->t_size = itemsz;
		newitem->t_undo_ptr = undo_ptr;

		databegin = newitem->t_payload;

		result = (ZSBtreeItem *) newitem;
	}

	/*
	 * Copy the data.
	 *
	 * This is largely copied from heaptuple.c's fill_val().
	 */
	if (!isnull)
	{
		char	   *data = databegin;

		if (datums)
		{
			for (int i = 0; i < nelements; i++)
			{
				Datum		datum = datums[i];
				Size		data_length;

				/*
				 * XXX we use the att_align macros on the pointer value itself, not on an
				 * offset.  This is a bit of a hack.
				 */
				if (att->attbyval)
				{
					/* pass-by-value */
					data = (char *) att_align_nominal(data, att->attalign);
					store_att_byval(data, datum, att->attlen);
					data_length = att->attlen;
				}
				else if (att->attlen == -1)
				{
					/* varlena */
					Pointer		val = DatumGetPointer(datum);

					if (VARATT_IS_EXTERNAL(val))
					{
						if (VARATT_IS_EXTERNAL_EXPANDED(val))
						{
							/*
							 * we want to flatten the expanded value so that the
							 * constructed tuple doesn't depend on it
							 */
							/* FIXME: This should happen earlier, because if the
							 * datum is very large, it should be toasted, and
							 * that should happen earlier.
							 */
							ExpandedObjectHeader *eoh = DatumGetEOHP(datum);

							data = (char *) att_align_nominal(data,
															  att->attalign);
							data_length = EOH_get_flat_size(eoh);
							EOH_flatten_into(eoh, data, data_length);
						}
						else if (VARATT_IS_EXTERNAL(val) && VARTAG_EXTERNAL(val) == VARTAG_ZEDSTORE)
						{
							data_length = sizeof(varatt_zs_toastptr);
							memcpy(data, val, data_length);
						}
						else
						{
							/* no alignment, since it's short by definition */
							data_length = VARSIZE_EXTERNAL(val);
							memcpy(data, val, data_length);
						}
					}
					else if (VARATT_IS_SHORT(val))
					{
						/* no alignment for short varlenas */
						data_length = VARSIZE_SHORT(val);
						memcpy(data, val, data_length);
					}
					else if (VARLENA_ATT_IS_PACKABLE(att) &&
							 VARATT_CAN_MAKE_SHORT(val))
					{
						/* convert to short varlena -- no alignment */
						data_length = VARATT_CONVERTED_SHORT_SIZE(val);
						SET_VARSIZE_SHORT(data, data_length);
						memcpy(data + 1, VARDATA(val), data_length - 1);
					}
					else
					{
						/* full 4-byte header varlena */
						data = (char *) att_align_nominal(data,
														  att->attalign);
						data_length = VARSIZE(val);
						memcpy(data, val, data_length);
					}
				}
				else if (att->attlen == -2)
				{
					/* cstring ... never needs alignment */
					Assert(att->attalign == 'c');
					data_length = strlen(DatumGetCString(datum)) + 1;
					memcpy(data, DatumGetPointer(datum), data_length);
				}
				else
				{
					/* fixed-length pass-by-reference */
					data = (char *) att_align_nominal(data, att->attalign);
					Assert(att->attlen > 0);
					data_length = att->attlen;
					memcpy(data, DatumGetPointer(datum), data_length);
				}
				data += data_length;
			}
			Assert(data - databegin == datasz);
		}
		else
			memcpy(data, datasrc, datasz);
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
zsbt_replace_item(Relation rel, AttrNumber attno, Buffer buf,
				  zstid oldtid,
				  ZSBtreeItem *replacementitem,
				  List       *newitems)
{
	Form_pg_attribute attr;
	int16		attlen;
	bool		attbyval;
	Page		page = BufferGetPage(buf);
	OffsetNumber off;
	OffsetNumber maxoff;
	List	   *items;
	bool		found_old_item = false;
	/* We might need to decompress up to two previously compressed items */
	ZSDecompressContext decompressor;
	bool		decompressor_used = false;
	bool		decompressing;

	if (attno == ZS_META_ATTRIBUTE_NUM)
	{
		attr = NULL;
		attlen = 0;
		attbyval = true;
	}
	else
	{
		attr = &rel->rd_att->attrs[attno - 1];
		attlen = attr->attlen;
		attbyval = attr->attbyval;
	}

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
				Size		olddatalen;
				int			nelements = aitem->t_nelements;
				bool		isnull = (aitem->t_flags & ZSBT_NULL) != 0;
				char	   *dataptr;

				cutoff = oldtid - item->t_tid;

				/* Array slice before the target TID */
				dataptr = aitem->t_payload;
				if (cutoff > 0)
				{
					ZSBtreeItem *item1;
					Size		datalen1;

					datalen1 = zsbt_get_array_slice_len(attlen, attbyval, isnull,
														dataptr, cutoff);
					item1 = zsbt_create_item(attr, aitem->t_tid, aitem->t_undo_ptr,
											 cutoff, NULL, dataptr, datalen1, isnull);
					dataptr += datalen1;
					items = lappend(items, item1);
				}

				/*
				 * Skip over the target element, and store the replacement
				 * item, if any, in its place
				 */
				olddatalen = zsbt_get_array_slice_len(attlen, attbyval, isnull,
													  dataptr, 1);
				dataptr += olddatalen;
				if (replacementitem)
					items = lappend(items, replacementitem);

				/* Array slice after the target */
				if (cutoff + 1 < nelements)
				{
					ZSBtreeItem *item2;
					Size		datalen2;

					datalen2 = zsbt_get_array_slice_len(attlen, attbyval, isnull,
														dataptr, nelements - (cutoff + 1));
					item2 = zsbt_create_item(attr, oldtid + 1, aitem->t_undo_ptr,
											 nelements - (cutoff + 1), NULL, dataptr, datalen2, isnull);
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
		zsbt_recompress_replace(rel, attno, buf, items);
	}
	else
	{
		zs_split_stack *stack;

		stack = zsbt_unlink_page(rel, attno, buf, 0);

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

	AttrNumber	attno;
	zstid		hikey;
} zsbt_recompress_context;

static void
zsbt_recompress_newpage(zsbt_recompress_context *cxt, zstid nexttid, int flags)
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

static void
zsbt_recompress_add_to_page(zsbt_recompress_context *cxt, ZSBtreeItem *item)
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
zsbt_recompress_add_to_compressor(zsbt_recompress_context *cxt, ZSBtreeItem *item)
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
zsbt_recompress_flush(zsbt_recompress_context *cxt)
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
zsbt_recompress_replace(Relation rel, AttrNumber attno, Buffer oldbuf, List *items)
{
	ListCell   *lc;
	zsbt_recompress_context cxt;
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
	cxt.attno = attno;
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
