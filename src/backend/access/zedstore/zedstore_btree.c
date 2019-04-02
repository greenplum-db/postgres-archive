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

#include "access/htup_details.h"
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
static Buffer zsbt_descend(Relation rel, BlockNumber rootblk, zstid key);
static Buffer zsbt_find_downlink(Relation rel, AttrNumber attno,
				   zstid key, BlockNumber childblk, int level,
				   int *itemno);
static void zsbt_recompress_replace(Relation rel, AttrNumber attno,
									Buffer oldbuf, List *items);
static void zsbt_insert_downlink(Relation rel, AttrNumber attno, Buffer leftbuf,
					 zstid rightlokey, BlockNumber rightblkno);
static void zsbt_split_internal_page(Relation rel, AttrNumber attno, Buffer leftbuf, Buffer childbuf,
					OffsetNumber newoff, zstid newkey, BlockNumber childblk);
static void zsbt_newroot(Relation rel, AttrNumber attno, int level,
			 zstid key1, BlockNumber blk1,
			 zstid key2, BlockNumber blk2,
			 Buffer leftchildbuf);
static ZSBtreeItem *zsbt_scan_next_internal(ZSBtreeScan *scan);
static void zsbt_replace_item(Relation rel, AttrNumber attno, Buffer buf,
				  ZSBtreeItem *olditem, ZSBtreeItem *replacementitem, ZSBtreeItem *newitem);

static int zsbt_binsrch_internal(zstid key, ZSBtreeInternalPageItem *arr, int arr_elems);

/* ----------------------------------------------------------------
 *						 Public interface
 * ----------------------------------------------------------------
 */

/*
 * Begin a scan of the btree.
 */
void
zsbt_begin_scan(Relation rel, AttrNumber attno, zstid starttid, Snapshot snapshot, ZSBtreeScan *scan)
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
		scan->lastbuf_is_locked = false;
		scan->lastoff = InvalidOffsetNumber;
		scan->snapshot = NULL;
		memset(&scan->recent_oldest_undo, 0, sizeof(scan->recent_oldest_undo));
		scan->nexttid = InvalidZSTid;
		return;
	}

	buf = zsbt_descend(rel, rootblk, starttid);
	LockBuffer(buf, BUFFER_LOCK_UNLOCK);

	scan->rel = rel;
	scan->attno = attno;
	scan->snapshot = snapshot;
	scan->for_update = false;		/* caller can change this */

	scan->active = true;
	scan->lastbuf = buf;
	scan->lastbuf_is_locked = false;
	scan->lastoff = InvalidOffsetNumber;
	scan->nexttid = starttid;

	scan->has_decompressed = false;
	zs_decompress_init(&scan->decompressor);

	memset(&scan->recent_oldest_undo, 0, sizeof(scan->recent_oldest_undo));
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
}

/*
 * Return true if there was another tuple. The datum is returned in *datum,
 * and its TID in *tid. For a pass-by-ref datum, it's a palloc'd copy.
 */
bool
zsbt_scan_next(ZSBtreeScan *scan, Datum *datum, bool *isnull, zstid *tid)
{
	TupleDesc	desc;
	Form_pg_attribute attr;
	ZSBtreeItem *item;

	if (!scan->active)
		return false;

	desc = RelationGetDescr(scan->rel);
	attr = &desc->attrs[scan->attno - 1];

	while ((item = zsbt_scan_next_internal(scan)) != NULL)
	{
		if (zs_SatisfiesVisibility(scan, item))
		{
			char		*ptr = item->t_payload;

			*tid = item->t_tid;
			if (item->t_flags & ZSBT_NULL)
				*isnull = true;
			else
			{
				*isnull = false;
				*datum = fetchatt(attr, ptr);
				*datum = zs_datumCopy(*datum, attr->attbyval, attr->attlen);
			}

			if (scan->lastbuf_is_locked)
			{
				LockBuffer(scan->lastbuf, BUFFER_LOCK_UNLOCK);
				scan->lastbuf_is_locked = false;
			}

			return true;
		}
	}
	return false;
}

/*
 * Get the last tid (plus one) in the tree.
 */
zstid
zsbt_get_last_tid(Relation rel, AttrNumber attno)
{
	BlockNumber	rootblk;
	zstid		rightmostkey;
	zstid		tid;
	Buffer		buf;
	Page		page;
	ZSBtreePageOpaque *opaque;
	OffsetNumber maxoff;

	/* Find the rightmost leaf */
	rootblk = zsmeta_get_root_for_attribute(rel, attno, true);
	rightmostkey = MaxZSTid;
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
		ZSBtreeItem	*hitup = (ZSBtreeItem *) PageGetItem(page, iid);

		tid = hitup->t_tid;
		tid = ZSTidIncrement(tid);
	}
	else
	{
		tid = opaque->zs_lokey;
	}
	UnlockReleaseBuffer(buf);

	return tid;
}

/*
 * Insert a new datum to the given attribute's btree.
 *
 * Returns the TID of the new tuple.
 *
 * If 'tid' is valid, then that TID is used. It better not be in use already. If
 * it's invalid, then a new TID is allocated, as we see best. (When inserting the
 * first column of the row, pass invalid, and for other columns, pass the TID
 * you got for the first column.)
 */
zstid
zsbt_insert(Relation rel, AttrNumber attno, Datum datum, bool isnull,
			TransactionId xid, CommandId cid, zstid tid)
{
	TupleDesc	desc = RelationGetDescr(rel);
	Form_pg_attribute attr = &desc->attrs[attno - 1];
	BlockNumber	rootblk;
	Buffer		buf;
	Page		page;
	ZSBtreePageOpaque *opaque;
	OffsetNumber maxoff;
	zstid		lasttid;
	Size		datumsz;
	Size		itemsz;
	ZSBtreeItem	*newitem;
	char	   *dataptr;
	ZSUndoRecPtr undorecptr;
	zstid		insert_target_key;

	rootblk = zsmeta_get_root_for_attribute(rel, attno, true);

	/*
	 * If TID was given, find the right place for it. Otherwise, insert to
	 * the rightmost leaf.
	 *
	 * TODO: use a Free Space Map to find suitable target.
	 */
	if (tid != InvalidZSTid)
		insert_target_key = tid;
	else
		insert_target_key = MaxZSTid;

	buf = zsbt_descend(rel, rootblk, insert_target_key);
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

		if ((hitup->t_flags & ZSBT_COMPRESSED) != 0)
			lasttid = hitup->t_lasttid;
		else
			lasttid = hitup->t_tid;

		if (tid == InvalidZSTid)
		{
			tid = lasttid;
			tid = ZSTidIncrement(tid);
		}
	}
	else
	{
		lasttid = opaque->zs_lokey;
		if (tid == InvalidZSTid)
			tid = lasttid;
	}

	/* Form an undo record */
	{
		ZSUndoRec_Insert undorec;

		undorec.rec.size = sizeof(ZSUndoRec_Insert);
		undorec.rec.type = ZSUNDO_TYPE_INSERT;
		undorec.rec.attno = attno;
		undorec.rec.xid = xid;
		undorec.rec.cid = cid;
		undorec.rec.tid = tid;

		undorecptr = zsundo_insert(rel, &undorec.rec);
	}

	/*
	 * Form a ZSBtreeItem to insert.
	 */
	if (isnull)
		datumsz = 0;
	else
		datumsz = zs_datumGetSize(datum, attr->attbyval, attr->attlen);
	itemsz = offsetof(ZSBtreeItem, t_payload) + datumsz;

	newitem = palloc(itemsz);
	newitem->t_tid = tid;
	newitem->t_flags = 0;
	newitem->t_size = itemsz;
	newitem->t_undo_ptr = undorecptr;

	if (isnull)
		newitem->t_flags |= ZSBT_NULL;
	else
	{
		dataptr = ((char *) newitem) + offsetof(ZSBtreeItem, t_payload);
		if (attr->attbyval)
			store_att_byval(dataptr, datum, attr->attlen);
		else
			memcpy(dataptr, DatumGetPointer(datum), datumsz);
	}

	/*
	 * If there's enough space on the page, insert it directly. Otherwise, try to
	 * compress all existing items. If that still doesn't create enough space, we
	 * have to split the page.
	 *
	 * TODO: We also resort to the slow way, if the new TID is not at the end of
	 * the page. Things get difficult, if the new TID is covered by the range of
	 * an existing compressed item.
	 */
	if (PageGetFreeSpace(page) >= MAXALIGN(itemsz) &&
		(maxoff > FirstOffsetNumber || tid > lasttid))
	{
		OffsetNumber off;

		off = PageAddItemExtended(page, (Item) newitem, itemsz, maxoff + 1, PAI_OVERWRITE);
		if (off == InvalidOffsetNumber)
			elog(ERROR, "didn't fit, after all?");
		MarkBufferDirty(buf);
		/* TODO: WAL-log */

		UnlockReleaseBuffer(buf);

		return tid;
	}
	else
	{
		/* recompress and possibly split the page */
		zsbt_replace_item(rel, attno, buf, NULL, NULL, newitem);
		/* zsbt_replace_item unlocked 'buf' */
		ReleaseBuffer(buf);

		return tid;
	}
}

TM_Result
zsbt_delete(Relation rel, AttrNumber attno, zstid tid,
			TransactionId xid, CommandId cid,
			Snapshot snapshot, Snapshot crosscheck, bool wait,
			TM_FailureData *hufd, bool changingPart)
{
	ZSBtreeScan scan;
	ZSBtreeItem *item;
	TM_Result	result;
	ZSUndoRecPtr undorecptr;
	ZSBtreeItem *deleteditem;

	zsbt_begin_scan(rel, attno, tid, snapshot, &scan);
	scan.for_update = true;

	/* Find the item to delete. (It could be compressed) */
	item = zsbt_scan_next_internal(&scan);
	if (item->t_tid != tid)
	{
		/*
		 * or should this be TM_Invisible? The heapam at least just throws
		 * an error, I think..
		 */
		elog(ERROR, "could not find tuple to delete with TID (%u, %u)",
			 ZSTidGetBlockNumber(tid), ZSTidGetOffsetNumber(tid));
	}
	result = zs_SatisfiesUpdate(&scan, item);
	if (result != TM_Ok)
	{
		zsbt_end_scan(&scan);
		return result;
	}

	/* Create UNDO record. */
	{
		ZSUndoRec_Delete undorec;

		undorec.rec.size = sizeof(ZSUndoRec_Delete);
		undorec.rec.type = ZSUNDO_TYPE_DELETE;
		undorec.rec.attno = attno;
		undorec.rec.xid = xid;
		undorec.rec.cid = cid;
		undorec.rec.tid = tid;

		undorecptr = zsundo_insert(rel, &undorec.rec);
	}

	/* Replace the ZSBreeItem with a DELETED item. */
	deleteditem = palloc(item->t_size);
	memcpy(deleteditem, item, item->t_size);
	deleteditem->t_flags |= ZSBT_DELETED;
	deleteditem->t_undo_ptr = undorecptr;

	zsbt_replace_item(rel, attno, scan.lastbuf, item, deleteditem, NULL);
	scan.lastbuf_is_locked = false;	/* zsbt_replace_item released */
	zsbt_end_scan(&scan);

	return TM_Ok;
}


/*
 * If 'newtid' is valid, then that TID is used for the new item. It better not
 * be in use already. If it's invalid, then a new TID is allocated, as we see
 * best. (When inserting the first column of the row, pass invalid, and for
 * other columns, pass the TID you got for the first column.)
 */
TM_Result
zsbt_update(Relation rel, AttrNumber attno, zstid otid, Datum newdatum,
			bool newisnull, TransactionId xid, CommandId cid, Snapshot snapshot,
			Snapshot crosscheck, bool wait, TM_FailureData *hufd,
			zstid *newtid_p)
{
	TupleDesc	desc = RelationGetDescr(rel);
	Form_pg_attribute attr = &desc->attrs[attno - 1];
	ZSBtreeScan scan;
	ZSBtreeItem *olditem;
	TM_Result	result;
	ZSUndoRecPtr undorecptr;
	ZSBtreeItem *deleteditem;
	ZSBtreeItem *newitem;
	OffsetNumber maxoff;
	Buffer		buf;
	Page		page;
	ZSBtreePageOpaque *opaque;
	zstid		newtid = *newtid_p;
	Size		datumsz;
	Size		newitemsz;
	char	   *dataptr;

	/*
	 * Find the item to delete.  It could be part of a compressed item,
	 * we let zsbt_scan_next_internal() handle that.
	 */
	zsbt_begin_scan(rel, attno, otid, snapshot, &scan);
	scan.for_update = true;

	olditem = zsbt_scan_next_internal(&scan);
	if (olditem->t_tid != otid)
	{
		/*
		 * or should this be TM_Invisible? The heapam at least just throws
		 * an error, I think..
		 */
		elog(ERROR, "could not find tuple to delete with TID (%u, %u)",
			 ZSTidGetBlockNumber(otid), ZSTidGetOffsetNumber(otid));
	}

	/*
	 * Is it visible to us?
	 */
	result = zs_SatisfiesUpdate(&scan, olditem);
	if (result != TM_Ok)
	{
		zsbt_end_scan(&scan);
		return result;
	}

	/*
	 * Look at the last item, for its tid. We will use that + 1, as the TID of
	 * the new item.
	 */
	buf = scan.lastbuf;
	page = BufferGetPage(buf);
	opaque = ZSBtreePageGetOpaque(page);
	maxoff = PageGetMaxOffsetNumber(page);
	if (maxoff >= FirstOffsetNumber)
	{
		ItemId		iid = PageGetItemId(page, maxoff);
		ZSBtreeItem	*hitup = (ZSBtreeItem *) PageGetItem(page, iid);

		if ((hitup->t_flags & ZSBT_COMPRESSED) != 0)
			newtid = hitup->t_lasttid;
		else
			newtid = hitup->t_tid;
		newtid = ZSTidIncrement(newtid);
	}
	else
	{
		newtid = opaque->zs_lokey;
	}

	if (newtid >= opaque->zs_hikey)
	{
		/* no more free TIDs on the page. Bail out */
		/*
		 * TODO: what we should do, is to find another target page for the
		 * new tuple.
		 */
		elog(ERROR, "out of TID space on page");
	}

	/* Create UNDO record. */
	{
		ZSUndoRec_Update undorec;

		undorec.rec.size = sizeof(ZSUndoRec_Update);
		undorec.rec.type = ZSUNDO_TYPE_UPDATE;
		undorec.rec.attno = attno;
		undorec.rec.xid = xid;
		undorec.rec.cid = cid;
		undorec.rec.tid = newtid;
		undorec.otid = otid;

		undorecptr = zsundo_insert(rel, &undorec.rec);
	}

	/* Replace the ZSBreeItem with an UPDATED item. */
	deleteditem = palloc(olditem->t_size);
	memcpy(deleteditem, olditem, olditem->t_size);
	deleteditem->t_flags |= ZSBT_UPDATED;
	deleteditem->t_undo_ptr = undorecptr;

	/*
	 * Form a ZSBtreeItem to insert.
	 */
	if (newisnull)
		datumsz = 0;
	else
		datumsz = zs_datumGetSize(newdatum, attr->attbyval, attr->attlen);
	newitemsz = offsetof(ZSBtreeItem, t_payload) + datumsz;

	newitem = palloc(newitemsz);
	newitem->t_tid = newtid;
	newitem->t_flags = 0;
	newitem->t_size = newitemsz;
	newitem->t_undo_ptr = undorecptr;

	if (newisnull)
		newitem->t_flags |= ZSBT_NULL;
	else
	{
		dataptr = ((char *) newitem) + offsetof(ZSBtreeItem, t_payload);
		if (attr->attbyval)
			store_att_byval(dataptr, newdatum, attr->attlen);
		else
			memcpy(dataptr, DatumGetPointer(newdatum), datumsz);
	}

	zsbt_replace_item(rel, attno, scan.lastbuf, olditem, deleteditem, newitem);
	scan.lastbuf_is_locked = false;	/* zsbt_recompress_replace released */
	zsbt_end_scan(&scan);

	*newtid_p = newtid;
	return TM_Ok;
}

/* ----------------------------------------------------------------
 *						 Internal routines
 * ----------------------------------------------------------------
 */

/*
 * Find the leaf buffer containing the given key TID.
 */
static Buffer
zsbt_descend(Relation rel, BlockNumber rootblk, zstid key)
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
		if (key >= opaque->zs_hikey)
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
					 ZSTidGetBlockNumber(key), ZSTidGetOffsetNumber(key));
			next = items[itemno].childblk;
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
				   zstid key, BlockNumber childblk, int level,
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
		if (key >= opaque->zs_hikey)
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
					 ZSTidGetBlockNumber(key), ZSTidGetOffsetNumber(key));

			if (opaque->zs_level == level + 1)
			{
				if (items[itemno].childblk != childblk)
					elog(ERROR, "could not re-find downlink for block %u", childblk);
				*itemno_p = itemno;
				return buf;
			}

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
 *
 */
static void
zsbt_newroot(Relation rel, AttrNumber attno, int level,
			 zstid key1, BlockNumber blk1,
			 zstid key2, BlockNumber blk2,
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

	Assert(key1 < key2);

	buf = zs_getnewbuf(rel);
	page = BufferGetPage(buf);
	PageInit(page, BLCKSZ, sizeof(ZSBtreePageOpaque));
	opaque = ZSBtreePageGetOpaque(page);
	opaque->zs_next = InvalidBlockNumber;
	opaque->zs_lokey = MinZSTid;
	opaque->zs_hikey = MaxPlusOneZSTid;
	opaque->zs_level = level;
	opaque->zs_flags = 0;
	opaque->zs_page_id = ZS_BTREE_PAGE_ID;

	items = ZSBtreeInternalPageGetItems(page);
	items[0].tid = key1;
	items[0].childblk =  blk1;
	items[1].tid = key2;
	items[1].childblk = blk2;
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
 *
 * On entry, 'leftbuf' must be pinned exclusive-locked. It is released on exit.
 */
static void
zsbt_insert_downlink(Relation rel, AttrNumber attno, Buffer leftbuf,
					 zstid rightlokey, BlockNumber rightblkno)
{
	BlockNumber	leftblkno = BufferGetBlockNumber(leftbuf);
	Page		leftpage = BufferGetPage(leftbuf);
	ZSBtreePageOpaque *leftopaque = ZSBtreePageGetOpaque(leftpage);
	zstid		leftlokey = leftopaque->zs_lokey;
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
	if (itemno < 1 || items[itemno].tid != leftlokey ||
		items[itemno].childblk != leftblkno)
	{
		elog(ERROR, "could not find downlink");
	}
	itemno++;

	if (ZSBtreeInternalPageIsFull(parentpage))
	{
		/* split internal page */
		zsbt_split_internal_page(rel, attno, parentbuf, leftbuf, itemno, rightlokey, rightblkno);
	}
	else
	{
		/* insert the new downlink for the right page. */
		memmove(&items[itemno + 1],
				&items[itemno],
				(nitems - itemno) * sizeof(ZSBtreeInternalPageItem));
		items[itemno].tid = rightlokey;
		items[itemno].childblk = rightblkno;
		((PageHeader) parentpage)->pd_lower += sizeof(ZSBtreeInternalPageItem);

		leftopaque->zs_flags &= ~ZS_FOLLOW_RIGHT;

		/* TODO: WAL-log */

		MarkBufferDirty(leftbuf);
		MarkBufferDirty(parentbuf);
		UnlockReleaseBuffer(leftbuf);
		UnlockReleaseBuffer(parentbuf);
	}
}

/*
 * Split an internal page.
 *
 * The new downlink specified by 'newkey' and 'childblk' is inserted to
 * position 'newoff', on 'leftbuf'. The page is split.
 */
static void
zsbt_split_internal_page(Relation rel, AttrNumber attno, Buffer leftbuf, Buffer childbuf,
						 OffsetNumber newoff, zstid newkey, BlockNumber childblk)
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
	zstid		splittid;
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
	newitemonleft = (newkey < splittid);

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
	newitem.childblk = childblk;

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

	/* recurse to insert downlink. (this releases 'leftbuf') */
	zsbt_insert_downlink(rel, attno, leftbuf, splittid, rightblkno);
}

/*
 * Returns the next item in the scan. This doesn't pay attention to visibility.
 *
 * The returned pointer might point directly to a btree-buffer, or it might be
 * palloc'd copy. If it points to a buffer, scan->lastbuf_is_locked is true,
 * otherwise false.
 */
static ZSBtreeItem *
zsbt_scan_next_internal(ZSBtreeScan *scan)
{
	Buffer		buf;
	Page		page;
	ZSBtreePageOpaque *opaque;
	OffsetNumber off;
	OffsetNumber maxoff;
	BlockNumber	next;

	if (!scan->active)
		return false;

	for (;;)
	{
		while (scan->has_decompressed)
		{
			ZSBtreeItem *item = zs_decompress_read_item(&scan->decompressor);

			if (item == NULL)
			{
				scan->has_decompressed = false;
				break;
			}
			if (item->t_tid >= scan->nexttid)
			{
				scan->nexttid = item->t_tid;
				scan->nexttid = ZSTidIncrement(scan->nexttid);
				return item;
			}
		}

		buf = scan->lastbuf;
		page = BufferGetPage(buf);
		opaque = ZSBtreePageGetOpaque(page);

		if (!scan->lastbuf_is_locked)
			LockBuffer(buf, scan->for_update ? BUFFER_LOCK_EXCLUSIVE : BUFFER_LOCK_SHARE);
		scan->lastbuf_is_locked = true;

		/* TODO: check that the page is a valid zs btree page */

		/* TODO: check the last offset first, as an optimization */
		maxoff = PageGetMaxOffsetNumber(page);
		for (off = FirstOffsetNumber; off <= maxoff; off++)
		{
			ItemId		iid = PageGetItemId(page, off);
			ZSBtreeItem	*item = (ZSBtreeItem *) PageGetItem(page, iid);

			if ((item->t_flags & ZSBT_COMPRESSED) != 0)
			{
				if (item->t_lasttid >= scan->nexttid)
				{
					zs_decompress_chunk(&scan->decompressor, item);
					scan->has_decompressed = true;
					if (!scan->for_update)
					{
						LockBuffer(buf, BUFFER_LOCK_UNLOCK);
						scan->lastbuf_is_locked = false;
					}
					break;
				}
			}
			else
			{
				if (item->t_tid >= scan->nexttid)
				{
					scan->nexttid = item->t_tid;
					scan->nexttid = ZSTidIncrement(scan->nexttid);
					return item;
				}
			}
		}

		if (scan->has_decompressed)
			continue;

		/* No more items on this page. Walk right, if possible */
		next = opaque->zs_next;
		if (next == BufferGetBlockNumber(buf))
			elog(ERROR, "btree page %u next-pointer points to itself", next);
		LockBuffer(buf, BUFFER_LOCK_UNLOCK);
		scan->lastbuf_is_locked = false;

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

/*
 * This helper function is used to implement INSERT, UPDATE and DELETE.
 *
 * If 'olditem' is not NULL, then 'olditem' on the page is replaced with
 * 'replacementitem'.
 *
 * If 'newitem' is not NULL, it is added to the page, to the correct position.
 *
 * This function handles decompressing and recompressing items, and splitting
 * the page if needed.
 */
static void
zsbt_replace_item(Relation rel, AttrNumber attno, Buffer buf,
				  ZSBtreeItem *olditem,
				  ZSBtreeItem *replacementitem,
				  ZSBtreeItem *newitem)
{
	Page		page = BufferGetPage(buf);
	OffsetNumber off;
	OffsetNumber maxoff;
	List	   *items;
	bool		found_old_item = false;
	/* We might need to decompress up to two previously compressed items */
	ZSDecompressContext decompressors[2];
	int			numdecompressors = 0;

	/*
	 * Helper routine, to append the given old item 'x' to the list.
	 * If the 'x' matches the old item, then append 'replacementitem' instead.
	 * And if thew 'newitem' shoudl go before 'x', then append that first.
	 *
	 * TODO: We could also leave out any old, deleted, items that are no longer
	 * visible to anyone.
	 */
#define PROCESS_ITEM(x) \
	do { \
		if (newitem && (x)->t_tid >= newitem->t_tid) \
		{ \
			Assert((x)->t_tid != newitem->t_tid); \
			items = lappend(items, newitem); \
			newitem = NULL; \
		} \
		if (olditem && (x)->t_tid == olditem->t_tid) \
		{ \
			Assert(!found_old_item); \
			found_old_item = true; \
			items = lappend(items, replacementitem); \
		} \
		else \
			items = lappend(items, x); \
	} while(0)

	/* Loop through all old items on the page */
	items = NIL;
	maxoff = PageGetMaxOffsetNumber(page);
	for (off = FirstOffsetNumber; off <= maxoff; off++)
	{
		ItemId		iid = PageGetItemId(page, off);
		ZSBtreeItem	*item = (ZSBtreeItem *) PageGetItem(page, iid);

		if ((item->t_flags & ZSBT_COMPRESSED) != 0)
		{
			if ((olditem && item->t_tid <= olditem->t_tid && olditem->t_tid <= item->t_lasttid) ||
				(newitem && item->t_tid <= newitem->t_tid && newitem->t_tid <= item->t_lasttid))
			{
				/* Found it, this compressed item covers the target or the new TID. */
				/* We have to decompress it, and recompress */
				ZSDecompressContext *decompressor = &decompressors[numdecompressors++];
				Assert(numdecompressors <= 2);

				zs_decompress_init(decompressor);
				zs_decompress_chunk(decompressor, item);

				while ((item = zs_decompress_read_item(decompressor)) != NULL)
					PROCESS_ITEM(item);
			}
			else
			{
				/* this item does not cover the target, nor the newitem. Add as it is. */
				items = lappend(items, item);
				continue;
			}
		}
		else
			PROCESS_ITEM(item);
	}

	if (olditem && !found_old_item)
		elog(ERROR, "could not find old item to replace");

	/* if the new item was not added in the loop, it goes to the end */
	if (newitem)
		items = lappend(items, newitem);

	/* Now pass the list to the recompressor. */
	IncrBufferRefCount(buf);
	zsbt_recompress_replace(rel, attno, buf, items);

	/*
	 * We can now free the decompression contexts. The pointers in the 'items' list
	 * point to decompression buffers, so we cannot free them until after writing out
	 * the pages.
	 */
	for (int i = 0; i < numdecompressors; i++)
		zs_decompress_free(&decompressors[i]);
}

/*
 * Recompressor routines
 */
typedef struct
{
	ZSBtreeItem *newitem;

	Page		currpage;
	ZSCompressContext compressor;
	int			compressed_items;
	List	   *pages;		/* first page writes over the old buffer,
							 * subsequent pages get newly-allocated buffers */

	int			total_items;
	int			total_compressed_items;
	int			total_already_compressed_items;

	zstid		hikey;
} zsbt_recompress_context;

static void
zsbt_recompress_newpage(zsbt_recompress_context *cxt, zstid nexttid)
{
	Page		newpage;
	ZSBtreePageOpaque *newopaque;

	if (cxt->currpage)
	{
		/* set the last tid on previous page */
		ZSBtreePageOpaque *oldopaque = ZSBtreePageGetOpaque(cxt->currpage);

		oldopaque->zs_hikey = nexttid;
	}

	newpage = (Page) palloc(BLCKSZ);
	PageInit(newpage, BLCKSZ, sizeof(ZSBtreePageOpaque));
	cxt->pages = lappend(cxt->pages, newpage);
	cxt->currpage = newpage;

	newopaque = ZSBtreePageGetOpaque(newpage);
	newopaque->zs_next = InvalidBlockNumber; /* filled in later */
	newopaque->zs_lokey = nexttid;
	newopaque->zs_hikey = cxt->hikey;		/* overwritten later, if this is not last page */
	newopaque->zs_level = 0;
	newopaque->zs_flags = 0;
	newopaque->zs_page_id = ZS_BTREE_PAGE_ID;
}

static void
zsbt_recompress_add_to_page(zsbt_recompress_context *cxt, ZSBtreeItem *item)
{
	if (PageGetFreeSpace(cxt->currpage) < MAXALIGN(item->t_size))
		zsbt_recompress_newpage(cxt, item->t_tid);

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
	ZSBtreeItem *item;

	if (cxt->compressed_items == 0)
		return;

	item = zs_compress_finish(&cxt->compressor);

	zsbt_recompress_add_to_page(cxt, item);
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
 */
static void
zsbt_recompress_replace(Relation rel, AttrNumber attno, Buffer oldbuf, List *items)
{
	ListCell   *lc;
	ListCell   *lc2;
	zsbt_recompress_context cxt;
	ZSBtreePageOpaque *oldopaque = ZSBtreePageGetOpaque(BufferGetPage(oldbuf));

	cxt.currpage = NULL;
	zs_compress_init(&cxt.compressor);
	cxt.compressed_items = 0;
	cxt.pages = NIL;

	cxt.total_items = 0;
	cxt.total_compressed_items = 0;
	cxt.total_already_compressed_items = 0;

	zsbt_recompress_newpage(&cxt, oldopaque->zs_lokey);

	foreach(lc, items)
	{
		ZSBtreeItem *item = (ZSBtreeItem *) lfirst(lc);

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

	//elog(NOTICE, "recompressing att %d: %d already, %d compressed, %d items on %d pages",
	//	 attno,
	//	 cxt.total_already_compressed_items, cxt.total_compressed_items, cxt.total_items,
	//	 list_length(cxt.pages));

	/* Ok, we now have a list of pages, to replace the original page. */
	{
		List	   *bufs;
		int			i;
		BlockNumber orignextblk;
		Buffer		leftbuf;
		Buffer		rightbuf;

		/*
		 * allocate all the pages before entering critical section, so that
		 * out-of-disk-space doesn't lead to PANIC
		 */
		bufs = list_make1_int(oldbuf);
		for (i = 0; i < list_length(cxt.pages) - 1; i++)
		{
			Buffer		newbuf = zs_getnewbuf(rel);
			bufs = lappend_int(bufs, newbuf);
		}

		START_CRIT_SECTION();

		orignextblk = oldopaque->zs_next;
		forboth(lc, cxt.pages, lc2, bufs)
		{
			Page		page_copy = (Page) lfirst(lc);
			Buffer		buf = (Buffer) lfirst_int(lc2);
			Page		page = BufferGetPage(buf);
			ZSBtreePageOpaque *opaque;

			PageRestoreTempPage(page_copy, page);
			opaque = ZSBtreePageGetOpaque(page);

			/* TODO: WAL-log */
			if (lnext(lc2))
			{
				Buffer nextbuf = (Buffer) lfirst_int(lnext(lc2));

				opaque->zs_next = BufferGetBlockNumber(nextbuf);
				opaque->zs_flags |= ZS_FOLLOW_RIGHT;
			}
			else
			{
				/* last one in the chain. */
				opaque->zs_next = orignextblk;
			}

			MarkBufferDirty(buf);
		}

		END_CRIT_SECTION();

		/* If there were more than one page, insert downlinks for new pages. */
		rightbuf = oldbuf;
		bufs = list_delete_first(bufs);
		while (bufs)
		{
			leftbuf = rightbuf;
			rightbuf = (Buffer) linitial_int(bufs);
			zsbt_insert_downlink(rel, attno, leftbuf,
								 ZSBtreePageGetOpaque(BufferGetPage(leftbuf))->zs_hikey,
								 BufferGetBlockNumber(rightbuf));
			bufs = list_delete_first(bufs);
		}
		UnlockReleaseBuffer(rightbuf);
	}
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
