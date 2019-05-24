/*
 * zedstore_attpage.c
 *		Routines for handling attribute leaf pages.
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
 *	  src/backend/access/zedstore/zedstore_attpage.c
 */
#include "postgres.h"

#include "access/zedstore_compression.h"
#include "access/zedstore_internal.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "utils/datum.h"
#include "utils/rel.h"

/* prototypes for local functions */
static void zsbt_attr_recompress_replace(Relation rel, AttrNumber attno,
										 Buffer oldbuf, List *items);
static bool zsbt_attr_exists(Relation rel, AttrNumber attno,
							 zstid tid, Buffer *buf_p);
static void zsbt_attr_add_items(Relation rel, AttrNumber attno, Buffer buf,
								List *newitems);
static void zsbt_attr_remove_item(Relation rel, AttrNumber attno, Buffer buf,
								  zstid oldtid);
static Size zsbt_compute_data_size(Form_pg_attribute atti, Datum val, bool isnull);
static ZSAttributeItem *zsbt_attr_create_item_from_data(Form_pg_attribute att, zstid tid, bool isnull,
														int nelements, char *datasrc, Size datasz);
static ZSAttributeItem *zsbt_attr_create_item_from_datums(Form_pg_attribute att, zstid tid, bool isnull,
														  int nelements, Datum *datums,
														  Size datasz);

/* ----------------------------------------------------------------
 *						 Public interface
 * ----------------------------------------------------------------
 */

/*
 * Begin a scan of the btree.
 */
void
zsbt_attr_begin_scan(Relation rel, TupleDesc tdesc, AttrNumber attno, zstid starttid,
					 zstid endtid, ZSBtreeScan *scan)
{
	Buffer		buf;

	scan->rel = rel;
	scan->attno = attno;
	scan->tupledesc = tdesc;

	scan->snapshot = NULL;
	scan->context = CurrentMemoryContext;
	scan->lastoff = InvalidOffsetNumber;
	scan->has_decompressed = false;
	scan->nexttid = starttid;
	scan->endtid = endtid;
	memset(&scan->recent_oldest_undo, 0, sizeof(scan->recent_oldest_undo));
	memset(&scan->array_undoptr, 0, sizeof(scan->array_undoptr));
	scan->array_datums = MemoryContextAlloc(scan->context, sizeof(Datum));
	scan->array_datums_allocated_size = 1;
	scan->array_elements_left = 0;

	buf = zsbt_descend(rel, attno, starttid, 0, true);
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
zsbt_attr_reset_scan(ZSBtreeScan *scan, zstid starttid)
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
zsbt_attr_end_scan(ZSBtreeScan *scan)
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
 * Helper function of zsbt_attr_scan_next(), to extract Datums from the given
 * array item into the scan->array_* fields.
 */
static void
zsbt_attr_scan_extract_array(ZSBtreeScan *scan, ZSAttributeArrayItem *aitem)
{
	int			nelements = aitem->t_nelements;
	zstid		tid = aitem->t_tid;
	bool		isnull = (aitem->t_flags & ZSBT_ATTR_NULL) != 0;
	char	   *p = aitem->t_payload;

	/* skip over elements that we are not interested in */
	while (tid < scan->nexttid && nelements > 0)
	{
		Form_pg_attribute attr = ZSBtreeScanGetAttInfo(scan);
		if (!isnull)
		{
			if (attr->attlen > 0)
			{
				p += att_align_nominal(attr->attlen, attr->attalign);
			}
			else
			{
				p = (Pointer) att_align_pointer(p, attr->attalign, attr->attlen, p);

				if (attr->attlen == -1 &&
					VARATT_IS_EXTERNAL(p) && VARTAG_EXTERNAL(p) == VARTAG_ZEDSTORE)
				{
					p += sizeof(varatt_zs_toastptr);
				}
				else
					p = att_addlength_pointer(p, attr->attlen, p);
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
		scan->array_datums = MemoryContextAlloc(scan->context, nelements * sizeof(Datum));
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
		Form_pg_attribute attr = ZSBtreeScanGetAttInfo(scan);
		int16		attlen = attr->attlen;

		if (attr->attbyval)
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
				p += att_align_nominal(attr->attlen, attr->attalign);
			}
		}
		else if (attlen == -1)
		{
			for (int i = 0; i < nelements; i++)
			{
				p = (Pointer) att_align_pointer(p, attr->attalign, attlen, p);
				scan->array_datums[i] = PointerGetDatum(p);
				if (VARATT_IS_EXTERNAL(p) && VARTAG_EXTERNAL(p) == VARTAG_ZEDSTORE)
					p += sizeof(varatt_zs_toastptr);
				else
					p = att_addlength_pointer(p, attlen, p);
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
	if (scan->nexttid < tid)
		scan->nexttid = tid;
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
zsbt_attr_scan_next(ZSBtreeScan *scan)
{
	Buffer		buf;
	bool		buf_is_locked = false;
	Page		page;
	ZSBtreePageOpaque *opaque;
	OffsetNumber off;
	OffsetNumber maxoff;
	BlockNumber	next;

	Assert(scan->active);

	/*
	 * Advance to the next TID >= nexttid.
	 *
	 * This advances scan->nexttid as it goes.
	 */
	while (scan->nexttid < scan->endtid)
	{
		/*
		 * If we are still processing an array item, return next element from it.
		 */
		if (scan->array_elements_left > 0)
			return true;

		/*
		 * If we are still processing a compressed item, process the next item
		 * from the it. If it's an array item, we start iterating the array by
		 * setting the scan->array_* fields, and loop back to top to return the
		 * first element from the array.
		 */
		if (scan->has_decompressed)
		{
			zstid		lasttid;
			ZSAttributeItem *uitem;

			uitem = zs_decompress_read_item(&scan->decompressor);

			if (uitem == NULL)
			{
				scan->has_decompressed = false;
				continue;
			}

			/* a compressed item cannot contain nested compressed items */
			Assert((uitem->t_flags & ZSBT_ATTR_COMPRESSED) == 0);

			lasttid = zsbt_attr_item_lasttid(uitem);
			if (lasttid < scan->nexttid)
				continue;

			if (uitem->t_tid >= scan->endtid)
				break;

			/* no need to make a copy, because the uncompressed buffer
			 * is already a copy */

			/*
			 * XXX: currently, there's only one kind of an item, but we'll probably get
			 * different more or less compact formats in the future.
			 */
			zsbt_attr_scan_extract_array(scan, (ZSAttributeArrayItem *) uitem);
			continue;
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
			ZSAttributeItem *item = (ZSAttributeItem *) PageGetItem(page, iid);
			zstid		lasttid;

			lasttid = zsbt_attr_item_lasttid(item);

			if (scan->nexttid > lasttid)
				continue;

			if (item->t_tid >= scan->endtid)
			{
				scan->nexttid = scan->endtid;
				break;
			}

			if ((item->t_flags & ZSBT_ATTR_COMPRESSED) != 0)
			{
				ZSAttributeCompressedItem *citem = (ZSAttributeCompressedItem *) item;
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
				/* copy the item, because we can't hold a lock on the page  */
				ZSAttributeArrayItem *aitem;

				aitem = MemoryContextAlloc(scan->context, item->t_size);
				memcpy(aitem, item, item->t_size);

				zsbt_attr_scan_extract_array(scan, aitem);

				if (scan->array_elements_left > 0)
				{
					LockBuffer(scan->lastbuf, BUFFER_LOCK_UNLOCK);
					buf_is_locked = false;
					break;
				}
			}
		}

		if (scan->array_elements_left > 0 || scan->has_decompressed)
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
zsbt_attr_multi_insert(Relation rel, AttrNumber attno,
					   Datum *datums, bool *isnulls, zstid *tids, int nitems)
{
	Form_pg_attribute attr;
	zstid		tid = tids[0];
	Buffer		buf;
	zstid		insert_target_key;
	int			i;
	List	   *newitems;

	Assert (attno >= 1);
	attr = &rel->rd_att->attrs[attno - 1];

	/*
	 * Find the right place for the given TID.
	 */
	insert_target_key = tid;

	buf = zsbt_descend(rel, attno, insert_target_key, 0, false);

	/* Create items to insert. */
	newitems = NIL;
	i = 0;
	while (i < nitems)
	{
		Size		datasz;
		int			j;
		ZSAttributeItem *newitem;

		/*
		 * Try to collapse as many items as possible into an Array item.
		 * The first item in the array is now at tids[i]/datums[i]/isnulls[i].
		 * Items can be stored in the same array as long as the TIDs are
		 * consecutive, they all have the same isnull flag, and the array
		 * isn't too large to be stored on a single leaf page. Scan the
		 * arrays, checking those conditions.
		 */
		datasz = zsbt_compute_data_size(attr, datums[i], isnulls[i]);
		for (j = i + 1; j < nitems; j++)
		{
			if (isnulls[j] != isnulls[i])
				break;

			if (tids[j] != tids[j - 1] + 1)
				break;

			/*
			 * Will the array still fit on a leaf page, if this datum is
			 * included in it? We actually use 1/4 of the page, to avoid
			 * making very large arrays, which might be slower to update in
			 * the future. Also, using an array that completely fills a page
			 * might cause more fragmentation. (XXX: The 1/4 threshold
			 * is arbitrary, though, and this probably needs more smarts
			 * or testing to determine the optimum.)
			 */
			if (!isnulls[i])
			{
				Datum		val = datums[j];
				Size		datum_sz;
				Size		newdatasz;

				datum_sz = zsbt_compute_data_size(attr, val, false);

				newdatasz = att_align_datum(newdatasz, attr->attalign, attr->attlen, val) + datum_sz;

				if (newdatasz > MaxZedStoreDatumSize / 4)
					break;
				datasz = newdatasz;
			}
		}

		/*
		 * 'i' is now the first entry to store in the array, and 'j' is the
		 * last + 1 elemnt to store. If j == i + 1, then there is only one
		 * element and zsbt_create_item() will create a 'single' item rather
		 * than an array.
		 */
		newitem = zsbt_attr_create_item_from_datums(attr, tids[i], isnulls[i],
													j - i, &datums[i], datasz);

		newitems = lappend(newitems, newitem);
		i = j;
	}

	/* recompress and possibly split the page */
	zsbt_attr_add_items(rel, attno, buf, newitems);

	/* zsbt_attr_add_items unlocked 'buf' */
	ReleaseBuffer(buf);
}

void
zsbt_attr_remove(Relation rel, AttrNumber attno, zstid tid)
{
	Buffer		buf;
	bool		found;

	/* Find the item to delete. (It could be compressed) */
	found = zsbt_attr_exists(rel, attno, tid, &buf);
	if (!found)
	{
		elog(WARNING, "could not find tuple to remove with TID (%u, %u) for attribute %d",
			 ZSTidGetBlockNumber(tid), ZSTidGetOffsetNumber(tid), attno);
		return;
	}

	/* remove it */
	zsbt_attr_remove_item(rel, attno, buf, tid);

	ReleaseBuffer(buf); 	/* zsbt_attr_remove_item unlocke 'buf' */
}

/* ----------------------------------------------------------------
 *						 Internal routines
 * ----------------------------------------------------------------
 */

/*
 * Check if an item with given TID exists. The page containing the item is kept locked, and
 * returned to the caller in *buf_p. This is used to locate a tuple for updating
 * or deleting it.
 */
static bool
zsbt_attr_exists(Relation rel, AttrNumber attno, zstid tid, Buffer *buf_p)
{
	Buffer		buf;
	Page		page;
	ZSAttributeItem *item = NULL;
	bool		found = false;
	OffsetNumber maxoff;
	OffsetNumber off;

	buf = zsbt_descend(rel, attno, tid, 0, false);
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
		item = (ZSAttributeItem *) PageGetItem(page, iid);

		if ((item->t_flags & ZSBT_ATTR_COMPRESSED) != 0)
		{
			ZSAttributeCompressedItem *citem = (ZSAttributeCompressedItem *) item;
			ZSDecompressContext decompressor;

			zs_decompress_init(&decompressor);
			zs_decompress_chunk(&decompressor, citem);

			while ((item = zs_decompress_read_item(&decompressor)) != NULL)
			{
				zstid		lasttid = zsbt_attr_item_lasttid(item);

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
			zstid		lasttid = zsbt_attr_item_lasttid(item);

			if (item->t_tid <= tid && lasttid >= tid)
			{
				found = true;
				break;
			}
		}
	}

	if (found)
	{
		/* Found an item that covers this TID. */
		Assert(tid - item->t_tid < ((ZSAttributeArrayItem *) item)->t_nelements);

		*buf_p = buf;
		return true;
	}
	else
	{
		UnlockReleaseBuffer(buf);
		*buf_p = InvalidBuffer;
		return NULL;
	}
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
		return data_length;

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
 * Form a ZSAttributeItem out of data that's already in on-disk array format.
 */
static ZSAttributeItem *
zsbt_attr_create_item_from_data(Form_pg_attribute att, zstid tid, bool isnull,
								int nelements, char *datasrc, Size datasz)
{
	ZSAttributeArrayItem *newitem;
	Size		itemsz;
	char	   *databegin;

	Assert(nelements > 0);

	itemsz = offsetof(ZSAttributeArrayItem, t_payload) + datasz;

	newitem = palloc(itemsz);
	newitem->t_tid = tid;
	newitem->t_size = itemsz;
	newitem->t_flags = 0;
	if (isnull)
		newitem->t_flags |= ZSBT_ATTR_NULL;
	newitem->t_nelements = nelements;
	newitem->t_padding = 0; /* zero padding */

	databegin = newitem->t_payload;

	/*
	 * Copy the data.
	 *
	 * This is largely copied from heaptuple.c's fill_val().
	 */
	if (!isnull)
		memcpy(databegin, datasrc, datasz);

	return (ZSAttributeItem *) newitem;
}

/*
 * Form a ZSAttributeItem out of the given datums.
 */
static ZSAttributeItem *
zsbt_attr_create_item_from_datums(Form_pg_attribute att, zstid tid, bool isnull,
								  int nelements, Datum *datums,
								  Size datasz)
{
	ZSAttributeArrayItem *newitem;
	Size		itemsz;
	char	   *databegin;

	Assert(nelements > 0);

	itemsz = offsetof(ZSAttributeArrayItem, t_payload) + datasz;

	newitem = palloc(itemsz);
	newitem->t_tid = tid;
	newitem->t_size = itemsz;
	newitem->t_flags = 0;
	if (isnull)
		newitem->t_flags |= ZSBT_ATTR_NULL;
	newitem->t_nelements = nelements;
	newitem->t_padding = 0; /* zero padding */

	databegin = newitem->t_payload;

	/*
	 * Copy the data.
	 *
	 * This is largely copied from heaptuple.c's fill_val().
	 */
	if (!isnull)
	{
		char	   *data = databegin;

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

	return (ZSAttributeItem *) newitem;
}

static ZSAttributeItem *
zsbt_attr_merge_items(Form_pg_attribute attr, ZSAttributeItem *aitem, ZSAttributeItem *bitem)
{
	ZSAttributeArrayItem *newitem;
	Size		adatasz;
	Size		bdatasz;
	Size		totaldatasz;
	Size		newitemsz;
	bool		isnull;
	ZSAttributeArrayItem *a_arr_item;
	ZSAttributeArrayItem *b_arr_item;
	Size		paddingsz = 0;

	/* don't bother trying to merge compressed items */
	if ((aitem->t_flags & ZSBT_ATTR_COMPRESSED) != 0)
		return NULL;
	if ((bitem->t_flags & ZSBT_ATTR_COMPRESSED) != 0)
		return NULL;

	a_arr_item = (ZSAttributeArrayItem *) aitem;
	b_arr_item = (ZSAttributeArrayItem *) bitem;

	/* The arrays must have consecutive TIDs */
	if (b_arr_item->t_tid != a_arr_item->t_tid + a_arr_item->t_nelements)
		return NULL;

	/* And they must have compatible NULL flag */
	if ((aitem->t_flags & ZSBT_ATTR_NULL) != (bitem->t_flags & ZSBT_ATTR_NULL))
		return NULL;
	isnull = (aitem->t_flags & ZSBT_ATTR_NULL);

	/* Create a new item that covers both. */
	adatasz = aitem->t_size - offsetof(ZSAttributeArrayItem, t_payload);

	if (attr->attlen > 0)
	{
		Size	alignedsz = att_align_nominal(adatasz, attr->attalign);

		bdatasz = bitem->t_size - offsetof(ZSAttributeArrayItem, t_payload);
		paddingsz = alignedsz - adatasz;
		totaldatasz = adatasz + paddingsz + bdatasz;
	}
	else if (attr->attlen == -1)
	{
		/*
		 * For varlenas, we have to work harder, to get the alignment right.
		 * We have to walk through all the elements, and figure out where
		 * each element will land and what alignment padding they need, when
		 * they're appended to the first array. (TODO: We could stop short as
		 * soon as we reach a point where the alignments in the source and
		 * destination are the same)
		 */
		char		*src;
		int			offset;

		offset = adatasz;
		src = b_arr_item->t_payload;
		for (int i = 0; i < b_arr_item->t_nelements; i++)
		{
			Size		data_length;

			/* Walk to the next element in the source */
			src = (Pointer) att_align_pointer(src, attr->attalign, attr->attlen, src);

			/* Write it to dest, with proper alignment */
			if (VARATT_IS_EXTERNAL(src) && VARTAG_EXTERNAL(src) == VARTAG_ZEDSTORE)
			{
				data_length = sizeof(varatt_zs_toastptr);
			}
			else if (VARATT_IS_SHORT(src))
			{
				data_length = VARSIZE_SHORT(src);
			}
			else
			{
				/* full 4-byte header varlena */
				offset = att_align_nominal(offset, attr->attalign);
				data_length = VARSIZE(src);
			}
			src += data_length;
			offset += data_length;
		}

		totaldatasz = offset;
		bdatasz = offset - adatasz;
	}
	else
		elog(ERROR, "unexpected attlen %d", attr->attlen);

	/* Like in zsbt_attr_multi_insert(), enforce a practical limit on the
	 * size of the array tuple. */
	if (totaldatasz >= MaxZedStoreDatumSize / 4)
		return NULL;

	return NULL;
	newitemsz = offsetof(ZSAttributeArrayItem, t_payload) + totaldatasz;

	newitem = palloc(newitemsz);
	newitem->t_tid = aitem->t_tid;
	newitem->t_size = newitemsz;
	newitem->t_flags = 0;
	if (isnull)
		newitem->t_flags |= ZSBT_ATTR_NULL;
	newitem->t_nelements = a_arr_item->t_nelements + b_arr_item->t_nelements;
	newitem->t_padding = 0; /* zero padding */

	if (!isnull)
	{
		char	   *p = newitem->t_payload;

		memcpy(p, a_arr_item->t_payload, adatasz);

		p += adatasz;

		if (attr->attlen > 0)
		{
			memset(p, 0, paddingsz);
			p += paddingsz;

			memcpy(newitem->t_payload + adatasz, b_arr_item->t_payload, bdatasz);
			p += bdatasz;
		}
		else
		{
			char	   *src;

			/* clear alignment padding */
			memset(p, 0, bdatasz);

			src = b_arr_item->t_payload;
			for (int i = 0; i < b_arr_item->t_nelements; i++)
			{
				Size		data_length;

				/* Walk to the next element in the source */
				src = (Pointer) att_align_pointer(src, attr->attalign, attr->attlen, src);

				/* Write it to dest, with proper alignment */
				if (VARATT_IS_EXTERNAL(src) && VARTAG_EXTERNAL(src) == VARTAG_ZEDSTORE)
				{
					data_length = sizeof(varatt_zs_toastptr);
				}
				else if (VARATT_IS_SHORT(src))
				{
					data_length = VARSIZE_SHORT(src);
				}
				else
				{
					/* full 4-byte header varlena */
					p = (char *) att_align_nominal(p, attr->attalign);
					data_length = VARSIZE(src);
				}
				memcpy(p, src, data_length);
				p += data_length;
				src += data_length;
			}
		}
		Assert(p == (char *) newitem + newitem->t_size);
	}

	return (ZSAttributeItem *) newitem;
}

/*
 * This helper function is used to implement INSERT, UPDATE and DELETE.
 *
 * The items in the 'newitems' list are added to the page, to the correct position.
 *
 * FIXME: Actually, they're always just added to the end of the page, and that
 * better be the correct position.
 *
 * This function handles decompressing and recompressing items, and splitting
 * the page if needed.
 */
static void
zsbt_attr_add_items(Relation rel, AttrNumber attno, Buffer buf, List *newitems)
{
	Page		page = BufferGetPage(buf);
	OffsetNumber off;
	OffsetNumber maxoff;
	List	   *items;
	ZSAttributeItem *mergeditem = NULL;
	Size		last_item_size = 0;
	Size		growth;
	ListCell   *lc;

	Assert(newitems != NIL);

	maxoff = PageGetMaxOffsetNumber(page);

	/*
	 * Add any new items to the end.
	 *
	 * As a quick but very effective optimization, check if the last existing
	 * item on the page happens to be an array item, such that we could merge
	 * the new item with it. That makes single row INSERTs much more efficient.
	 */
	if (maxoff >= 1)
	{
		ItemId		last_iid = PageGetItemId(page, maxoff);
		ZSAttributeItem *last_old_item = (ZSAttributeItem *) PageGetItem(page, last_iid);
		ZSAttributeItem *first_new_item = (ZSAttributeItem *) linitial(newitems);

		mergeditem = zsbt_attr_merge_items(TupleDescAttr(RelationGetDescr(rel), attno - 1),
										   last_old_item, first_new_item);
		if (mergeditem)
		{
			/*
			 * Yes, we could append to the last array item. 'mergeditem' is now
			 * the item that will replace the old last item on the page. The
			 * values of the first new item are now included in 'mergeditem',
			 * so remove it from the list of new items.
			 */
			newitems = list_delete_first(newitems);
		}

		last_item_size = last_old_item->t_size;
	}

	/* Can we make the new items fit without splitting the page? */
	growth = 0;
	if (mergeditem)
		growth += MAXALIGN(mergeditem->t_size) - last_item_size;
	foreach (lc, newitems)
	{
		ZSAttributeItem *item = (ZSAttributeItem *) lfirst(lc);

		growth += MAXALIGN(item->t_size) + sizeof(ItemId);
	}

	if (growth <= PageGetExactFreeSpace(page))
	{
		/* The new items fit on the page. Add them. */
		START_CRIT_SECTION();

		if (mergeditem)
		{
			PageIndexTupleDelete(page, maxoff);
			if (PageAddItemExtended(page,
									(Item) mergeditem, mergeditem->t_size,
									maxoff,
									PAI_OVERWRITE) == InvalidOffsetNumber)
				elog(ERROR, "could not add item to attribute page");
		}

		foreach(lc, newitems)
		{
			ZSTidItem *item = (ZSTidItem *) lfirst(lc);

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
	}
	else
	{
		/* need to split */

		/* Loop through all old items on the page */
		items = NIL;
		for (off = 1; off <= maxoff; off++)
		{
			ZSAttributeItem *item;

			if (off == maxoff && mergeditem)
				item = mergeditem;
			else
			{
				ItemId		iid = PageGetItemId(page, off);
				item = (ZSAttributeItem *) PageGetItem(page, iid);
			}

			items = lappend(items, item);
		}
		items = list_concat(items, newitems);

		/* Now pass the list to the recompressor. */
		IncrBufferRefCount(buf);

		zsbt_attr_recompress_replace(rel, attno, buf, items);

		list_free(items);
	}
}

/*
 * Remove the value with the given TID from an array item,
 * creating two new array items.
 */
static void
zsbt_attr_split_item(Form_pg_attribute atti,
					 ZSAttributeArrayItem *olditem, zstid removetid,
					 ZSAttributeItem **newitem1, ZSAttributeItem **newitem2)
{
	bool		isnull = (olditem->t_flags & ZSBT_ATTR_NULL) != 0;
	int			offset;

	*newitem1 = NULL;
	*newitem2 = NULL;

	/* Only varlenas and fixed-width attributes are supported (not cstrings) */
	Assert(atti->attlen > 0 || atti->attlen == -1);

	offset = removetid - olditem->t_tid;

	if (isnull || atti->attlen > 0)
	{
		/* Fixed-length (or all NULL) */
		size_t		attlen;
		size_t		aligned_attlen;

		if (isnull)
		{
			attlen = 0;
			aligned_attlen = 0;
		}
		else
		{
			attlen = atti->attlen;
			aligned_attlen = att_align_nominal(atti->attlen, atti->attalign);
		}

		if (offset > 0)
		{
			ZSAttributeItem *item;
			int			nelem = offset - 1;
			size_t		datalen;

			if (isnull)
				datalen = 0;
			else
				datalen = aligned_attlen * nelem + attlen;

			item = zsbt_attr_create_item_from_data(atti, olditem->t_tid, isnull, nelem,
										 olditem->t_payload, datalen);
			*newitem1 = (ZSAttributeItem *) item;
		}

		if (offset < olditem->t_nelements - 1)
		{
			ZSAttributeItem *item;
			int			nelem = olditem->t_nelements - offset - 1;
			size_t		datalen;

			if (isnull)
				datalen = 0;
			else
				datalen = aligned_attlen * (nelem - 1) + attlen;

			item = zsbt_attr_create_item_from_data(atti, removetid + 1, nelem, isnull,
												   olditem->t_payload + aligned_attlen * (offset + 1),
												   datalen);
			*newitem2 = (ZSAttributeItem *) item;
		}
	}
	else
	{
		char	   *src;

		Assert(!isnull);
		Assert(atti->attlen == -1);

		src = olditem->t_payload;

		if (offset > 0)
		{
			ZSAttributeItem *item;
			int			nelem = offset - 1;
			size_t		datalen;

			/* Walk the elements, up to the target element */
			for (int i = 0; i < nelem; i++)
			{
				src = (Pointer) att_align_pointer(src, atti->attalign, atti->attlen, src);

				if (VARATT_IS_EXTERNAL(src) && VARTAG_EXTERNAL(src) == VARTAG_ZEDSTORE)
					src += sizeof(varatt_zs_toastptr);
				else
					src = att_addlength_pointer(src, atti->attlen, src);

				/*
				 * The array should already use short varlen representation whenever
				 * possible.
				 */
				Assert(!VARATT_CAN_MAKE_SHORT(DatumGetPointer(src)));
			}
			datalen = src - olditem->t_payload;

			item = zsbt_attr_create_item_from_data(atti, olditem->t_tid, nelem, isnull,
												   olditem->t_payload, datalen);
			*newitem1 = (ZSAttributeItem *) item;
		}

		if (offset < olditem->t_nelements - 1)
		{
			ZSAttributeArrayItem *item;
			int			nelem = olditem->t_nelements - offset - 1;
			size_t		allocsz;
			char	   *dst;

			allocsz = olditem->t_size - (src - olditem->t_payload) + MAXIMUM_ALIGNOF;
			item = (ZSAttributeArrayItem *) palloc(allocsz);
			item->t_tid = removetid + 1;
			/* item->t_size is filled in later */
			item->t_flags = 0;
			item->t_nelements = nelem;
			item->t_padding = 0; /* zero padding */

			/* Copy the remaining elements */
			dst = item->t_payload;
			for (int i = 0; i < nelem; i++)
			{
				size_t		this_length;

				src = (Pointer) att_align_pointer(src, atti->attalign, atti->attlen, src);

				if (VARATT_IS_EXTERNAL(src) && VARTAG_EXTERNAL(src) == VARTAG_ZEDSTORE)
					this_length = sizeof(varatt_zs_toastptr);
				else if (VARATT_IS_SHORT(src))
					this_length = VARSIZE_SHORT(src);
				else
				{
					/* full 4-byte header varlena */
					this_length = VARSIZE(src);
					dst = (char *) att_align_nominal(dst, atti->attalign);
				}

				memcpy(dst, src, this_length);
				src += this_length;
				dst += this_length;
			}
			Assert(src == (char *) olditem + olditem->t_size);
			Assert(dst <= (char *) item + allocsz);
			item->t_size = dst - (char *) item;

			*newitem2 = (ZSAttributeItem *) item;
		}
	}
}


/*
 * This helper function is used to implement vacuum cleanup after a DELETE,
 * to delete attribute data belonging to dead tuples.
 *
 * This function handles decompressing and recompressing items, and splitting
 * the page if needed.
 */
static void
zsbt_attr_remove_item(Relation rel, AttrNumber attno, Buffer buf, zstid oldtid)
{
	Form_pg_attribute attr;
	Page		page = BufferGetPage(buf);
	OffsetNumber off;
	OffsetNumber maxoff;
	List	   *items;
	bool		found_old_item = false;
	/* We might need to decompress up to two previously compressed items */
	ZSDecompressContext decompressor;
	bool		decompressor_used = false;
	bool		decompressing;

	attr = &rel->rd_att->attrs[attno - 1];

	/* Loop through all old items on the page */
	items = NIL;
	maxoff = PageGetMaxOffsetNumber(page);
	decompressing = false;
	off = 1;
	for (;;)
	{
		ZSAttributeItem *item;

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

			item = (ZSAttributeItem *) PageGetItem(page, iid);
			off++;

		}
		else
		{
			/* out of items */
			break;
		}

		/* we now have an item to process, either straight from the page or from
		 * the decompressor */
		if ((item->t_flags & ZSBT_ATTR_COMPRESSED) != 0)
		{
			zstid		item_lasttid = zsbt_attr_item_lasttid(item);

			/* there shouldn't nested compressed items */
			if (decompressing)
				elog(ERROR, "nested compressed items on zedstore page not supported");

			if (oldtid != InvalidZSTid && item->t_tid <= oldtid && oldtid <= item_lasttid)
			{
				ZSAttributeCompressedItem *citem = (ZSAttributeCompressedItem *) item;

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
		else
		{
			/* array item */
			ZSAttributeArrayItem *aitem = (ZSAttributeArrayItem *) item;
			zstid		item_lasttid = zsbt_attr_item_lasttid(item);

			if (oldtid != InvalidZSTid && item->t_tid <= oldtid && oldtid <= item_lasttid)
			{
				/*
				 * The target TID is currently part of an array item. We have to split
				 * the array item into two, and put the replacement item in the middle.
				 */
				ZSAttributeItem *item1;
				ZSAttributeItem *item2;

				zsbt_attr_split_item(attr, aitem, oldtid, &item1, &item2);
				if (item1)
					items = lappend(items, item1);
				if (item2)
					items = lappend(items, item2);

				found_old_item = true;
			}
			else
				items = lappend(items, item);
		}
	}

	if (oldtid != InvalidZSTid && !found_old_item)
		elog(ERROR, "could not find old item to replace");

	/* Now pass the list to the recompressor. */
	IncrBufferRefCount(buf);
	if (items)
	{
		zsbt_attr_recompress_replace(rel, attno, buf, items);
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
	ZSCompressContext *compressor;
	int			compressed_items;

	/* first page writes over the old buffer, subsequent pages get newly-allocated buffers */
	zs_split_stack *stack_head;
	zs_split_stack *stack_tail;

	int			total_items;
	int			total_compressed_items;
	int			total_already_compressed_items;

	AttrNumber	attno;
	zstid		hikey;
} zsbt_attr_recompress_context;

static void
zsbt_attr_recompress_newpage(zsbt_attr_recompress_context *cxt, zstid nexttid, int flags)
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
zsbt_attr_recompress_add_to_page(zsbt_attr_recompress_context *cxt, ZSAttributeItem *item)
{
	if (PageGetFreeSpace(cxt->currpage) < MAXALIGN(item->t_size))
		zsbt_attr_recompress_newpage(cxt, item->t_tid, 0);

	if (PageAddItemExtended(cxt->currpage,
							(Item) item, item->t_size,
							PageGetMaxOffsetNumber(cxt->currpage) + 1,
							PAI_OVERWRITE) == InvalidOffsetNumber)
		elog(ERROR, "could not add item to page while recompressing");

	cxt->total_items++;
}

static bool
zsbt_attr_recompress_add_to_compressor(zsbt_attr_recompress_context *cxt, ZSAttributeItem *item)
{
	bool		result;

	if (cxt->compressed_items == 0)
	{
		if (!cxt->compressor)
		{
			cxt->compressor = palloc(sizeof(ZSCompressContext));
			zs_compress_init(cxt->compressor);
		}
		zs_compress_begin(cxt->compressor, PageGetFreeSpace(cxt->currpage));
	}

	result = zs_compress_add(cxt->compressor, item);
	if (result)
	{
		cxt->compressed_items++;

		cxt->total_compressed_items++;
	}

	return result;
}

static void
zsbt_attr_recompress_flush(zsbt_attr_recompress_context *cxt, bool last)
{
	ZSAttributeCompressedItem *citem;

	if (cxt->compressed_items == 0)
		return;

	/*
	 * If we've seen all the entries, and the last batch fits on the page without
	 * compression, then just store it. Compression doesn't really buy us
	 * anything, if the data would fit without it. This also gives the next insert
	 * a chance to merge the new item into the last array item.
	 */
	if (last && cxt->compressor->rawsize <= PageGetFreeSpace(cxt->currpage))
		citem = NULL;
	else
		citem = zs_compress_finish(cxt->compressor);

	if (citem)
		zsbt_attr_recompress_add_to_page(cxt, (ZSAttributeItem *) citem);
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
		for (int i = 0; i < cxt->compressor->nitems; i++)
		{
			ZSAttributeItem *item = (ZSAttributeItem *) (cxt->compressor->uncompressedbuffer + size);

			zsbt_attr_recompress_add_to_page(cxt, item);

			size += MAXALIGN(item->t_size);
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
zsbt_attr_recompress_replace(Relation rel, AttrNumber attno, Buffer oldbuf, List *items)
{
	ListCell   *lc;
	zsbt_attr_recompress_context cxt;
	ZSBtreePageOpaque *oldopaque = ZSBtreePageGetOpaque(BufferGetPage(oldbuf));
	BlockNumber orignextblk;
	zs_split_stack *stack;
	List	   *downlinks = NIL;

	orignextblk = oldopaque->zs_next;
	Assert(orignextblk != BufferGetBlockNumber(oldbuf));

	cxt.currpage = NULL;
	cxt.compressor = NULL;
	cxt.compressed_items = 0;
	cxt.stack_head = cxt.stack_tail = NULL;
	cxt.attno = attno;
	cxt.hikey = oldopaque->zs_hikey;

	cxt.total_items = 0;
	cxt.total_compressed_items = 0;
	cxt.total_already_compressed_items = 0;

	zsbt_attr_recompress_newpage(&cxt, oldopaque->zs_lokey, (oldopaque->zs_flags & ZSBT_ROOT));

	foreach(lc, items)
	{
		ZSAttributeItem *item = (ZSAttributeItem *) lfirst(lc);

		if ((item->t_flags & ZSBT_ATTR_COMPRESSED) != 0)
		{
			/* already compressed, add as it is. */
			zsbt_attr_recompress_flush(&cxt, false);
			cxt.total_already_compressed_items++;
			zsbt_attr_recompress_add_to_page(&cxt, item);
		}
		else
		{
			/* try to add this item to the compressor */
			if (!zsbt_attr_recompress_add_to_compressor(&cxt, item))
			{
				if (cxt.compressed_items > 0)
				{
					/* flush, and retry */
					zsbt_attr_recompress_flush(&cxt, false);

					if (!zsbt_attr_recompress_add_to_compressor(&cxt, item))
					{
						/* could not compress, even on its own. Store it uncompressed, then */
						zsbt_attr_recompress_add_to_page(&cxt, item);
					}
				}
				else
				{
					/* could not compress, even on its own. Store it uncompressed, then */
					zsbt_attr_recompress_add_to_page(&cxt, item);
				}
			}
		}
	}

	/* flush the last one, if any */
	zsbt_attr_recompress_flush(&cxt, true);

	if (cxt.compressor)
		zs_compress_free(cxt.compressor);

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
