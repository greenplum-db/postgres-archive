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

#include "access/xlogutils.h"
#include "access/zedstore_compression.h"
#include "access/zedstore_internal.h"
#include "access/zedstore_wal.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "utils/datum.h"
#include "utils/memutils.h"
#include "utils/rel.h"

/* prototypes for local functions */
static ZSAttStream *get_page_lowerstream(Page page);
static ZSAttStream *get_page_upperstream(Page page);
static void zsbt_attr_add_to_page(Relation rel, AttrNumber attno, Buffer buf,
								  ZSAttStream *newstream);
static void wal_log_attstream_change(Relation rel, Buffer buf, ZSAttStream *attstream, bool is_upper,
									 uint16 begin_offset, uint16 end_offset);

struct zsbt_attr_repack_context;
static void zsbt_attr_repack(Relation rel, AttrNumber attno, Buffer oldbuf,
							 ZSAttStream *attstream, bool append, zstid firstnewkey);
static void zsbt_attr_repack_write_pages(struct zsbt_attr_repack_context *cxt,
										 Relation rel, AttrNumber attno,
										 Buffer oldbuf, BlockNumber orignextblk);
static void zsbt_attr_repack_newpage(struct zsbt_attr_repack_context *cxt, zstid nexttid, int flags);
static void zsbt_attr_pack_attstream(Form_pg_attribute attr, chopper_state *chopper, Page page, zstid *remaintid);

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
zsbt_attr_begin_scan(Relation rel, TupleDesc tdesc, AttrNumber attno,
					 ZSAttrTreeScan *scan)
{
	scan->rel = rel;
	scan->attno = attno;
	scan->attdesc = TupleDescAttr(tdesc, attno - 1);

	scan->context = CurrentMemoryContext;
	scan->array_datums = NULL;
	scan->array_isnulls = NULL;
	scan->array_tids = NULL;
	scan->array_datums_allocated_size = 0;
	scan->array_num_elements = 0;
	scan->array_curr_idx = -1;
	scan->array_cxt = NULL;

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
	if (scan->attr_buf)
		pfree(scan->attr_buf);
	if (scan->array_cxt)
		MemoryContextDelete(scan->array_cxt);
}

/*
 * Load scan->array_* arrays with data that contains 'nexttid'.
 *
 * Return true if data containing 'nexttid' was found. The tid/Datum/isnull
 * data are placed into scan->array_* fields. The data is valid until the
 * next call of this function. Note that the item's range contains 'nexttid',
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
	 * Find the item containing nexttid.
	 */
	for (;;)
	{
		Buffer		buf;
		Page		page;
		ZSAttStream *stream = NULL;
		bool		found_candidate = false;

		scan->array_num_elements = 0;

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
			 * Completely empty tree. This should only happen at the beginning
			 * of a scan - a tree cannot go missing after it's been created -
			 * but we don't currently check for that.
			 */
			break;
		}
		page = BufferGetPage(buf);

		/* See if the upper stream matches the target tid */
		stream = get_page_upperstream(page);
		if (stream && stream->t_lasttid >= nexttid)
		{
			decode_attstream(scan, stream);
			found_candidate = true;
		}

		/* How about the lower stream? */
		if (!found_candidate)
		{
			stream = get_page_lowerstream(page);
			if (stream && stream->t_lasttid >= nexttid)
			{
				decode_attstream(scan, stream);
				found_candidate = true;
			}
		}

		LockBuffer(buf, BUFFER_LOCK_UNLOCK);

		if (scan->array_num_elements > 0 && scan->array_tids[0] <= nexttid)
		{
			/* Found it! */
			return true;
		}
		else
		{
			/* No matching items. XXX: we should remember the 'next' block, for
			 * the next call. When we're seqscanning, we will almost certainly need
			 * that next.
			 */
			return false;
		}
	}

	/* Reached end of scan. */
	scan->array_num_elements = 0;
	scan->array_curr_idx = -1;
	if (BufferIsValid(scan->lastbuf))
		ReleaseBuffer(scan->lastbuf);
	scan->lastbuf = InvalidBuffer;
	return false;
}

/*
 * Insert multiple items to the given attribute's btree.
 */
void
zsbt_attr_multi_insert(Relation rel, AttrNumber attno,
					   Datum *datums, bool *isnulls, zstid *tids, int nitems)
{
	Form_pg_attribute attr;
	Buffer		buf;
	zstid		insert_target_key;
	ZSAttStream *attstream;

	Assert (attno >= 1);
	attr = &rel->rd_att->attrs[attno - 1];

	/* Create items to insert. */
	attstream = create_attstream(attr, nitems, tids, datums, isnulls);

	/*
	 * Find the right place for the given TID.
	 */
	insert_target_key = tids[0];

	buf = zsbt_descend(rel, attno, insert_target_key, 0, false);

	/*
	 * FIXME: I think it's possible, that the target page has been split by
	 * a concurrent backend, so that it contains only part of the keyspace.
	 * zsbt_attr_modify_page() would not handle that correctly.
	 */

	/* recompress and possibly split the page */
	zsbt_attr_add_to_page(rel, attno, buf, attstream);

	/* zsbt_attr_modify_page unlocked 'buf' */
	ReleaseBuffer(buf);
}

/*
 * Remove data for the given TIDs from the attribute tree.
 */
void
zsbt_attr_remove(Relation rel, AttrNumber attno, IntegerSet *tids)
{
	Form_pg_attribute attr = &rel->rd_att->attrs[attno - 1];
	Buffer		buf;
	Page		page;
	ZSBtreePageOpaque *opaque;
	zstid		nexttid;
	MemoryContext oldcontext;
	MemoryContext tmpcontext;
	zstid	   *tids_to_remove;
	int			num_to_remove;
	int			allocated_size;

	tids_to_remove = palloc(1000 * sizeof(zstid));
	allocated_size = 1000;

	tmpcontext = AllocSetContextCreate(CurrentMemoryContext,
									   "ZedstoreAMVacuumContext",
									   ALLOCSET_DEFAULT_SIZES);
	oldcontext = MemoryContextSwitchTo(tmpcontext);

	intset_begin_iterate(tids);
	if (!intset_iterate_next(tids, &nexttid))
		nexttid = InvalidZSTid;

	while (nexttid < MaxPlusOneZSTid)
	{
		ZSAttStream *lowerstream;
		ZSAttStream *upperstream;
		ZSAttStream *newstream;

		buf = zsbt_descend(rel, attno, nexttid, 0, false);
		page = BufferGetPage(buf);
		opaque = ZSBtreePageGetOpaque(page);

		/*
		 * We now have a page at hand, that (should) contain at least one
		 * of the TIDs we want to remove.
		 */
		num_to_remove = 0;
		while (nexttid < opaque->zs_hikey)
		{
			if (num_to_remove == allocated_size)
			{
				tids_to_remove = repalloc(tids_to_remove, (allocated_size * 2) * sizeof(zstid));
				allocated_size *= 2;
			}
			tids_to_remove[num_to_remove++] = nexttid;
			if (!intset_iterate_next(tids, &nexttid))
				nexttid = MaxPlusOneZSTid;
		}


		/* Remove the data for those TIDs, and rewrite the page */
		lowerstream = get_page_lowerstream(page);
		upperstream = get_page_upperstream(page);

		if (lowerstream || upperstream)
		{
			newstream = merge_attstreams(rel, attr, lowerstream, upperstream,
										 tids_to_remove, num_to_remove);

			/*
			 * Now we have a list of non-overlapping items, containing all the old and
			 * new data. zsbt_attr_repack_replace() takes care of storing them on the
			 * page, splitting the page if needed.
			 */
			zsbt_attr_repack(rel, attno, buf, newstream, false, InvalidZSTid);
			/* zsbt_attr_repack() unlocked and released the buffer */
		}
		else
			UnlockReleaseBuffer(buf);

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

static ZSAttStream *
get_page_lowerstream(Page page)
{
	int			lowersize;
	ZSAttStream *lowerstream;

	/* Read old uncompressed (lower) attstream */
	lowersize = ((PageHeader) page)->pd_lower - SizeOfPageHeaderData;
	if (lowersize > SizeOfZSAttStreamHeader)
	{
		lowerstream = (ZSAttStream *) (((char *) page) + SizeOfPageHeaderData);
		Assert((lowerstream)->t_size == lowersize);
	}
	else
	{
		Assert (lowersize == 0);
		lowerstream = NULL;
	}

	return lowerstream;
}

static ZSAttStream *
get_page_upperstream(Page page)
{
	int			uppersize;
	ZSAttStream *upperstream;

	uppersize = ((PageHeader) page)->pd_special - ((PageHeader) page)->pd_upper;
	if (uppersize > SizeOfZSAttStreamHeader)
	{
		upperstream = (ZSAttStream *) (((char *) page) + ((PageHeader) page)->pd_upper);
		Assert(upperstream->t_size == uppersize);
	}
	else
	{
		upperstream = NULL;
		Assert(uppersize == 0);
	}
	return upperstream;
}

/*
 * Modify an attribute leaf page, adding new data.
 *
 * 'newstream' contains new attribute data that is to be added to the page.
 *
 * This function handles decompressing and recompressing items, and splitting
 * the page, as needed.
 *
 * The page should be pinned and locked on entry. This function will release
 * the lock, but will keep the page pinned.
 */
static void
zsbt_attr_add_to_page(Relation rel, AttrNumber attno, Buffer buf, ZSAttStream *newstream)
{
	Form_pg_attribute attr = &rel->rd_att->attrs[attno - 1];
	Page		page = BufferGetPage(buf);
	ZSAttStream *merged_stream;
	ZSAttStream *lowerstream = NULL;
	ZSAttStream *upperstream = NULL;
	uint16		old_pd_lower;
	uint16		new_pd_lower;
	zstid		firstnewtid;

	Assert(newstream);

	lowerstream = get_page_lowerstream(page);
	upperstream = get_page_upperstream(page);

	/* Is there space to add the new attstream as it is? */
	old_pd_lower = ((PageHeader) page)->pd_lower;

	if (lowerstream == NULL)
	{
		/*
		 * No existing uncompressed data on page, see if the new data fits as is.
		 */
		Assert(old_pd_lower == SizeOfPageHeaderData);

		if (newstream->t_size <= PageGetExactFreeSpace(page))
		{
			new_pd_lower = SizeOfPageHeaderData + newstream->t_size;

			START_CRIT_SECTION();

			memcpy(page + SizeOfPageHeaderData, newstream, newstream->t_size);
			((PageHeader) page)->pd_lower = new_pd_lower;

			MarkBufferDirty(buf);

			if (RelationNeedsWAL(rel))
				wal_log_attstream_change(rel, buf, newstream, false,
										 old_pd_lower, new_pd_lower);

			END_CRIT_SECTION();

			LockBuffer(buf, BUFFER_LOCK_UNLOCK);
			return;
		}
	}
	else
	{
		/*
		 * Try to append the new data to the old data
		 */
		Assert(lowerstream->t_size == old_pd_lower - SizeOfPageHeaderData);

		START_CRIT_SECTION();

		if (append_attstreams_inplace(attr, lowerstream,
									  PageGetExactFreeSpace(page),
									  newstream))
		{
			new_pd_lower = SizeOfPageHeaderData + lowerstream->t_size;

			/* fast path succeeded */
			MarkBufferDirty(buf);

			/*
			 * NOTE: in theory, if append_attstream_inplace() was smarter, it might
			 * modify the existing data. The new combined stream might even be smaller
			 * than the old stream, if the last codewords are packed more tighthly.
			 * But at the moment, append_attstreams_inplace() doesn't do anything
			 * that smart. So we asume that the existing data didn't change, and we
			 * only need to WAL log the new data at the end of the stream.
			 */
			((PageHeader) page)->pd_lower = new_pd_lower;

			if (RelationNeedsWAL(rel))
				wal_log_attstream_change(rel, buf, lowerstream, false,
										 old_pd_lower, new_pd_lower);

			END_CRIT_SECTION();

			LockBuffer(buf, BUFFER_LOCK_UNLOCK);
			return;
		}

		END_CRIT_SECTION();
	}

	/*
	 * If the old page contains already-compressed data, and it is almost full,
	 * leave the old data untouched and create a new page. That avoids repeatedly
	 * recompressing pages when inserting rows one by one. Somewhat arbitrarily,
	 * we put the threshold at 2.5%.
	 */
	firstnewtid = get_attstream_first_tid(attr->attlen, newstream);
	if (PageGetExactFreeSpace(page) + (lowerstream ? lowerstream->t_size : 0) < (int) (BLCKSZ * 0.025) &&
		(lowerstream == NULL || firstnewtid > lowerstream->t_lasttid) &&
		upperstream &&  firstnewtid > upperstream->t_lasttid)
	{
		/*
		 * repack the new data, to make sure it's dense. XXX: Do we really need
		 * to do this?
		 */
		int			origsize = newstream->t_size;

		newstream = merge_attstreams(rel, attr, newstream, NULL, NULL, 0);

		elog(DEBUG2, "appending new page, %d -> %d", origsize, newstream->t_size);

		/* Now pass the list to the repacker, to distribute the items to pages. */
		IncrBufferRefCount(buf);
		zsbt_attr_repack(rel, attno, buf, newstream, true, firstnewtid);
		return;
	}

	/*
	 * Need to recompress and/or split the hard way.
	 */

	/* merge the old uncompressed items to decompressed items */
	if (upperstream && lowerstream)
	{
		/* Merge old compressed data, old uncompressed data, and new data */
		ZSAttStream *tmp;

		tmp = merge_attstreams(rel, attr, upperstream, lowerstream, NULL, 0);

		/* append new data */
		merged_stream = merge_attstreams(rel, attr, tmp, newstream, NULL, 0);

		elog(DEBUG2, "merged upper %d, lower %d, new %d -> %d",
			 upperstream->t_size, lowerstream->t_size,
			 newstream->t_size,
			 merged_stream->t_size);
	}
	else if (upperstream)
	{
		merged_stream = merge_attstreams(rel, attr, upperstream, newstream, NULL, 0);

		elog(DEBUG2, "merged upper %d, new %d -> %d",
			 upperstream->t_size, newstream->t_size,
			 merged_stream->t_size);
	}
	else if (lowerstream)
	{
		merged_stream = merge_attstreams(rel, attr, lowerstream, newstream, NULL, 0);
		elog(DEBUG2, "merged lower %d, new %d -> %d",
			 lowerstream->t_size, newstream->t_size,
			 merged_stream->t_size);
	}
	else
	{
		/*
		 * nothing old on page. But force the new stream to be re-encoded. Hopefully
		 * that makes it more compact.
		 */
		merged_stream = merge_attstreams(rel, attr, newstream, NULL, NULL, 0);

		elog(DEBUG2, "repacked new %d -> %d", newstream->t_size,
			 merged_stream->t_size);
	}

	/*
	 * Now we have a list of non-overlapping items, containing all the old and
	 * new data. zsbt_attr_repack() takes care of storing them on the
	 * page, splitting the page if needed.
	 */
	IncrBufferRefCount(buf);
	zsbt_attr_repack(rel, attno, buf, merged_stream, false, InvalidZSTid);
}


/*
 * zsbt_attr_repack - Rewrite a leaf page with new content.
 *
 * If 'append' is false, all content on page is replaced with the data from
 * 'attstream'. If 'append' is true, old content on the page is kept unmodified,
 * and the data in 'attstream' is added to a newly allocated page, after
 * this page. When 'append' is true, 'firstnewkey' must be passed. It is the
 * divider between the old and new page. Typically, it should be the first TID
 * in 'attstream'.
 *
 * The input stream is repacked, chopped and compressed, for optimal space
 * usage. If the data don't fit on the page, then the page is split. It is
 * entirely possible that they don't fit even on two pages; we split the page
 * into as many pages as needed. Hopefully not more than a few pages, though,
 * because otherwise you might hit limits on the number of buffer pins (with
 * tiny shared_buffers).
 *
 * On entry, 'oldbuf' must be pinned and exclusive-locked. It is released and
 * unpinned on exit.
 */
struct zsbt_attr_repack_context
{
	Page		currpage;

	/* first page writes over the old buffer, subsequent pages get newly-allocated buffers */
	zs_split_stack *stack_head;
	zs_split_stack *stack_tail;

	AttrNumber	attno;
	zstid		hikey;
};

static void
zsbt_attr_repack(Relation rel, AttrNumber attno, Buffer oldbuf, ZSAttStream *attstream,
				 bool append, zstid firstnewkey)
{
	Form_pg_attribute attr = &rel->rd_att->attrs[attno - 1];
	struct zsbt_attr_repack_context cxt;
	Page		oldpage = BufferGetPage(oldbuf);
	ZSBtreePageOpaque *oldopaque = ZSBtreePageGetOpaque(oldpage);
	BlockNumber orignextblk;
	zstid		lokey;

	orignextblk = oldopaque->zs_next;

	cxt.currpage = NULL;
	cxt.stack_head = cxt.stack_tail = NULL;
	cxt.attno = attno;
	cxt.hikey = oldopaque->zs_hikey;

	lokey = oldopaque->zs_lokey;
	zsbt_attr_repack_newpage(&cxt, lokey, (oldopaque->zs_flags & ZSBT_ROOT));

	if (append && !attstream)
	{
		/* The caller asked to not modify old page, and provided no new data. Then
		 * we have nothing todo; but why did we get into this situation in the first
		 * place? XXX
		 */
		LockBuffer(oldbuf, BUFFER_LOCK_UNLOCK);
		return;
	}

	/*
	 * In append mode, keep the old page unmodified, and allocate a new page
	 * for the new data.
	 */
	if (append)
	{
		ZSAttStream *lowerstream = get_page_lowerstream(oldpage);
		ZSAttStream *upperstream = get_page_upperstream(oldpage);

		if (lowerstream == NULL && upperstream == NULL)
		{
			/* completely empty page. Probably can't happen... */
			append = false;
		}
		else
		{
			/*
			 * we are not changing the data on the old buffer, only the
			 * hikey
			 */
			cxt.stack_head->special_only = true;

			if ((attstream->t_flags & ATTSTREAM_COMPRESSED) != 0)
			{
				/*
				 * We cannot get the first TID of a compressed attstream easily.
				 * Perhaps we should add it to the attstream header? But currently,
				 * this doesn't happen, the caller never need to do this.
				 */
				elog(ERROR, "cannot append already-compressed attstream");
			}

			/* high key for the original page and lo key for the next page */
			lokey = get_attstream_first_tid(attr->attlen, attstream);

			/*
			 * Restore the original page, and change its hi key. create a new page
			 * for the new data
			 */
			memcpy(cxt.currpage, oldpage, BLCKSZ);
			zsbt_attr_repack_newpage(&cxt, firstnewkey, 0);

			/* continue to add the new attstream to the new page */
		}
	}

	/*
	 * Write out all the new data, compressing and chopping it as needed
	 * to fit on pages.
	 */
	if (attstream)
	{
		chopper_state chopper;

		chopper.data = (char *) attstream;
		chopper.len = attstream->t_size;
		chopper.cursor = SizeOfZSAttStreamHeader;
		chopper.lasttid = attstream->t_lasttid;

		/*
		 * Then, store them on the page, creating new pages as needed.
		 */
		Assert(orignextblk != BufferGetBlockNumber(oldbuf));

		zsbt_attr_pack_attstream(attr, &chopper, cxt.currpage, &lokey);

		while (chopper.cursor < chopper.len)
		{
			zsbt_attr_repack_newpage(&cxt, lokey, 0);
			zsbt_attr_pack_attstream(attr, &chopper, cxt.currpage, &lokey);
		}
	}
	zsbt_attr_repack_write_pages(&cxt, rel, attno, oldbuf, orignextblk);
}

/*
 * internal routines of zsbt_attr_repack.
 */

static void
zsbt_attr_repack_write_pages(struct zsbt_attr_repack_context *cxt,
							 Relation rel, AttrNumber attno,
							 Buffer oldbuf, BlockNumber orignextblk)
{
	Page		oldpage = BufferGetPage(oldbuf);
	ZSBtreePageOpaque *oldopaque = ZSBtreePageGetOpaque(oldpage);
	zs_split_stack *stack;
	List	   *downlinks = NIL;

	/*
	 * Ok, we now have a list of pages, to replace the original page, as private
	 * in-memory copies. Allocate buffers for them, and write them out.
	 *
	 * allocate all the pages before entering critical section, so that
	 * out-of-disk-space doesn't lead to PANIC
	 */
	stack = cxt->stack_head;
	Assert(stack->buf == InvalidBuffer);
	stack->buf = oldbuf;
	while (stack->next)
	{
		Page	thispage = stack->page;
		ZSBtreePageOpaque *thisopaque = ZSBtreePageGetOpaque(thispage);
		ZSBtreeInternalPageItem *downlink;
		Buffer	nextbuf;

		Assert(stack->next->buf == InvalidBuffer);

		nextbuf = zspage_getnewbuf(rel);
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
	if (cxt->stack_head->next)
	{
		oldopaque = ZSBtreePageGetOpaque(cxt->stack_head->page);

		if ((oldopaque->zs_flags & ZSBT_ROOT) != 0)
		{
			ZSBtreeInternalPageItem *downlink;

			downlink = palloc(sizeof(ZSBtreeInternalPageItem));
			downlink->tid = MinZSTid;
			downlink->childblk = BufferGetBlockNumber(cxt->stack_head->buf);
			downlinks = lcons(downlink, downlinks);

			cxt->stack_tail->next = zsbt_newroot(rel, attno, oldopaque->zs_level + 1, downlinks);

			/* clear the ZSBT_ROOT flag on the old root page */
			oldopaque->zs_flags &= ~ZSBT_ROOT;
		}
		else
		{
			cxt->stack_tail->next = zsbt_insert_downlinks(rel, attno,
														  oldopaque->zs_lokey,
														  BufferGetBlockNumber(oldbuf),
														  oldopaque->zs_level + 1,
														  downlinks);
		}
		/* note: stack_tail is not the real tail anymore */
	}

	/* Finally, overwrite all the pages we had to modify */
	zs_apply_split_changes(rel, cxt->stack_head, NULL);
}


static void
zsbt_attr_repack_newpage(struct zsbt_attr_repack_context *cxt, zstid nexttid, int flags)
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
 * Subroutine of zsbt_attr_repack(). Compress and write as much of the data
 * from 'chopper' onto 'page' as fits. *remaintid is set to the first TID
 * in 'chopper' that did not fit (it will be used as the high key of the
 * page, when the page is split for the remaining data).
 */
static void
zsbt_attr_pack_attstream(Form_pg_attribute attr, chopper_state *chopper,
						Page page, zstid *remaintid)
{
	Size		freespc;
	int			orig_bytes;
	char	   *pstart;
	char	   *pend;
	char	   *dst;
	int			complete_chunks_len;
	zstid		lasttid = 0;
	int			srcSize;
	int			compressed_size;
	ZSAttStream *hdr;
	char		compressbuf[BLCKSZ];

	/* this should only be called on an empty page */
	Assert(((PageHeader) page)->pd_lower == SizeOfPageHeaderData);
	freespc = PageGetExactFreeSpace(page);

	pstart = &chopper->data[chopper->cursor];
	pend = &chopper->data[chopper->len];
	orig_bytes = pend - pstart;

	freespc -= SizeOfZSAttStreamHeader;

	/*
	 * Try compressing.
	 *
	 * Note: we try compressing, even if the data fits uncompressed. That might seem
	 * like a waste of time, but compression is very cheap, and this leaves more free
	 * space on the page for new additions.
	 */
	srcSize = orig_bytes;
	compressed_size = zs_compress_destSize(pstart, compressbuf, &srcSize, freespc);
	if (compressed_size > 0)
	{
		/* store compressed, in upper stream */
		int			bytes_compressed = srcSize;

		/*
		 * How many complete chunks did we compress?
		 */
		if (bytes_compressed == orig_bytes)
		{
			complete_chunks_len = orig_bytes;
			lasttid = chopper->lasttid;
		}
		else
			complete_chunks_len = truncate_attstream(attr, pstart, bytes_compressed, &lasttid);

		if (complete_chunks_len == 0)
			elog(ERROR, "could not fit any chunks on page");

		dst = (char *) page + ((PageHeader) page)->pd_special - (SizeOfZSAttStreamHeader + compressed_size);
		hdr = (ZSAttStream *) dst;
		hdr->t_size = SizeOfZSAttStreamHeader + compressed_size;
		hdr->t_flags = ATTSTREAM_COMPRESSED;
		hdr->t_decompressed_size = complete_chunks_len;
		hdr->t_decompressed_bufsize = bytes_compressed;
		hdr->t_lasttid = lasttid;

		dst = hdr->t_payload;
		memcpy(dst, compressbuf, compressed_size);
		((PageHeader) page)->pd_upper -= hdr->t_size;
	}
	else
	{
		/* Store uncompressed, in lower stream. */

		/*
		 * How many complete chunks can we fit?
		 */
		if (orig_bytes < freespc)
		{
			complete_chunks_len = orig_bytes;
			lasttid = chopper->lasttid;
		}
		else
			complete_chunks_len = truncate_attstream(attr, pstart, freespc, &lasttid);

		if (complete_chunks_len == 0)
			elog(ERROR, "could not fit any chunks on page");

		hdr = (ZSAttStream *) ((char *) page + SizeOfPageHeaderData);

		hdr->t_size = SizeOfZSAttStreamHeader + complete_chunks_len;
		hdr->t_flags = 0;
		hdr->t_decompressed_size = 0;
		hdr->t_decompressed_bufsize = 0;
		hdr->t_lasttid = lasttid;

		dst = hdr->t_payload;
		memcpy(dst, pstart, complete_chunks_len);
		((PageHeader) page)->pd_lower += hdr->t_size;
	}

	/*
	 * Since we split the chunk stream, inject a reference point to the beginning of
	 * the remainder.
	 */
	*remaintid = chop_attstream(attr, chopper, complete_chunks_len, lasttid);
}

static void
wal_log_attstream_change(Relation rel, Buffer buf, ZSAttStream *attstream,
						 bool is_upper, uint16 begin_offset, uint16 end_offset)
{
	/*
	 * log only the modified portion.
	 */
	Page		page = BufferGetPage(buf);
	XLogRecPtr	recptr;
	wal_zedstore_attstream_change xlrec;
#ifdef USE_ASSERT_CHECKING
	uint16		pd_lower = ((PageHeader) page)->pd_lower;
	uint16		pd_upper = ((PageHeader) page)->pd_upper;
	uint16		pd_special = ((PageHeader) page)->pd_special;
#endif

	Assert(begin_offset < end_offset);

	memset(&xlrec, 0, sizeof(xlrec)); /* clear padding */
	xlrec.is_upper = is_upper;

	xlrec.new_attstream_size = attstream->t_size;
	xlrec.new_decompressed_size = attstream->t_decompressed_size;
	xlrec.new_decompressed_bufsize = attstream->t_decompressed_bufsize;
	xlrec.new_lasttid = attstream->t_lasttid;

	xlrec.begin_offset = begin_offset;
	xlrec.end_offset = end_offset;

	if (is_upper)
		Assert(begin_offset >= pd_upper && end_offset <= pd_special);
	else
		Assert(begin_offset >= SizeOfPageHeaderData && end_offset <= pd_lower);

	XLogBeginInsert();
	XLogRegisterBuffer(0, buf, REGBUF_STANDARD);
	XLogRegisterData((char *) &xlrec, SizeOfZSWalAttstreamChange);
	XLogRegisterBufData(0, (char *) page + begin_offset, end_offset - begin_offset);

	recptr = XLogInsert(RM_ZEDSTORE_ID, WAL_ZEDSTORE_ATTSTREAM_CHANGE);

	PageSetLSN(page, recptr);
}

void
zsbt_attstream_change_redo(XLogReaderState *record)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	wal_zedstore_attstream_change *xlrec =
		(wal_zedstore_attstream_change *) XLogRecGetData(record);
	Buffer		buffer;

	if (XLogReadBufferForRedo(record, 0, &buffer) == BLK_NEEDS_REDO)
	{
		Page		page = (Page) BufferGetPage(buffer);
		Size		datasz;
		char	   *data = XLogRecGetBlockData(record, 0, &datasz);
		ZSAttStream *attstream;

		Assert(datasz == xlrec->end_offset - xlrec->begin_offset);

		if (xlrec->is_upper)
		{
			/*
			 * In the upper stream, if the size changes, the old data is moved
			 * to begin at pd_upper, and then the new data is applied.
			 *
			 * XXX: we could be much smarter about this, and not move data that
			 * we will overwrite on the next line.
			 */
			uint16		pd_special = ((PageHeader) page)->pd_special;
			uint16		new_pd_upper = pd_special - xlrec->new_attstream_size;
			uint16		old_pd_upper = ((PageHeader) page)->pd_upper;
			uint16		old_size = old_pd_upper - pd_special;
			uint16		new_size = new_pd_upper - pd_special;

			memmove(page + new_pd_upper, page + old_pd_upper, Min(old_size, new_size));

			((PageHeader) page)->pd_upper = new_pd_upper;
		}
		else
		{
			uint16		new_pd_lower = SizeOfPageHeaderData + xlrec->new_attstream_size;

			((PageHeader) page)->pd_lower = new_pd_lower;
		}

		memcpy(page + xlrec->begin_offset, data, datasz);

		/*
		 * Finally, adjust the size in the attstream header to match.
		 * (if the replacement data in the WAL record covered the attstream
		 * header, this is unnecessarily but harmless)
		 */
		attstream = (ZSAttStream *) (
			xlrec->is_upper ? (page + ((PageHeader) page)->pd_upper) :
			(page + SizeOfPageHeaderData));
		attstream->t_size = xlrec->new_attstream_size;
		attstream->t_decompressed_size = xlrec->new_decompressed_size;
		attstream->t_decompressed_bufsize = xlrec->new_decompressed_bufsize;
		attstream->t_lasttid = xlrec->new_lasttid;

		PageSetLSN(page, lsn);
		MarkBufferDirty(buffer);
	}
	if (BufferIsValid(buffer))
		UnlockReleaseBuffer(buffer);
}
