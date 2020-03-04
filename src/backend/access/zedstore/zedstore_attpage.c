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

typedef struct
{
	Page		currpage;

	/* first page writes over the old buffer, subsequent pages get newly-allocated buffers */
	zs_split_stack *stack_head;
	zs_split_stack *stack_tail;

	AttrNumber	attno;
	zstid		hikey;

	BlockNumber	nextblkno;
} zsbt_attr_repack_context;

/* prototypes for local functions */
static ZSAttStream *get_page_lowerstream(Page page);
static ZSAttStream *get_page_upperstream(Page page);
static void wal_log_attstream_change(Relation rel, Buffer buf, ZSAttStream *attstream, bool is_upper,
									 uint16 begin_offset, uint16 end_offset);

static void zsbt_attr_repack_init(zsbt_attr_repack_context *cxt, AttrNumber attno, Buffer oldbuf, bool append);
static void zsbt_attr_repack_newpage(zsbt_attr_repack_context *cxt, zstid nexttid);
static void zsbt_attr_pack_attstream(Form_pg_attribute attr, attstream_buffer *buf, Page page);
static void zsbt_attr_repack_writeback_pages(zsbt_attr_repack_context *cxt,
											 Relation rel, AttrNumber attno,
											 Buffer oldbuf);

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

	init_attstream_decoder(&scan->decoder, scan->attdesc->attbyval, scan->attdesc->attlen);
	scan->decoder.tmpcxt = AllocSetContextCreate(scan->context,
												"ZedstoreAMAttrScanContext",
												ALLOCSET_DEFAULT_SIZES);

	scan->decoder_last_idx = -1;

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

	destroy_attstream_decoder(&scan->decoder);
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
	Buffer		buf;
	Page		page;
	ZSAttStream *stream;

	if (!scan->active)
		return InvalidZSTid;

	/*
	 * If the TID we're looking for is in the current attstream, we just
	 * need to decoder more of it.
	 *
	 * TODO: We could restart the decoder, if the current attstream
	 * covers the target TID, but we already decoded past it.
	 */
	if (scan->decoder.pos < scan->decoder.chunks_len &&
		nexttid >= scan->decoder.firsttid &&
		nexttid <= scan->decoder.lasttid)
	{
		if (nexttid <= scan->decoder.prevtid)
		{
			/*
			 * The target TID is in this attstream, but we already scanned
			 * past it. Restart the decoder.
			 */
			scan->decoder.pos = 0;
			scan->decoder.prevtid = 0;
		}

		/* Advance the scan, until we have reached the target TID */
		while (nexttid > scan->decoder.prevtid)
			(void) decode_attstream_cont(&scan->decoder);

		if (scan->decoder.num_elements == 0 ||
			nexttid < scan->decoder.tids[0])
			return false;
		else
			return true;
	}

	/* reset the decoder */
	scan->decoder.num_elements = 0;
	scan->decoder.chunks_len = 0;
	scan->decoder.pos = 0;
	scan->decoder.prevtid = 0;

	/*
	 * Descend the tree, tind and lock the leaf page containing 'nexttid'.
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
		return false;
	}
	page = BufferGetPage(buf);

	/* See if the upper stream covers the target tid */
	stream = get_page_upperstream(page);
	if (stream && nexttid <= stream->t_lasttid)
	{
		decode_attstream_begin(&scan->decoder, stream);
	}
	/*
	 * How about the lower stream? (We assume that the upper stream is < lower
	 * stream, and there's no overlap).
	 */
	else
	{
		stream = get_page_lowerstream(page);
		if (stream && nexttid <= stream->t_lasttid)
		{
			/* If there is a match, it will be in this attstream */
			decode_attstream_begin(&scan->decoder, stream);
		}
	}

	LockBuffer(buf, BUFFER_LOCK_UNLOCK);

	/*
	 * We now have the attstream we need copied into scan->decoder (or not, if
	 * no covering attstream was found)
	 */
	if (scan->decoder.pos < scan->decoder.chunks_len &&
		nexttid >= scan->decoder.firsttid &&
		nexttid <= scan->decoder.lasttid)
	{
		/* Advance the scan, until we have reached the target TID */
		while (nexttid > scan->decoder.prevtid)
			(void) decode_attstream_cont(&scan->decoder);

		if (scan->decoder.num_elements == 0 ||
			nexttid < scan->decoder.tids[0])
			return false;
		else
			return true;
	}
	else
		return false;
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
			attstream_buffer upperbuf;
			attstream_buffer lowerbuf;
			attstream_buffer *newbuf;
			zsbt_attr_repack_context cxt;

			upperbuf.len = 0;
			upperbuf.cursor = 0;
			lowerbuf.len = 0;
			lowerbuf.cursor = 0;

			if (upperstream)
				vacuum_attstream(rel, attno, &upperbuf, upperstream,
								 tids_to_remove, num_to_remove);

			if (lowerstream)
				vacuum_attstream(rel, attno, &lowerbuf, lowerstream,
								 tids_to_remove, num_to_remove);

			if (upperbuf.len - upperbuf.cursor > 0 &&
				lowerbuf.len - lowerbuf.cursor > 0)
			{
				merge_attstream_buffer(attr, &upperbuf, &lowerbuf);
				newbuf = &upperbuf;
			}
			else if (upperbuf.len - upperbuf.cursor > 0)
				newbuf = &upperbuf;
			else
				newbuf = &lowerbuf;

			/*
			 * Now we have a list of non-overlapping items, containing all the old and
			 * new data. zsbt_attr_rewrite_page() takes care of storing them on the
			 * page, splitting the page if needed.
			 */
			zsbt_attr_repack_init(&cxt, attno, buf, false);
			if (newbuf->len - newbuf->cursor > 0)
			{
				/*
				 * Then, store them on the page, creating new pages as needed.
				 */
				zsbt_attr_pack_attstream(attr, newbuf, cxt.currpage);
				while (newbuf->cursor < newbuf->len)
				{
					zsbt_attr_repack_newpage(&cxt, newbuf->firsttid);
					zsbt_attr_pack_attstream(attr, newbuf, cxt.currpage);
				}
			}
			zsbt_attr_repack_writeback_pages(&cxt, rel, attno, buf);
			/* zsbt_attr_rewriteback_pages() unlocked and released the buffer */
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

		/* by convention, lower stream is always uncompressed */
		Assert((lowerstream->t_flags & ATTSTREAM_COMPRESSED) == 0);
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
		/* by convention, upper stream is always compressed */
		Assert((upperstream->t_flags & ATTSTREAM_COMPRESSED) != 0);
	}
	else
	{
		upperstream = NULL;
		Assert(uppersize == 0);
	}
	return upperstream;
}

/*
 * Add data to attribute leaf pages.
 *
 * 'attbuf' contains the new attribute data that is to be added to the page.
 *
 * This function writes as much data as is convenient; typically, as much
 * as fits on a single page, after compression. Some data is always written.
 * If you want to flush all data to disk, call zsbt_attr_add() repeatedly,
 * until 'attbuf' is empty.
 *
 * 'attbuf' is updated, so that on exit, it contains the data that remains,
 * i.e. data that was not yet written out.
 *
 * This function handles decompressing and recompressing items, and splitting
 * the page, as needed.
 */
void
zsbt_attr_add(Relation rel, AttrNumber attno, attstream_buffer *attbuf)
{
	Form_pg_attribute attr = &rel->rd_att->attrs[attno - 1];
	Buffer		origbuf;
	Page		origpage;
	ZSBtreePageOpaque *origpageopaque;
	ZSAttStream *lowerstream;
	ZSAttStream *upperstream;
	int			lowerstreamsz;
	uint16		orig_pd_lower;
	uint16		new_pd_lower;
	zstid		firstnewtid;
	zstid 		splittid;
	zsbt_attr_repack_context cxt;
	bool		split = false;

	Assert (attbuf->len - attbuf->cursor > 0);

	/*
	 * Find the right place to insert the new data.
	 */
	origbuf = zsbt_descend(rel, attno, attbuf->firsttid, 0, false);
	origpage = BufferGetPage(origbuf);
	origpageopaque = ZSBtreePageGetOpaque(origpage);
	splittid = origpageopaque->zs_hikey - 1;

	Assert (attbuf->firsttid <= splittid);

	lowerstream = get_page_lowerstream(origpage);
	upperstream = get_page_upperstream(origpage);

	/* Is there space to add the new attstream as it is? */
	orig_pd_lower = ((PageHeader) origpage)->pd_lower;

	if (lowerstream == NULL)
	{
		/*
		 * No existing uncompressed data on page, see if the new data can fit
		 * into the uncompressed area.
		 */
		Assert(orig_pd_lower == SizeOfPageHeaderData);

		if (SizeOfZSAttStreamHeader + (attbuf->len - attbuf->cursor) <= PageGetExactFreeSpace(origpage))
		{
			ZSAttStream newhdr;
			attstream_buffer newattbuf;

			newhdr.t_flags = 0;
			newhdr.t_decompressed_size = 0;
			newhdr.t_decompressed_bufsize = 0;

			if (attbuf->lasttid > splittid)
			{
				/*
				 * We should not accommodate items with tids greater than the
				 * hikey of the target leaf page. So if our attbuf does have such
				 * items, we split the attbuf into two buffers at tid: hikey - 1.
				 * This will ensure that we only insert the tids that fit into
				 * the page's range.
				 */
				split_attstream_buffer(attbuf, &newattbuf, splittid);
				split = true;
			}

			newhdr.t_size = SizeOfZSAttStreamHeader + (attbuf->len - attbuf->cursor);
			newhdr.t_lasttid = attbuf->lasttid;
			new_pd_lower = SizeOfPageHeaderData + newhdr.t_size;

			START_CRIT_SECTION();

			memcpy(origpage + SizeOfPageHeaderData, &newhdr, SizeOfZSAttStreamHeader);
			memcpy(origpage + SizeOfPageHeaderData + SizeOfZSAttStreamHeader,
				   attbuf->data + attbuf->cursor, attbuf->len - attbuf->cursor);
			((PageHeader) origpage)->pd_lower = new_pd_lower;

			MarkBufferDirty(origbuf);

			if (RelationNeedsWAL(rel))
				wal_log_attstream_change(rel, origbuf,
										 (ZSAttStream *) (origpage + SizeOfPageHeaderData), false,
										 orig_pd_lower, new_pd_lower);

			END_CRIT_SECTION();

			UnlockReleaseBuffer(origbuf);
			if (split)
			{
				/*
				 * Make attbuf represent the chunks that were on the right hand
				 * side of the split. These are the chunks that are left over.
				 */
				pfree(attbuf->data);
				memcpy(attbuf, &newattbuf, sizeof(attstream_buffer));
			}
			else
				attbuf->cursor = attbuf->len;
			return;
		}
	}
	else
	{
		/*
		 * Try to append the new data to the existing uncompressed data first
		 */
		START_CRIT_SECTION();

		if (attbuf->lasttid <= splittid &&
			append_attstream_inplace(attr, lowerstream,
									 PageGetExactFreeSpace(origpage),
									 attbuf))
		{
			new_pd_lower = SizeOfPageHeaderData + lowerstream->t_size;

			/* fast path succeeded */
			MarkBufferDirty(origbuf);

			/*
			 * NOTE: in theory, if append_attstream_inplace() was smarter, it might
			 * modify the existing data. The new combined stream might even be smaller
			 * than the old stream, if the last codewords are packed more tighthly.
			 * But at the moment, append_attstreams_inplace() doesn't do anything
			 * that smart. So we assume that the existing data didn't change, and we
			 * only need to WAL log the new data at the end of the stream.
			 */
			((PageHeader) origpage)->pd_lower = new_pd_lower;

			if (RelationNeedsWAL(rel))
				wal_log_attstream_change(rel, origbuf, lowerstream, false,
										 orig_pd_lower, new_pd_lower);

			END_CRIT_SECTION();

			UnlockReleaseBuffer(origbuf);
			return;
		}

		END_CRIT_SECTION();
	}

	/*
	 * If the orig page contains already-compressed data, and it is almost full,
	 * leave the old data untouched and create a new page. This avoids repeatedly
	 * recompressing pages when inserting rows one by one. Somewhat arbitrarily,
	 * we put the threshold at 2.5%.
	 *
	 * TODO: skipping allocating new page here if attbuf->lasttid > splittid,
	 * because we don't know how to handle that without calling merge_attstream()
	 */
	firstnewtid = attbuf->firsttid;
	lowerstreamsz = lowerstream ? lowerstream->t_size : 0;
	if (attbuf->lasttid <= splittid &&
		PageGetExactFreeSpace(origpage) + lowerstreamsz < (int) (BLCKSZ * 0.025) &&
		(lowerstream == NULL || firstnewtid > lowerstream->t_lasttid) &&
		upperstream &&  firstnewtid > upperstream->t_lasttid)
	{
		/*
		 * Keep the original page unmodified, and allocate a new page
		 * for the new data.
		 */
		zsbt_attr_repack_init(&cxt, attno, origbuf, true);
		zsbt_attr_repack_newpage(&cxt, attbuf->firsttid);

		/* write out the new data (or part of it) */
		zsbt_attr_pack_attstream(attr, attbuf, cxt.currpage);
	}
	else
	{
		/*
		 * Rewrite existing data on the page, and add as much of the
		 * new data as fits. But make sure that we write at least one
		 * chunk of new data, otherwise we might get stuck in a loop
		 * without making any progress.
		 */
		zstid		mintid = attbuf->firsttid;
		attstream_buffer rightattbuf;

#if 0
		if (upperstream && lowerstream)
			elog(NOTICE, "merging upper %d lower %d new %d", upperstream->t_decompressed_size, lowerstream->t_size, attbuf->len - attbuf->cursor);
		else if (upperstream)
			elog(NOTICE, "merging upper %d new %d", upperstream->t_decompressed_size, attbuf->len - attbuf->cursor);
		else if (lowerstream)
			elog(NOTICE, "merging lower %d new %d", lowerstream->t_size, attbuf->len - attbuf->cursor);
		else if (lowerstream)
			elog(NOTICE, "merging new %d", attbuf->len - attbuf->cursor);
#endif

		/* merge the old items to the working buffer */
		if (upperstream && lowerstream)
		{
			attstream_buffer tmpbuf;

			mintid = Max(mintid, lowerstream->t_lasttid);
			mintid = Max(mintid, upperstream->t_lasttid);

			init_attstream_buffer_from_stream(&tmpbuf, attr->attbyval,
				attr->attlen, upperstream, GetMemoryChunkContext(attbuf->data));

			merge_attstream(attr, attbuf, lowerstream);

			merge_attstream_buffer(attr, &tmpbuf, attbuf);

			pfree(attbuf->data);
			*attbuf = tmpbuf;
		}
		else if (lowerstream)
		{
			mintid = Max(mintid, lowerstream->t_lasttid);
			merge_attstream(attr, attbuf, lowerstream);
		}
		else if (upperstream)
		{
			mintid = Max(mintid, upperstream->t_lasttid);
			merge_attstream(attr, attbuf, upperstream);
		}

		/*
		 * Now we have a list of non-overlapping items, containing all the old and
		 * new data. Write it out, making sure that at least all the old data is
		 * written out (otherwise, we'd momentarily remove existing data!)
		 */
		zsbt_attr_repack_init(&cxt, attno, origbuf, false);

		if (attbuf->lasttid > splittid)
		{
			/*
			 * We should not accommodate items with tids greater than the
			 * hikey of the target leaf page. So if our attbuf does have such
			 * items, we split the attbuf into two buffers at tid: hikey - 1.
			 * This will ensure that we only insert the tids that fit into
			 * the page's range.
			 */
			split_attstream_buffer(attbuf, &rightattbuf, splittid);
			split = true;
		}

		zsbt_attr_pack_attstream(attr, attbuf, cxt.currpage);

		while (attbuf->cursor < attbuf->len && (split || attbuf->firsttid <= mintid))
		{
			zsbt_attr_repack_newpage(&cxt, attbuf->firsttid);
			zsbt_attr_pack_attstream(attr, attbuf, cxt.currpage);
		}

		if (split)
		{
			/*
			 * Make attbuf represent the chunks that were on the right hand
			 * side of the split. These are the chunks that are left over.
			 */
			Assert(attbuf->cursor == attbuf->len);
			pfree(attbuf->data);
			memcpy(attbuf, &rightattbuf, sizeof(attstream_buffer));
		}
	}
	zsbt_attr_repack_writeback_pages(&cxt, rel, attno, origbuf);
}

/*
 * Repacker routines
 *
 * Usage:
 *
 * 1. Call zsbt_attr_repack_init() to start a repacking operation.
 * 2. Call zsbt_attr_pack_attstream() to compress and chop a page-sied slice
 *    of incoming data, and store it on the current page copy.
 * 3. Call zsbt_attr_repack_newpage() to allocate a new page, if you want
 *    to compress and write more data, and goto 2.
 * 4. Call zsbt_attr_repack_writeback_pages() to finish the repacking
 *    operation, making all on-disk changes.
 *
 * Steps 1-3 happen on in-memory pages copies. No data on-disk is
 * is modified until step 4.
 */
static void
zsbt_attr_repack_init(zsbt_attr_repack_context *cxt, AttrNumber attno, Buffer origbuf, bool append)
{
	Page		origpage;
	ZSBtreePageOpaque *origopaque;
	Page		newpage;
	ZSBtreePageOpaque *newopaque;
	zs_split_stack *stack;

	origpage = BufferGetPage(origbuf);
	origopaque = ZSBtreePageGetOpaque(origpage);

	cxt->stack_head = cxt->stack_tail = NULL;
	cxt->attno = attno;
	cxt->hikey = origopaque->zs_hikey;
	cxt->nextblkno = origopaque->zs_next;

	newpage = (Page) palloc(BLCKSZ);
	if (append)
		memcpy(newpage, origpage, BLCKSZ);
	else
		PageInit(newpage, BLCKSZ, sizeof(ZSBtreePageOpaque));
	cxt->currpage = newpage;

	stack = zs_new_split_stack_entry(InvalidBuffer, /* will be assigned later */
									 newpage);
	if (append)
		stack->special_only = true;
	cxt->stack_head = stack;
	cxt->stack_tail = stack;

	newopaque = ZSBtreePageGetOpaque(newpage);
	newopaque->zs_attno = cxt->attno;
	newopaque->zs_next = InvalidBlockNumber; /* filled in later */
	newopaque->zs_lokey = origopaque->zs_lokey;
	newopaque->zs_hikey = cxt->hikey;		/* overwritten later, if this is not last page */
	newopaque->zs_level = 0;
	newopaque->zs_flags = origopaque->zs_flags & ZSBT_ROOT;
	newopaque->zs_page_id = ZS_BTREE_PAGE_ID;
}

static void
zsbt_attr_repack_newpage(zsbt_attr_repack_context *cxt, zstid nexttid)
{
	Page		newpage;
	ZSBtreePageOpaque *newopaque;
	zs_split_stack *stack;
	ZSBtreePageOpaque *oldopaque = ZSBtreePageGetOpaque(cxt->currpage);

	/* set the last tid on previous page */
	oldopaque->zs_hikey = nexttid;

	newpage = (Page) palloc(BLCKSZ);
	PageInit(newpage, BLCKSZ, sizeof(ZSBtreePageOpaque));

	stack = zs_new_split_stack_entry(InvalidBuffer, /* will be assigned later */
									 newpage);
	cxt->stack_tail->next = stack;
	cxt->stack_tail = stack;

	cxt->currpage = newpage;

	newopaque = ZSBtreePageGetOpaque(newpage);
	newopaque->zs_attno = cxt->attno;
	newopaque->zs_next = InvalidBlockNumber; /* filled in later */
	newopaque->zs_lokey = nexttid;
	newopaque->zs_hikey = cxt->hikey;		/* overwritten later, if this is not last page */
	newopaque->zs_level = 0;
	newopaque->zs_flags = 0;
	newopaque->zs_page_id = ZS_BTREE_PAGE_ID;
}

/*
 * Compress and write as much of the data from 'attbuf' onto 'page' as fits.
 * 'attbuf' is updated in place, so that on exit, it contains the remaining chunks
 * that did not fit on 'page'.
 */
static void
zsbt_attr_pack_attstream(Form_pg_attribute attr, attstream_buffer *attbuf,
						 Page page)
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

	pstart = &attbuf->data[attbuf->cursor];
	pend = &attbuf->data[attbuf->len];
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
			lasttid = attbuf->lasttid;
		}
		else
			complete_chunks_len =
				find_chunk_for_offset(attbuf, bytes_compressed, &lasttid);

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
			lasttid = attbuf->lasttid;
		}
		else
			complete_chunks_len =
				find_chunk_for_offset(attbuf, freespc, &lasttid);

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
	 * Chop off the part of the chunk stream in 'attbuf' that we wrote out.
	 */
	trim_attstream_upto_offset(attbuf, complete_chunks_len, lasttid);
}
static void
zsbt_attr_repack_writeback_pages(zsbt_attr_repack_context *cxt,
								 Relation rel, AttrNumber attno,
								 Buffer oldbuf)
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

		nextbuf = zspage_getnewbuf(rel, attno);
		stack->next->buf = nextbuf;

		thisopaque->zs_next = BufferGetBlockNumber(nextbuf);

		downlink = palloc(sizeof(ZSBtreeInternalPageItem));
		downlink->tid = thisopaque->zs_hikey;
		downlink->childblk = BufferGetBlockNumber(nextbuf);
		downlinks = lappend(downlinks, downlink);

		stack = stack->next;
	}
	/* last one in the chain */
	ZSBtreePageGetOpaque(stack->page)->zs_next = cxt->nextblkno;

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
	zs_apply_split_changes(rel, cxt->stack_head, NULL, attno);
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
