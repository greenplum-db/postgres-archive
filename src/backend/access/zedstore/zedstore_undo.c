/*
 * zedstore_undo.c
 *		Temporary UNDO-logging for zedstore.
 *
 * XXX: This is hopefully replaced with an upstream UNDO facility later.
 *
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/zedstore/zedstore_undo.c
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/multixact.h"
#include "access/zedstore_internal.h"
#include "access/zedstore_undo.h"
#include "commands/progress.h"
#include "commands/vacuum.h"
#include "miscadmin.h"
#include "postmaster/autovacuum.h"
#include "pgstat.h"
#include "utils/memutils.h"
#include "utils/pg_rusage.h"
#include "utils/rel.h"
#include "utils/lsyscache.h"

typedef struct ZSVacRelStats
{
	int			elevel;
	BufferAccessStrategy vac_strategy;

	/* hasindex = true means two-pass strategy; false means one-pass */
	bool		hasindex;
	/* Overall statistics about rel */
	BlockNumber old_rel_pages;	/* previous value of pg_class.relpages */
	BlockNumber rel_pages;		/* total number of pages */
	BlockNumber scanned_pages;	/* number of pages we examined */
	BlockNumber pinskipped_pages;	/* # of pages we skipped due to a pin */
	BlockNumber frozenskipped_pages;	/* # of frozen pages we skipped */
	BlockNumber tupcount_pages; /* pages whose tuples we counted */
	double		old_live_tuples;	/* previous value of pg_class.reltuples */
	double		new_rel_tuples; /* new estimated total # of tuples */
	double		new_live_tuples;	/* new estimated total # of live tuples */
	double		new_dead_tuples;	/* new estimated total # of dead tuples */
	BlockNumber pages_removed;
	double		tuples_deleted;
	BlockNumber nonempty_pages; /* actually, last nonempty page + 1 */
	/* List of TIDs of tuples we intend to delete */
	/* NB: this list is ordered by TID address */
	int			num_dead_tuples;	/* current # of entries */
	int			max_dead_tuples;	/* # slots allocated in array */
	ItemPointer dead_tuples;	/* array of ItemPointerData */
	bool		dead_tuples_overflowed;
	int			num_index_scans;
	TransactionId latestRemovedXid;

	/* ZedStore specific stats */
	BlockNumber	deleted_undo_pages;
} ZSVacRelStats;

/*
 * Guesstimation of number of dead tuples per page.  This is used to
 * provide an upper limit to memory allocated when vacuuming small
 * tables.
 */
#define LAZY_ALLOC_TUPLES		MaxHeapTuplesPerPage

static int zs_vac_cmp_itemptr(const void *left, const void *right);
static bool zs_lazy_tid_reaped(ItemPointer itemptr, void *state);
static void lazy_space_alloc(ZSVacRelStats *vacrelstats, BlockNumber relblocks);
static void lazy_vacuum_index(Relation indrel,
				  IndexBulkDeleteResult **stats,
				  ZSVacRelStats *vacrelstats);
static void lazy_cleanup_index(Relation indrel,
				   IndexBulkDeleteResult *stats,
				   ZSVacRelStats *vacrelstats);
static ZSUndoRecPtr zsundo_trim(Relation rel, TransactionId OldestXmin, ZSVacRelStats *vacrelstats, BlockNumber *oldest_undopage);
static void zsundo_update_oldest_ptr(Relation rel, ZSUndoRecPtr oldest_undorecptr, BlockNumber oldest_undopage);
static void zsundo_record_dead_tuple(ZSVacRelStats *vacrelstats, zstid tid);

/*
 * Insert the given UNDO record to the UNDO log.
 */
ZSUndoRecPtr
zsundo_insert(Relation rel, ZSUndoRec *rec)
{
	Buffer		metabuf;
	Page		metapage;
	ZSMetaPageOpaque *metaopaque;
	BlockNumber	tail_blk;
	Buffer		tail_buf = InvalidBuffer;
	Page		tail_pg = NULL;
	ZSUndoPageOpaque *tail_opaque = NULL;
	char	   *dst;
	ZSUndoRecPtr undorecptr;
	int			offset;
	uint64		undo_counter;

	metabuf = ReadBuffer(rel, ZS_META_BLK);
	metapage = BufferGetPage(metabuf);

	/* TODO: get share lock to begin with, for more concurrency */
	LockBuffer(metabuf, BUFFER_LOCK_EXCLUSIVE);
	metaopaque = (ZSMetaPageOpaque *) PageGetSpecialPointer(metapage);

	tail_blk = metaopaque->zs_undo_tail;

	/* Is there space on the tail page? */
	if (tail_blk != InvalidBlockNumber)
	{
		tail_buf = ReadBuffer(rel, tail_blk);
		LockBuffer(tail_buf, BUFFER_LOCK_EXCLUSIVE);
		tail_pg = BufferGetPage(tail_buf);
		tail_opaque = (ZSUndoPageOpaque *) PageGetSpecialPointer(tail_pg);
	}
	if (tail_blk == InvalidBlockNumber || PageGetExactFreeSpace(tail_pg) < rec->size)
	{
		Buffer 		newbuf;
		BlockNumber newblk;
		Page		newpage;
		ZSUndoPageOpaque *newopaque;

		/* new page */
		newbuf = zs_getnewbuf(rel);
		newblk = BufferGetBlockNumber(newbuf);
		newpage = BufferGetPage(newbuf);
		PageInit(newpage, BLCKSZ, sizeof(ZSUndoPageOpaque));
		newopaque = (ZSUndoPageOpaque *) PageGetSpecialPointer(newpage);
		newopaque->next = InvalidBlockNumber;
		newopaque->zs_page_id = ZS_UNDO_PAGE_ID;

		metaopaque->zs_undo_tail = newblk;
		if (tail_blk == InvalidBlockNumber)
			metaopaque->zs_undo_head = newblk;

		MarkBufferDirty(metabuf);

		if (tail_blk != InvalidBlockNumber)
		{
			tail_opaque->next = newblk;
			MarkBufferDirty(tail_buf);
			UnlockReleaseBuffer(tail_buf);
		}

		tail_blk = newblk;
		tail_buf = newbuf;
		tail_pg = newpage;
		tail_opaque = newopaque;
	}

	undo_counter = metaopaque->zs_undo_counter++;
	MarkBufferDirty(metabuf);

	UnlockReleaseBuffer(metabuf);

	/* insert the record to this page */
	offset = ((PageHeader) tail_pg)->pd_lower;

	undorecptr.counter = undo_counter;
	undorecptr.blkno = tail_blk;
	undorecptr.offset = offset;
	rec->undorecptr = undorecptr;
	dst = ((char *) tail_pg) + offset;
	memcpy(dst, rec, rec->size);
	((PageHeader) tail_pg)->pd_lower += rec->size;
	MarkBufferDirty(tail_buf);
	UnlockReleaseBuffer(tail_buf);

	return undorecptr;
}

/*
 * Fetch the UNDO record with the given undo-pointer.
 *
 * The returned record is a palloc'd copy.
 */
ZSUndoRec *
zsundo_fetch(Relation rel, ZSUndoRecPtr undoptr)
{
	Buffer		buf;
	Page		page;
	PageHeader	pagehdr;
	ZSUndoPageOpaque *opaque;
	ZSUndoRec  *undorec;
	ZSUndoRec  *undorec_copy;

	buf = ReadBuffer(rel, undoptr.blkno);
	page = BufferGetPage(buf);
	pagehdr = (PageHeader) page;

	LockBuffer(buf, BUFFER_LOCK_SHARE);
	opaque = (ZSUndoPageOpaque *) PageGetSpecialPointer(page);
	if (opaque->zs_page_id != ZS_UNDO_PAGE_ID)
		elog(ERROR, "could not find UNDO record " UINT64_FORMAT " at blk %u offset %u; not an UNDO page",
			 undoptr.counter, undoptr.blkno, undoptr.offset);

	/* Sanity check that the pointer pointed to a valid place */
	if (undoptr.offset < SizeOfPageHeaderData ||
		undoptr.offset + sizeof(ZSUndoRec) > pagehdr->pd_lower)
		elog(ERROR, "could not find UNDO record " UINT64_FORMAT " at blk %u offset %u",
			 undoptr.counter, undoptr.blkno, undoptr.offset);

	undorec = (ZSUndoRec *) (((char *) page) + undoptr.offset);

	if (memcmp(&undorec->undorecptr, &undoptr, sizeof(ZSUndoRecPtr)) != 0)
		elog(ERROR, "could not find UNDO record");

	undorec_copy = palloc(undorec->size);
	memcpy(undorec_copy, undorec, undorec->size);

	UnlockReleaseBuffer(buf);

	return undorec_copy;
}

static bool
zs_lazy_tid_reaped(ItemPointer itemptr, void *state)
{
	ZSVacRelStats *vacrelstats = (ZSVacRelStats *) state;
	ItemPointer res;

	res = (ItemPointer) bsearch((void *) itemptr,
								(void *) vacrelstats->dead_tuples,
								vacrelstats->num_dead_tuples,
								sizeof(ItemPointerData),
								zs_vac_cmp_itemptr);

	return (res != NULL);
}

/*
 * Comparator routines for use with qsort() and bsearch().
 */
static int
zs_vac_cmp_itemptr(const void *left, const void *right)
{
	BlockNumber lblk,
				rblk;
	OffsetNumber loff,
				roff;

	lblk = ItemPointerGetBlockNumber((ItemPointer) left);
	rblk = ItemPointerGetBlockNumber((ItemPointer) right);

	if (lblk < rblk)
		return -1;
	if (lblk > rblk)
		return 1;

	loff = ItemPointerGetOffsetNumber((ItemPointer) left);
	roff = ItemPointerGetOffsetNumber((ItemPointer) right);

	if (loff < roff)
		return -1;
	if (loff > roff)
		return 1;

	return 0;
}

void
zsundo_vacuum(Relation rel, VacuumParams *params, BufferAccessStrategy bstrategy,
			  TransactionId OldestXmin)
{
	ZSVacRelStats *vacrelstats;
	Relation   *Irel;
	int			nindexes;
	IndexBulkDeleteResult **indstats;
	BlockNumber	nblocks;

	nblocks = RelationGetNumberOfBlocks(rel);
	if (nblocks == 0)
		return;		/* empty table */

	vacrelstats = (ZSVacRelStats *) palloc0(sizeof(ZSVacRelStats));

	if (params->options & VACOPT_VERBOSE)
		vacrelstats->elevel = INFO;
	else
		vacrelstats->elevel = DEBUG2;
	vacrelstats->vac_strategy = bstrategy;

	/* Open all indexes of the relation */
	vac_open_indexes(rel, RowExclusiveLock, &nindexes, &Irel);
	vacrelstats->hasindex = (nindexes > 0);
	indstats = (IndexBulkDeleteResult **)
		palloc0(nindexes * sizeof(IndexBulkDeleteResult *));

	lazy_space_alloc(vacrelstats, nblocks);

	ereport(vacrelstats->elevel,
			(errmsg("vacuuming \"%s.%s\"",
					get_namespace_name(RelationGetNamespace(rel)),
					RelationGetRelationName(rel))));

	do
	{
		ZSUndoRecPtr reaped_upto;
		BlockNumber oldest_undopage;
		int			j;

		vacrelstats->dead_tuples_overflowed = false;
		vacrelstats->num_dead_tuples = 0;
		vacrelstats->deleted_undo_pages = 0;

		reaped_upto = zsundo_trim(rel, OldestXmin, vacrelstats, &oldest_undopage);

		if (vacrelstats->num_dead_tuples > 0)
		{
			pg_qsort(vacrelstats->dead_tuples, vacrelstats->num_dead_tuples, sizeof(ItemPointerData),
					 zs_vac_cmp_itemptr);
			/* TODO: currently, we write a separate UNDO record for each attribute, so there will
			 * be duplicates. Eliminate them. */
			j = 1;
			for (int i = 1; i < vacrelstats->num_dead_tuples; i++)
			{
				if (!ItemPointerEquals(&vacrelstats->dead_tuples[j - 1], &vacrelstats->dead_tuples[i]))
					vacrelstats->dead_tuples[j++] = vacrelstats->dead_tuples[i];
			}
			vacrelstats->num_dead_tuples = j;

			/* Remove index entries */
			for (int i = 0; i < nindexes; i++)
				lazy_vacuum_index(Irel[i],
								  &indstats[i],
								  vacrelstats);

			/*
			 * Mark the items as dead in the attribute b-trees.
			 *
			 * We cannot remove them immediately, because we must prevent the TIDs from
			 * being reused, until we have trimmed the UNDO records. Otherwise, this might
			 * happen:
			 *
			 * 1. We remove items from all the B-trees.
			 * 2. An inserter reuses the now-unused TID for a new tuple
			 * 3. We abort the VACUUM, for some reason
			 * 4. We start VACUUM again. We will now try to remove the item again, but
			 *    we will remove the new item with the same TID instead.
			 *
			 * There would be other ways to deal with it. One easy optimization would be
			 * to leave the DEAD item in only one of the attributes, and remove all others
			 * completely. Or in step #4, we could refrain from removing items, whose
			 * UNDO pointers are newer than expected. But that's tricky, because we scan
			 * the indexes first, and we must refrain from removing index entries for
			 * new items, too.
			 */
			for (int attno = 1; attno <= RelationGetNumberOfAttributes(rel); attno++)
			{
				for (int i = 0; i < vacrelstats->num_dead_tuples; i++)
					zsbt_mark_item_dead(rel, attno, ZSTidFromItemPointer(vacrelstats->dead_tuples[i]), reaped_upto);
			}
		}

		/*
		 * The UNDO records for the tuple versions we just removed are no longer
		 * interesting to anyone. Advance the UNDO tail, so that the UNDO pages
		 * can be recycled.
		 */
		zsundo_update_oldest_ptr(rel, reaped_upto, oldest_undopage);

		ereport(vacrelstats->elevel,
				(errmsg("\"%s\": removed %d row versions and %d undo pages",
						RelationGetRelationName(rel),
						vacrelstats->num_dead_tuples,
						vacrelstats->deleted_undo_pages)));
	} while(vacrelstats->dead_tuples_overflowed);

	/* Do post-vacuum cleanup and statistics update for each index */
	for (int i = 0; i < nindexes; i++)
		lazy_cleanup_index(Irel[i], indstats[i], vacrelstats);

	/* Done with indexes */
	vac_close_indexes(nindexes, Irel, NoLock);
}


/*
 * lazy_space_alloc - space allocation decisions for lazy vacuum
 *
 * See the comments at the head of this file for rationale.
 */
static void
lazy_space_alloc(ZSVacRelStats *vacrelstats, BlockNumber relblocks)
{
	long		maxtuples;
	int			vac_work_mem = IsAutoVacuumWorkerProcess() &&
	autovacuum_work_mem != -1 ?
	autovacuum_work_mem : maintenance_work_mem;

	if (vacrelstats->hasindex)
	{
		maxtuples = (vac_work_mem * 1024L) / sizeof(ItemPointerData);
		maxtuples = Min(maxtuples, INT_MAX);
		maxtuples = Min(maxtuples, MaxAllocSize / sizeof(ItemPointerData));

		/* curious coding here to ensure the multiplication can't overflow */
		if ((BlockNumber) (maxtuples / LAZY_ALLOC_TUPLES) > relblocks)
			maxtuples = relblocks * LAZY_ALLOC_TUPLES;

		/* stay sane if small maintenance_work_mem */
		maxtuples = Max(maxtuples, MaxHeapTuplesPerPage);
	}
	else
	{
		maxtuples = MaxHeapTuplesPerPage;
	}

	vacrelstats->num_dead_tuples = 0;
	vacrelstats->max_dead_tuples = (int) maxtuples;
	vacrelstats->dead_tuples = (ItemPointer)
		palloc(maxtuples * sizeof(ItemPointerData));
}

/*
 *	lazy_vacuum_index() -- vacuum one index relation.
 *
 *		Delete all the index entries pointing to tuples listed in
 *		vacrelstats->dead_tuples, and update running statistics.
 */
static void
lazy_vacuum_index(Relation indrel,
				  IndexBulkDeleteResult **stats,
				  ZSVacRelStats *vacrelstats)
{
	IndexVacuumInfo ivinfo;
	PGRUsage	ru0;

	pg_rusage_init(&ru0);

	ivinfo.index = indrel;
	ivinfo.analyze_only = false;
	ivinfo.estimated_count = true;
	ivinfo.message_level = vacrelstats->elevel;
	/* We can only provide an approximate value of num_heap_tuples here */
	ivinfo.num_heap_tuples = vacrelstats->old_live_tuples;
	ivinfo.strategy = vacrelstats->vac_strategy;

	/* Do bulk deletion */
	*stats = index_bulk_delete(&ivinfo, *stats,
							   zs_lazy_tid_reaped, (void *) vacrelstats);

	ereport(vacrelstats->elevel,
			(errmsg("scanned index \"%s\" to remove %d row versions",
					RelationGetRelationName(indrel),
					vacrelstats->num_dead_tuples),
			 errdetail_internal("%s", pg_rusage_show(&ru0))));
}

/*
 *	lazy_cleanup_index() -- do post-vacuum cleanup for one index relation.
 */
static void
lazy_cleanup_index(Relation indrel,
				   IndexBulkDeleteResult *stats,
				   ZSVacRelStats *vacrelstats)
{
	IndexVacuumInfo ivinfo;
	PGRUsage	ru0;

	pg_rusage_init(&ru0);

	ivinfo.index = indrel;
	ivinfo.analyze_only = false;
	ivinfo.estimated_count = (vacrelstats->tupcount_pages < vacrelstats->rel_pages);
	ivinfo.message_level = vacrelstats->elevel;

	/*
	 * Now we can provide a better estimate of total number of surviving
	 * tuples (we assume indexes are more interested in that than in the
	 * number of nominally live tuples).
	 */
	ivinfo.num_heap_tuples = vacrelstats->new_rel_tuples;
	ivinfo.strategy = vacrelstats->vac_strategy;

	stats = index_vacuum_cleanup(&ivinfo, stats);

	if (!stats)
		return;

	/*
	 * Now update statistics in pg_class, but only if the index says the count
	 * is accurate.
	 */
	if (!stats->estimated_count)
		vac_update_relstats(indrel,
							stats->num_pages,
							stats->num_index_tuples,
							0,
							false,
							InvalidTransactionId,
							InvalidMultiXactId,
							false);

	ereport(vacrelstats->elevel,
			(errmsg("index \"%s\" now contains %.0f row versions in %u pages",
					RelationGetRelationName(indrel),
					stats->num_index_tuples,
					stats->num_pages),
			 errdetail("%.0f index row versions were removed.\n"
					   "%u index pages have been deleted, %u are currently reusable.\n"
					   "%s.",
					   stats->tuples_removed,
					   stats->pages_deleted, stats->pages_free,
					   pg_rusage_show(&ru0))));

	pfree(stats);
}

/*
 * Scan the UNDO log, starting from oldest entry. For every tuple that is
 * now considered dead, add it to 'dead_tuples'. Records for committed
 * transactions can be trimmed away immediately.
 *
 * Returns the value that the oldest UNDO ptr can be trimmed upto, after
 * removing all the dead TIDs.
 */
static ZSUndoRecPtr
zsundo_trim(Relation rel, TransactionId OldestXmin, ZSVacRelStats *vacrelstats,
			BlockNumber *oldest_undopage)
{
	/* Scan the undo log from oldest to newest */
	Buffer		metabuf;
	Page		metapage;
	ZSMetaPageOpaque *metaopaque;
	BlockNumber	firstblk;
	BlockNumber	lastblk;
	ZSUndoRecPtr oldest_undorecptr;
	char	   *ptr;
	char	   *endptr;

	/*
	 * Get the current oldest undo page from the metapage.
	 */
	metabuf = ReadBuffer(rel, ZS_META_BLK);
	metapage = BufferGetPage(metabuf);
	LockBuffer(metabuf, BUFFER_LOCK_SHARE);
	metaopaque = (ZSMetaPageOpaque *) PageGetSpecialPointer(metapage);

	firstblk = metaopaque->zs_undo_head;

	oldest_undorecptr = metaopaque->zs_undo_oldestptr;

	/*
	 * If we assume that only one process can call TRIM at a time, then we
	 * don't need to hold the metapage locked. Alternatively, if multiple
	 * concurrent trims is possible, we could check after reading the head
	 * page, that it is the page we expect, and re-read the metapage if it's
	 * not.
	 */
	UnlockReleaseBuffer(metabuf);

	/*
	 * Loop through UNDO records, starting from the oldest page, until we
	 * hit a record that we cannot remove.
	 */
	lastblk = firstblk;
	while (lastblk != InvalidBlockNumber && !vacrelstats->dead_tuples_overflowed)
	{
		Buffer		buf;
		Page		page;
		ZSUndoPageOpaque *opaque;

		CHECK_FOR_INTERRUPTS();

		/* Read the UNDO page */
		buf = ReadBuffer(rel, lastblk);
		page = BufferGetPage(buf);
		LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
		opaque = (ZSUndoPageOpaque *) PageGetSpecialPointer(page);

		if (opaque->zs_page_id != ZS_UNDO_PAGE_ID)
			elog(ERROR, "unexpected page id on UNDO page");

		/* loop through all records on the page */
		endptr = (char *) page + ((PageHeader) page)->pd_lower;
		ptr = (char *) page + SizeOfPageHeaderData;
		while (ptr < endptr && !vacrelstats->dead_tuples_overflowed)
		{
			ZSUndoRec *undorec = (ZSUndoRec *) ptr;
			bool		did_commit;

			Assert(undorec->undorecptr.blkno == lastblk);

			oldest_undorecptr = undorec->undorecptr;

			if (undorec->undorecptr.counter < oldest_undorecptr.counter)
			{
				ptr += undorec->size;
				continue;
			}

			if (!TransactionIdPrecedes(undorec->xid, OldestXmin))
			{
				/* This is still needed. Bail out */
				break;
			}

			/*
			 * No one thinks this transaction is in-progress anymore. If it
			 * committed, we can just trim away its UNDO record. If it aborted,
			 * we need to apply the UNDO record first.
			 *
			 * TODO: applying UNDO actions has not been implemented, so if we
			 * encounter an aborted record, we just stop dead right there, and
			 * never trim past that point.
			 */
			did_commit = TransactionIdDidCommit(undorec->xid);

			switch (undorec->type)
			{
				case ZSUNDO_TYPE_INSERT:
					if (!did_commit)
						zsundo_record_dead_tuple(vacrelstats, undorec->tid);
					break;
				case ZSUNDO_TYPE_DELETE:
					if (did_commit)
						zsundo_record_dead_tuple(vacrelstats, undorec->tid);
					break;
				case ZSUNDO_TYPE_UPDATE:
					if (did_commit)
						zsundo_record_dead_tuple(vacrelstats, ((ZSUndoRec_Update *) undorec)->otid);
					else
						zsundo_record_dead_tuple(vacrelstats, undorec->tid);
					break;
			}
			ptr += undorec->size;
		}

		if (ptr < endptr)
		{
			UnlockReleaseBuffer(buf);
			break;
		}
		else
		{
			/* We processed all records on the page. Step to the next one, if any. */
			Assert(ptr == endptr);
			lastblk = opaque->next;
			UnlockReleaseBuffer(buf);
			if (lastblk != InvalidBlockNumber)
				vacrelstats->deleted_undo_pages++;
		}
	}

	*oldest_undopage = lastblk;
	return oldest_undorecptr;
}

/* Update metapage with the oldest value */
static void
zsundo_update_oldest_ptr(Relation rel, ZSUndoRecPtr oldest_undorecptr, BlockNumber oldest_undopage)
{
	/* Scan the undo log from oldest to newest */
	Buffer		metabuf;
	Page		metapage;
	ZSMetaPageOpaque *metaopaque;

	metabuf = ReadBuffer(rel, ZS_META_BLK);
	metapage = BufferGetPage(metabuf);
	LockBuffer(metabuf, BUFFER_LOCK_EXCLUSIVE);
	metaopaque = (ZSMetaPageOpaque *) PageGetSpecialPointer(metapage);

	metaopaque->zs_undo_oldestptr = oldest_undorecptr;
	if (oldest_undopage == InvalidBlockNumber)
	{
		metaopaque->zs_undo_head = InvalidBlockNumber;
		metaopaque->zs_undo_tail = InvalidBlockNumber;
	}
	else
		metaopaque->zs_undo_head = oldest_undopage;

	/* TODO: WAL-log */

	MarkBufferDirty(metabuf);
	UnlockReleaseBuffer(metabuf);

	/* TODO: we leak all the removed undo pages */
}

/*
 * zsundo_record_dead_tuple - remember one deletable tuple
 */
static void
zsundo_record_dead_tuple(ZSVacRelStats *vacrelstats, zstid tid)
{
	/*
	 * The array shouldn't overflow under normal behavior, but perhaps it
	 * could if we are given a really small maintenance_work_mem. In that
	 * case, just forget the last few tuples (we'll get 'em next time).
	 */
	if (vacrelstats->num_dead_tuples < vacrelstats->max_dead_tuples)
	{
		vacrelstats->dead_tuples[vacrelstats->num_dead_tuples] = ItemPointerFromZSTid(tid);
		vacrelstats->num_dead_tuples++;
		pgstat_progress_update_param(PROGRESS_VACUUM_NUM_DEAD_TUPLES,
									 vacrelstats->num_dead_tuples);
	}
	else
		vacrelstats->dead_tuples_overflowed = true;
}
