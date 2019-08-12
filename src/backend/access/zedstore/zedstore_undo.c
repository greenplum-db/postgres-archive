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
#include "lib/integerset.h"
#include "miscadmin.h"
#include "postmaster/autovacuum.h"
#include "pgstat.h"
#include "storage/lmgr.h"
#include "utils/memutils.h"
#include "utils/pg_rusage.h"
#include "utils/rel.h"
#include "utils/lsyscache.h"

/*
 * Working area for VACUUM.
 */
typedef struct ZSVacRelStats
{
	int			elevel;
	BufferAccessStrategy vac_strategy;

	/* hasindex = true means two-pass strategy; false means one-pass */
	bool		hasindex;
	/* Overall statistics about rel */
	BlockNumber rel_pages;		/* total number of pages */
	BlockNumber tupcount_pages; /* pages whose tuples we counted */
	double		old_live_tuples;	/* previous value of pg_class.reltuples */
	double		new_rel_tuples; /* new estimated total # of tuples */
	double		new_live_tuples;	/* new estimated total # of live tuples */
	double		new_dead_tuples;	/* new estimated total # of dead tuples */
	BlockNumber pages_removed;
	double		tuples_deleted;

	IntegerSet *dead_tids;
} ZSVacRelStats;

static bool zs_lazy_tid_reaped(ItemPointer itemptr, void *state);
static void lazy_vacuum_index(Relation indrel,
				  IndexBulkDeleteResult **stats,
				  ZSVacRelStats *vacrelstats);
static void lazy_cleanup_index(Relation indrel,
				   IndexBulkDeleteResult *stats,
				   ZSVacRelStats *vacrelstats);
static ZSUndoRecPtr zsundo_trim(Relation rel, TransactionId OldestXmin);
static void zsundo_update_oldest_ptr(Relation rel, ZSUndoRecPtr oldest_undorecptr, BlockNumber oldest_undopage, List *unused_pages);
static ZSUndoRec *zsundo_fetch_lock(Relation rel, ZSUndoRecPtr undoptr, Buffer *buf_p, int lockmode, bool missing_ok);

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

retry_lock_tail:
	tail_blk = metaopaque->zs_undo_tail;

	/*
	 * Is there space on the tail page? If not, allocate a new UNDO page.
	 */
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

		/*
		 * Release the lock on the metapage while we find a new block, because
		 * that could take a while. (And accessing the Free Page Map might lock
		 * the metapage, too, causing self-deadlock.)
		 */
		LockBuffer(metabuf, BUFFER_LOCK_UNLOCK);

		/* new page */
		newbuf = zspage_getnewbuf(rel, metabuf);

		LockBuffer(metabuf, BUFFER_LOCK_EXCLUSIVE);
		if (metaopaque->zs_undo_tail != tail_blk)
		{
			/*
			 * It should not be possible for another backend to extend the UNDO log
			 * while we're holding the tail block locked.
			 */
			if (tail_blk != InvalidBlockNumber)
				elog(ERROR, "UNDO tail block pointer was changed unexpectedly");

			/*
			 * we don't need the new page, after all. (Or maybe we do, if the new
			 * tail block is already full, but we're not smart about it.)
			 */
			zspage_delete_page(rel, newbuf);
			goto retry_lock_tail;
		}

		newblk = BufferGetBlockNumber(newbuf);
		newpage = BufferGetPage(newbuf);
		PageInit(newpage, BLCKSZ, sizeof(ZSUndoPageOpaque));
		newopaque = (ZSUndoPageOpaque *) PageGetSpecialPointer(newpage);
		newopaque->next = InvalidBlockNumber;
		newopaque->first_undorecptr = InvalidUndoPtr;
		newopaque->last_undorecptr = InvalidUndoPtr;
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

	/*
	 * if this is the first record on the page, initialize the field in
	 * the page header, too.
	 */
	if (((PageHeader) tail_pg)->pd_lower == SizeOfPageHeaderData)
		tail_opaque->first_undorecptr = undorecptr;
	tail_opaque->last_undorecptr = undorecptr;
	((PageHeader) tail_pg)->pd_lower += rec->size;

	MarkBufferDirty(tail_buf);
	UnlockReleaseBuffer(tail_buf);

	return undorecptr;
}

/*
 * Fetch the UNDO record with the given undo-pointer.
 *
 * The returned record is a palloc'd copy.
 *
 * If the record could not be found, returns NULL. That can happen if you try
 * to fetch an UNDO record that has already been discarded. I.e. if undoptr
 * is smaller than the oldest UNDO pointer stored in the metapage.
 */
ZSUndoRec *
zsundo_fetch(Relation rel, ZSUndoRecPtr undoptr)
{
	ZSUndoRec  *undorec_copy;
	ZSUndoRec  *undorec;
	Buffer		buf;

	undorec = zsundo_fetch_lock(rel, undoptr, &buf, BUFFER_LOCK_SHARE, true);

	if (undorec)
	{
		undorec_copy = palloc(undorec->size);
		memcpy(undorec_copy, undorec, undorec->size);
	}
	else
		undorec_copy = NULL;

	if (BufferIsValid(buf))
		UnlockReleaseBuffer(buf);

	return undorec_copy;
}

/*
 * Lock page containing the given UNDO record, and return pointer to it
 * within the buffer.
 *
 * If missing_ok is true, it's OK if the UNDO record has been trimmed / discarded away
 * already. Will return NULL in that case. If missing_ok is false, throws an error if
 * the record cannot be found.
 */
static ZSUndoRec *
zsundo_fetch_lock(Relation rel, ZSUndoRecPtr undoptr, Buffer *buf_p, int lockmode, bool missing_ok)
{
	Buffer		buf;
	Page		page;
	PageHeader	pagehdr;
	ZSUndoPageOpaque *opaque;
	ZSUndoRec  *undorec;

	buf = ReadBuffer(rel, undoptr.blkno);
	page = BufferGetPage(buf);
	pagehdr = (PageHeader) page;

	/*
	 * FIXME: If the page might've been trimmed away, there's a small chance of deadlock if
	 * the buffer now holds an unrelated page, and we or someone else is holding a lock on
	 * it already. We could optimistically try lock the page without blocking first, and
	 * and update oldest undo pointer from the metapage if that fails. And only if the
	 * oldest undo pointer indicates that the record should still be there, wait for the lock.
	 */
	LockBuffer(buf, lockmode);
	if (PageIsNew(page))
		goto record_missing;
	opaque = (ZSUndoPageOpaque *) PageGetSpecialPointer(page);
	if (opaque->zs_page_id != ZS_UNDO_PAGE_ID)
		goto record_missing;

	/* Check that this page contains the given record */
	if (undoptr.counter < opaque->first_undorecptr.counter ||
		undoptr.counter > opaque->last_undorecptr.counter)
		goto record_missing;

	/* Sanity check that the pointer pointed to a valid place */
	if (undoptr.offset < SizeOfPageHeaderData ||
		undoptr.offset + sizeof(ZSUndoRec) > pagehdr->pd_lower)
	{
		/*
		 * this should not happen in the case that the page was recycled for
		 * other use, so error even if 'missing_ok' is true
		 */
		elog(ERROR, "could not find UNDO record " UINT64_FORMAT " at blk %u offset %u",
			 undoptr.counter, undoptr.blkno, undoptr.offset);
	}

	undorec = (ZSUndoRec *) (((char *) page) + undoptr.offset);

	if (memcmp(&undorec->undorecptr, &undoptr, sizeof(ZSUndoRecPtr)) != 0)
	{
		/*
		 * this should not happen in the case that the page was recycled for
		 * other use, so error even if 'fail_ok' is true
		 */
		elog(ERROR, "could not find UNDO record " UINT64_FORMAT " at blk %u offset %u",
			 undoptr.counter, undoptr.blkno, undoptr.offset);
	}

	*buf_p = buf;
	return undorec;

record_missing:
	UnlockReleaseBuffer(buf);
	*buf_p = InvalidBuffer;

	if (missing_ok)
		return NULL;
	else
		elog(ERROR, "could not find UNDO record " UINT64_FORMAT " at blk %u offset %u; not an UNDO page",
			 undoptr.counter, undoptr.blkno, undoptr.offset);
}

void
zsundo_clear_speculative_token(Relation rel, ZSUndoRecPtr undoptr)
{
	ZSUndoRec_Insert *undorec;
	Buffer		buf;

	undorec = (ZSUndoRec_Insert *) zsundo_fetch_lock(rel, undoptr, &buf, BUFFER_LOCK_EXCLUSIVE, false);

	if (undorec->rec.type != ZSUNDO_TYPE_INSERT)
		elog(ERROR, "unexpected undo record type %d on speculatively inserted row",
			 undorec->rec.type);

	undorec->speculative_token = INVALID_SPECULATIVE_TOKEN;

	MarkBufferDirty(buf);
	UnlockReleaseBuffer(buf);
}

static bool
zs_lazy_tid_reaped(ItemPointer itemptr, void *state)
{
	ZSVacRelStats *vacrelstats = (ZSVacRelStats *) state;
	zstid		tid = ZSTidFromItemPointer(*itemptr);

	return intset_is_member(vacrelstats->dead_tids, tid);
}

/*
 * Entry point of VACUUM for zedstore tables.
 *
 * Vacuum on a zedstore table works quite differently from the heap. We don't
 * scan the table. Instead, we scan just the active UNDO log, and remove any
 * garbage left behind by aborts or deletions based on the UNDO log.
 */
void
zsundo_vacuum(Relation rel, VacuumParams *params, BufferAccessStrategy bstrategy,
			  TransactionId OldestXmin)
{
	ZSVacRelStats *vacrelstats;
	Relation   *Irel;
	int			nindexes;
	IndexBulkDeleteResult **indstats;
	Form_pg_class pgcform;
	zstid		starttid;
	zstid		endtid;

	/* do nothing if the table is completely empty. */
	if (RelationGetTargetBlock(rel) == 0 ||
		RelationGetTargetBlock(rel) == InvalidBlockNumber)
	{
		/* don't believe the cached value without checking */
		BlockNumber nblocks = RelationGetNumberOfBlocks(rel);

		RelationSetTargetBlock(rel, nblocks);
		if (nblocks == 0)
			return;
	}

	/*
	 * Scan the UNDO log, and discard what we can.
	 */
	(void) zsundo_trim(rel, RecentGlobalXmin);

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

	ereport(vacrelstats->elevel,
			(errmsg("vacuuming \"%s.%s\"",
					get_namespace_name(RelationGetNamespace(rel)),
					RelationGetRelationName(rel))));

	starttid = MinZSTid;
	do
	{
		IntegerSet *dead_tids;

		/* Scan the TID tree, to collect TIDs that have been marked dead. */
		dead_tids = zsbt_collect_dead_tids(rel, starttid, &endtid);
		vacrelstats->dead_tids = dead_tids;

		if (intset_num_entries(dead_tids) > 0)
		{
			/* Remove index entries */
			for (int i = 0; i < nindexes; i++)
				lazy_vacuum_index(Irel[i],
								  &indstats[i],
								  vacrelstats);

			/*
			 * Remove the attribute data for the dead rows, and finally their
			 * TID tree entries.
			 */
			for (int attno = 1; attno <= RelationGetNumberOfAttributes(rel); attno++)
				zsbt_attr_remove(rel, attno, dead_tids);
			zsbt_tid_remove(rel, dead_tids);
		}

		ereport(vacrelstats->elevel,
				(errmsg("\"%s\": removed " UINT64_FORMAT " row versions",
						RelationGetRelationName(rel),
						intset_num_entries(dead_tids))));

		starttid = endtid;
	} while(starttid < MaxZSTid);

	/* Do post-vacuum cleanup and statistics update for each index */
	for (int i = 0; i < nindexes; i++)
		lazy_cleanup_index(Irel[i], indstats[i], vacrelstats);

	/* Done with indexes */
	vac_close_indexes(nindexes, Irel, NoLock);

	/*
	 * Update pg_class to reflect new info we know. The main thing we know for
	 * sure here is relhasindex or not currently. Using OldestXmin as new
	 * frozenxid. And since we don't now the new multixid passing it as
	 * invalid to avoid update. Plus, using false for relallisvisible as don't
	 * know that either.
	 *
	 * FIXME: pass correct numbers for relpages, reltuples and other
	 * arguments.
	 */
	pgcform = RelationGetForm(rel);
	vac_update_relstats(rel,
						pgcform->relpages,
						pgcform->reltuples,
						false,
						nindexes > 0,
						OldestXmin,
						InvalidMultiXactId,
						false);
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
			(errmsg("scanned index \"%s\" to remove " UINT64_FORMAT " row versions",
					RelationGetRelationName(indrel),
					intset_num_entries(vacrelstats->dead_tids)),
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
 * Scan the UNDO log, starting from oldest entry. Undo the effects of any
 * aborted transactions. Records for committed transactions can be trimmed
 * away immediately.
 *
 * Returns the oldest valid UNDO ptr, after the trim.
 */
static ZSUndoRecPtr
zsundo_trim(Relation rel, TransactionId OldestXmin)
{
	/* Scan the undo log from oldest to newest */
	Buffer		metabuf;
	Page		metapage;
	ZSMetaPageOpaque *metaopaque;
	BlockNumber	firstblk;
	BlockNumber	lastblk;
	ZSUndoRecPtr oldest_undorecptr;
	bool		can_advance_oldestundorecptr;
	char	   *ptr;
	char	   *endptr;
	List	   *unused_pages = NIL;
	BlockNumber deleted_undo_pages = 0;

	oldest_undorecptr = InvalidUndoPtr;

	/*
	 * Ensure that only one process discards at a time. We use a page lock on the
	 * metapage for that.
	 */
	LockPage(rel, ZS_META_BLK, ExclusiveLock);

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
	can_advance_oldestundorecptr = false;
	while (lastblk != InvalidBlockNumber)
	{
		Buffer		buf;
		Page		page;
		ZSUndoPageOpaque *opaque;

		CHECK_FOR_INTERRUPTS();

		/* Read the UNDO page */
		buf = ReadBuffer(rel, lastblk);
		page = BufferGetPage(buf);
		LockBuffer(buf, BUFFER_LOCK_SHARE);

		opaque = (ZSUndoPageOpaque *) PageGetSpecialPointer(page);

		if (opaque->zs_page_id != ZS_UNDO_PAGE_ID)
			elog(ERROR, "unexpected page id on UNDO page");

		/* loop through all records on the page */
		endptr = (char *) page + ((PageHeader) page)->pd_lower;
		ptr = (char *) page + SizeOfPageHeaderData;
		while (ptr < endptr)
		{
			ZSUndoRec *undorec = (ZSUndoRec *) ptr;
			bool		did_commit;

			Assert(undorec->undorecptr.blkno == lastblk);

			if (undorec->undorecptr.counter < oldest_undorecptr.counter)
			{
				ptr += undorec->size;
				continue;
			}
			oldest_undorecptr = undorec->undorecptr;

			if (!TransactionIdPrecedes(undorec->xid, OldestXmin))
			{
				/* This is still needed. Bail out */
				break;
			}

			/*
			 * No one thinks this transaction is in-progress anymore. If it
			 * committed, we can just trim away its UNDO record. If it aborted,
			 * we need to apply the UNDO record first. (For deletions, it's
			 * the other way round, though.)
			 *
			 * TODO: It would be much more efficient to do these in batches.
			 * So we should just collect the TIDs to mark dead here, and pass
			 * the whole list to zsbt_tid_mark_dead() after the loop.
			 */
			did_commit = TransactionIdDidCommit(undorec->xid);

			switch (undorec->type)
			{
				case ZSUNDO_TYPE_INSERT:
					if (!did_commit)
					{
						ZSUndoRec_Insert *insertrec  = (ZSUndoRec_Insert *) undorec;

						for (zstid tid = insertrec->firsttid; tid < insertrec->endtid; tid++)
							zsbt_tid_mark_dead(rel, tid, oldest_undorecptr);
					}
					break;
				case ZSUNDO_TYPE_DELETE:
					{
						ZSUndoRec_Delete *deleterec  = (ZSUndoRec_Delete *) undorec;

						if (did_commit)
						{
							/* The deletion is now visible to everyone */
							for (int i = 0; i < deleterec->num_tids; i++)
								zsbt_tid_mark_dead(rel, deleterec->tids[i], oldest_undorecptr);
						}
						else
						{
							/*
							 * must clear the item's UNDO pointer, otherwise the deletion
							 * becomes visible to everyone when the UNDO record is trimmed
							 * away.
							 */
							for (int i = 0; i < deleterec->num_tids; i++)
								zsbt_tid_undo_deletion(rel, deleterec->tids[i], undorec->undorecptr,
									oldest_undorecptr);
						}
					}
					break;
				case ZSUNDO_TYPE_UPDATE:
					if (did_commit)
					{
						ZSUndoRec_Update *updaterec  = (ZSUndoRec_Update *) undorec;

						zsbt_tid_mark_dead(rel, updaterec->oldtid, oldest_undorecptr);
					}
					break;
			}

			ptr += undorec->size;
			can_advance_oldestundorecptr = true;
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
			unused_pages = lappend_int(unused_pages, lastblk);
			lastblk = opaque->next;
			UnlockReleaseBuffer(buf);
			if (lastblk != InvalidBlockNumber)
				deleted_undo_pages++;
		}
	}

	if (can_advance_oldestundorecptr && lastblk == InvalidBlockNumber)
	{
		/*
		 * We stopped after the last valid record. Advance by one, to the next
		 * record which hasn't been created yet, and which is still needed.
		 */
		oldest_undorecptr.counter++;
		oldest_undorecptr.blkno = InvalidBlockNumber;
		oldest_undorecptr.offset = 0;
	}

	if (can_advance_oldestundorecptr)
		zsundo_update_oldest_ptr(rel, oldest_undorecptr, lastblk, unused_pages);

	UnlockPage(rel, ZS_META_BLK, ExclusiveLock);

	return oldest_undorecptr;
}

/* Update metapage with the oldest value */
static void
zsundo_update_oldest_ptr(Relation rel, ZSUndoRecPtr oldest_undorecptr,
						 BlockNumber oldest_undopage, List *unused_pages)
{
	/* Scan the undo log from oldest to newest */
	Buffer		metabuf;
	Page		metapage;
	ZSMetaPageOpaque *metaopaque;
	ListCell   *lc;

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

	foreach(lc, unused_pages)
	{
		BlockNumber blk = (BlockNumber) lfirst_int(lc);
		Buffer		buf;
		Page		page;
		ZSUndoPageOpaque *opaque;

		/* check that the page still looks like what we'd expect. */
		buf = ReadBuffer(rel, blk);
		LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
		page = BufferGetPage(buf);
		if (PageIsEmpty(page) ||
			PageGetSpecialSize(page) != MAXALIGN(sizeof(ZSUndoPageOpaque)))
		{
			UnlockReleaseBuffer(buf);
			continue;
		}
		opaque = (ZSUndoPageOpaque *) PageGetSpecialPointer(page);
		if (opaque->zs_page_id != ZS_UNDO_PAGE_ID)
		{
			UnlockReleaseBuffer(buf);
			continue;
		}

		/* FIXME: Also check here that the max UndoRecPtr on the page is less
		 * than the new 'oldest_undorecptr'
		 */

		zspage_delete_page(rel, buf);
		UnlockReleaseBuffer(buf);
	}
}

/*
 * Return the current "Oldest undo pointer". The effects of any actions with
 * undo pointer older than this is known to be visible to everyone. (i.e.
 * an inserted tuple is known to be visible, and a deleted tuple is known to
 * be invisible.)
 */
ZSUndoRecPtr
zsundo_get_oldest_undo_ptr(Relation rel)
{
	/* do nothing if the table is completely empty. */
	if (RelationGetTargetBlock(rel) == 0 ||
		RelationGetTargetBlock(rel) == InvalidBlockNumber)
	{
		/* don't believe a cached 0 size without checking */
		BlockNumber nblocks;

		nblocks = RelationGetNumberOfBlocks(rel);
		RelationSetTargetBlock(rel, nblocks);
		if (nblocks == 0)
			return InvalidUndoPtr;
	}

	/*
	 * Scan the UNDO log, to discard as much of it as possible. This
	 * advances the oldest UNDO pointer past as many transactions as possible.
	 *
	 * TODO:
	 * We could get the latest cached value directly from the metapage, but
	 * this allows trimming the UNDO log more aggressively, whenever we're
	 * scanning. Fetching records from the UNDO log is pretty expensive,
	 * so until that is somehow sped up, it is a good tradeoff to be
	 * aggressive about that.
	 */
	return zsundo_trim(rel, RecentGlobalXmin);
}


/*
 * Higher-level functions for constructing UNDO records, with caching.
 *
 * If you perform multiple operations in the same transaction and command, we
 * reuse the same UNDO record for it. There's a one-element cache of each
 * operation type, so this only takes effect in simple cases.
 *
 * TODO: make the caching work in more cases. A hash table or something..
 * Currently, we do this for DELETEs and INSERTs. We could perhaps do this
 * for UPDATEs as well, although they're more a bit more tricky, as we need
 * to also store the 'ctid' pointer to the new tuple in an UPDATE.
 */
ZSUndoRecPtr
zsundo_create_for_delete(Relation rel, TransactionId xid, CommandId cid, zstid tid,
						 bool changedPart, ZSUndoRecPtr prev_undo_ptr)
{
	ZSUndoRec_Delete undorec;
	ZSUndoRecPtr undoptr;

	static RelFileNode cached_relfilenode;
	static TransactionId cached_xid;
	static CommandId cached_cid;
	static bool		cached_changedPart;
	static ZSUndoRecPtr cached_prev_undo_ptr;
	static ZSUndoRecPtr cached_undo_ptr;

	if (RelFileNodeEquals(rel->rd_node, cached_relfilenode) &&
		xid == cached_xid &&
		cid == cached_cid &&
		changedPart == cached_changedPart &&
		prev_undo_ptr.counter == cached_prev_undo_ptr.counter)
	{
		Buffer		buf;
		ZSUndoRec_Delete *undorec_p;

		undorec_p = (ZSUndoRec_Delete *) zsundo_fetch_lock(rel, cached_undo_ptr,
														   &buf, BUFFER_LOCK_EXCLUSIVE, false);

		if (undorec_p->rec.type != ZSUNDO_TYPE_DELETE)
			elog(ERROR, "unexpected undo record type %d, expected DELETE", undorec_p->rec.type);

		/* Is there space for a new TID in the record? */
		if (undorec_p->num_tids < ZSUNDO_NUM_TIDS_PER_DELETE)
		{
			undorec_p->tids[undorec_p->num_tids] = tid;
			undorec_p->num_tids++;

			MarkBufferDirty(buf);
			UnlockReleaseBuffer(buf);

			return cached_undo_ptr;
		}
		UnlockReleaseBuffer(buf);
	}

	/*
	 * Cache miss. Create a new UNDO record.
	 */
	undorec.rec.size = sizeof(ZSUndoRec_Delete);
	undorec.rec.type = ZSUNDO_TYPE_DELETE;
	undorec.rec.xid = xid;
	undorec.rec.cid = cid;
	undorec.changedPart = changedPart;
	undorec.rec.prevundorec = prev_undo_ptr;
	undorec.tids[0] = tid;
	undorec.num_tids = 1;

	undoptr = zsundo_insert(rel, &undorec.rec);

	cached_relfilenode = rel->rd_node;
	cached_xid = xid;
	cached_cid = cid;
	cached_changedPart = changedPart;
	cached_prev_undo_ptr = prev_undo_ptr;
	cached_undo_ptr = undoptr;

	return undoptr;
}

/*
 * Create an UNDO record for insertion.
 *
 * The undo record stores the 'tid' of the row, as well as visibility information.
 *
 * There's a primitive caching mechanism here: If you perform multiple insertions
 * with same visibility information, and consecutive TIDs, we will keep modifying
 * the range of TIDs in the same UNDO record, instead of creating new records.
 * That greatly reduces the space required for UNDO log of bulk inserts.
 */
ZSUndoRecPtr
zsundo_create_for_insert(Relation rel, TransactionId xid, CommandId cid, zstid tid,
						 int nitems, uint32 speculative_token, ZSUndoRecPtr prev_undo_ptr)
{
	ZSUndoRec_Insert undorec;
	ZSUndoRecPtr undoptr;

	static RelFileNode cached_relfilenode;
	static TransactionId cached_xid;
	static CommandId cached_cid;
	static zstid	cached_endtid;
	static ZSUndoRecPtr cached_prev_undo_ptr;
	static ZSUndoRecPtr cached_undo_ptr;

	if (speculative_token == INVALID_SPECULATIVE_TOKEN &&
		RelFileNodeEquals(rel->rd_node, cached_relfilenode) &&
		xid == cached_xid &&
		cid == cached_cid &&
		tid == cached_endtid &&
		prev_undo_ptr.counter == cached_prev_undo_ptr.counter)
	{
		Buffer		buf;
		ZSUndoRec_Insert *undorec_p;

		undorec_p = (ZSUndoRec_Insert *) zsundo_fetch_lock(rel, cached_undo_ptr,
														   &buf, BUFFER_LOCK_EXCLUSIVE, false);

		if (undorec_p->rec.type != ZSUNDO_TYPE_INSERT)
			elog(ERROR, "unexpected undo record type %d, expected INSERT", undorec_p->rec.type);

		/* Extend the range of the old record to cover the new TID */
		Assert(undorec_p->endtid == tid);
		Assert(undorec_p->speculative_token == INVALID_SPECULATIVE_TOKEN);
		undorec_p->endtid = tid + nitems;

		MarkBufferDirty(buf);
		UnlockReleaseBuffer(buf);

		cached_endtid = tid + nitems;
		return cached_undo_ptr;
	}

	/*
	 * Cache miss. Create a new UNDO record.
	 */
	undorec.rec.size = sizeof(ZSUndoRec_Insert);
	undorec.rec.type = ZSUNDO_TYPE_INSERT;
	/* undorecptr will be filed in by zsundo_insert() */
	undorec.rec.xid = xid;
	undorec.rec.cid = cid;
	undorec.rec.prevundorec = prev_undo_ptr;
	undorec.firsttid = tid;
	undorec.endtid = tid + nitems;
	undorec.speculative_token = speculative_token;

	undoptr = zsundo_insert(rel, &undorec.rec);

	if (speculative_token == INVALID_SPECULATIVE_TOKEN)
	{
		cached_relfilenode = rel->rd_node;
		cached_xid = xid;
		cached_cid = cid;
		cached_endtid = tid + nitems;
		cached_prev_undo_ptr = prev_undo_ptr;
		cached_undo_ptr = undoptr;
	}

	return undoptr;
}
