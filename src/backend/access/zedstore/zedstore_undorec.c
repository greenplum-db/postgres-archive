/*
 * zedstore_undorec.c
 *		Functions for working on UNDO records.
 *
 * This file contains higher-level functions for constructing UNDO records
 * for different kinds of WAL records.
 *
 * If you perform multiple operations in the same transaction and command, we
 * reuse the same UNDO record for it. There's a one-element cache of each
 * operation type, so this only takes effect in simple cases.
 *
 * TODO: make the caching work in more cases. A hash table or something..
 * Currently, we do this for DELETEs and INSERTs. We could perhaps do this
 * for UPDATEs as well, although they're more a bit more tricky, as we need
 * to also store the 'ctid' pointer to the new tuple in an UPDATE.
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/zedstore/zedstore_undorec.c
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/multixact.h"
#include "access/xlogreader.h"
#include "access/xlogutils.h"
#include "access/zedstore_internal.h"
#include "access/zedstore_undolog.h"
#include "access/zedstore_undorec.h"
#include "access/zedstore_wal.h"
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
zsundo_fetch_record(Relation rel, ZSUndoRecPtr undoptr)
{
	ZSUndoRec  *undorec_copy;
	ZSUndoRec  *undorec;
	Buffer		buf;

	undorec = (ZSUndoRec *) zsundo_fetch(rel, undoptr, &buf, BUFFER_LOCK_SHARE, true);

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


zs_pending_undo_op *
zsundo_create_for_delete(Relation rel, TransactionId xid, CommandId cid, zstid tid,
						 bool changedPart, ZSUndoRecPtr prev_undo_ptr)
{
	ZSUndoRec_Delete *undorec;
	zs_pending_undo_op *pending_op;

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
		ZSUndoRec_Delete *orig_undorec;

		orig_undorec = (ZSUndoRec_Delete *) zsundo_fetch(rel, cached_undo_ptr,
														 &buf, BUFFER_LOCK_EXCLUSIVE, false);

		if (orig_undorec->rec.type != ZSUNDO_TYPE_DELETE)
			elog(ERROR, "unexpected undo record type %d, expected DELETE", orig_undorec->rec.type);

		/* Is there space for a new TID in the record? */
		if (orig_undorec->num_tids < ZSUNDO_NUM_TIDS_PER_DELETE)
		{
			pending_op = palloc(offsetof(zs_pending_undo_op, payload) + sizeof(ZSUndoRec_Delete));
			undorec = (ZSUndoRec_Delete *) pending_op->payload;

			pending_op->reservation.undobuf = buf;
			pending_op->reservation.undorecptr = cached_undo_ptr;
			pending_op->reservation.length = sizeof(ZSUndoRec_Delete);
			pending_op->reservation.ptr = (char *) orig_undorec;
			pending_op->is_update = true;

			memcpy(undorec, orig_undorec, sizeof(ZSUndoRec_Delete));
			undorec->tids[undorec->num_tids] = tid;
			undorec->num_tids++;

			return pending_op;
		}
		UnlockReleaseBuffer(buf);
	}

	/*
	 * Cache miss. Create a new UNDO record.
	 */
	pending_op = palloc(offsetof(zs_pending_undo_op, payload) + sizeof(ZSUndoRec_Delete));
	pending_op->is_update = false;

	zsundo_insert_reserve(rel, sizeof(ZSUndoRec_Delete), &pending_op->reservation);

	undorec = (ZSUndoRec_Delete *) pending_op->payload;
	undorec->rec.size = sizeof(ZSUndoRec_Delete);
	undorec->rec.type = ZSUNDO_TYPE_DELETE;
	undorec->rec.undorecptr = pending_op->reservation.undorecptr;
	undorec->rec.xid = xid;
	undorec->rec.cid = cid;
	undorec->changedPart = changedPart;
	undorec->rec.prevundorec = prev_undo_ptr;
	undorec->tids[0] = tid;
	undorec->num_tids = 1;

	/* XXX: this caching mechanism assumes that once we've reserved the undo record,
	 * we never change our minds and don't write the undo record, after all.
	 */
	cached_relfilenode = rel->rd_node;
	cached_xid = xid;
	cached_cid = cid;
	cached_changedPart = changedPart;
	cached_prev_undo_ptr = prev_undo_ptr;
	cached_undo_ptr = pending_op->reservation.undorecptr;

	return pending_op;
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
zs_pending_undo_op *
zsundo_create_for_insert(Relation rel, TransactionId xid, CommandId cid, zstid tid,
						 int nitems, uint32 speculative_token, ZSUndoRecPtr prev_undo_ptr)
{
	ZSUndoRec_Insert *undorec;
	zs_pending_undo_op *pending_op;

	/*
	 * Cache miss. Create a new UNDO record.
	 */
	pending_op = palloc(offsetof(zs_pending_undo_op, payload) + sizeof(ZSUndoRec_Insert));
	pending_op->is_update = false;
	zsundo_insert_reserve(rel, sizeof(ZSUndoRec_Insert), &pending_op->reservation);
	undorec = (ZSUndoRec_Insert *) pending_op->payload;

	undorec->rec.size = sizeof(ZSUndoRec_Insert);
	undorec->rec.type = ZSUNDO_TYPE_INSERT;
	undorec->rec.undorecptr = pending_op->reservation.undorecptr;
	undorec->rec.xid = xid;
	undorec->rec.cid = cid;
	undorec->rec.prevundorec = prev_undo_ptr;
	undorec->firsttid = tid;
	undorec->endtid = tid + nitems;
	undorec->speculative_token = speculative_token;

	return pending_op;
}

zs_pending_undo_op *
zsundo_create_for_update(Relation rel, TransactionId xid, CommandId cid,
						 zstid oldtid, zstid newtid, ZSUndoRecPtr prev_undo_ptr,
						 bool key_update)
{
	ZSUndoRec_Update *undorec;
	zs_pending_undo_op *pending_op;

	/*
	 * Create a new UNDO record.
	 */
	pending_op = palloc(offsetof(zs_pending_undo_op, payload) + sizeof(ZSUndoRec_Update));
	pending_op->is_update = false;
	zsundo_insert_reserve(rel, sizeof(ZSUndoRec_Update), &pending_op->reservation);

	undorec = (ZSUndoRec_Update *) pending_op->payload;
	undorec->rec.size = sizeof(ZSUndoRec_Update);
	undorec->rec.type = ZSUNDO_TYPE_UPDATE;
	undorec->rec.undorecptr = pending_op->reservation.undorecptr;
	undorec->rec.xid = xid;
	undorec->rec.cid = cid;
	undorec->rec.prevundorec = prev_undo_ptr;
	undorec->oldtid = oldtid;
	undorec->newtid = newtid;
	undorec->key_update = key_update;

	return pending_op;
}

zs_pending_undo_op *
zsundo_create_for_tuple_lock(Relation rel, TransactionId xid, CommandId cid,
							 zstid tid, LockTupleMode lockmode,
							 ZSUndoRecPtr prev_undo_ptr)
{
	ZSUndoRec_TupleLock *undorec;
	zs_pending_undo_op *pending_op;

	/*
	 * Create a new UNDO record.
	 */
	pending_op = palloc(offsetof(zs_pending_undo_op, payload) + sizeof(ZSUndoRec_TupleLock));
	pending_op->is_update = false;
	zsundo_insert_reserve(rel, sizeof(ZSUndoRec_TupleLock), &pending_op->reservation);

	undorec = (ZSUndoRec_TupleLock *) pending_op->payload;
	undorec->rec.size = sizeof(ZSUndoRec_TupleLock);
	undorec->rec.type = ZSUNDO_TYPE_TUPLE_LOCK;
	undorec->rec.undorecptr = pending_op->reservation.undorecptr;
	undorec->rec.xid = xid;
	undorec->rec.cid = cid;
	undorec->rec.prevundorec = prev_undo_ptr;
	undorec->lockmode = lockmode;

	return pending_op;
}


/*
 * Scan the UNDO log, starting from oldest entry. Undo the effects of any
 * aborted transactions. Records for committed transactions can be discarded
 * away immediately.
 *
 * Returns the oldest valid UNDO ptr, after discarding.
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
	 * concurrent trims was possible, we could check after reading the head
	 * page, that it is the page we expect, and re-read the metapage if it's
	 * not.
	 */
	UnlockReleaseBuffer(metabuf);

	/*
	 * Don't trim undo pages in recovery mode to avoid writing new WALs.
	 */
	if(RecoveryInProgress())
		return oldest_undorecptr;

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
			 * committed, we can just discard away its UNDO record. If it aborted,
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
							 * becomes visible to everyone when the UNDO record is discarded
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
			lastblk = opaque->next;
			UnlockReleaseBuffer(buf);
			if (lastblk != InvalidBlockNumber)
				deleted_undo_pages++;
		}
	}

	if (can_advance_oldestundorecptr)
	{
		if (lastblk == InvalidBlockNumber)
		{
			/*
			 * We stopped after the last valid record. Advance by one, to the next
			 * record which hasn't been created yet, and which is still needed.
			 */
			oldest_undorecptr.counter++;
			oldest_undorecptr.blkno = InvalidBlockNumber;
			oldest_undorecptr.offset = 0;
		}

		zsundo_discard(rel, oldest_undorecptr);
	}

	UnlockPage(rel, ZS_META_BLK, ExclusiveLock);

	return oldest_undorecptr;
}

void
zsundo_finish_pending_op(zs_pending_undo_op *pendingop, char *payload)
{
	/*
	 * This should be used as part of a bigger critical section that
	 * writes a WAL record of the change.
	 */
	Assert(CritSectionCount > 0);

	memcpy(pendingop->reservation.ptr, payload, pendingop->reservation.length);

	if (!pendingop->is_update)
		zsundo_insert_finish(&pendingop->reservation);
	else
		MarkBufferDirty(pendingop->reservation.undobuf);
}


void
zsundo_clear_speculative_token(Relation rel, ZSUndoRecPtr undoptr)
{
	ZSUndoRec_Insert *undorec;
	Buffer		buf;

	undorec = (ZSUndoRec_Insert *) zsundo_fetch(rel, undoptr, &buf, BUFFER_LOCK_EXCLUSIVE, false);

	if (undorec->rec.type != ZSUNDO_TYPE_INSERT)
		elog(ERROR, "unexpected undo record type %d on speculatively inserted row",
			 undorec->rec.type);

	START_CRIT_SECTION();

	MarkBufferDirty(buf);

	undorec->speculative_token = INVALID_SPECULATIVE_TOKEN;

	/*
	 * The speculative insertion token becomes irrelevant, if we crash, so no
	 * need to WAL-log it. However, if checksums are enabled, we may need to take
	 * a full-page image of the page, if a checkpoint happened between the
	 * speculative insertion and this call.
	 */
	if (RelationNeedsWAL(rel))
	{
		if (XLogHintBitIsNeeded())
		{
			XLogRecPtr lsn;

			lsn = XLogSaveBufferForHint(buf, true);
			PageSetLSN(BufferGetPage(buf), lsn);
		}
	}

	END_CRIT_SECTION();

	UnlockReleaseBuffer(buf);
}

/*
 * Support functions for WAL-logging the insertion/modification of an
 * UNDO record, as part of another WAL-logged change.
 */
void
XLogRegisterUndoOp(uint8 block_id, zs_pending_undo_op *undo_op)
{
	zs_wal_undo_op *xlrec = &undo_op->waldata;

	xlrec->undoptr = undo_op->reservation.undorecptr;
	xlrec->length = undo_op->reservation.length;
	xlrec->is_update = undo_op->is_update;

	XLogRegisterBuffer(block_id, undo_op->reservation.undobuf,
					   REGBUF_STANDARD);
	XLogRegisterBufData(block_id, (char *) xlrec, SizeOfZSWalUndoOp);
	XLogRegisterBufData(block_id, (char *) undo_op->payload, undo_op->reservation.length);
}

/* redo support for the above */
Buffer
XLogRedoUndoOp(XLogReaderState *record, uint8 block_id)
{
	Buffer		buffer;
	zs_pending_undo_op op;

	if (XLogReadBufferForRedo(record, block_id, &buffer) == BLK_NEEDS_REDO)
	{
		zs_wal_undo_op xlrec;
		Size		len;
		char	   *p = XLogRecGetBlockData(record, block_id, &len);

		Assert(len >= SizeOfZSWalUndoOp);

		memcpy(&xlrec, p, SizeOfZSWalUndoOp);
		p += SizeOfZSWalUndoOp;
		len -= SizeOfZSWalUndoOp;
		Assert(xlrec.length == len);

		op.reservation.undobuf = buffer;
		op.reservation.undorecptr = xlrec.undoptr;
		op.reservation.length = xlrec.length;
		op.reservation.ptr = ((char *) BufferGetPage(buffer)) + xlrec.undoptr.offset;
		op.is_update = xlrec.is_update;

		START_CRIT_SECTION();
		zsundo_finish_pending_op(&op, p);
		END_CRIT_SECTION();
	}
	return buffer;
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
	zstid		starttid;
	zstid		endtid;
	uint64		num_live_tuples;

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
	num_live_tuples = 0;
	do
	{
		IntegerSet *dead_tids;

		/* Scan the TID tree, to collect TIDs that have been marked dead. */
		dead_tids = zsbt_collect_dead_tids(rel, starttid, &endtid, &num_live_tuples);
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
	} while(starttid < MaxPlusOneZSTid);

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
	 * FIXME: pass correct numbers for other arguments.
	 */
	vac_update_relstats(rel,
						RelationGetNumberOfBlocks(rel),
						num_live_tuples,
						false,
						nindexes > 0,
						OldestXmin,
						InvalidMultiXactId,
						false);

	/* report results to the stats collector, too */
	pgstat_report_vacuum(RelationGetRelid(rel),
						 rel->rd_rel->relisshared,
						 num_live_tuples,
						 0); /* FIXME: # of dead tuples */
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
