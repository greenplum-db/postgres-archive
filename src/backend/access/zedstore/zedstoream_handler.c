/*-------------------------------------------------------------------------
 *
 * zedstoream_handler.c
 *	  ZedStore table access method code
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/zedstore/zedstoream_handler.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "access/genam.h"
#include "access/heapam.h"
#include "access/multixact.h"
#include "access/relscan.h"
#include "access/tableam.h"
#include "access/tsmapi.h"
#include "access/tupdesc_details.h"
#include "access/xact.h"
#include "access/zedstore_internal.h"
#include "access/zedstore_undorec.h"
#include "catalog/catalog.h"
#include "catalog/index.h"
#include "catalog/storage.h"
#include "catalog/storage_xlog.h"
#include "commands/progress.h"
#include "commands/vacuum.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "optimizer/plancat.h"
#include "pgstat.h"
#include "parser/parse_relation.h"
#include "storage/lmgr.h"
#include "storage/predicate.h"
#include "storage/procarray.h"
#include "utils/builtins.h"
#include "utils/rel.h"

typedef struct ZedStoreProjectData
{
	int			num_proj_atts;
	Bitmapset   *project_columns;
	int		   *proj_atts;
	ZSTidTreeScan tid_scan;
	ZSAttrTreeScan *attr_scans;
	MemoryContext context;
}  ZedStoreProjectData;

typedef struct ZedStoreDescData
{
	/* scan parameters */
	TableScanDescData rs_scan;  /* */
	ZedStoreProjectData proj_data;

	bool		started;
	zstid		cur_range_start;
	zstid		cur_range_end;

	/* These fields are used for bitmap scans, to hold a "block's" worth of data */
#define	MAX_ITEMS_PER_LOGICAL_BLOCK		MaxHeapTuplesPerPage
	int			bmscan_ntuples;
	zstid	   *bmscan_tids;
	int			bmscan_nexttuple;

	/* These fields are use for TABLESAMPLE scans */
	zstid       min_tid_to_scan;
	zstid       max_tid_to_scan;
	zstid       next_tid_to_scan;

} ZedStoreDescData;

typedef struct ZedStoreDescData *ZedStoreDesc;

typedef struct ZedStoreIndexFetchData
{
	IndexFetchTableData idx_fetch_data;
	ZedStoreProjectData proj_data;
} ZedStoreIndexFetchData;

typedef struct ZedStoreIndexFetchData *ZedStoreIndexFetch;

typedef struct ParallelZSScanDescData *ParallelZSScanDesc;

static IndexFetchTableData *zedstoream_begin_index_fetch(Relation rel);
static void zedstoream_fetch_set_column_projection(struct IndexFetchTableData *scan,
									   Bitmapset *project_cols);
static void zedstoream_end_index_fetch(IndexFetchTableData *scan);
static bool zedstoream_fetch_row(ZedStoreIndexFetchData *fetch,
								 ItemPointer tid_p,
								 Snapshot snapshot,
								 TupleTableSlot *slot);
static bool zs_acquire_tuplock(Relation relation, ItemPointer tid, LockTupleMode mode,
							   LockWaitPolicy wait_policy, bool *have_tuple_lock);

static bool zs_blkscan_next_block(TableScanDesc sscan,
								  BlockNumber blkno, OffsetNumber *offsets, int noffsets,
								  bool predicatelocks);
static bool zs_blkscan_next_tuple(TableScanDesc sscan, TupleTableSlot *slot);

static Size zs_parallelscan_estimate(Relation rel);
static Size zs_parallelscan_initialize(Relation rel, ParallelTableScanDesc pscan);
static void zs_parallelscan_reinitialize(Relation rel, ParallelTableScanDesc pscan);
static bool zs_parallelscan_nextrange(Relation rel, ParallelZSScanDesc pzscan,
									  zstid *start, zstid *end);
static void zsbt_fill_missing_attribute_value(TupleDesc tupleDesc, int attno, Datum *datum, bool *isnull);

/* ----------------------------------------------------------------
 *				storage AM support routines for zedstoream
 * ----------------------------------------------------------------
 */

static bool
zedstoream_fetch_row_version(Relation rel,
							 ItemPointer tid_p,
							 Snapshot snapshot,
							 TupleTableSlot *slot,
							 Bitmapset *project_cols)
{
	IndexFetchTableData *fetcher;
	bool		result;

	zsbt_tuplebuffer_flush(rel);

	fetcher = zedstoream_begin_index_fetch(rel);
	zedstoream_fetch_set_column_projection(fetcher, project_cols);

	result = zedstoream_fetch_row((ZedStoreIndexFetchData *) fetcher,
								  tid_p, snapshot, slot);
	if (result)
	{
		/* FIXME: heapam acquires the predicate lock first, and then
		 * calls CheckForSerializableConflictOut(). We do it in the
		 * opposite order, because CheckForSerializableConflictOut()
		 * call as done in zsbt_get_last_tid() already. Does it matter?
		 * I'm not sure.
		 *
		 * We pass in InvalidTransactionId as we are sure that the current
		 * transaction hasn't locked tid_p.
		 */
		PredicateLockTID(rel, tid_p, snapshot, InvalidTransactionId);
	}
	ExecMaterializeSlot(slot);
	slot->tts_tableOid = RelationGetRelid(rel);
	slot->tts_tid = *tid_p;

	zedstoream_end_index_fetch(fetcher);

	return result;
}

static void
zedstoream_get_latest_tid(TableScanDesc sscan,
						  ItemPointer tid)
{
	zstid		ztid = ZSTidFromItemPointer(*tid);

	zsbt_tuplebuffer_flush(sscan->rs_rd);

	zsbt_find_latest_tid(sscan->rs_rd, &ztid, sscan->rs_snapshot);
	*tid = ItemPointerFromZSTid(ztid);
}

static inline void
zedstoream_insert_internal(Relation relation, TupleTableSlot *slot, CommandId cid,
						   int options, struct BulkInsertStateData *bistate, uint32 speculative_token)
{
	zstid		tid;
	TransactionId xid = GetCurrentTransactionId();
	MemoryContext oldcontext;
	MemoryContext insert_mcontext;

	/*
	 * insert code performs allocations for creating items and merging
	 * items. These are small allocations but add-up based on number of
	 * columns and rows being inserted. Hence, creating context to track them
	 * and wholesale free instead of retail freeing them. TODO: in long term
	 * try if can avoid creating context here, retail free in normal case and
	 * only create context for page splits maybe.
	 */
	insert_mcontext = AllocSetContextCreate(CurrentMemoryContext,
											   "ZedstoreAMContext",
											   ALLOCSET_DEFAULT_SIZES);
	oldcontext = MemoryContextSwitchTo(insert_mcontext);

	if (slot->tts_tupleDescriptor->natts != relation->rd_att->natts)
		elog(ERROR, "slot's attribute count doesn't match relcache entry");

	if (speculative_token == INVALID_SPECULATIVE_TOKEN)
		tid = zsbt_tuplebuffer_allocate_tids(relation, xid, cid, 1);
	else
		tid = zsbt_tid_multi_insert(relation, 1, xid, cid, speculative_token,
									InvalidUndoPtr);

	/*
	 * We only need to check for table-level SSI locks. Our
	 * new tuple can't possibly conflict with existing tuple locks, and
	 * page locks are only consolidated versions of tuple locks; they do not
	 * lock "gaps" as index page locks do.
	 */
	CheckForSerializableConflictIn(relation, NULL, InvalidBlockNumber);

	slot_getallattrs(slot);
	zsbt_tuplebuffer_spool_tuple(relation, tid, slot->tts_values, slot->tts_isnull);

	slot->tts_tableOid = RelationGetRelid(relation);
	slot->tts_tid = ItemPointerFromZSTid(tid);
	/* XXX: should we set visi_info here? */

	MemoryContextSwitchTo(oldcontext);
	MemoryContextDelete(insert_mcontext);

	/* Note: speculative insertions are counted too, even if aborted later */
	pgstat_count_heap_insert(relation, 1);
}

static void
zedstoream_insert(Relation relation, TupleTableSlot *slot, CommandId cid,
					int options, struct BulkInsertStateData *bistate)
{
	zedstoream_insert_internal(relation, slot, cid, options, bistate, INVALID_SPECULATIVE_TOKEN);
}

static void
zedstoream_insert_speculative(Relation relation, TupleTableSlot *slot, CommandId cid,
							  int options, BulkInsertState bistate, uint32 specToken)
{
	zedstoream_insert_internal(relation, slot, cid, options, bistate, specToken);
}

static void
zedstoream_complete_speculative(Relation relation, TupleTableSlot *slot, uint32 spekToken,
								bool succeeded)
{
	zstid		tid;

	tid = ZSTidFromItemPointer(slot->tts_tid);
	zsbt_tid_clear_speculative_token(relation, tid, spekToken, true /* for complete */);
	/*
	 * there is a conflict
	 *
	 * FIXME: Shouldn't we mark the TID dead first?
	 */
	if (!succeeded)
	{
		ZSUndoRecPtr recent_oldest_undo = zsundo_get_oldest_undo_ptr(relation, true);

		zsbt_tid_mark_dead(relation, tid, recent_oldest_undo);
	}
}

static void
zedstoream_multi_insert(Relation relation, TupleTableSlot **slots, int ntuples,
						CommandId cid, int options, BulkInsertState bistate)
{
	int			i;
	TransactionId xid = GetCurrentTransactionId();
	zstid		firsttid;
	zstid	   *tids;

	if (ntuples == 0)
	{
		/* COPY sometimes calls us with 0 tuples. */
		return;
	}

	firsttid = zsbt_tuplebuffer_allocate_tids(relation, xid, cid, ntuples);

	tids = palloc(ntuples * sizeof(zstid));
	for (i = 0; i < ntuples; i++)
		tids[i] = firsttid + i;

	/*
	 * We only need to check for table-level SSI locks. Our
	 * new tuple can't possibly conflict with existing tuple locks, and
	 * page locks are only consolidated versions of tuple locks; they do not
	 * lock "gaps" as index page locks do.
	 */
	CheckForSerializableConflictIn(relation, NULL, InvalidBlockNumber);

	zsbt_tuplebuffer_spool_slots(relation, tids, slots, ntuples);

	for (i = 0; i < ntuples; i++)
	{
		slots[i]->tts_tableOid = RelationGetRelid(relation);
		slots[i]->tts_tid = ItemPointerFromZSTid(firsttid + i);
	}

	pgstat_count_heap_insert(relation, ntuples);
}

static TM_Result
zedstoream_delete(Relation relation, ItemPointer tid_p, CommandId cid,
				  Snapshot snapshot, Snapshot crosscheck, bool wait,
				  TM_FailureData *hufd, bool changingPart)
{
	zstid		tid = ZSTidFromItemPointer(*tid_p);
	TransactionId xid = GetCurrentTransactionId();
	TM_Result result = TM_Ok;
	bool		this_xact_has_lock = false;
	bool		have_tuple_lock = false;

	zsbt_tuplebuffer_flush(relation);

retry:
	result = zsbt_tid_delete(relation, tid, xid, cid,
							 snapshot, crosscheck, wait, hufd, changingPart,
							 &this_xact_has_lock);

	if (result != TM_Ok)
	{
		if (result == TM_Invisible)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("attempted to delete invisible tuple")));
		else if (result == TM_BeingModified && wait)
		{
			TransactionId	xwait = hufd->xmax;

			if (!TransactionIdIsCurrentTransactionId(xwait))
			{
				/*
				 * Acquire tuple lock to establish our priosity for the tuple
				 * See zedstoream_lock_tuple().
				 */
				if (!this_xact_has_lock)
				{
					zs_acquire_tuplock(relation, tid_p, LockTupleExclusive, LockWaitBlock,
									   &have_tuple_lock);
				}

				XactLockTableWait(xwait, relation, tid_p, XLTW_Delete);
				goto retry;
			}
		}
	}

	/*
	 * Check for SSI conflicts.
	 */
	CheckForSerializableConflictIn(relation, tid_p, ItemPointerGetBlockNumber(tid_p));

	if (result == TM_Ok)
		pgstat_count_heap_delete(relation);

	return result;
}


/*
 * Each tuple lock mode has a corresponding heavyweight lock, and one or two
 * corresponding MultiXactStatuses (one to merely lock tuples, another one to
 * update them).  This table (and the macros below) helps us determine the
 * heavyweight lock mode and MultiXactStatus values to use for any particular
 * tuple lock strength.
 *
 * Don't look at lockstatus/updstatus directly!  Use get_mxact_status_for_lock
 * instead.
 */
static const struct
{
	LOCKMODE	hwlock;
	int			lockstatus;
	int			updstatus;
}

			tupleLockExtraInfo[MaxLockTupleMode + 1] =
{
	{							/* LockTupleKeyShare */
		AccessShareLock,
		MultiXactStatusForKeyShare,
		-1						/* KeyShare does not allow updating tuples */
	},
	{							/* LockTupleShare */
		RowShareLock,
		MultiXactStatusForShare,
		-1						/* Share does not allow updating tuples */
	},
	{							/* LockTupleNoKeyExclusive */
		ExclusiveLock,
		MultiXactStatusForNoKeyUpdate,
		MultiXactStatusNoKeyUpdate
	},
	{							/* LockTupleExclusive */
		AccessExclusiveLock,
		MultiXactStatusForUpdate,
		MultiXactStatusUpdate
	}
};


/*
 * Acquire heavyweight locks on tuples, using a LockTupleMode strength value.
 * This is more readable than having every caller translate it to lock.h's
 * LOCKMODE.
 */
#define LockTupleTuplock(rel, tup, mode) \
	LockTuple((rel), (tup), tupleLockExtraInfo[mode].hwlock)
#define UnlockTupleTuplock(rel, tup, mode) \
	UnlockTuple((rel), (tup), tupleLockExtraInfo[mode].hwlock)
#define ConditionalLockTupleTuplock(rel, tup, mode) \
	ConditionalLockTuple((rel), (tup), tupleLockExtraInfo[mode].hwlock)

/*
 * Acquire heavyweight lock on the given tuple, in preparation for acquiring
 * its normal, Xmax-based tuple lock.
 *
 * have_tuple_lock is an input and output parameter: on input, it indicates
 * whether the lock has previously been acquired (and this function does
 * nothing in that case).  If this function returns success, have_tuple_lock
 * has been flipped to true.
 *
 * Returns false if it was unable to obtain the lock; this can only happen if
 * wait_policy is Skip.
 *
 * XXX: This is identical to heap_acquire_tuplock
 */

static bool
zs_acquire_tuplock(Relation relation, ItemPointer tid, LockTupleMode mode,
				   LockWaitPolicy wait_policy, bool *have_tuple_lock)
{
	if (*have_tuple_lock)
		return true;

	switch (wait_policy)
	{
		case LockWaitBlock:
			LockTupleTuplock(relation, tid, mode);
			break;

		case LockWaitSkip:
			if (!ConditionalLockTupleTuplock(relation, tid, mode))
				return false;
			break;

		case LockWaitError:
			if (!ConditionalLockTupleTuplock(relation, tid, mode))
				ereport(ERROR,
						(errcode(ERRCODE_LOCK_NOT_AVAILABLE),
						 errmsg("could not obtain lock on row in relation \"%s\"",
								RelationGetRelationName(relation))));
			break;
	}
	*have_tuple_lock = true;

	return true;
}


static TM_Result
zedstoream_lock_tuple(Relation relation, ItemPointer tid_p, Snapshot snapshot,
					  TupleTableSlot *slot, CommandId cid, LockTupleMode mode,
					  LockWaitPolicy wait_policy, uint8 flags,
					  TM_FailureData *tmfd)
{
	zstid		tid = ZSTidFromItemPointer(*tid_p);
	TransactionId xid = GetCurrentTransactionId();
	TM_Result result;
	bool		this_xact_has_lock = false;
	bool		have_tuple_lock = false;
	zstid		next_tid = tid;
	SnapshotData SnapshotDirty;
	bool		locked_something = false;
	ZSUndoSlotVisibility *visi_info = &((ZedstoreTupleTableSlot *) slot)->visi_info_buf;
	bool		follow_updates = false;

	zsbt_tuplebuffer_flush(relation);

	slot->tts_tableOid = RelationGetRelid(relation);
	slot->tts_tid = *tid_p;

	tmfd->traversed = false;
	/*
	 * For now, we lock just the first attribute. As long as everyone
	 * does that, that's enough.
	 */
retry:
	result = zsbt_tid_lock(relation, tid, xid, cid, mode, follow_updates,
						   snapshot, tmfd, &next_tid, &this_xact_has_lock, visi_info);
	((ZedstoreTupleTableSlot *) slot)->visi_info = visi_info;

	if (result == TM_Invisible)
	{
		/*
		 * This is possible, but only when locking a tuple for ON CONFLICT
		 * UPDATE and some other cases handled below.  We return this value
		 * here rather than throwing an error in order to give that case the
		 * opportunity to throw a more specific error.
		 */
		/*
		 * This can also happen, if we're locking an UPDATE chain for KEY SHARE mode:
		 * A tuple has been inserted, and then updated, by a different transaction.
		 * The updating transaction is still in progress. We can lock the row
		 * in KEY SHARE mode, assuming the key columns were not updated, and we will
		 * try to lock all the row version, even the still in-progress UPDATEs.
		 * It's possible that the UPDATE aborts while we're chasing the update chain,
		 * so that the updated tuple becomes invisible to us. That's OK.
		 */
		 if (mode == LockTupleKeyShare && locked_something)
			 return TM_Ok;

		 /*
		  * This can also happen, if the caller asked for the latest version
		  * of the tuple and if tuple was inserted by our own transaction, we
		  * have to check cmin against cid: cmin >= current CID means our
		  * command cannot see the tuple, so we should ignore it.
		  */
		 Assert(visi_info->cmin != InvalidCommandId);
		 if ((flags & TUPLE_LOCK_FLAG_FIND_LAST_VERSION) != 0 &&
			 TransactionIdIsCurrentTransactionId(visi_info->xmin) &&
			 visi_info->cmin >= cid)
		 {
			 tmfd->xmax = visi_info->xmin;
			 tmfd->cmax = visi_info->cmin;
			 return TM_SelfModified;
		 }

		 return TM_Invisible;
	}
	else if (result == TM_Updated ||
			 (result == TM_SelfModified && tmfd->cmax >= cid))
	{
		/*
		 * The other transaction is an update and it already committed.
		 *
		 * If the caller asked for the latest version, find it.
		 */
		if ((flags & TUPLE_LOCK_FLAG_FIND_LAST_VERSION) != 0 && next_tid != tid)
		{
			if (have_tuple_lock)
			{
				UnlockTupleTuplock(relation, tid_p, mode);
				have_tuple_lock = false;
			}

			if (ItemPointerIndicatesMovedPartitions(&tmfd->ctid))
				ereport(ERROR,
						(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
						 errmsg("tuple to be locked was already moved to another partition due to concurrent update")));

			/* it was updated, so look at the updated version */
			*tid_p = ItemPointerFromZSTid(next_tid);

			/* signal that a tuple later in the chain is getting locked */
			tmfd->traversed = true;

			/* loop back to fetch next in chain */

			/* FIXME: In the corresponding code in heapam, we cross-check the xmin/xmax
			 * of the old and new tuple. Should we do the same here?
			 */

			InitDirtySnapshot(SnapshotDirty);
			snapshot = &SnapshotDirty;
			tid = next_tid;
			goto retry;
		}

		return result;
	}
	else if (result == TM_Deleted)
	{
		/*
		 * The other transaction is a delete and it already committed.
		 */
		return result;
	}
	else if (result == TM_BeingModified)
	{
		TransactionId xwait = tmfd->xmax;

		/*
		 * Acquire tuple lock to establish our priority for the tuple, or
		 * die trying.  LockTuple will release us when we are next-in-line
		 * for the tuple.  We must do this even if we are share-locking,
		 * but not if we already have a weaker lock on the tuple.
		 *
		 * If we are forced to "start over" below, we keep the tuple lock;
		 * this arranges that we stay at the head of the line while
		 * rechecking tuple state.
		 *
		 * Explanation for why we don't acquire heavy-weight lock when we
		 * already hold a weaker lock:
		 *
		 * Disable acquisition of the heavyweight tuple lock.
		 * Otherwise, when promoting a weaker lock, we might
		 * deadlock with another locker that has acquired the
		 * heavyweight tuple lock and is waiting for our
		 * transaction to finish.
		 *
		 * Note that in this case we still need to wait for
		 * the xid if required, to avoid acquiring
		 * conflicting locks.
		 *
		 */
		if (!this_xact_has_lock &&
			!zs_acquire_tuplock(relation, tid_p, mode, wait_policy,
								  &have_tuple_lock))
		{
			/*
			 * This can only happen if wait_policy is Skip and the lock
			 * couldn't be obtained.
			 */
			return TM_WouldBlock;
		}

		/* wait for regular transaction to end, or die trying */
		switch (wait_policy)
		{
			case LockWaitBlock:
				XactLockTableWait(xwait, relation, tid_p, XLTW_Lock);
				break;
			case LockWaitSkip:
				if (!ConditionalXactLockTableWait(xwait))
				{
					/* FIXME: should we release the hwlock here? */
					return TM_WouldBlock;
				}
				break;
			case LockWaitError:
				if (!ConditionalXactLockTableWait(xwait))
					ereport(ERROR,
							(errcode(ERRCODE_LOCK_NOT_AVAILABLE),
							 errmsg("could not obtain lock on row in relation \"%s\"",
									RelationGetRelationName(relation))));
				break;
		}

		/*
		 * xwait is done. Retry.
		 */
		goto retry;
	}
	if (result == TM_Ok)
		locked_something = true;

	/*
	 * Now that we have successfully marked the tuple as locked, we can
	 * release the lmgr tuple lock, if we had it.
	 */
	if (have_tuple_lock)
	{
		UnlockTupleTuplock(relation, tid_p, mode);
		have_tuple_lock = false;
	}

	if (mode == LockTupleKeyShare)
	{
		/* lock all row versions, if it's a KEY SHARE lock */
		follow_updates = (flags & TUPLE_LOCK_FLAG_LOCK_UPDATE_IN_PROGRESS) != 0;
		if (result == TM_Ok && tid != next_tid && next_tid != InvalidZSTid)
		{
			tid = next_tid;
			goto retry;
		}
	}

	/* Fetch the tuple, too. */
	if (!zedstoream_fetch_row_version(relation, tid_p, SnapshotAny, slot, NULL))
		elog(ERROR, "could not fetch locked tuple");

	return TM_Ok;
}

/* like heap_tuple_attr_equals */
static bool
zs_tuple_attr_equals(int attrnum, TupleTableSlot *slot1, TupleTableSlot *slot2)
{
	TupleDesc	tupdesc = slot1->tts_tupleDescriptor;
	Datum		value1,
				value2;
	bool		isnull1,
				isnull2;
	Form_pg_attribute att;

	/*
	 * If it's a whole-tuple reference, say "not equal".  It's not really
	 * worth supporting this case, since it could only succeed after a no-op
	 * update, which is hardly a case worth optimizing for.
	 */
	if (attrnum == 0)
		return false;

	/*
	 * Likewise, automatically say "not equal" for any system attribute other
	 * than tableOID; we cannot expect these to be consistent in a HOT chain,
	 * or even to be set correctly yet in the new tuple.
	 */
	if (attrnum < 0)
	{
		if (attrnum != TableOidAttributeNumber)
			return false;
	}

	/*
	 * Extract the corresponding values.  XXX this is pretty inefficient if
	 * there are many indexed columns.  Should HeapDetermineModifiedColumns do
	 * a single heap_deform_tuple call on each tuple, instead?	But that
	 * doesn't work for system columns ...
	 */
	value1 = slot_getattr(slot1, attrnum, &isnull1);
	value2 = slot_getattr(slot2, attrnum, &isnull2);

	/*
	 * If one value is NULL and other is not, then they are certainly not
	 * equal
	 */
	if (isnull1 != isnull2)
		return false;

	/*
	 * If both are NULL, they can be considered equal.
	 */
	if (isnull1)
		return true;

	/*
	 * We do simple binary comparison of the two datums.  This may be overly
	 * strict because there can be multiple binary representations for the
	 * same logical value.  But we should be OK as long as there are no false
	 * positives.  Using a type-specific equality operator is messy because
	 * there could be multiple notions of equality in different operator
	 * classes; furthermore, we cannot safely invoke user-defined functions
	 * while holding exclusive buffer lock.
	 */
	if (attrnum <= 0)
	{
		/* The only allowed system columns are OIDs, so do this */
		return (DatumGetObjectId(value1) == DatumGetObjectId(value2));
	}
	else
	{
		Assert(attrnum <= tupdesc->natts);
		att = TupleDescAttr(tupdesc, attrnum - 1);
		return datumIsEqual(value1, value2, att->attbyval, att->attlen);
	}
}

static bool
is_key_update(Relation relation, TupleTableSlot *oldslot, TupleTableSlot *newslot)
{
	Bitmapset  *key_attrs;
	Bitmapset  *interesting_attrs;
	Bitmapset  *modified_attrs;
	int			attnum;

	/*
	 * Fetch the list of attributes to be checked for various operations.
	 *
	 * For HOT considerations, this is wasted effort if we fail to update or
	 * have to put the new tuple on a different page.  But we must compute the
	 * list before obtaining buffer lock --- in the worst case, if we are
	 * doing an update on one of the relevant system catalogs, we could
	 * deadlock if we try to fetch the list later.  In any case, the relcache
	 * caches the data so this is usually pretty cheap.
	 *
	 * We also need columns used by the replica identity and columns that are
	 * considered the "key" of rows in the table.
	 *
	 * Note that we get copies of each bitmap, so we need not worry about
	 * relcache flush happening midway through.
	 */
	key_attrs = RelationGetIndexAttrBitmap(relation, INDEX_ATTR_BITMAP_KEY);

	interesting_attrs = NULL;
	interesting_attrs = bms_add_members(interesting_attrs, key_attrs);

	/* Determine columns modified by the update. */
	modified_attrs = NULL;
	while ((attnum = bms_first_member(interesting_attrs)) >= 0)
	{
		attnum += FirstLowInvalidHeapAttributeNumber;

		if (!zs_tuple_attr_equals(attnum, oldslot, newslot))
			modified_attrs = bms_add_member(modified_attrs,
											attnum - FirstLowInvalidHeapAttributeNumber);
	}

	return bms_overlap(modified_attrs, key_attrs);
}

static TM_Result
zedstoream_update(Relation relation, ItemPointer otid_p, TupleTableSlot *slot,
				  CommandId cid, Snapshot snapshot, Snapshot crosscheck,
				  bool wait, TM_FailureData *hufd,
				  LockTupleMode *lockmode, bool *update_indexes)
{
	zstid		otid = ZSTidFromItemPointer(*otid_p);
	TransactionId xid = GetCurrentTransactionId();
	bool		key_update;
	Datum	   *d;
	bool	   *isnulls;
	TM_Result	result;
	zstid		newtid;
	TupleTableSlot *oldslot;
	IndexFetchTableData *fetcher;
	MemoryContext oldcontext;
	MemoryContext insert_mcontext;
	bool		this_xact_has_lock = false;
	bool		have_tuple_lock = false;

	zsbt_tuplebuffer_flush(relation);

	/*
	 * insert code performs allocations for creating items and merging
	 * items. These are small allocations but add-up based on number of
	 * columns and rows being inserted. Hence, creating context to track them
	 * and wholesale free instead of retail freeing them. TODO: in long term
	 * try if can avoid creating context here, retail free in normal case and
	 * only create context for page splits maybe.
	 */
	insert_mcontext = AllocSetContextCreate(CurrentMemoryContext,
											   "ZedstoreAMContext",
											   ALLOCSET_DEFAULT_SIZES);
	oldcontext = MemoryContextSwitchTo(insert_mcontext);

	slot_getallattrs(slot);
	d = slot->tts_values;
	isnulls = slot->tts_isnull;

	oldslot = table_slot_create(relation, NULL);
	fetcher = zedstoream_begin_index_fetch(relation);

	/*
	 * The meta-attribute holds the visibility information, including the "t_ctid"
	 * pointer to the updated version. All the real attributes are just inserted,
	 * as if for a new row.
	 */
retry:
	newtid = InvalidZSTid;

	/*
	 * Fetch the old row, so that we can figure out which columns were modified.
	 *
	 * FIXME: if we have to follow the update chain, we should look at the
	 * currently latest tuple version, rather than the one visible to our snapshot.
	 */
	if (!zedstoream_fetch_row((ZedStoreIndexFetchData *) fetcher,
							 otid_p, SnapshotAny, oldslot))
	{
		return TM_Invisible;
	}
	key_update = is_key_update(relation, oldslot, slot);

	*lockmode = key_update ? LockTupleExclusive : LockTupleNoKeyExclusive;

	result = zsbt_tid_update(relation, otid,
							 xid, cid, key_update, snapshot, crosscheck,
							 wait, hufd, &newtid, &this_xact_has_lock);

	*update_indexes = (result == TM_Ok);
	if (result == TM_Ok)
	{
		/*
		 * Check for SSI conflicts.
		 */
		CheckForSerializableConflictIn(relation, otid_p, ItemPointerGetBlockNumber(otid_p));

		zsbt_tuplebuffer_spool_tuple(relation, newtid, d, isnulls);

		slot->tts_tableOid = RelationGetRelid(relation);
		slot->tts_tid = ItemPointerFromZSTid(newtid);

		pgstat_count_heap_update(relation, false);
	}
	else
	{
		if (result == TM_Invisible)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("attempted to update invisible tuple")));
		else if (result == TM_BeingModified && wait)
		{
			TransactionId	xwait = hufd->xmax;

			if (!TransactionIdIsCurrentTransactionId(xwait))
			{
				/*
				 * Acquire tuple lock to establish our priosity for the tuple
				 * See zedstoream_lock_tuple().
				 */
				if (!this_xact_has_lock)
				{
					zs_acquire_tuplock(relation, otid_p, LockTupleExclusive, LockWaitBlock,
									   &have_tuple_lock);
				}

				XactLockTableWait(xwait, relation, otid_p, XLTW_Update);
				goto retry;
			}
		}
	}

	/*
	 * Now that we have successfully updated the tuple, we can
	 * release the lmgr tuple lock, if we had it.
	 */
	if (have_tuple_lock)
	{
		UnlockTupleTuplock(relation, otid_p, LockTupleExclusive);
		have_tuple_lock = false;
	}

	zedstoream_end_index_fetch(fetcher);
	ExecDropSingleTupleTableSlot(oldslot);

	MemoryContextSwitchTo(oldcontext);
	MemoryContextDelete(insert_mcontext);

	return result;
}

static const TupleTableSlotOps *
zedstoream_slot_callbacks(Relation relation)
{
	return &TTSOpsZedstore;
}

static void
zs_initialize_proj_attributes(TupleDesc tupledesc, ZedStoreProjectData *proj_data)
{
	MemoryContext oldcontext;

	if (proj_data->num_proj_atts != 0)
		return;

	oldcontext = MemoryContextSwitchTo(proj_data->context);
	/* add one for meta-attribute */
	proj_data->proj_atts = palloc((tupledesc->natts + 1) * sizeof(int));
	proj_data->attr_scans = palloc0(tupledesc->natts * sizeof(ZSAttrTreeScan));
	proj_data->tid_scan.active = false;

	proj_data->proj_atts[proj_data->num_proj_atts++] = ZS_META_ATTRIBUTE_NUM;

	/*
	 * convert booleans array into an array of the attribute numbers of the
	 * required columns.
	 */
	for (int idx = 0; idx < tupledesc->natts; idx++)
	{
		int			att_no = idx + 1;

		/*
		 * never project dropped columns, null will be returned for them
		 * in slot by default.
		 */
		if  (TupleDescAttr(tupledesc, idx)->attisdropped)
			continue;

		/* project_columns empty also conveys need all the columns */
		if (proj_data->project_columns == NULL ||
			bms_is_member(att_no, proj_data->project_columns))
			proj_data->proj_atts[proj_data->num_proj_atts++] = att_no;
	}

	MemoryContextSwitchTo(oldcontext);
}

static void
zs_initialize_proj_attributes_extended(ZedStoreDesc scan, TupleDesc tupledesc)
{
	MemoryContext oldcontext;
	ZedStoreProjectData *proj_data = &scan->proj_data;

	/* if already initialized return */
	if (proj_data->num_proj_atts != 0)
		return;

	zs_initialize_proj_attributes(tupledesc, proj_data);

	oldcontext = MemoryContextSwitchTo(proj_data->context);
	/* Extra setup for bitmap and sample scans */
	if ((scan->rs_scan.rs_flags & SO_TYPE_BITMAPSCAN) ||
		(scan->rs_scan.rs_flags & SO_TYPE_SAMPLESCAN) ||
		(scan->rs_scan.rs_flags & SO_TYPE_ANALYZE))
	{
		scan->bmscan_ntuples = 0;
		scan->bmscan_tids = palloc(MAX_ITEMS_PER_LOGICAL_BLOCK * sizeof(zstid));
	}
	MemoryContextSwitchTo(oldcontext);
}

static TableScanDesc
zedstoream_beginscan_with_column_projection(Relation relation, Snapshot snapshot,
											int nkeys, ScanKey key,
											ParallelTableScanDesc parallel_scan,
											uint32 flags,
											Bitmapset *project_columns)
{
	ZedStoreDesc scan;

	zsbt_tuplebuffer_flush(relation);

	/* Sample scans have no snapshot, but we need one */
	if (!snapshot)
	{
		Assert(!(flags & SO_TYPE_SAMPLESCAN));
		snapshot = SnapshotAny;
	}

	/*
	 * allocate and initialize scan descriptor
	 */
	scan = (ZedStoreDesc) palloc0(sizeof(ZedStoreDescData));

	scan->rs_scan.rs_rd = relation;
	scan->rs_scan.rs_snapshot = snapshot;
	scan->rs_scan.rs_nkeys = nkeys;
	scan->rs_scan.rs_flags = flags;
	scan->rs_scan.rs_parallel = parallel_scan;

	/*
	 * we can use page-at-a-time mode if it's an MVCC-safe snapshot
	 */

	/*
	 * we do this here instead of in initscan() because heap_rescan also calls
	 * initscan() and we don't want to allocate memory again
	 */
	if (nkeys > 0)
		scan->rs_scan.rs_key = (ScanKey) palloc(sizeof(ScanKeyData) * nkeys);
	else
		scan->rs_scan.rs_key = NULL;

	scan->proj_data.context = CurrentMemoryContext;
	scan->proj_data.project_columns = project_columns;

	/*
	 * For a seqscan in a serializable transaction, acquire a predicate lock
	 * on the entire relation. This is required not only to lock all the
	 * matching tuples, but also to conflict with new insertions into the
	 * table. In an indexscan, we take page locks on the index pages covering
	 * the range specified in the scan qual, but in a heap scan there is
	 * nothing more fine-grained to lock. A bitmap scan is a different story,
	 * there we have already scanned the index and locked the index pages
	 * covering the predicate. But in that case we still have to lock any
	 * matching heap tuples.
	 */
	if (flags & (SO_TYPE_SEQSCAN | SO_TYPE_SAMPLESCAN))
		PredicateLockRelation(relation, snapshot);

	/*
	 * Currently, we don't have a stats counter for bitmap heap scans (but the
	 * underlying bitmap index scans will be counted) or sample scans (we only
	 * update stats for tuple fetches there)
	 */
	if (!(flags & SO_TYPE_BITMAPSCAN) && !(flags & SO_TYPE_SAMPLESCAN))
		pgstat_count_heap_scan(relation);

	return (TableScanDesc) scan;
}

static TableScanDesc
zedstoream_beginscan(Relation relation, Snapshot snapshot,
					 int nkeys, ScanKey key,
					 ParallelTableScanDesc parallel_scan,
					 uint32 flags)
{
	return zedstoream_beginscan_with_column_projection(relation, snapshot,
													   nkeys, key, parallel_scan, flags, NULL);
}

static void
zedstoream_endscan(TableScanDesc sscan)
{
	ZedStoreDesc scan = (ZedStoreDesc) sscan;
	ZedStoreProjectData *proj_data = &scan->proj_data;

	if (proj_data->proj_atts)
		pfree(proj_data->proj_atts);

	if (proj_data->num_proj_atts > 0)
	{
		zsbt_tid_end_scan(&proj_data->tid_scan);
		for (int i = 1; i < proj_data->num_proj_atts; i++)
			zsbt_attr_end_scan(&proj_data->attr_scans[i - 1]);
	}

	if (scan->rs_scan.rs_flags & SO_TEMP_SNAPSHOT)
		UnregisterSnapshot(scan->rs_scan.rs_snapshot);

	if (proj_data->attr_scans)
		pfree(proj_data->attr_scans);
	pfree(scan);
}

static void
zedstoream_rescan(TableScanDesc sscan, struct ScanKeyData *key,
				  bool set_params, bool allow_strat,
				  bool allow_sync, bool allow_pagemode)
{
	ZedStoreDesc scan = (ZedStoreDesc) sscan;

	/* these params don't do much in zedstore yet, but whatever */
	if (set_params)
	{
		if (allow_strat)
			scan->rs_scan.rs_flags |= SO_ALLOW_STRAT;
		else
			scan->rs_scan.rs_flags &= ~SO_ALLOW_STRAT;

		if (allow_sync)
			scan->rs_scan.rs_flags |= SO_ALLOW_SYNC;
		else
			scan->rs_scan.rs_flags &= ~SO_ALLOW_SYNC;

		if (allow_pagemode && scan->rs_scan.rs_snapshot &&
			IsMVCCSnapshot(scan->rs_scan.rs_snapshot))
			scan->rs_scan.rs_flags |= SO_ALLOW_PAGEMODE;
		else
			scan->rs_scan.rs_flags &= ~SO_ALLOW_PAGEMODE;
	}

	if (scan->proj_data.num_proj_atts > 0)
	{
		zsbt_tid_reset_scan(&scan->proj_data.tid_scan,
							scan->cur_range_start, scan->cur_range_end, scan->cur_range_start - 1);

		if ((scan->rs_scan.rs_flags & SO_TYPE_SAMPLESCAN) != 0)
			scan->next_tid_to_scan = ZSTidFromBlkOff(0, 1);
	}
}

static bool
zedstoream_getnextslot(TableScanDesc sscan, ScanDirection direction,
					   TupleTableSlot *slot)
{
	ZedStoreDesc scan = (ZedStoreDesc) sscan;
	ZedStoreProjectData *scan_proj = &scan->proj_data;
	int			slot_natts = slot->tts_tupleDescriptor->natts;
	Datum	   *slot_values = slot->tts_values;
	bool	   *slot_isnull = slot->tts_isnull;
	zstid		this_tid;
	Datum		datum;
	bool        isnull;
	ZSUndoSlotVisibility *visi_info;
	uint8		slotno;

	if (direction != ForwardScanDirection && scan->rs_scan.rs_parallel)
		elog(ERROR, "parallel backward scan not implemented");

	if (!scan->started)
	{
		MemoryContext oldcontext;

		zs_initialize_proj_attributes(slot->tts_tupleDescriptor, scan_proj);

		if (scan->rs_scan.rs_parallel)
		{
			/* Allocate next range of TIDs to scan */
			if (!zs_parallelscan_nextrange(scan->rs_scan.rs_rd,
										   (ParallelZSScanDesc) scan->rs_scan.rs_parallel,
										   &scan->cur_range_start, &scan->cur_range_end))
			{
				ExecClearTuple(slot);
				return false;
			}
		}
		else
		{
			scan->cur_range_start = MinZSTid;
			scan->cur_range_end = MaxPlusOneZSTid;
		}

		oldcontext = MemoryContextSwitchTo(scan_proj->context);
		zsbt_tid_begin_scan(scan->rs_scan.rs_rd,
							scan->cur_range_start,
							scan->cur_range_end,
							scan->rs_scan.rs_snapshot,
							&scan_proj->tid_scan);
		scan_proj->tid_scan.serializable = true;
		for (int i = 1; i < scan_proj->num_proj_atts; i++)
		{
			int			attno = scan_proj->proj_atts[i];

			zsbt_attr_begin_scan(scan->rs_scan.rs_rd,
								 slot->tts_tupleDescriptor,
								 attno,
								 &scan_proj->attr_scans[i - 1]);
		}
		MemoryContextSwitchTo(oldcontext);
		scan->started = true;
	}
	Assert((scan_proj->num_proj_atts - 1) <= slot_natts);

	/*
	 * Initialize the slot.
	 *
	 * We initialize all columns to NULL. The values for columns that are projected
	 * will be set to the actual values below, but it's important that non-projected
	 * columns are NULL.
	 */
	ExecClearTuple(slot);
	for (int i = 0; i < slot_natts; i++)
		slot_isnull[i] = true;

	/*
	 * Find the next visible TID.
	 */
	for (;;)
	{
		this_tid = zsbt_tid_scan_next(&scan_proj->tid_scan, direction);
		if (this_tid == InvalidZSTid)
		{
			if (scan->rs_scan.rs_parallel)
			{
				/* Allocate next range of TIDs to scan */
				if (!zs_parallelscan_nextrange(scan->rs_scan.rs_rd,
											   (ParallelZSScanDesc) scan->rs_scan.rs_parallel,
											   &scan->cur_range_start, &scan->cur_range_end))
				{
					ExecClearTuple(slot);
					return false;
				}

				zsbt_tid_reset_scan(&scan_proj->tid_scan,
									scan->cur_range_start, scan->cur_range_end, scan->cur_range_start - 1);
				continue;
			}
			else
			{
				ExecClearTuple(slot);
				return false;
			}
		}
		Assert (this_tid < scan->cur_range_end);
		break;
	}

	/* Note: We don't need to predicate-lock tuples in Serializable mode,
	 * because in a sequential scan, we predicate-locked the whole table.
	 */

	/* Fetch the datums of each attribute for this row */
	for (int i = 1; i < scan_proj->num_proj_atts; i++)
	{
		ZSAttrTreeScan *btscan = &scan_proj->attr_scans[i - 1];
		Form_pg_attribute attr = btscan->attdesc;
		int			natt;

		if (!zsbt_attr_fetch(btscan, &datum, &isnull, this_tid))
			zsbt_fill_missing_attribute_value(slot->tts_tupleDescriptor, btscan->attno,
											  &datum, &isnull);

		/*
		 * flatten any ZS-TOASTed values, because the rest of the system
		 * doesn't know how to deal with them.
		 */
		natt = scan_proj->proj_atts[i];

		if (!isnull && attr->attlen == -1 &&
			VARATT_IS_EXTERNAL(datum) && VARTAG_EXTERNAL(datum) == VARTAG_ZEDSTORE)
		{
			MemoryContext oldcxt = CurrentMemoryContext;

			if (btscan->decoder.tmpcxt)
				MemoryContextSwitchTo(btscan->decoder.tmpcxt);
			datum = zedstore_toast_flatten(scan->rs_scan.rs_rd, natt, this_tid, datum);
			MemoryContextSwitchTo(oldcxt);
		}

		/* Check that the values coming out of the b-tree are aligned properly */
		if (!isnull && attr->attlen == -1)
		{
			Assert (VARATT_IS_1B(datum) || INTALIGN(datum) == datum);
		}

		Assert(natt > 0);
		slot_values[natt - 1] = datum;
		slot_isnull[natt - 1] = isnull;
	}

	/* Fill in the rest of the fields in the slot, and return the tuple */
	slotno = ZSTidScanCurUndoSlotNo(&scan_proj->tid_scan);
	visi_info = &scan_proj->tid_scan.array_iter.visi_infos[slotno];
	((ZedstoreTupleTableSlot *) slot)->visi_info = visi_info;

	slot->tts_tableOid = RelationGetRelid(scan->rs_scan.rs_rd);
	slot->tts_tid = ItemPointerFromZSTid(this_tid);
	slot->tts_nvalid = slot->tts_tupleDescriptor->natts;
	slot->tts_flags &= ~TTS_FLAG_EMPTY;

	pgstat_count_heap_getnext(scan->rs_scan.rs_rd);
	return true;
}

static bool
zedstoream_tuple_tid_valid(TableScanDesc sscan, ItemPointer tid)
{
	ZedStoreDesc scan;
	zstid ztid;

	if (!ItemPointerIsValid(tid))
		return false;

	scan = (ZedStoreDesc) sscan;
	ztid = ZSTidFromItemPointer(*tid);

	if (scan->min_tid_to_scan == InvalidZSTid)
	{
		/*
		 * get the min tid once and store it
		 */
		scan->min_tid_to_scan = zsbt_get_first_tid(sscan->rs_rd);
	}

	if (scan->max_tid_to_scan == InvalidZSTid)
	{
		/*
		 * get the max tid once and store it
		 */
		scan->max_tid_to_scan = zsbt_get_last_tid(sscan->rs_rd);
	}

	if ( ztid >= scan->min_tid_to_scan && ztid < scan->max_tid_to_scan)
		return true;
	else
		return false;
}

static bool
zedstoream_tuple_satisfies_snapshot(Relation rel, TupleTableSlot *slot,
									Snapshot snapshot)
{
	zstid		tid = ZSTidFromItemPointer(slot->tts_tid);
	TransactionId obsoleting_xid;
	ZSTidTreeScan meta_scan;
	bool		result;
	ZedstoreTupleTableSlot *zslot = (ZedstoreTupleTableSlot *) slot;

	/* Use the meta-data tree for the visibility information. */
	zsbt_tid_begin_scan(rel, tid, tid + 1, snapshot, &meta_scan);
	result = zs_SatisfiesVisibility(&meta_scan, &obsoleting_xid, NULL, zslot->visi_info);
	zsbt_tid_end_scan(&meta_scan);

	return result;
}

static TransactionId
zedstoream_compute_xid_horizon_for_tuples(Relation rel,
										  ItemPointerData *items,
										  int nitems)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("function %s not implemented yet", __func__)));
}

static IndexFetchTableData *
zedstoream_begin_index_fetch(Relation rel)
{
	ZedStoreIndexFetch zscan;

	zsbt_tuplebuffer_flush(rel);

	zscan = palloc0(sizeof(ZedStoreIndexFetchData));
	zscan->idx_fetch_data.rel = rel;
	zscan->proj_data.context = CurrentMemoryContext;

	return (IndexFetchTableData *) zscan;
}

static void
zedstoream_fetch_set_column_projection(struct IndexFetchTableData *scan,
									   Bitmapset *project_cols)
{
	ZedStoreIndexFetch zscan = (ZedStoreIndexFetch) scan;
	zscan->proj_data.project_columns = project_cols;
}

static void
zedstoream_reset_index_fetch(IndexFetchTableData *scan)
{
	/* TODO: we could close the scans here, but currently we don't bother */
}

static void
zedstoream_end_index_fetch(IndexFetchTableData *scan)
{
	ZedStoreIndexFetch zscan = (ZedStoreIndexFetch) scan;
	ZedStoreProjectData *zscan_proj = &zscan->proj_data;

	if (zscan_proj->num_proj_atts > 0)
	{
		zsbt_tid_end_scan(&zscan_proj->tid_scan);
		for (int i = 1; i < zscan_proj->num_proj_atts; i++)
			zsbt_attr_end_scan(&zscan_proj->attr_scans[i - 1]);
	}

	if (zscan_proj->proj_atts)
		pfree(zscan_proj->proj_atts);

	if (zscan_proj->attr_scans)
		pfree(zscan_proj->attr_scans);
	pfree(zscan);
}

static bool
zedstoream_index_fetch_tuple(struct IndexFetchTableData *scan,
							 ItemPointer tid_p,
							 Snapshot snapshot,
							 TupleTableSlot *slot,
							 bool *call_again, bool *all_dead)
{
	bool		result;

	/*
	 * we don't do in-place updates, so this is essentially the same as
	 * fetch_row_version.
	 */
	if (call_again)
		*call_again = false;
	if (all_dead)
		*all_dead = false;

	result = zedstoream_fetch_row((ZedStoreIndexFetchData *) scan, tid_p, snapshot, slot);
	if (result)
	{
		/* FIXME: heapam acquires the predicate lock first, and then
		 * calls CheckForSerializableConflictOut(). We do it in the
		 * opposite order, because CheckForSerializableConflictOut()
		 * call as done in zsbt_get_last_tid() already. Does it matter?
		 * I'm not sure.
		 *
		 * We pass in InvalidTransactionId as we are sure that the current
		 * transaction hasn't locked tid_p.
		 */
		PredicateLockTID(scan->rel, tid_p, snapshot, InvalidTransactionId);
	}

	return result;
}

/*
 * Shared implementation of fetch_row_version and index_fetch_tuple callbacks.
 */
static bool
zedstoream_fetch_row(ZedStoreIndexFetchData *fetch,
					 ItemPointer tid_p,
					 Snapshot snapshot,
					 TupleTableSlot *slot)
{
	Relation	rel = fetch->idx_fetch_data.rel;
	zstid		tid = ZSTidFromItemPointer(*tid_p);
	bool		found = true;
	ZedStoreProjectData *fetch_proj = &fetch->proj_data;

	/* first time here, initialize */
	if (fetch_proj->num_proj_atts == 0)
	{
		TupleDesc	reldesc = RelationGetDescr(rel);
		MemoryContext oldcontext;

		zs_initialize_proj_attributes(slot->tts_tupleDescriptor, fetch_proj);

		oldcontext = MemoryContextSwitchTo(fetch_proj->context);
		zsbt_tid_begin_scan(rel, tid, tid + 1,
							snapshot,
							&fetch_proj->tid_scan);
		fetch_proj->tid_scan.serializable = true;
		for (int i = 1; i < fetch_proj->num_proj_atts; i++)
		{
			int			attno = fetch_proj->proj_atts[i];

			zsbt_attr_begin_scan(rel,  reldesc, attno,
								 &fetch_proj->attr_scans[i - 1]);
		}
		MemoryContextSwitchTo(oldcontext);
	}
	else
		zsbt_tid_reset_scan(&fetch_proj->tid_scan, tid, tid + 1, tid - 1);

	/*
	 * Initialize the slot.
	 *
	 * If we're not fetching all columns, initialize the unfetched values
	 * in the slot to NULL. (Actually, this initializes all to NULL, and the
	 * code below will overwrite them for the columns that are projected)
	 */
	ExecClearTuple(slot);
	for (int i = 0; i < slot->tts_tupleDescriptor->natts; i++)
		slot->tts_isnull[i] = true;

	found = zsbt_tid_scan_next(&fetch_proj->tid_scan, ForwardScanDirection) != InvalidZSTid;
	if (found)
	{
		for (int i = 1; i < fetch_proj->num_proj_atts; i++)
		{
			int         natt = fetch_proj->proj_atts[i];
			ZSAttrTreeScan *btscan = &fetch_proj->attr_scans[i - 1];
			Form_pg_attribute attr;
			Datum		datum;
			bool        isnull;

			attr = btscan->attdesc;
			if (zsbt_attr_fetch(btscan, &datum, &isnull, tid))
			{
				/*
				 * flatten any ZS-TOASTed values, because the rest of the system
				 * doesn't know how to deal with them.
				 */
				if (!isnull && attr->attlen == -1 &&
					VARATT_IS_EXTERNAL(datum) && VARTAG_EXTERNAL(datum) == VARTAG_ZEDSTORE)
				{
					MemoryContext oldcxt = CurrentMemoryContext;

					if (btscan->decoder.tmpcxt)
						MemoryContextSwitchTo(btscan->decoder.tmpcxt);
					datum = zedstore_toast_flatten(rel, natt, tid, datum);
					MemoryContextSwitchTo(oldcxt);
				}
			}
			else
				zsbt_fill_missing_attribute_value(slot->tts_tupleDescriptor, btscan->attno,
												  &datum, &isnull);

			slot->tts_values[natt - 1] = datum;
			slot->tts_isnull[natt - 1] = isnull;
		}
	}

	if (found)
	{
		uint8		slotno = ZSTidScanCurUndoSlotNo(&fetch_proj->tid_scan);
		ZSUndoSlotVisibility *visi_info;

		visi_info = &fetch_proj->tid_scan.array_iter.visi_infos[slotno];

		((ZedstoreTupleTableSlot *) slot)->visi_info = visi_info;
		slot->tts_tableOid = RelationGetRelid(rel);
		slot->tts_tid = ItemPointerFromZSTid(tid);
		slot->tts_nvalid = slot->tts_tupleDescriptor->natts;
		slot->tts_flags &= ~TTS_FLAG_EMPTY;
		return true;
	}

	return false;
}

static void
zedstoream_index_validate_scan(Relation baseRelation,
							   Relation indexRelation,
							   IndexInfo *indexInfo,
							   Snapshot snapshot,
							   ValidateIndexState *state)
{
	Datum		values[INDEX_MAX_KEYS];
	bool		isnull[INDEX_MAX_KEYS];
	ExprState  *predicate;
	TupleTableSlot *slot;
	EState	   *estate;
	ExprContext *econtext;
	int			attno;
	TableScanDesc scan;
	ItemPointerData idx_ptr;
	bool		tuplesort_empty = false;
	Bitmapset   *proj = NULL;

	/*
	 * sanity checks
	 */
	Assert(OidIsValid(indexRelation->rd_rel->relam));

	/*
	 * Need an EState for evaluation of index expressions and partial-index
	 * predicates.  Also a slot to hold the current tuple.
	 */
	estate = CreateExecutorState();
	econtext = GetPerTupleExprContext(estate);
	slot = table_slot_create(baseRelation, NULL);

	/* Arrange for econtext's scan tuple to be the tuple under test */
	econtext->ecxt_scantuple = slot;

	/* Set up execution state for predicate, if any. */
	predicate = ExecPrepareQual(indexInfo->ii_Predicate, estate);

	/*
	 * Prepare for scan of the base relation.  We need just those tuples
	 * satisfying the passed-in reference snapshot.  We must disable syncscan
	 * here, because it's critical that we read from block zero forward to
	 * match the sorted TIDs.
	 */

	/*
	 * TODO: It would be very good to fetch only the columns we need.
	 */
	for (attno = 0; attno < indexInfo->ii_NumIndexKeyAttrs; attno++)
	{
		Assert(indexInfo->ii_IndexAttrNumbers[attno] <= baseRelation->rd_att->natts);
		proj = bms_add_member(proj, indexInfo->ii_IndexAttrNumbers[attno]);
	}
	PopulateNeededColumnsForNode((Node *)indexInfo->ii_Predicate,
								 baseRelation->rd_att->natts,
								 &proj);
	PopulateNeededColumnsForNode((Node *)indexInfo->ii_Expressions,
								 baseRelation->rd_att->natts,
								 &proj);

	scan = table_beginscan_with_column_projection(baseRelation,	/* relation */
												  snapshot,	/* snapshot */
												  0, /* number of keys */
												  NULL,	/* scan key */
												  proj);

	/*
	 * Scan all tuples matching the snapshot.
	 */
	ItemPointerSet(&idx_ptr, 0, 0); /* this is less than any real TID */
	while (table_scan_getnextslot(scan, ForwardScanDirection, slot))
	{
		ItemPointerData tup_ptr = slot->tts_tid;
		int			cmp;

		CHECK_FOR_INTERRUPTS();

		/*
		 * TODO: Once we have in-place updates, like HOT, this will need
		 * to work harder, like heapam's function.
		 */

		MemoryContextReset(econtext->ecxt_per_tuple_memory);

		if (tuplesort_empty)
			cmp = -1;
		else
		{
			while ((cmp = ItemPointerCompare(&tup_ptr, &idx_ptr)) > 0)
			{
				Datum		ts_val;
				bool		ts_isnull;

				tuplesort_empty = !tuplesort_getdatum(state->tuplesort, true,
													  &ts_val, &ts_isnull, NULL);
				if (!tuplesort_empty)
				{
					Assert(!ts_isnull);
					itemptr_decode(&idx_ptr, DatumGetInt64(ts_val));

					/* If int8 is pass-by-ref, free (encoded) TID Datum memory */
#ifndef USE_FLOAT8_BYVAL
					pfree(DatumGetPointer(ts_val));
#endif
					break;
				}
				else
				{
					/* Be tidy */
					ItemPointerSetInvalid(&idx_ptr);
					cmp = -1;
				}
			}
		}
		if (cmp < 0)
		{
			/* This item is not in the index */

			/*
			 * In a partial index, discard tuples that don't satisfy the
			 * predicate.
			 */
			if (predicate != NULL)
			{
				if (!ExecQual(predicate, econtext))
					continue;
			}

			/*
			 * For the current heap tuple, extract all the attributes we use in
			 * this index, and note which are null.  This also performs evaluation
			 * of any expressions needed.
			 */
			FormIndexDatum(indexInfo,
						   slot,
						   estate,
						   values,
						   isnull);

			/* Call the AM's callback routine to process the tuple */
			index_insert(indexRelation, values, isnull, &tup_ptr, baseRelation,
						 indexInfo->ii_Unique ?
						 UNIQUE_CHECK_YES : UNIQUE_CHECK_NO,
						 indexInfo);

			state->tups_inserted += 1;
		}
	}

	table_endscan(scan);

	ExecDropSingleTupleTableSlot(slot);

	FreeExecutorState(estate);

	/* These may have been pointing to the now-gone estate */
	indexInfo->ii_ExpressionsState = NIL;
	indexInfo->ii_PredicateState = NULL;
}

static double
zedstoream_index_build_range_scan(Relation baseRelation,
								  Relation indexRelation,
								  IndexInfo *indexInfo,
								  bool allow_sync,
								  bool anyvisible,
								  bool progress,
								  BlockNumber start_blockno,
								  BlockNumber numblocks,
								  IndexBuildCallback callback,
								  void *callback_state,
								  TableScanDesc scan)
{
	Datum		values[INDEX_MAX_KEYS];
	bool		isnull[INDEX_MAX_KEYS];
	double		reltuples;
	ExprState  *predicate;
	TupleTableSlot *slot;
	EState	   *estate;
	ExprContext *econtext;
	Snapshot	snapshot;
	SnapshotData NonVacuumableSnapshot;
	bool		need_unregister_snapshot = false;
	TransactionId OldestXmin;
	bool        tupleIsAlive;

#ifdef USE_ASSERT_CHECKING
	bool		checking_uniqueness;
	/* See whether we're verifying uniqueness/exclusion properties */
	checking_uniqueness = (indexInfo->ii_Unique ||
						   indexInfo->ii_ExclusionOps != NULL);

	/*
	 * "Any visible" mode is not compatible with uniqueness checks; make sure
	 * only one of those is requested.
	 */
	Assert(!(anyvisible && checking_uniqueness));
#endif

	/*
	 * sanity checks
	 */
	Assert(OidIsValid(indexRelation->rd_rel->relam));

	/*
	 * Need an EState for evaluation of index expressions and partial-index
	 * predicates.  Also a slot to hold the current tuple.
	 */
	estate = CreateExecutorState();
	econtext = GetPerTupleExprContext(estate);
	slot = table_slot_create(baseRelation, NULL);

	/* Arrange for econtext's scan tuple to be the tuple under test */
	econtext->ecxt_scantuple = slot;

	/* Set up execution state for predicate, if any. */
	predicate = ExecPrepareQual(indexInfo->ii_Predicate, estate);

	/*
	 * Prepare for scan of the base relation.  In a normal index build, we use
	 * SnapshotAny because we must retrieve all tuples and do our own time
	 * qual checks (because we have to index RECENTLY_DEAD tuples). In a
	 * concurrent build, or during bootstrap, we take a regular MVCC snapshot
	 * and index whatever's live according to that.
	 */
	OldestXmin = InvalidTransactionId;

	/* okay to ignore lazy VACUUMs here */
	if (!IsBootstrapProcessingMode() && !indexInfo->ii_Concurrent)
		OldestXmin = GetOldestXmin(baseRelation, PROCARRAY_FLAGS_VACUUM);

	zsbt_tuplebuffer_flush(baseRelation);
	if (!scan)
	{
		int			attno;
		Bitmapset  *proj = NULL;

		/*
		 * Serial index build.
		 *
		 * Must begin our own zedstore scan in this case.  We may also need to
		 * register a snapshot whose lifetime is under our direct control.
		 */
		if (!TransactionIdIsValid(OldestXmin))
		{
			snapshot = RegisterSnapshot(GetTransactionSnapshot());
			need_unregister_snapshot = true;
		}
		else
		{
			/* leave out completely dead items even with SnapshotAny */
			InitNonVacuumableSnapshot(NonVacuumableSnapshot, OldestXmin);
			snapshot = &NonVacuumableSnapshot;
		}

		for (attno = 0; attno < indexInfo->ii_NumIndexKeyAttrs; attno++)
		{
			Assert(indexInfo->ii_IndexAttrNumbers[attno] <= baseRelation->rd_att->natts);
			proj = bms_add_member(proj, indexInfo->ii_IndexAttrNumbers[attno]);
		}
		PopulateNeededColumnsForNode((Node *)indexInfo->ii_Predicate,
									 baseRelation->rd_att->natts,
									 &proj);
		PopulateNeededColumnsForNode((Node *)indexInfo->ii_Expressions,
									 baseRelation->rd_att->natts,
									 &proj);

		scan = table_beginscan_with_column_projection(baseRelation,	/* relation */
													  snapshot,	/* snapshot */
													  0, /* number of keys */
													  NULL,	/* scan key */
													  proj);

		if (start_blockno != 0 || numblocks != InvalidBlockNumber)
		{
			ZedStoreDesc zscan = (ZedStoreDesc) scan;
			ZedStoreProjectData *zscan_proj = &zscan->proj_data;

			zscan->cur_range_start = ZSTidFromBlkOff(start_blockno, 1);
			zscan->cur_range_end = ZSTidFromBlkOff(numblocks, 1);

			/* FIXME: when can 'num_proj_atts' be 0? */
			if (zscan_proj->num_proj_atts > 0)
			{
				zsbt_tid_begin_scan(zscan->rs_scan.rs_rd,
									zscan->cur_range_start,
									zscan->cur_range_end,
									zscan->rs_scan.rs_snapshot,
									&zscan_proj->tid_scan);
				for (int i = 1; i < zscan_proj->num_proj_atts; i++)
				{
					int			natt = zscan_proj->proj_atts[i];

					zsbt_attr_begin_scan(zscan->rs_scan.rs_rd,
										 RelationGetDescr(zscan->rs_scan.rs_rd),
										 natt,
										 &zscan_proj->attr_scans[i - 1]);
				}
			}
		}
	}
	else
	{
		/*
		 * Parallel index build.
		 *
		 * Parallel case never registers/unregisters own snapshot.  Snapshot
		 * is taken from parallel zedstore scan, and is SnapshotAny or an MVCC
		 * snapshot, based on same criteria as serial case.
		 */
		Assert(!IsBootstrapProcessingMode());
		Assert(allow_sync);
		Assert(start_blockno == 0);
		Assert(numblocks == InvalidBlockNumber);
		snapshot = scan->rs_snapshot;

		if (snapshot == SnapshotAny)
		{
			/* leave out completely dead items even with SnapshotAny */
			InitNonVacuumableSnapshot(NonVacuumableSnapshot, OldestXmin);
			snapshot = &NonVacuumableSnapshot;
		}
	}

	/*
	 * Must call GetOldestXmin() with SnapshotAny.  Should never call
	 * GetOldestXmin() with MVCC snapshot. (It's especially worth checking
	 * this for parallel builds, since ambuild routines that support parallel
	 * builds must work these details out for themselves.)
	 */
	Assert(snapshot == &NonVacuumableSnapshot || IsMVCCSnapshot(snapshot));
	Assert(snapshot == &NonVacuumableSnapshot ? TransactionIdIsValid(OldestXmin) :
		   !TransactionIdIsValid(OldestXmin));
	Assert(snapshot == &NonVacuumableSnapshot || !anyvisible);

	reltuples = 0;

	/*
	 * Scan all tuples in the base relation.
	 */
	while (zedstoream_getnextslot(scan, ForwardScanDirection, slot))
	{
		ZSUndoSlotVisibility *visi_info;

		if (numblocks != InvalidBlockNumber &&
			ItemPointerGetBlockNumber(&slot->tts_tid) >= numblocks)
			break;

		CHECK_FOR_INTERRUPTS();

		/*
		 * Is the tuple deleted, but still visible to old transactions?
		 *
		 * We need to include such tuples in the index, but exclude them
		 * from unique-checking.
		 *
		 * TODO: Heap checks for DELETE_IN_PROGRESS do we need as well?
		 */
		visi_info = ((ZedstoreTupleTableSlot *) slot)->visi_info;
		tupleIsAlive = (visi_info->nonvacuumable_status != ZSNV_RECENTLY_DEAD);

		if (tupleIsAlive)
			reltuples += 1;

		/*
		 * TODO: Once we have in-place updates, like HOT, this will need
		 * to work harder, to figure out which tuple version to index.
		 */

		MemoryContextReset(econtext->ecxt_per_tuple_memory);

		/*
		 * In a partial index, discard tuples that don't satisfy the
		 * predicate.
		 */
		if (predicate != NULL)
		{
			if (!ExecQual(predicate, econtext))
				continue;
		}

		/*
		 * For the current heap tuple, extract all the attributes we use in
		 * this index, and note which are null.  This also performs evaluation
		 * of any expressions needed.
		 */
		FormIndexDatum(indexInfo,
					   slot,
					   estate,
					   values,
					   isnull);

		/* Call the AM's callback routine to process the tuple */
		callback(indexRelation, &slot->tts_tid, values, isnull, tupleIsAlive,
				 callback_state);
	}

	table_endscan(scan);

	/* we can now forget our snapshot, if set and registered by us */
	if (need_unregister_snapshot)
		UnregisterSnapshot(snapshot);

	ExecDropSingleTupleTableSlot(slot);

	FreeExecutorState(estate);

	/* These may have been pointing to the now-gone estate */
	indexInfo->ii_ExpressionsState = NIL;
	indexInfo->ii_PredicateState = NULL;

	return reltuples;
}

static void
zedstoream_finish_bulk_insert(Relation relation, int options)
{
	zsbt_tuplebuffer_flush(relation);

	/*
	 * If we skipped writing WAL, then we need to sync the zedstore (but not
	 * indexes since those use WAL anyway / don't go through tableam)
	 */
	if (options & HEAP_INSERT_SKIP_WAL)
		heap_sync(relation);
}

/* ------------------------------------------------------------------------
 * DDL related callbacks for zedstore AM.
 * ------------------------------------------------------------------------
 */

static void
zedstoream_relation_set_new_filenode(Relation rel,
									 const RelFileNode *newrnode,
									 char persistence,
									 TransactionId *freezeXid,
									 MultiXactId *minmulti)
{
	SMgrRelation srel;

	/* XXX: I think we could just throw away all data in the buffer */
	zsbt_tuplebuffer_flush(rel);

	/*
	 * Initialize to the minimum XID that could put tuples in the table. We
	 * know that no xacts older than RecentXmin are still running, so that
	 * will do.
	 */
	*freezeXid = RecentXmin;

	/*
	 * Similarly, initialize the minimum Multixact to the first value that
	 * could possibly be stored in tuples in the table.  Running transactions
	 * could reuse values from their local cache, so we are careful to
	 * consider all currently running multis.
	 *
	 * XXX this could be refined further, but is it worth the hassle?
	 */
	*minmulti = GetOldestMultiXactId();

	srel = RelationCreateStorage(*newrnode, persistence);

	/*
	 * If required, set up an init fork for an unlogged table so that it can
	 * be correctly reinitialized on restart.  An immediate sync is required
	 * even if the page has been logged, because the write did not go through
	 * shared_buffers and therefore a concurrent checkpoint may have moved the
	 * redo pointer past our xlog record.  Recovery may as well remove it
	 * while replaying, for example, XLOG_DBASE_CREATE or XLOG_TBLSPC_CREATE
	 * record. Therefore, logging is necessary even if wal_level=minimal.
	 */
	if (persistence == RELPERSISTENCE_UNLOGGED)
	{
		Assert(rel->rd_rel->relkind == RELKIND_RELATION ||
			   rel->rd_rel->relkind == RELKIND_MATVIEW ||
			   rel->rd_rel->relkind == RELKIND_TOASTVALUE);
		smgrcreate(srel, INIT_FORKNUM, false);
		log_smgrcreate(newrnode, INIT_FORKNUM);
		smgrimmedsync(srel, INIT_FORKNUM);
	}
}

static void
zedstoream_relation_nontransactional_truncate(Relation rel)
{
	/* XXX: I think we could just throw away all data in the buffer */
	zsbt_tuplebuffer_flush(rel);
	zsmeta_invalidate_cache(rel);
	RelationTruncate(rel, 0);
}

static void
zedstoream_relation_copy_data(Relation rel, const RelFileNode *newrnode)
{
	SMgrRelation dstrel;

	zsbt_tuplebuffer_flush(rel);

	dstrel = smgropen(*newrnode, rel->rd_backend);
	RelationOpenSmgr(rel);

	/*
	 * Since we copy the file directly without looking at the shared buffers,
	 * we'd better first flush out any pages of the source relation that are
	 * in shared buffers.  We assume no new changes will be made while we are
	 * holding exclusive lock on the rel.
	 */
	FlushRelationBuffers(rel);

	/*
	 * Create and copy all the relation, and schedule unlinking of the
	 * old physical file.
	 *
	 * NOTE: any conflict in relfilenode value will be caught in
	 * RelationCreateStorage().
	 *
	 * NOTE: There is only the main fork in zedstore. Otherwise
	 * this would need to copy other forks, too.
	 */
	RelationCreateStorage(*newrnode, rel->rd_rel->relpersistence);

	/* copy main fork */
	RelationCopyStorage(rel->rd_smgr, dstrel, MAIN_FORKNUM,
						rel->rd_rel->relpersistence);

	/* drop old relation, and close new one */
	RelationDropStorage(rel);
	smgrclose(dstrel);
}

/*
 * Subroutine of the zedstoream_relation_copy_for_cluster() callback.
 *
 * Creates the TID item with correct visibility information for the
 * given tuple in the old table. Returns the tid of the tuple in the
 * new table, or InvalidZSTid if this tuple can be left out completely.
 *
 * FIXME: This breaks UPDATE chains. I.e. after this is done, an UPDATE
 * looks like DELETE + INSERT, instead of an UPDATE, to any transaction that
 * might try to follow the update chain.
 */
static zstid
zs_cluster_process_tuple(Relation OldHeap, Relation NewHeap,
						 zstid oldtid, ZSUndoRecPtr old_undoptr,
						 ZSUndoRecPtr recent_oldest_undo,
						 TransactionId OldestXmin)
{
	TransactionId this_xmin;
	CommandId this_cmin;
	TransactionId this_xmax;
	CommandId this_cmax;
	bool		this_changedPart;
	ZSUndoRecPtr undo_ptr;
	ZSUndoRec  *undorec;

	/*
	 * Follow the chain of UNDO records for this tuple, to find the
	 * transaction that originally inserted the row  (xmin/cmin), and
	 * the transaction that deleted or updated it away, if any (xmax/cmax)
	 */
	this_xmin = FrozenTransactionId;
	this_cmin = InvalidCommandId;
	this_xmax = InvalidTransactionId;
	this_cmax = InvalidCommandId;

	undo_ptr = old_undoptr;
	for (;;)
	{
		if (undo_ptr.counter < recent_oldest_undo.counter)
		{
			/* This tuple version is visible to everyone. */
			break;
		}

		/* Fetch the next UNDO record. */
		undorec = zsundo_fetch_record(OldHeap, undo_ptr);

		if (undorec->type == ZSUNDO_TYPE_INSERT)
		{
			if (!TransactionIdIsCurrentTransactionId(undorec->xid) &&
				!TransactionIdIsInProgress(undorec->xid) &&
				!TransactionIdDidCommit(undorec->xid))
			{
				/*
				 * inserter aborted or crashed. This row is not visible to
				 * anyone. Including any later tuple versions we might have
				 * seen.
				 */
				this_xmin = InvalidTransactionId;
				break;
			}
			else
			{
				/* Inserter committed. */
				this_xmin = undorec->xid;
				this_cmin = undorec->cid;

				/* we know everything there is to know about this tuple version. */
				break;
			}
		}
		else if (undorec->type == ZSUNDO_TYPE_TUPLE_LOCK)
		{
			/* Ignore tuple locks for now.
			 *
			 * FIXME: we should propagate them to the new copy of the table
			 */
			undo_ptr = undorec->prevundorec;
			continue;
		}
		else if (undorec->type == ZSUNDO_TYPE_DELETE ||
				undorec->type == ZSUNDO_TYPE_UPDATE)
		{
			/* Row was deleted (or updated away). */
			if (!TransactionIdIsCurrentTransactionId(undorec->xid) &&
				!TransactionIdIsInProgress(undorec->xid) &&
				!TransactionIdDidCommit(undorec->xid))
			{
				/* deleter aborted or crashed. The previous record should
				 * be an insertion (possibly with some tuple-locking in
				 * between). We'll remember the tuple when we see the
				 * insertion.
				 */
				undo_ptr = undorec->prevundorec;
				continue;
			}
			else
			{
				/* deleter committed or is still in progress. */
				if (TransactionIdPrecedes(undorec->xid, OldestXmin))
				{
					/* the deletion is visible to everyone. We can skip the row completely. */
					this_xmin = InvalidTransactionId;
					break;
				}
				else
				{
					/* deleter committed or is in progress. Remember that it was
					 * deleted by this XID.
					 */
					this_xmax = undorec->xid;
					this_cmax = undorec->cid;
					if (undorec->type == ZSUNDO_TYPE_DELETE)
						this_changedPart = ((ZSUndoRec_Delete *) undorec)->changedPart;
					else
						this_changedPart = false;

					/* follow the UNDO chain to find information about the inserting
					 * transaction (xmin/cmin)
					 */
					undo_ptr = undorec->prevundorec;
					continue;
				}
			}
		}
	}

	/*
	 * We now know the visibility of this tuple. Re-create it in the new table.
	 */
	if (this_xmin != InvalidTransactionId)
	{
		/* Insert the first version of the row. */
		zstid		newtid;

		/* First, insert the tuple. */
		newtid = zsbt_tid_multi_insert(NewHeap,
									   1,
									   this_xmin,
									   this_cmin,
									   INVALID_SPECULATIVE_TOKEN,
									   InvalidUndoPtr);

		/* And if the tuple was deleted/updated away, do the same in the new table. */
		if (this_xmax != InvalidTransactionId)
		{
			TM_Result	delete_result;
			bool		this_xact_has_lock;

			/* tuple was deleted. */
			delete_result = zsbt_tid_delete(NewHeap, newtid,
											this_xmax, this_cmax,
											NULL, NULL, false, NULL, this_changedPart,
											&this_xact_has_lock);
			if (delete_result != TM_Ok)
				elog(ERROR, "tuple deletion failed during table rewrite");
		}
		return newtid;
	}
	else
		return InvalidZSTid;
}


static void
zedstoream_relation_copy_for_cluster(Relation OldHeap, Relation NewHeap,
									 Relation OldIndex, bool use_sort,
									 TransactionId OldestXmin,
									 TransactionId *xid_cutoff,
									 MultiXactId *multi_cutoff,
									 double *num_tuples,
									 double *tups_vacuumed,
									 double *tups_recently_dead)
{
	TupleDesc	olddesc;
	ZSTidTreeScan tid_scan;
	ZSAttrTreeScan *attr_scans;
	ZSUndoRecPtr recent_oldest_undo = zsundo_get_oldest_undo_ptr(OldHeap, true);
	int			attno;
	IndexScanDesc indexScan;
	Datum	   *newdatums;
	bool       *newisnulls;

	zsbt_tuplebuffer_flush(OldHeap);

	olddesc = RelationGetDescr(OldHeap),

	attr_scans = palloc(olddesc->natts * sizeof(ZSAttrTreeScan));

	/*
	 * Scan the old table. We ignore any old updated-away tuple versions,
	 * and only stop at the latest tuple version of each row. At the latest
	 * version, follow the update chain to get all the old versions of that
	 * row, too. That way, the whole update chain is processed in one go,
	 * and can be reproduced in the new table.
	 */
	zsbt_tid_begin_scan(OldHeap, MinZSTid, MaxPlusOneZSTid,
						SnapshotAny, &tid_scan);

	for (attno = 1; attno <= olddesc->natts; attno++)
	{
		if (TupleDescAttr(olddesc, attno - 1)->attisdropped)
			continue;

		zsbt_attr_begin_scan(OldHeap,
							 olddesc,
							 attno,
							 &attr_scans[attno - 1]);
	}

	newdatums = palloc(olddesc->natts * sizeof(Datum));
	newisnulls = palloc(olddesc->natts * sizeof(bool));

	/* TODO: sorting not implemented yet. (it would require materializing each
	 * row into a HeapTuple or something like that, which could carry the xmin/xmax
	 * information through the sorter).
	 */
	use_sort = false;

	/*
	 * Prepare to scan the OldHeap.  To ensure we see recently-dead tuples
	 * that still need to be copied, we scan with SnapshotAny and use
	 * HeapTupleSatisfiesVacuum for the visibility test.
	 */
	if (OldIndex != NULL && !use_sort)
	{
		const int	ci_index[] = {
			PROGRESS_CLUSTER_PHASE,
			PROGRESS_CLUSTER_INDEX_RELID
		};
		int64		ci_val[2];

		/* Set phase and OIDOldIndex to columns */
		ci_val[0] = PROGRESS_CLUSTER_PHASE_INDEX_SCAN_HEAP;
		ci_val[1] = RelationGetRelid(OldIndex);
		pgstat_progress_update_multi_param(2, ci_index, ci_val);

		indexScan = index_beginscan(OldHeap, OldIndex, SnapshotAny, 0, 0);
		index_rescan(indexScan, NULL, 0, NULL, 0);
	}
	else
	{
		/* In scan-and-sort mode and also VACUUM FULL, set phase */
		pgstat_progress_update_param(PROGRESS_CLUSTER_PHASE,
									 PROGRESS_CLUSTER_PHASE_SEQ_SCAN_HEAP);

		indexScan = NULL;

		/* Set total heap blocks */
		/* TODO */
#if 0
		pgstat_progress_update_param(PROGRESS_CLUSTER_TOTAL_HEAP_BLKS,
									 heapScan->rs_nblocks);
#endif
	}

	for (;;)
	{
		zstid		old_tid;
		ZSUndoRecPtr old_undoptr;
		zstid		new_tid;
		zstid		fetchtid = InvalidZSTid;

		CHECK_FOR_INTERRUPTS();

		if (indexScan != NULL)
		{
			ItemPointer itemptr;

			itemptr = index_getnext_tid(indexScan, ForwardScanDirection);
			if (!itemptr)
				break;

			/* Since we used no scan keys, should never need to recheck */
			if (indexScan->xs_recheck)
				elog(ERROR, "CLUSTER does not support lossy index conditions");

			fetchtid = ZSTidFromItemPointer(*itemptr);
			zsbt_tid_reset_scan(&tid_scan, MinZSTid, MaxPlusOneZSTid, fetchtid - 1);
			old_tid = zsbt_tid_scan_next(&tid_scan, ForwardScanDirection);
			if (old_tid == InvalidZSTid)
				continue;
		}
		else
		{
			old_tid = zsbt_tid_scan_next(&tid_scan, ForwardScanDirection);
			if (old_tid == InvalidZSTid)
				break;
			fetchtid = old_tid;
		}
		if (old_tid != fetchtid)
			continue;

		old_undoptr = tid_scan.array_iter.visi_infos[ZSTidScanCurUndoSlotNo(&tid_scan)].undoptr;

		new_tid = zs_cluster_process_tuple(OldHeap, NewHeap,
										   old_tid, old_undoptr,
										   recent_oldest_undo,
										   OldestXmin);
		if (new_tid != InvalidZSTid)
		{
			/* Fetch the attributes and write them out */
			for (attno = 1; attno <= olddesc->natts; attno++)
			{
				Form_pg_attribute att = TupleDescAttr(olddesc, attno - 1);
				Datum		datum;
				bool		isnull;

				if (att->attisdropped)
				{
					datum = (Datum) 0;
					isnull = true;
				}
				else
				{
					if (!zsbt_attr_fetch(&attr_scans[attno - 1], &datum, &isnull, old_tid))
						zsbt_fill_missing_attribute_value(olddesc, attno, &datum, &isnull);
				}

				/* flatten and re-toast any ZS-TOASTed values */
				if (!isnull && att->attlen == -1)
				{
					if (VARATT_IS_EXTERNAL(datum) && VARTAG_EXTERNAL(datum) == VARTAG_ZEDSTORE)
					{
						datum = zedstore_toast_flatten(OldHeap, attno, old_tid, datum);
					}
				}
				newdatums[attno - 1] = datum;
				newisnulls[attno - 1] = isnull;
			}

			zsbt_tuplebuffer_spool_tuple(NewHeap, new_tid, newdatums, newisnulls);
		}
	}

	if (indexScan != NULL)
		index_endscan(indexScan);

	zsbt_tid_end_scan(&tid_scan);
	for (attno = 1; attno <= olddesc->natts; attno++)
	{
		if (TupleDescAttr(olddesc, attno - 1)->attisdropped)
			continue;

		zsbt_attr_end_scan(&attr_scans[attno - 1]);
	}

	zsbt_tuplebuffer_flush(NewHeap);
}

static void 
zedstoream_scan_analyze_beginscan(Relation onerel, AnalyzeSampleContext *context)
{
	zstid 	tid;
	List	*va_cols = context->anl_cols;		
	Bitmapset	*project_columns = NULL;	

	/* zedstore can sample rows on specified columns only */
	if (!va_cols)
		context->scan = table_beginscan_analyze(onerel);
	else
	{
		ListCell	*le;

		foreach(le, va_cols)
		{
			char	   *col = strVal(lfirst(le));

			project_columns =
				bms_add_member(project_columns, attnameAttNum(onerel, col, false));
		}

		context->scan = 
			zedstoream_beginscan_with_column_projection(onerel, NULL, 0, NULL,
														NULL, SO_TYPE_ANALYZE,
														project_columns);
	}

	/* zedstore use a logical block number to acquire sample rows */
	tid = zsbt_get_last_tid(onerel);
	context->totalblocks = ZSTidGetBlockNumber(tid) + 1;
}

/*
 * Get next logical block.
 */
static bool
zedstoream_scan_analyze_next_block(BlockNumber blockno,
								   AnalyzeSampleContext *context)
{
	return zs_blkscan_next_block(context->scan, blockno, NULL, -1, false);
}

static bool
zedstoream_scan_analyze_next_tuple(TransactionId OldestXmin, AnalyzeSampleContext *context)
{
	int		i;
	bool	result;
	AttrNumber		attno;
	TableScanDesc	scan = context->scan;
	ZedStoreDesc	sscan = (ZedStoreDesc) scan;
	ZSAttrTreeScan	*attr_scan;
	TupleTableSlot	*slot = AnalyzeGetSampleSlot(context, scan->rs_rd, ANALYZE_SAMPLE_DATA);

	result = zs_blkscan_next_tuple(scan, slot);

	if (result)
	{
		/* provide extra disk info when analyzing on full columns */
		if (!context->anl_cols)
		{
			slot = AnalyzeGetSampleSlot(context, scan->rs_rd, ANALYZE_SAMPLE_DISKSIZE);

			ExecClearTuple(slot);

			for (i = 0; i < scan->rs_rd->rd_att->natts; i++)
				slot->tts_isnull[i] = true;

			for (i = 1; i < sscan->proj_data.num_proj_atts; i++)
			{
				attr_scan = &sscan->proj_data.attr_scans[i - 1];	
				attno = sscan->proj_data.proj_atts[i];

				slot->tts_values[attno - 1] =
					Float8GetDatum(attr_scan->decoder.avg_elements_size); 
				slot->tts_isnull[attno - 1] = false;
			}

			slot->tts_flags &= ~TTS_FLAG_EMPTY;
		}

		context->liverows++;
	}

	return result;
}

static void
zedstoream_scan_analyze_sample_tuple(int pos, bool replace, AnalyzeSampleContext *context)
{
	TupleTableSlot *slot;
	Relation onerel = context->scan->rs_rd;

	slot = AnalyzeGetSampleSlot(context, onerel, ANALYZE_SAMPLE_DATA);
	AnalyzeRecordSampleRow(context, slot, NULL, ANALYZE_SAMPLE_DATA, pos, replace, false);

	/* only record */
	if (!context->anl_cols)
	{
		slot = AnalyzeGetSampleSlot(context, onerel, ANALYZE_SAMPLE_DISKSIZE);
		AnalyzeRecordSampleRow(context, slot, NULL, ANALYZE_SAMPLE_DISKSIZE, pos, replace, false);
	}
}

static void
zedstoream_scan_analyze_endscan(AnalyzeSampleContext *context)
{
	table_endscan(context->scan);
}

/* ------------------------------------------------------------------------
 * Miscellaneous callbacks for the heap AM
 * ------------------------------------------------------------------------
 */

/*
 * FIXME: Implement this function as best for zedstore. The return value is
 * for example leveraged by analyze to find which blocks to sample.
 */
static uint64
zedstoream_relation_size(Relation rel, ForkNumber forkNumber)
{
	uint64		nblocks = 0;

	/* Open it at the smgr level if not already done */
	RelationOpenSmgr(rel);
	nblocks = smgrnblocks(rel->rd_smgr, MAIN_FORKNUM);
	return nblocks * BLCKSZ;
}

/*
 * Zedstore stores TOAST chunks within the table file itself. Hence, doesn't
 * need separate toast table to be created. Return false for this callback
 * avoids creation of toast table.
 */
static bool
zedstoream_relation_needs_toast_table(Relation rel)
{
	return false;
}

/* ------------------------------------------------------------------------
 * Planner related callbacks for the zedstore AM
 * ------------------------------------------------------------------------
 */

/*
 * currently this is exact duplicate of heapam_estimate_rel_size().
 * TODO fix to tune it based on zedstore storage.
 */
static void
zedstoream_relation_estimate_size(Relation rel, int32 *attr_widths,
								  BlockNumber *pages, double *tuples,
								  double *allvisfrac)
{
	BlockNumber curpages;
	BlockNumber relpages;
	double		reltuples;
	BlockNumber relallvisible;
	double		density;

	/* it has storage, ok to call the smgr */
	curpages = RelationGetNumberOfBlocks(rel);

	/* coerce values in pg_class to more desirable types */
	relpages = (BlockNumber) rel->rd_rel->relpages;
	reltuples = (double) rel->rd_rel->reltuples;
	relallvisible = (BlockNumber) rel->rd_rel->relallvisible;

	/*
	 * HACK: if the relation has never yet been vacuumed, use a minimum size
	 * estimate of 10 pages.  The idea here is to avoid assuming a
	 * newly-created table is really small, even if it currently is, because
	 * that may not be true once some data gets loaded into it.  Once a vacuum
	 * or analyze cycle has been done on it, it's more reasonable to believe
	 * the size is somewhat stable.
	 *
	 * (Note that this is only an issue if the plan gets cached and used again
	 * after the table has been filled.  What we're trying to avoid is using a
	 * nestloop-type plan on a table that has grown substantially since the
	 * plan was made.  Normally, autovacuum/autoanalyze will occur once enough
	 * inserts have happened and cause cached-plan invalidation; but that
	 * doesn't happen instantaneously, and it won't happen at all for cases
	 * such as temporary tables.)
	 *
	 * We approximate "never vacuumed" by "has relpages = 0", which means this
	 * will also fire on genuinely empty relations.  Not great, but
	 * fortunately that's a seldom-seen case in the real world, and it
	 * shouldn't degrade the quality of the plan too much anyway to err in
	 * this direction.
	 *
	 * If the table has inheritance children, we don't apply this heuristic.
	 * Totally empty parent tables are quite common, so we should be willing
	 * to believe that they are empty.
	 */
	if (curpages < 10 &&
		relpages == 0 &&
		!rel->rd_rel->relhassubclass)
		curpages = 10;

	/* report estimated # pages */
	*pages = curpages;
	/* quick exit if rel is clearly empty */
	if (curpages == 0)
	{
		*tuples = 0;
		*allvisfrac = 0;
		return;
	}

	/* estimate number of tuples from previous tuple density */
	if (relpages > 0)
		density = reltuples / (double) relpages;
	else
	{
		/*
		 * When we have no data because the relation was truncated, estimate
		 * tuple width from attribute datatypes.  We assume here that the
		 * pages are completely full, which is OK for tables (since they've
		 * presumably not been VACUUMed yet) but is probably an overestimate
		 * for indexes.  Fortunately get_relation_info() can clamp the
		 * overestimate to the parent table's size.
		 *
		 * Note: this code intentionally disregards alignment considerations,
		 * because (a) that would be gilding the lily considering how crude
		 * the estimate is, and (b) it creates platform dependencies in the
		 * default plans which are kind of a headache for regression testing.
		 */
		int32		tuple_width;

		tuple_width = get_rel_data_width(rel, attr_widths);
		tuple_width += MAXALIGN(SizeofHeapTupleHeader);
		tuple_width += sizeof(ItemIdData);
		/* note: integer division is intentional here */
		density = (BLCKSZ - SizeOfPageHeaderData) / tuple_width;
	}
	*tuples = rint(density * (double) curpages);

	/*
	 * We use relallvisible as-is, rather than scaling it up like we do for
	 * the pages and tuples counts, on the theory that any pages added since
	 * the last VACUUM are most likely not marked all-visible.  But costsize.c
	 * wants it converted to a fraction.
	 */
	if (relallvisible == 0 || curpages <= 0)
		*allvisfrac = 0;
	else if ((double) relallvisible >= curpages)
		*allvisfrac = 1;
	else
		*allvisfrac = (double) relallvisible / curpages;
}

/* ------------------------------------------------------------------------
 * Executor related callbacks for the zedstore AM
 * ------------------------------------------------------------------------
 */

/*
 * zs_blkscan_next_block() and zs_blkscan_next_tuple() are used to implement
 * bitmap scans, and sample scans. The tableam interface for those are similar
 * enough that they can share most code.
 */
static bool
zs_blkscan_next_block(TableScanDesc sscan,
					  BlockNumber blkno, OffsetNumber *offsets, int noffsets,
					  bool predicatelocks)
{
	ZedStoreDesc scan = (ZedStoreDesc) sscan;
	ZedStoreProjectData *scan_proj = &scan->proj_data;
	int			ntuples;
	zstid		tid;
	int			idx;

	if (!scan->started)
	{
		Relation	rel = scan->rs_scan.rs_rd;
		TupleDesc	reldesc = RelationGetDescr(rel);
		MemoryContext oldcontext;

		zs_initialize_proj_attributes_extended(scan, reldesc);

		oldcontext = MemoryContextSwitchTo(scan_proj->context);
		zsbt_tid_begin_scan(rel,
							ZSTidFromBlkOff(blkno, 1),
							ZSTidFromBlkOff(blkno + 1, 1),
							scan->rs_scan.rs_snapshot,
							&scan_proj->tid_scan);
		scan_proj->tid_scan.serializable = true;
		for (int i = 1; i < scan_proj->num_proj_atts; i++)
		{
			int			attno = scan_proj->proj_atts[i];

			zsbt_attr_begin_scan(rel,  reldesc, attno,
								 &scan_proj->attr_scans[i - 1]);
		}
		MemoryContextSwitchTo(oldcontext);
		scan->started = true;
	}
	else
	{
		zsbt_tid_reset_scan(&scan_proj->tid_scan,
							ZSTidFromBlkOff(blkno, 1),
							ZSTidFromBlkOff(blkno + 1, 1),
							ZSTidFromBlkOff(blkno, 1) - 1);
	}

	/*
	 * Our strategy for a bitmap scan is to scan the TID tree in
	 * next_block() function, starting at the given logical block number, and
	 * store all the matching TIDs in in the scan struct. next_tuple() will
	 * fetch the attribute data from the attribute trees.
	 *
	 * TODO: it might be good to pass the next expected TID down to
	 * zsbt_tid_scan_next, so that it could skip over to the next match more
	 * efficiently.
	 */
	ntuples = 0;
	idx = 0;
	while ((tid = zsbt_tid_scan_next(&scan_proj->tid_scan, ForwardScanDirection)) != InvalidZSTid)
	{
		OffsetNumber off = ZSTidGetOffsetNumber(tid);
		ItemPointerData itemptr;

		Assert(ZSTidGetBlockNumber(tid) == blkno);

		ItemPointerSet(&itemptr, blkno, off);

		if (noffsets != -1)
		{
			while (off > offsets[idx] && idx < noffsets)
			{
				/*
				 * Acquire predicate lock on all tuples that we scan, even those that are
				 * not visible to the snapshot.
				 */
				if (predicatelocks)
					/*
					 * We pass in InvalidTransactionId as we are sure that the current
					 * transaction hasn't locked itemptr.
					 */
					PredicateLockTID(scan->rs_scan.rs_rd, &itemptr, scan->rs_scan.rs_snapshot, InvalidTransactionId);

				idx++;
			}

			if (idx == noffsets)
				break;

			if (off < offsets[idx])
				continue;
		}

		/* FIXME: heapam acquires the predicate lock first, and then
		 * calls CheckForSerializableConflictOut(). We do it in the
		 * opposite order, because CheckForSerializableConflictOut()
		 * call as done in zsbt_get_last_tid() already. Does it matter?
		 * I'm not sure.
		 */
		if (predicatelocks)
			/*
			 * We pass in InvalidTransactionId as we are sure that the current
			 * transaction hasn't locked itemptr.
			 */
			PredicateLockTID(scan->rs_scan.rs_rd, &itemptr, scan->rs_scan.rs_snapshot, InvalidTransactionId);

		scan->bmscan_tids[ntuples] = tid;
		ntuples++;
	}

	scan->bmscan_nexttuple = 0;
	scan->bmscan_ntuples = ntuples;

	return ntuples > 0;
}

static bool
zs_blkscan_next_tuple(TableScanDesc sscan, TupleTableSlot *slot)
{
	ZedStoreDesc scan = (ZedStoreDesc) sscan;
	zstid		tid;

	if (scan->bmscan_nexttuple >= scan->bmscan_ntuples)
		return false;

	/*
	 * Initialize the slot.
	 *
	 * We initialize all columns to NULL. The values for columns that are projected
	 * will be set to the actual values below, but it's important that non-projected
	 * columns are NULL.
	 */
	ExecClearTuple(slot);
	for (int i = 0; i < sscan->rs_rd->rd_att->natts; i++)
		slot->tts_isnull[i] = true;

	/*
	 * projection attributes were created based on Relation tuple descriptor
	 * it better match TupleTableSlot.
	 */
	Assert((scan->proj_data.num_proj_atts - 1) <= slot->tts_tupleDescriptor->natts);
	tid = scan->bmscan_tids[scan->bmscan_nexttuple];
	for (int i = 1; i < scan->proj_data.num_proj_atts; i++)
	{
		ZSAttrTreeScan *attr_scan = &scan->proj_data.attr_scans[i - 1];
		AttrNumber	attno = scan->proj_data.proj_atts[i];
		Form_pg_attribute att = TupleDescAttr(slot->tts_tupleDescriptor, attno - 1);
		Datum		datum;
		bool        isnull;

		if (!zsbt_attr_fetch(attr_scan, &datum, &isnull, tid))
			zsbt_fill_missing_attribute_value(slot->tts_tupleDescriptor, attno, &datum, &isnull);

		/*
		 * flatten any ZS-TOASTed values, because the rest of the system
		 * doesn't know how to deal with them.
		 */
		if (!isnull && att->attlen == -1 &&
			VARATT_IS_EXTERNAL(datum) && VARTAG_EXTERNAL(datum) == VARTAG_ZEDSTORE)
		{
			datum = zedstore_toast_flatten(scan->rs_scan.rs_rd, attno, tid, datum);
		}

		Assert(attno > 0);
		slot->tts_values[attno - 1] = datum;
		slot->tts_isnull[attno - 1] = isnull;
	}

	/* FIXME: Don't we need to set visi_info, like in a seqscan? */
	slot->tts_tableOid = RelationGetRelid(scan->rs_scan.rs_rd);
	slot->tts_tid = ItemPointerFromZSTid(tid);
	slot->tts_nvalid = slot->tts_tupleDescriptor->natts;
	slot->tts_flags &= ~TTS_FLAG_EMPTY;

	scan->bmscan_nexttuple++;

	pgstat_count_heap_fetch(scan->rs_scan.rs_rd);

	return true;
}



static bool
zedstoream_scan_bitmap_next_block(TableScanDesc sscan,
								  TBMIterateResult *tbmres)
{
	return zs_blkscan_next_block(sscan, tbmres->blockno, tbmres->offsets, tbmres->ntuples, true);
}

static bool
zedstoream_scan_bitmap_next_tuple(TableScanDesc sscan,
								  TBMIterateResult *tbmres,
								  TupleTableSlot *slot)
{
	return zs_blkscan_next_tuple(sscan, slot);
}

static bool
zedstoream_scan_sample_next_block(TableScanDesc sscan, SampleScanState *scanstate)
{
	ZedStoreDesc scan = (ZedStoreDesc) sscan;
	TsmRoutine *tsm = scanstate->tsmroutine;
	BlockNumber blockno;

	if (scan->next_tid_to_scan == InvalidZSTid)
	{
		/* initialize next tid with the first tid */
		scan->next_tid_to_scan = zsbt_get_first_tid(scan->rs_scan.rs_rd);
	}

	if (scan->max_tid_to_scan == InvalidZSTid)
	{
		/*
		 * get the max tid once and store it, used to calculate max blocks to
		 * scan either for SYSTEM or BERNOULLI sampling.
		 */
		scan->max_tid_to_scan = zsbt_get_last_tid(scan->rs_scan.rs_rd);
	}

	if (tsm->NextSampleBlock)
	{
		/* Adding one below to convert block number to number of blocks. */
		blockno = tsm->NextSampleBlock(scanstate,
									   ZSTidGetBlockNumber(scan->max_tid_to_scan) + 1);

		if (!BlockNumberIsValid(blockno))
			return false;
	}
	else
	{
		/* scanning table sequentially */
		if (scan->next_tid_to_scan > scan->max_tid_to_scan)
			return false;

		blockno = ZSTidGetBlockNumber(scan->next_tid_to_scan);
		/* move on to next block of tids for next iteration of scan */
		scan->next_tid_to_scan = ZSTidFromBlkOff(blockno + 1, 1);
	}

	Assert(BlockNumberIsValid(blockno));

	/*
	 * Fetch all TIDs on the page.
	 */
	if (!zs_blkscan_next_block(sscan, blockno, NULL, -1, false))
		return false;

	/*
	 * Filter the list of TIDs, keeping only the TIDs that the sampling methods
	 * tells us to keep.
	 */
	if (scan->bmscan_ntuples > 0)
	{
		zstid		lasttid_for_block = scan->bmscan_tids[scan->bmscan_ntuples - 1];
		OffsetNumber maxoffset = ZSTidGetOffsetNumber(lasttid_for_block);
		OffsetNumber nextoffset;
		int			outtuples;
		int			idx;

		/* ask the tablesample method which tuples to check on this page. */
		nextoffset = tsm->NextSampleTuple(scanstate, blockno, maxoffset);

		outtuples = 0;
		idx = 0;
		while (idx < scan->bmscan_ntuples && OffsetNumberIsValid(nextoffset))
		{
			zstid		thistid = scan->bmscan_tids[idx];
			OffsetNumber thisoffset = ZSTidGetOffsetNumber(thistid);

			if (thisoffset > nextoffset)
				nextoffset = tsm->NextSampleTuple(scanstate, blockno, maxoffset);
			else
			{
				if (thisoffset == nextoffset)
					scan->bmscan_tids[outtuples++] = thistid;
				idx++;
			}
		}
		scan->bmscan_ntuples = outtuples;

		/*
		 * Must fast forward the sampler through all offsets on this page,
		 * until it returns InvalidOffsetNumber. Otherwise, the next
		 * call will continue to return offsets for this block.
		 *
		 * FIXME: It seems bogus that the sampler isn't reset, when you call
		 * NextSampleBlock(). Perhaps we should fix this in the TSM API?
		 */
		while (OffsetNumberIsValid(nextoffset))
			nextoffset = tsm->NextSampleTuple(scanstate, blockno, maxoffset);
	}

	return scan->bmscan_ntuples > 0;
}

static bool
zedstoream_scan_sample_next_tuple(TableScanDesc sscan, SampleScanState *scanstate,
								  TupleTableSlot *slot)
{
	/*
	 * We already filtered the rows in the next_block() function, so all TIDs in
	 * in scan->bmscan_tids belong to the sample.
	 */
	return zs_blkscan_next_tuple(sscan, slot);
}

static void
zedstoream_vacuum_rel(Relation onerel, VacuumParams *params,
					  BufferAccessStrategy bstrategy)
{
	zsbt_tuplebuffer_flush(onerel);
	zsundo_vacuum(onerel, params, bstrategy,
				  GetOldestXmin(onerel, PROCARRAY_FLAGS_VACUUM));
}

static const TableAmRoutine zedstoream_methods = {
	.type = T_TableAmRoutine,
	.scans_leverage_column_projection = true,

	.slot_callbacks = zedstoream_slot_callbacks,

	.scan_begin = zedstoream_beginscan,
	.scan_begin_with_column_projection = zedstoream_beginscan_with_column_projection,
	.scan_end = zedstoream_endscan,
	.scan_rescan = zedstoream_rescan,
	.scan_getnextslot = zedstoream_getnextslot,

	.parallelscan_estimate = zs_parallelscan_estimate,
	.parallelscan_initialize = zs_parallelscan_initialize,
	.parallelscan_reinitialize = zs_parallelscan_reinitialize,

	.index_fetch_begin = zedstoream_begin_index_fetch,
	.index_fetch_reset = zedstoream_reset_index_fetch,
	.index_fetch_end = zedstoream_end_index_fetch,
	.index_fetch_set_column_projection = zedstoream_fetch_set_column_projection,
	.index_fetch_tuple = zedstoream_index_fetch_tuple,

	.tuple_insert = zedstoream_insert,
	.tuple_insert_speculative = zedstoream_insert_speculative,
	.tuple_complete_speculative = zedstoream_complete_speculative,
	.multi_insert = zedstoream_multi_insert,
	.tuple_delete = zedstoream_delete,
	.tuple_update = zedstoream_update,
	.tuple_lock = zedstoream_lock_tuple,
	.finish_bulk_insert = zedstoream_finish_bulk_insert,

	.tuple_fetch_row_version = zedstoream_fetch_row_version,
	.tuple_get_latest_tid = zedstoream_get_latest_tid,
	.tuple_tid_valid = zedstoream_tuple_tid_valid,
	.tuple_satisfies_snapshot = zedstoream_tuple_satisfies_snapshot,
	.compute_xid_horizon_for_tuples = zedstoream_compute_xid_horizon_for_tuples,

	.relation_set_new_filenode = zedstoream_relation_set_new_filenode,
	.relation_nontransactional_truncate = zedstoream_relation_nontransactional_truncate,
	.relation_copy_data = zedstoream_relation_copy_data,
	.relation_copy_for_cluster = zedstoream_relation_copy_for_cluster,
	.relation_vacuum = zedstoream_vacuum_rel,
	.scan_analyze_beginscan = zedstoream_scan_analyze_beginscan,
	.scan_analyze_next_block = zedstoream_scan_analyze_next_block,
	.scan_analyze_next_tuple = zedstoream_scan_analyze_next_tuple,
	.scan_analyze_sample_tuple = zedstoream_scan_analyze_sample_tuple,
	.scan_analyze_endscan = zedstoream_scan_analyze_endscan,

	.index_build_range_scan = zedstoream_index_build_range_scan,
	.index_validate_scan = zedstoream_index_validate_scan,

	.relation_size = zedstoream_relation_size,
	.relation_needs_toast_table = zedstoream_relation_needs_toast_table,
	.relation_estimate_size = zedstoream_relation_estimate_size,

	.scan_bitmap_next_block = zedstoream_scan_bitmap_next_block,
	.scan_bitmap_next_tuple = zedstoream_scan_bitmap_next_tuple,
	.scan_sample_next_block = zedstoream_scan_sample_next_block,
	.scan_sample_next_tuple = zedstoream_scan_sample_next_tuple
};

Datum
zedstore_tableam_handler(PG_FUNCTION_ARGS)
{
	PG_RETURN_POINTER(&zedstoream_methods);
}


/*
 * Routines for dividing up the TID range for parallel seq scans
 */

/*
 * Number of TIDs to assign to a parallel worker in a parallel Seq Scan in
 * one batch.
 *
 * Not sure what the optimimum would be. If the chunk size is too small,
 * the parallel workers will waste effort, when two parallel workers both
 * need to decompress and process the pages at the boundary. But on the
 * other hand, if the chunk size is too large, we might not be able to make
 * good use of all the parallel workers.
 */
#define ZS_PARALLEL_CHUNK_SIZE	((uint64) 0x100000)

typedef struct ParallelZSScanDescData
{
	ParallelTableScanDescData base;

	zstid		pzs_endtid;		/* last tid + 1 in relation at start of scan */
	pg_atomic_uint64 pzs_allocatedtids;	/* TID space allocated to workers so far. */
} ParallelZSScanDescData;
typedef struct ParallelZSScanDescData *ParallelZSScanDesc;

static Size
zs_parallelscan_estimate(Relation rel)
{
	return sizeof(ParallelZSScanDescData);
}

static Size
zs_parallelscan_initialize(Relation rel, ParallelTableScanDesc pscan)
{
	ParallelZSScanDesc zpscan = (ParallelZSScanDesc) pscan;

	zpscan->base.phs_relid = RelationGetRelid(rel);
	zpscan->pzs_endtid = zsbt_get_last_tid(rel);
	pg_atomic_init_u64(&zpscan->pzs_allocatedtids, 1);

	return sizeof(ParallelZSScanDescData);
}

static void
zs_parallelscan_reinitialize(Relation rel, ParallelTableScanDesc pscan)
{
	ParallelZSScanDesc bpscan = (ParallelZSScanDesc) pscan;

	pg_atomic_write_u64(&bpscan->pzs_allocatedtids, 1);
}

/*
 * get the next TID range to scan
 *
 * Returns true if there is more to scan, false otherwise.
 *
 * Get the next TID range to scan.  Even if there are no TIDs left to scan,
 * another backend could have grabbed a range to scan and not yet finished
 * looking at it, so it doesn't follow that the scan is done when the first
 * backend gets 'false' return.
 */
static bool
zs_parallelscan_nextrange(Relation rel, ParallelZSScanDesc pzscan,
						  zstid *start, zstid *end)
{
	uint64		allocatedtids;

	/*
	 * zhs_allocatedtid tracks how much has been allocated to workers
	 * already.  When phs_allocatedtid >= rs_lasttid, all TIDs have been
	 * allocated.
	 *
	 * Because we use an atomic fetch-and-add to fetch the current value, the
	 * phs_allocatedtid counter will exceed rs_lasttid, because workers will
	 * still increment the value, when they try to allocate the next block but
	 * all blocks have been allocated already. The counter must be 64 bits
	 * wide because of that, to avoid wrapping around when rs_lasttid is close
	 * to 2^32.  That's also one reason we do this at granularity of 2^16 TIDs,
	 * even though zedstore isn't block-oriented.
	 */
	allocatedtids = pg_atomic_fetch_add_u64(&pzscan->pzs_allocatedtids, ZS_PARALLEL_CHUNK_SIZE);
	*start = (zstid) allocatedtids;
	*end = (zstid) (allocatedtids + ZS_PARALLEL_CHUNK_SIZE);

	return *start < pzscan->pzs_endtid;
}

/*
 * Get the value for a row, when no value has been stored in the attribute tree.
 *
 * This is used after ALTER TABLE ADD COLUMN, when reading rows that were
 * created before column was added. Usually, missing values are implicitly
 * NULLs, but you could specify a different value in the ALTER TABLE command,
 * too, with DEFAULT.
 */
static void
zsbt_fill_missing_attribute_value(TupleDesc tupleDesc, int attno, Datum *datum, bool *isnull)
{
	Form_pg_attribute attr = TupleDescAttr(tupleDesc, attno - 1);

	*isnull = true;
	*datum = (Datum) 0;

	/* This means catalog doesn't have the default value for this attribute */
	if (!attr->atthasmissing)
		return;

	if (tupleDesc->constr &&
		tupleDesc->constr->missing)
	{
		AttrMissing *attrmiss = NULL;
		/*
		 * If there are missing values we want to put them into the
		 * tuple.
		 */
		attrmiss = tupleDesc->constr->missing;

		if (attrmiss[attno - 1].am_present)
		{
			*isnull = false;
			if (attr->attbyval)
				*datum = fetch_att(&attrmiss[attno - 1].am_value, attr->attbyval, attr->attlen);
			else
				*datum = zs_datumCopy(attrmiss[attno - 1].am_value, attr->attbyval, attr->attlen);
		}
	}
}
