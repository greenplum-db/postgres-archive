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

#include "miscadmin.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/multixact.h"
#include "access/relscan.h"
#include "access/tableam.h"
#include "access/tsmapi.h"
#include "access/tupdesc_details.h"
#include "access/tuptoaster.h"
#include "access/xact.h"
#include "access/zedstore_internal.h"
#include "access/zedstore_undo.h"
#include "catalog/catalog.h"
#include "catalog/index.h"
#include "catalog/storage.h"
#include "catalog/storage_xlog.h"
#include "commands/progress.h"
#include "commands/vacuum.h"
#include "executor/executor.h"
#include "optimizer/plancat.h"
#include "pgstat.h"
#include "storage/lmgr.h"
#include "storage/predicate.h"
#include "storage/procarray.h"
#include "utils/builtins.h"
#include "utils/rel.h"


typedef enum
{
	ZSSCAN_STATE_UNSTARTED,
	ZSSCAN_STATE_SCANNING,
	ZSSCAN_STATE_FINISHED_RANGE,
	ZSSCAN_STATE_FINISHED
} zs_scan_state;

typedef struct ZedStoreProjectData
{
	int			num_proj_atts;
	bool       *project_columns;
	int		   *proj_atts;
	ZSBtreeScan *btree_scans;
	MemoryContext context;
}  ZedStoreProjectData;

typedef struct ZedStoreDescData
{
	/* scan parameters */
	TableScanDescData rs_scan;  /* */
	ZedStoreProjectData proj_data;

	zs_scan_state state;
	zstid		cur_range_start;
	zstid		cur_range_end;
	bool		finished;

	/* These fields are used for bitmap scans, to hold a "block's" worth of data */
#define	MAX_ITEMS_PER_LOGICAL_BLOCK		MaxHeapTuplesPerPage
	int			bmscan_ntuples;
	zstid	   *bmscan_tids;
	Datum	  **bmscan_datums;
	bool	  **bmscan_isnulls;
	int			bmscan_nexttuple;

	/* These fields are use for TABLESAMPLE scans */
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
static void zedstoream_end_index_fetch(IndexFetchTableData *scan);
static bool zedstoream_fetch_row(ZedStoreIndexFetchData *fetch,
								 ItemPointer tid_p,
								 Snapshot snapshot,
								 TupleTableSlot *slot);

static Size zs_parallelscan_estimate(Relation rel);
static Size zs_parallelscan_initialize(Relation rel, ParallelTableScanDesc pscan);
static void zs_parallelscan_reinitialize(Relation rel, ParallelTableScanDesc pscan);
static bool zs_parallelscan_nextrange(Relation rel, ParallelZSScanDesc pzscan,
									  zstid *start, zstid *end);
static void zsbt_fill_missing_attribute_value(ZSBtreeScan *scan, Datum *datum, bool *isnull);

/* ----------------------------------------------------------------
 *				storage AM support routines for zedstoream
 * ----------------------------------------------------------------
 */

static bool
zedstoream_fetch_row_version(Relation rel,
							 ItemPointer tid_p,
							 Snapshot snapshot,
							 TupleTableSlot *slot)
{
	IndexFetchTableData *fetcher;
	bool		result;

	fetcher = zedstoream_begin_index_fetch(rel);

	result = zedstoream_fetch_row((ZedStoreIndexFetchData *) fetcher,
								  tid_p, snapshot, slot);
	if (result)
	{
		/* FIXME: heapam acquires the predicate lock first, and then
		 * calls CheckForSerializableConflictOut(). We do it in the
		 * opposite order, because CheckForSerializableConflictOut()
		 * call as done in zsbt_get_last_tid() already. Does it matter?
		 * I'm not sure.
		 */
		PredicateLockTID(rel, tid_p, snapshot);
	}
	ExecMaterializeSlot(slot);
	slot->tts_tableOid = RelationGetRelid(rel);
	slot->tts_tid = *tid_p;

	zedstoream_end_index_fetch(fetcher);

	return result;
}

static void
zedstoream_get_latest_tid(Relation relation,
							 Snapshot snapshot,
							 ItemPointer tid)
{
	zstid ztid = ZSTidFromItemPointer(*tid);
	zsbt_find_latest_tid(relation, &ztid, snapshot);
	*tid = ItemPointerFromZSTid(ztid);
}

static inline void
zedstoream_insert_internal(Relation relation, TupleTableSlot *slot, CommandId cid,
				  int options, struct BulkInsertStateData *bistate, uint32 speculative_token)
{
	AttrNumber	attno;
	Datum	   *d;
	bool	   *isnulls;
	zstid		tid;
	TransactionId xid = GetCurrentTransactionId();
	bool        isnull;
	Datum       datum;
	ZSUndoRecPtr prevundoptr;

	ZSUndoRecPtrInitialize(&prevundoptr);

	if (slot->tts_tupleDescriptor->natts != relation->rd_att->natts)
		elog(ERROR, "slot's attribute count doesn't match relcache entry");

	slot_getallattrs(slot);
	d = slot->tts_values;
	isnulls = slot->tts_isnull;

	tid = InvalidZSTid;

	isnull = true;
	ZSUndoRecPtrInitialize(&prevundoptr);
	zsbt_multi_insert(relation, ZS_META_ATTRIBUTE_NUM,
					  &datum, &isnull, &tid, 1,
					  xid, cid, speculative_token, prevundoptr);

	/*
	 * We only need to check for table-level SSI locks. Our
	 * new tuple can't possibly conflict with existing tuple locks, and
	 * page locks are only consolidated versions of tuple locks; they do not
	 * lock "gaps" as index page locks do.
	 */
	CheckForSerializableConflictIn(relation, NULL, InvalidBlockNumber);

	for (attno = 1; attno <= relation->rd_att->natts; attno++)
	{
		Form_pg_attribute attr = TupleDescAttr(slot->tts_tupleDescriptor, attno - 1);
		Datum		toastptr = (Datum) 0;
		datum = d[attno - 1];
		isnull = isnulls[attno - 1];

		if (!isnull && attr->attlen < 0 && VARATT_IS_EXTERNAL(datum))
			datum = PointerGetDatum(heap_tuple_fetch_attr((struct varlena *) DatumGetPointer(datum)));

		/* If this datum is too large, toast it */
		if (!isnull && attr->attlen < 0 &&
			VARSIZE_ANY_EXHDR(datum) > MaxZedStoreDatumSize)
		{
			toastptr = datum = zedstore_toast_datum(relation, attno, datum);
		}

		zsbt_multi_insert(relation, attno,
						  &datum, &isnull, &tid, 1,
						  xid, cid, INVALID_SPECULATIVE_TOKEN, prevundoptr);

		if (toastptr != (Datum) 0)
			zedstore_toast_finish(relation, attno, toastptr, tid);
	}

	slot->tts_tableOid = RelationGetRelid(relation);
	slot->tts_tid = ItemPointerFromZSTid(tid);

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
	zstid tid;

	tid = ZSTidFromItemPointer(slot->tts_tid);
	zsbt_clear_speculative_token(relation, tid, spekToken, true /* for complete */);
	/*
	 * there is a conflict
	 */
	if (succeeded)
		elog(ERROR, "zedstoream_complete_speculative succeeded case is not handled ");
}

static void
zedstoream_multi_insert(Relation relation, TupleTableSlot **slots, int ntuples,
						CommandId cid, int options, BulkInsertState bistate)
{
	AttrNumber	attno;
	int			i;
	bool		slotgetandset = true;
	TransactionId xid = GetCurrentTransactionId();
	int		   *tupletoasted;
	Datum	   *datums;
	bool	   *isnulls;
	zstid	   *tids;
	ZSUndoRecPtr prevundoptr;

	tupletoasted = palloc(ntuples * sizeof(int));
	datums = palloc0(ntuples * sizeof(Datum));
	isnulls = palloc(ntuples * sizeof(bool));
	tids = palloc0(ntuples * sizeof(zstid));

	for (i = 0; i < ntuples; i++)
		isnulls[i] = true;

	ZSUndoRecPtrInitialize(&prevundoptr);
	zsbt_multi_insert(relation, ZS_META_ATTRIBUTE_NUM,
					  datums, isnulls, tids, ntuples,
					  xid, cid, INVALID_SPECULATIVE_TOKEN, prevundoptr);

	/*
	 * We only need to check for table-level SSI locks. Our
	 * new tuple can't possibly conflict with existing tuple locks, and
	 * page locks are only consolidated versions of tuple locks; they do not
	 * lock "gaps" as index page locks do.
	 */
	CheckForSerializableConflictIn(relation, NULL, InvalidBlockNumber);

	for (attno = 1; attno <= relation->rd_att->natts; attno++)
	{
		Form_pg_attribute attr = TupleDescAttr((slots[0])->tts_tupleDescriptor, attno - 1);
		int			ntupletoasted = 0;

		for (i = 0; i < ntuples; i++)
		{
			Datum		datum = slots[i]->tts_values[attno - 1];
			bool		isnull = slots[i]->tts_isnull[attno - 1];

			if (slotgetandset)
			{
				slot_getallattrs(slots[i]);
			}

			/* If this datum is too large, toast it */
			if (!isnull && attr->attlen < 0 &&
				VARSIZE_ANY_EXHDR(datum) > MaxZedStoreDatumSize)
			{
				datum = zedstore_toast_datum(relation, attno, datum);
				tupletoasted[ntupletoasted++] = i;
			}
			datums[i] = datum;
			isnulls[i] = isnull;
		}

		zsbt_multi_insert(relation, attno,
						  datums, isnulls, tids, ntuples,
						  xid, cid, INVALID_SPECULATIVE_TOKEN, prevundoptr);

		for (i = 0; i < ntupletoasted; i++)
		{
			int		idx = tupletoasted[i];

			zedstore_toast_finish(relation, attno, datums[idx], tids[idx]);
		}

		slotgetandset = false;
	}

	for (i = 0; i < ntuples; i++)
	{
		slots[i]->tts_tableOid = RelationGetRelid(relation);
		slots[i]->tts_tid = ItemPointerFromZSTid(tids[i]);
	}

	pgstat_count_heap_insert(relation, ntuples);

	pfree(tids);
	pfree(tupletoasted);
	pfree(datums);
	pfree(isnulls);
}

static TM_Result
zedstoream_delete(Relation relation, ItemPointer tid_p, CommandId cid,
				  Snapshot snapshot, Snapshot crosscheck, bool wait,
				  TM_FailureData *hufd, bool changingPart)
{
	zstid		tid = ZSTidFromItemPointer(*tid_p);
	TransactionId xid = GetCurrentTransactionId();
	TM_Result result = TM_Ok;

retry:
	result = zsbt_delete(relation, ZS_META_ATTRIBUTE_NUM, tid, xid, cid,
						 snapshot, crosscheck, wait, hufd, changingPart);

	if (result != TM_Ok)
	{
		if (result == TM_Invisible)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("attempted to delete invisible tuple")));
		else if (result == TM_BeingModified && wait)
		{
			TransactionId	xwait = hufd->xmax;

			/* TODO: use something like heap_acquire_tuplock() for priority */
			if (!TransactionIdIsCurrentTransactionId(xwait))
			{
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
	bool		have_tuple_lock = false;
	zstid		next_tid = tid;
	SnapshotData SnapshotDirty;
	bool		locked_something = false;

	slot->tts_tableOid = RelationGetRelid(relation);
	slot->tts_tid = *tid_p;

	tmfd->traversed = false;
	/*
	 * For now, we lock just the first attribute. As long as everyone
	 * does that, that's enough.
	 */
retry:
	result = zsbt_lock_item(relation, ZS_META_ATTRIBUTE_NUM /* attno */, tid, xid, cid,
							mode, snapshot, tmfd, &next_tid);

	if (result == TM_Invisible)
	{
		/*
		 * This is possible, but only when locking a tuple for ON CONFLICT
		 * UPDATE.  We return this value here rather than throwing an error in
		 * order to give that case the opportunity to throw a more specific
		 * error.
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
		 else
			 return TM_Invisible;
	}
	else if (result == TM_Updated ||
			 (result == TM_SelfModified && tmfd->cmax == cid))
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
		 * for the tuple.  We must do this even if we are share-locking.
		 *
		 * If we are forced to "start over" below, we keep the tuple lock;
		 * this arranges that we stay at the head of the line while
		 * rechecking tuple state.
		 */
		if (!zs_acquire_tuplock(relation, tid_p, mode, wait_policy,
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
		if (result == TM_Ok && tid != next_tid && next_tid != InvalidZSTid)
		{
			tid = next_tid;
			goto retry;
		}
	}

	/* Fetch the tuple, too. */
	if (!zedstoream_fetch_row_version(relation, tid_p, SnapshotAny, slot))
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
	AttrNumber	attno;
	bool		key_update;
	Datum	   *d;
	bool	   *isnulls;
	TM_Result	result;
	zstid		newtid;
	Datum       newdatum = 0;
	TupleTableSlot *oldslot;
	IndexFetchTableData *fetcher;
	ZSUndoRecPtr prevundoptr;

	ZSUndoRecPtrInitialize(&prevundoptr);

	*update_indexes = true;

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

	result = zsbt_update(relation, ZS_META_ATTRIBUTE_NUM, otid, newdatum, true,
						 xid, cid, key_update, snapshot, crosscheck,
						 wait, hufd, &newtid);

	if (result == TM_Ok)
	{
		/*
		 * Check for SSI conflicts.
		 */
		CheckForSerializableConflictIn(relation, otid_p, ItemPointerGetBlockNumber(otid_p));

		for (attno = 1; attno <= relation->rd_att->natts; attno++)
		{
			Form_pg_attribute attr = TupleDescAttr(relation->rd_att, attno - 1);
			Datum		newdatum = d[attno - 1];
			bool		newisnull = isnulls[attno - 1];
			Datum		toastptr = (Datum) 0;

			if (!newisnull && attr->attlen < 0 && VARATT_IS_EXTERNAL(newdatum))
				newdatum = PointerGetDatum(heap_tuple_fetch_attr((struct varlena *) DatumGetPointer(newdatum)));

			/* If this datum is too large, toast it */
			if (!newisnull && attr->attlen < 0 &&
				VARSIZE_ANY_EXHDR(newdatum) > MaxZedStoreDatumSize)
			{
				toastptr = newdatum = zedstore_toast_datum(relation, attno, newdatum);
			}

			zsbt_multi_insert(relation, attno,
							  &newdatum, &newisnull, &newtid, 1,
							  xid, cid, INVALID_SPECULATIVE_TOKEN, prevundoptr);

			if (toastptr != (Datum) 0)
				zedstore_toast_finish(relation, attno, toastptr, newtid);
		}

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

			/* TODO: use something like heap_acquire_tuplock() for priority */
			if (!TransactionIdIsCurrentTransactionId(xwait))
			{
				XactLockTableWait(xwait, relation, otid_p, XLTW_Delete);
				goto retry;
			}
		}
	}

	zedstoream_end_index_fetch(fetcher);
	ExecDropSingleTupleTableSlot(oldslot);

	return result;
}

static const TupleTableSlotOps *
zedstoream_slot_callbacks(Relation relation)
{
	return &TTSOpsZedstore;
}

static inline void
zs_initialize_proj_attributes(TupleDesc tupledesc, ZedStoreProjectData *proj_data)
{
	MemoryContext oldcontext;

	if (proj_data->num_proj_atts != 0)
		return;

	oldcontext = MemoryContextSwitchTo(proj_data->context);
	/* add one for meta-attribute */
	proj_data->proj_atts = palloc((tupledesc->natts + 1) * sizeof(int));
	proj_data->btree_scans = palloc0((tupledesc->natts + 1) * sizeof(ZSBtreeScan));

	proj_data->proj_atts[proj_data->num_proj_atts++] = ZS_META_ATTRIBUTE_NUM;

	/*
	 * convert booleans array into an array of the attribute numbers of the
	 * required columns.
	 */
	for (int idx = 0; idx < tupledesc->natts; idx++)
	{
		int att_no = idx + 1;

		/*
		 * never project dropped columns, null will be returned for them
		 * in slot by default.
		 */
		if  (TupleDescAttr(tupledesc, idx)->attisdropped)
			continue;

		/* project_columns empty also conveys need all the columns */
		if (proj_data->project_columns == NULL || proj_data->project_columns[idx])
			proj_data->proj_atts[proj_data->num_proj_atts++] = att_no;
	}

	MemoryContextSwitchTo(oldcontext);
}

static inline void
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
	if (scan->rs_scan.rs_bitmapscan || scan->rs_scan.rs_samplescan)
	{
		scan->bmscan_ntuples = 0;
		scan->bmscan_tids = palloc(MAX_ITEMS_PER_LOGICAL_BLOCK * sizeof(zstid));

		scan->bmscan_datums = palloc(proj_data->num_proj_atts * sizeof(Datum *));
		scan->bmscan_isnulls = palloc(proj_data->num_proj_atts * sizeof(bool *));
		for (int i = 0; i < proj_data->num_proj_atts; i++)
		{
			scan->bmscan_datums[i] = palloc(MAX_ITEMS_PER_LOGICAL_BLOCK * sizeof(Datum));
			scan->bmscan_isnulls[i] = palloc(MAX_ITEMS_PER_LOGICAL_BLOCK * sizeof(bool));
		}
	}
	MemoryContextSwitchTo(oldcontext);
}

static TableScanDesc
zedstoream_beginscan_with_column_projection(Relation relation, Snapshot snapshot,
											int nkeys, ScanKey key,
											ParallelTableScanDesc parallel_scan,
											bool *project_columns,
											bool allow_strat,
											bool allow_sync,
											bool allow_pagemode,
											bool is_bitmapscan,
											bool is_samplescan,
											bool temp_snap)
{
	ZedStoreDesc scan;

	/* Sample scans have no snapshot, but we need one */
	if (!snapshot)
	{
		Assert(is_samplescan);
		snapshot = SnapshotAny;
	}

	/*
	 * allocate and initialize scan descriptor
	 */
	scan = (ZedStoreDesc) palloc0(sizeof(ZedStoreDescData));

	scan->rs_scan.rs_rd = relation;
	scan->rs_scan.rs_snapshot = snapshot;
	scan->rs_scan.rs_nkeys = nkeys;
	scan->rs_scan.rs_bitmapscan = is_bitmapscan;
	scan->rs_scan.rs_samplescan = is_samplescan;
	scan->rs_scan.rs_allow_strat = allow_strat;
	scan->rs_scan.rs_allow_sync = allow_sync;
	scan->rs_scan.rs_temp_snap = temp_snap;
	scan->rs_scan.rs_parallel = parallel_scan;

	/*
	 * we can use page-at-a-time mode if it's an MVCC-safe snapshot
	 */
	scan->rs_scan.rs_pageatatime = allow_pagemode && snapshot && IsMVCCSnapshot(snapshot);
	scan->state = ZSSCAN_STATE_UNSTARTED;

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
	if (!is_bitmapscan)
		PredicateLockRelation(relation, snapshot);

	/*
	 * Currently, we don't have a stats counter for bitmap heap scans (but the
	 * underlying bitmap index scans will be counted) or sample scans (we only
	 * update stats for tuple fetches there)
	 */
	if (!is_bitmapscan && !is_samplescan)
		pgstat_count_heap_scan(relation);

	return (TableScanDesc) scan;
}

static TableScanDesc
zedstoream_beginscan(Relation relation, Snapshot snapshot,
					 int nkeys, ScanKey key,
					 ParallelTableScanDesc parallel_scan,
					 bool allow_strat,
					 bool allow_sync,
					 bool allow_pagemode,
					 bool is_bitmapscan,
					 bool is_samplescan,
					 bool temp_snap)
{
	return zedstoream_beginscan_with_column_projection(relation, snapshot, nkeys, key, parallel_scan,
													   NULL, allow_strat, allow_sync, allow_pagemode,
													   is_bitmapscan, is_samplescan, temp_snap);
}

static void
zedstoream_endscan(TableScanDesc sscan)
{
	ZedStoreDesc scan = (ZedStoreDesc) sscan;
	ZedStoreProjectData *proj_data = &scan->proj_data;

	if (proj_data->proj_atts)
		pfree(proj_data->proj_atts);

	for (int i = 0; i < proj_data->num_proj_atts; i++)
		zsbt_end_scan(&proj_data->btree_scans[i]);

	if (scan->rs_scan.rs_temp_snap)
		UnregisterSnapshot(scan->rs_scan.rs_snapshot);

	if (proj_data->btree_scans)
		pfree(proj_data->btree_scans);
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
		scan->rs_scan.rs_allow_strat = allow_strat;
		scan->rs_scan.rs_allow_sync = allow_sync;
		scan->rs_scan.rs_pageatatime =
			allow_pagemode && IsMVCCSnapshot(scan->rs_scan.rs_snapshot);
	}

	for (int i = 0; i < scan->proj_data.num_proj_atts; i++)
		zsbt_end_scan(&scan->proj_data.btree_scans[i]);

	scan->state = ZSSCAN_STATE_UNSTARTED;
}

static bool
zedstoream_getnextslot(TableScanDesc sscan, ScanDirection direction, TupleTableSlot *slot)
{
	ZedStoreDesc scan = (ZedStoreDesc) sscan;
	ZedStoreProjectData *scan_proj = &scan->proj_data;
	int			i;
	int			slot_natts = slot->tts_tupleDescriptor->natts;
	Datum	   *slot_values = slot->tts_values;
	bool	   *slot_isnull = slot->tts_isnull;

	if (direction != ForwardScanDirection)
		elog(ERROR, "backward scan not implemented in zedstore");

	zs_initialize_proj_attributes(slot->tts_tupleDescriptor, scan_proj);
	Assert((scan_proj->num_proj_atts - 1) <= slot_natts);

	/*
	 * Initialize the slot.
	 *
	 * We initialize all columns to NULL. The values for columns that are projected
	 * will be set to the actual values below, but it's important that non-projected
	 * columns are NULL.
	 */
	ExecClearTuple(slot);
	for (i = 0; i < slot_natts; i++)
		slot_isnull[i] = true;

	while (scan->state != ZSSCAN_STATE_FINISHED)
	{
		zstid		this_tid;
		Datum		datum;
		bool        isnull;

		if (scan->state == ZSSCAN_STATE_UNSTARTED ||
			scan->state == ZSSCAN_STATE_FINISHED_RANGE)
		{
			MemoryContext oldcontext;

			if (scan->rs_scan.rs_parallel)
			{
				/* Allocate next range of TIDs to scan */
				if (!zs_parallelscan_nextrange(scan->rs_scan.rs_rd,
											   (ParallelZSScanDesc) scan->rs_scan.rs_parallel,
											   &scan->cur_range_start, &scan->cur_range_end))
				{
					scan->state = ZSSCAN_STATE_FINISHED;
					break;
				}
			}
			else
			{
				if (scan->state == ZSSCAN_STATE_FINISHED_RANGE)
				{
					scan->state = ZSSCAN_STATE_FINISHED;
					break;
				}
				scan->cur_range_start = MinZSTid;
				scan->cur_range_end = MaxPlusOneZSTid;
			}

			oldcontext = MemoryContextSwitchTo(scan_proj->context);
			for (int i = 0; i < scan_proj->num_proj_atts; i++)
			{
				int			attno = scan_proj->proj_atts[i];

				zsbt_begin_scan(scan->rs_scan.rs_rd,
								slot->tts_tupleDescriptor,
								attno,
								scan->cur_range_start,
								scan->cur_range_end,
								scan->rs_scan.rs_snapshot,
								&scan_proj->btree_scans[i]);
			}
			scan_proj->btree_scans[0].serializable = true;
			MemoryContextSwitchTo(oldcontext);
			scan->state = ZSSCAN_STATE_SCANNING;
		}

		/* We now have a range to scan. Find the next visible TID. */
		Assert(scan->state == ZSSCAN_STATE_SCANNING);

		this_tid = zsbt_scan_next_tid(&scan_proj->btree_scans[0]);
		if (this_tid == InvalidZSTid)
		{
			scan->state = ZSSCAN_STATE_FINISHED_RANGE;
		}
		else
		{
			Assert (this_tid < scan->cur_range_end);

			/* Note: We don't need to predicate-lock tuples in Serializable mode,
			 * because in a sequential scan, we predicate-locked the whole table.
			 */

			/* Fetch the datums of each attribute for this row */
			for (int i = 1; i < scan_proj->num_proj_atts; i++)
			{
				ZSBtreeScan	*btscan = &scan_proj->btree_scans[i];
				Form_pg_attribute attr = ZSBtreeScanGetAttInfo(btscan);
				int			natt;

				if (!zsbt_scan_next_fetch(btscan, &datum, &isnull, this_tid))
					zsbt_fill_missing_attribute_value(btscan, &datum, &isnull);

				/*
				 * flatten any ZS-TOASTed values, because the rest of the system
				 * doesn't know how to deal with them.
				 */
				natt = scan_proj->proj_atts[i];

				if (!isnull && attr->attlen == -1 &&
					VARATT_IS_EXTERNAL(datum) && VARTAG_EXTERNAL(datum) == VARTAG_ZEDSTORE)
				{
					datum = zedstore_toast_flatten(scan->rs_scan.rs_rd, natt, this_tid, datum);
				}

				/* Check that the values coming out of the b-tree are aligned properly */
				if (!isnull && attr->attlen == -1)
				{
					Assert (VARATT_IS_1B(datum) || INTALIGN(datum) == datum);
				}

				if (natt != ZS_META_ATTRIBUTE_NUM)
				{
					Assert(natt > 0);
					slot_values[natt - 1] = datum;
					slot_isnull[natt - 1] = isnull;
				}
			}
		}

		if (scan->state == ZSSCAN_STATE_FINISHED_RANGE)
		{
			for (int i = 0; i < scan_proj->num_proj_atts; i++)
				zsbt_end_scan(&scan_proj->btree_scans[i]);
		}
		else
		{
			Assert(scan->state == ZSSCAN_STATE_SCANNING);
			slot->tts_tid = ItemPointerFromZSTid(this_tid);
			slot->tts_nvalid = slot->tts_tupleDescriptor->natts;
			slot->tts_flags &= ~TTS_FLAG_EMPTY;

			pgstat_count_heap_getnext(scan->rs_scan.rs_rd);
			return true;
		}
	}

	ExecClearTuple(slot);
	return false;
}

static bool
zedstoream_tuple_satisfies_snapshot(Relation rel, TupleTableSlot *slot,
									Snapshot snapshot)
{
	/*
	 * TODO: we didn't keep any visibility information about the tuple in the
	 * slot, so we have to fetch it again. A custom slot type might be a
	 * good idea..
	 */
	zstid		tid = ZSTidFromItemPointer(slot->tts_tid);
	ZSBtreeScan btree_scan;
	bool		found;

	/* Use the meta-data tree for the visibility information. */
	zsbt_begin_scan(rel, slot->tts_tupleDescriptor, ZS_META_ATTRIBUTE_NUM, tid,
					tid + 1, snapshot, &btree_scan);

	found = zsbt_scan_next_tid(&btree_scan) != InvalidZSTid;

	zsbt_end_scan(&btree_scan);

	return found;
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
	ZedStoreIndexFetch zscan = palloc0(sizeof(ZedStoreIndexFetchData));

	zscan->idx_fetch_data.rel = rel;
	zscan->proj_data.context = CurrentMemoryContext;

	return (IndexFetchTableData *) zscan;
}

static void
zedstoream_fetch_set_column_projection(struct IndexFetchTableData *scan,
									   bool *project_columns)
{
	ZedStoreIndexFetch zscan = (ZedStoreIndexFetch) scan;
	zscan->proj_data.project_columns = project_columns;
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

	for (int i = 0; i < zscan_proj->num_proj_atts; i++)
		zsbt_end_scan(&zscan_proj->btree_scans[i]);

	if (zscan_proj->proj_atts)
		pfree(zscan_proj->proj_atts);

	if (zscan_proj->btree_scans)
		pfree(zscan_proj->btree_scans);
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
		 */
		PredicateLockTID(scan->rel, tid_p, snapshot);
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
		zs_initialize_proj_attributes(slot->tts_tupleDescriptor, fetch_proj);
	else
	{
		/* If we had a previous fetches still open, close them first */
		for (int i = 0; i < fetch_proj->num_proj_atts; i++)
			zsbt_end_scan(&fetch_proj->btree_scans[i]);
	}

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

	zsbt_begin_scan(rel, slot->tts_tupleDescriptor, 0, tid, tid + 1,
					snapshot, &fetch_proj->btree_scans[0]);
	fetch_proj->btree_scans[0].serializable = true;
	found = zsbt_scan_next_tid(&fetch_proj->btree_scans[0]) != InvalidZSTid;
	if (found)
	{
		for (int i = 1; i < fetch_proj->num_proj_atts; i++)
		{
			int         natt = fetch_proj->proj_atts[i];
			ZSBtreeScan *btscan = &fetch_proj->btree_scans[i];
			Form_pg_attribute attr;
			Datum		datum;
			bool        isnull;

			zsbt_begin_scan(rel, slot->tts_tupleDescriptor, natt, tid, tid + 1,
							snapshot, btscan);

			attr = ZSBtreeScanGetAttInfo(btscan);
			if (zsbt_scan_next_fetch(btscan, &datum, &isnull, tid))
			{
				/*
				 * flatten any ZS-TOASTed values, because the rest of the system
				 * doesn't know how to deal with them.
				 */
				if (!isnull && attr->attlen == -1 &&
					VARATT_IS_EXTERNAL(datum) && VARTAG_EXTERNAL(datum) == VARTAG_ZEDSTORE)
				{
					datum = zedstore_toast_flatten(rel, natt, tid, datum);
				}
			}
			else
				zsbt_fill_missing_attribute_value(btscan, &datum, &isnull);

			slot->tts_values[natt - 1] = datum;
			slot->tts_isnull[natt - 1] = isnull;
		}
	}

	if (found)
	{
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
	bool	   *proj;
	int			attno;
	TableScanDesc scan;
	ItemPointerData idx_ptr;
	bool		tuplesort_empty = false;

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
	proj = palloc0(baseRelation->rd_att->natts * sizeof(bool));
	for (attno = 0; attno < indexInfo->ii_NumIndexKeyAttrs; attno++)
	{
		Assert(indexInfo->ii_IndexAttrNumbers[attno] <= baseRelation->rd_att->natts);
		/* skip expressions */
		if (indexInfo->ii_IndexAttrNumbers[attno] > 0)
			proj[indexInfo->ii_IndexAttrNumbers[attno] - 1] = true;
	}
	GetNeededColumnsForNode((Node *)indexInfo->ii_Predicate, proj,
							baseRelation->rd_att->natts);
	GetNeededColumnsForNode((Node *)indexInfo->ii_Expressions, proj,
							baseRelation->rd_att->natts);

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
		HeapTuple	heapTuple;
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
			heapTuple = ExecCopySlotHeapTuple(slot);
			heapTuple->t_self = slot->tts_tid;
			index_insert(indexRelation, values, isnull, &tup_ptr, baseRelation,
						 indexInfo->ii_Unique ?
						 UNIQUE_CHECK_YES : UNIQUE_CHECK_NO,
						 indexInfo);
			pfree(heapTuple);

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

	/*
	 * TODO: It would be very good to fetch only the columns we need.
	 */
	if (!scan)
	{
		bool	   *proj;
		int			attno;

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

		proj = palloc0(baseRelation->rd_att->natts * sizeof(bool));
		for (attno = 0; attno < indexInfo->ii_NumIndexKeyAttrs; attno++)
		{
			Assert(indexInfo->ii_IndexAttrNumbers[attno] <= baseRelation->rd_att->natts);
			/* skip expressions */
			if (indexInfo->ii_IndexAttrNumbers[attno] > 0)
				proj[indexInfo->ii_IndexAttrNumbers[attno] - 1] = true;
		}

		GetNeededColumnsForNode((Node *)indexInfo->ii_Predicate, proj,
								baseRelation->rd_att->natts);
		GetNeededColumnsForNode((Node *)indexInfo->ii_Expressions, proj,
								baseRelation->rd_att->natts);

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

			for (int i = 0; i < zscan_proj->num_proj_atts; i++)
			{
				int			natt = zscan_proj->proj_atts[i];

				zsbt_begin_scan(zscan->rs_scan.rs_rd,
								RelationGetDescr(zscan->rs_scan.rs_rd),
								natt,
								zscan->cur_range_start,
								zscan->cur_range_end,
								zscan->rs_scan.rs_snapshot,
								&zscan_proj->btree_scans[i]);
			}
			zscan->state = ZSSCAN_STATE_SCANNING;
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
	while (table_scan_getnextslot(scan, ForwardScanDirection, slot))
	{
		bool		tupleIsAlive;
		HeapTuple	heapTuple;

		if (numblocks != InvalidBlockNumber &&
			ItemPointerGetBlockNumber(&slot->tts_tid) >= numblocks)
			break;

		CHECK_FOR_INTERRUPTS();

		/* table_scan_getnextslot did the visibility check */
		tupleIsAlive = true;
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
		heapTuple = ExecCopySlotHeapTuple(slot);
		heapTuple->t_self = slot->tts_tid;
		callback(indexRelation, heapTuple, values, isnull, tupleIsAlive,
				 callback_state);
		pfree(heapTuple);
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
		smgrimmedsync(rel->rd_smgr, INIT_FORKNUM);
	}
}

static void
zedstoream_relation_nontransactional_truncate(Relation rel)
{
	RelationTruncate(rel, 0);
}

static void
zedstoream_relation_copy_data(Relation rel, const RelFileNode *newrnode)
{
	SMgrRelation dstrel;

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
 * FIXME: This break UPDATE chains. I.e. after this is done, an UPDATE
 * looks like DELETE + INSERT, instead of an UPDATe, to any transaction that
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
		undorec = zsundo_fetch(OldHeap, undo_ptr);

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
		ZSUndoRecPtr prevundoptr;
		Datum		datum = (Datum) 0;
		bool        isnull = true;
		zstid		newtid = InvalidZSTid;

		/* First, insert the tuple. */
		ZSUndoRecPtrInitialize(&prevundoptr);
		zsbt_multi_insert(NewHeap, ZS_META_ATTRIBUTE_NUM,
						  &datum, &isnull, &newtid, 1,
						  this_xmin,
						  this_cmin,
						  INVALID_SPECULATIVE_TOKEN,
						  prevundoptr);

		/* And if the tuple was deleted/updated away, do the same in the new table. */
		if (this_xmax != InvalidTransactionId)
		{
			TM_Result	delete_result;

			/* tuple was deleted. */
			delete_result = zsbt_delete(NewHeap, ZS_META_ATTRIBUTE_NUM, newtid,
										this_xmax, this_cmax,
										NULL, NULL, false, NULL, this_changedPart);
			Assert(delete_result == TM_Ok);
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
	ZSBtreeScan meta_scan;
	ZSBtreeScan	*attr_scans;
	ZSUndoRecPtr recent_oldest_undo = zsundo_get_oldest_undo_ptr(OldHeap);
	int			attno;
	IndexScanDesc indexScan;

	olddesc = RelationGetDescr(OldHeap),

	attr_scans = palloc((olddesc->natts + 1) * sizeof(ZSBtreeScan));

	/*
	 * Scan the old table. We ignore any old updated-away tuple versions,
	 * and only stop at the latest tuple version of each row. At the latest
	 * version, follow the update chain to get all the old versions of that
	 * row, too. That way, the whole update chain is processed in one go,
	 * and can be reproduced in the new table.
	 */
	zsbt_begin_scan(OldHeap, olddesc, ZS_META_ATTRIBUTE_NUM,
					MinZSTid, MaxPlusOneZSTid,
					SnapshotAny, &meta_scan);

	for (attno = 1; attno <= olddesc->natts; attno++)
	{
		if (TupleDescAttr(olddesc, attno - 1)->attisdropped)
			continue;

		zsbt_begin_scan(OldHeap,
						olddesc,
						attno,
						MinZSTid,
						MaxPlusOneZSTid,
						SnapshotAny,
						&attr_scans[attno]);
	}

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
		ZSUndoRecPtr prevundoptr;
		Datum		datum;
		bool        isnull;
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
			zsbt_reset_scan(&meta_scan, fetchtid);
			old_tid = zsbt_scan_next_tid(&meta_scan);
		}
		else
		{
			old_tid = zsbt_scan_next_tid(&meta_scan);
			fetchtid = old_tid;
		}
		if (old_tid == InvalidZSTid)
			break;
		if (old_tid != fetchtid)
			break;
		old_undoptr = meta_scan.array_undoptr;

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
				Datum		toastptr = (Datum) 0;

				if (att->attisdropped)
				{
					datum = (Datum) 0;
					isnull = true;
				}
				else
				{
					if (indexScan)
						zsbt_reset_scan(&attr_scans[attno], old_tid);

					if (!zsbt_scan_next_fetch(&attr_scans[attno], &datum, &isnull, old_tid))
						zsbt_fill_missing_attribute_value(&attr_scans[attno], &datum, &isnull);
				}

				/* flatten and re-toast any ZS-TOASTed values */
				if (!isnull && att->attlen == -1)
				{
					if (VARATT_IS_EXTERNAL(datum) && VARTAG_EXTERNAL(datum) == VARTAG_ZEDSTORE)
					{
						datum = zedstore_toast_flatten(OldHeap, attno, old_tid, datum);
					}

					if (VARSIZE_ANY_EXHDR(datum) > MaxZedStoreDatumSize)
					{
						toastptr = datum = zedstore_toast_datum(NewHeap, attno, datum);
					}
				}

				zsbt_multi_insert(NewHeap, attno,
								  &datum, &isnull, &new_tid, 1,
								  FrozenTransactionId /* FIXME */,
								  InvalidCommandId,
								  INVALID_SPECULATIVE_TOKEN, prevundoptr);

				if (toastptr != (Datum) 0)
					zedstore_toast_finish(NewHeap, attno, toastptr, new_tid);
			}
		}
	}

	if (indexScan != NULL)
		index_endscan(indexScan);

	zsbt_end_scan(&meta_scan);
	for (attno = 1; attno <= olddesc->natts; attno++)
	{
		if (TupleDescAttr(olddesc, attno - 1)->attisdropped)
			continue;

		zsbt_end_scan(&attr_scans[attno]);
	}
}

/*
 * FIXME: The ANALYZE API is problematic for us. acquire_sample_rows() calls
 * RelationGetNumberOfBlocks() directly on the relation, and chooses the
 * block numbers to sample based on that. But the logical block numbers
 * have little to do with physical ones in zedstore.
 */
static bool
zedstoream_scan_analyze_next_block(TableScanDesc sscan, BlockNumber blockno,
								   BufferAccessStrategy bstrategy)
{
	ZedStoreDesc scan = (ZedStoreDesc) sscan;
	Relation	rel = scan->rs_scan.rs_rd;
	int			ntuples;
	ZSBtreeScan	btree_scan;
	zstid		tid;

	/* TODO: for now, assume that we need all columns */
	zs_initialize_proj_attributes_extended(scan, RelationGetDescr(rel));

	ntuples = 0;
	zsbt_begin_scan(scan->rs_scan.rs_rd,
					RelationGetDescr(scan->rs_scan.rs_rd),
					ZS_META_ATTRIBUTE_NUM,
					ZSTidFromBlkOff(blockno, 1),
					ZSTidFromBlkOff(blockno + 1, 1),
					scan->rs_scan.rs_snapshot,
					&btree_scan);
	/*
	 * TODO: it would be good to pass the next expected TID down to zsbt_scan_next,
	 * so that it could skip over to it more efficiently.
	 */
	ntuples = 0;
	while ((tid = zsbt_scan_next_tid(&btree_scan)) != InvalidZSTid)
	{
		Assert(ZSTidGetBlockNumber(tid) == blockno);
		scan->bmscan_tids[ntuples] = tid;
		ntuples++;
	}
	zsbt_end_scan(&btree_scan);

	if (ntuples)
	{
		for (int i = 1; i < scan->proj_data.num_proj_atts; i++)
		{
			int			natt = scan->proj_data.proj_atts[i];
			ZSBtreeScan	btree_scan;
			Datum		datum;
			bool        isnull;
			Datum	   *datums = scan->bmscan_datums[i];
			bool	   *isnulls = scan->bmscan_isnulls[i];

			zsbt_begin_scan(scan->rs_scan.rs_rd,
							RelationGetDescr(scan->rs_scan.rs_rd),
							natt,
							ZSTidFromBlkOff(blockno, 1),
							ZSTidFromBlkOff(blockno + 1, 1),
							scan->rs_scan.rs_snapshot,
							&btree_scan);
			for (int n = 0; n < ntuples; n++)
			{
				zstid       tid = scan->bmscan_tids[n];
				if (zsbt_scan_next_fetch(&btree_scan, &datum, &isnull, tid))
				{
					Assert(ZSTidGetBlockNumber(tid) == blockno);
				}
				else
					zsbt_fill_missing_attribute_value(&btree_scan, &datum, &isnull);

				/*
				 * have to make a copy because we close the scan immediately.
				 * FIXME: I think this leaks into a too-long-lived context
				 */
				if (!isnull)
					datum = zs_datumCopy(datum,
										 ZSBtreeScanGetAttInfo(&btree_scan)->attbyval,
										 ZSBtreeScanGetAttInfo(&btree_scan)->attlen);
				datums[n] = datum;
				isnulls[n] = isnull;
			}
			zsbt_end_scan(&btree_scan);
		}
	}

	scan->bmscan_nexttuple = 0;
	scan->bmscan_ntuples = ntuples;

	return true;
}

static bool
zedstoream_scan_analyze_next_tuple(TableScanDesc sscan, TransactionId OldestXmin,
								   double *liverows, double *deadrows,
								   TupleTableSlot *slot)
{
	ZedStoreDesc scan = (ZedStoreDesc) sscan;
	zstid		tid;

	if (scan->bmscan_nexttuple >= scan->bmscan_ntuples)
		return false;
	/*
	 * projection attributes were created based on Relation tuple descriptor
	 * it better match TupleTableSlot.
	 */
	Assert((scan->proj_data.num_proj_atts - 1) <= slot->tts_tupleDescriptor->natts);
	tid = scan->bmscan_tids[scan->bmscan_nexttuple];
	for (int i = 1; i < scan->proj_data.num_proj_atts; i++)
	{
		int			natt = scan->proj_data.proj_atts[i];
		Form_pg_attribute att = TupleDescAttr(slot->tts_tupleDescriptor, natt - 1);

		Datum		datum;
		bool        isnull;

		datum = (scan->bmscan_datums[i])[scan->bmscan_nexttuple];
		isnull = (scan->bmscan_isnulls[i])[scan->bmscan_nexttuple];

		/*
		 * flatten any ZS-TOASTed values, because the rest of the system
		 * doesn't know how to deal with them.
		 */
		if (!isnull && att->attlen == -1 &&
			VARATT_IS_EXTERNAL(datum) && VARTAG_EXTERNAL(datum) == VARTAG_ZEDSTORE)
		{
			datum = zedstore_toast_flatten(scan->rs_scan.rs_rd, natt, tid, datum);
		}

		slot->tts_values[natt - 1] = datum;
		slot->tts_isnull[natt - 1] = isnull;
	}
	slot->tts_tid = ItemPointerFromZSTid(tid);
	slot->tts_nvalid = slot->tts_tupleDescriptor->natts;
	slot->tts_flags &= ~TTS_FLAG_EMPTY;

	scan->bmscan_nexttuple++;
	(*liverows)++;

	return true;
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

static bool
zedstoream_scan_bitmap_next_block(TableScanDesc sscan,
								  TBMIterateResult *tbmres)
{
	ZedStoreDesc scan = (ZedStoreDesc) sscan;
	BlockNumber tid_blkno = tbmres->blockno;
	int			ntuples;
	ZSBtreeScan	btree_scan;
	zstid		tid;
	int			noff = 0;

	zs_initialize_proj_attributes_extended(scan, RelationGetDescr(scan->rs_scan.rs_rd));

	/*
	 * Our strategy for a bitmap scan is to scan the tree of each attribute,
	 * starting at the given logical block number, and store all the datums
	 * in the scan struct. zedstoream_scan_analyze_next_tuple() then just
	 * needs to store the datums of the next TID in the slot.
	 *
	 * An alternative would be to keep the scans of each attribute open,
	 * like in a sequential scan. I'm not sure which is better.
	 */
	ntuples = 0;
	zsbt_begin_scan(scan->rs_scan.rs_rd, RelationGetDescr(scan->rs_scan.rs_rd),
					ZS_META_ATTRIBUTE_NUM,
					ZSTidFromBlkOff(tid_blkno, 1),
					ZSTidFromBlkOff(tid_blkno + 1, 1),
					scan->rs_scan.rs_snapshot,
					&btree_scan);
	btree_scan.serializable = true;
	while ((tid = zsbt_scan_next_tid(&btree_scan)) != InvalidZSTid)
	{
		ItemPointerData itemptr;

		Assert(ZSTidGetBlockNumber(tid) == tid_blkno);

		ItemPointerSet(&itemptr, tid_blkno, ZSTidGetOffsetNumber(tid));

		if (tbmres->ntuples != -1)
		{
			while (ZSTidGetOffsetNumber(tid) > tbmres->offsets[noff] && noff < tbmres->ntuples)
			{
				/*
				 * Acquire predicate lock on all tuples that we scan, even those that are
				 * not visible to the snapshot.
				 */
				PredicateLockTID(scan->rs_scan.rs_rd, &itemptr, scan->rs_scan.rs_snapshot);

				noff++;
			}

			if (noff == tbmres->ntuples)
				break;

			if (ZSTidGetOffsetNumber(tid) < tbmres->offsets[noff])
				continue;
		}

		Assert(ZSTidGetBlockNumber(tid) == tid_blkno);

		scan->bmscan_tids[ntuples] = tid;
		ntuples++;

		/* FIXME: heapam acquires the predicate lock first, and then
		 * calls CheckForSerializableConflictOut(). We do it in the
		 * opposite order, because CheckForSerializableConflictOut()
		 * call as done in zsbt_get_last_tid() already. Does it matter?
		 * I'm not sure.
		 */
		PredicateLockTID(scan->rs_scan.rs_rd, &itemptr, scan->rs_scan.rs_snapshot);
	}
	zsbt_end_scan(&btree_scan);

	if (ntuples)
	{
		for (int i = 1; i < scan->proj_data.num_proj_atts; i++)
		{
			int			natt = scan->proj_data.proj_atts[i];
			ZSBtreeScan	btree_scan;
			Datum		datum;
			bool        isnull;
			Datum	   *datums = scan->bmscan_datums[i];
			bool	   *isnulls = scan->bmscan_isnulls[i];

			zsbt_begin_scan(scan->rs_scan.rs_rd,
							RelationGetDescr(scan->rs_scan.rs_rd),
							natt,
							ZSTidFromBlkOff(tid_blkno, 1),
							ZSTidFromBlkOff(tid_blkno + 1, 1),
							scan->rs_scan.rs_snapshot,
							&btree_scan);
			for (int n = 0; n < ntuples; n++)
			{
				if (!zsbt_scan_next_fetch(&btree_scan, &datum, &isnull, scan->bmscan_tids[n]))
					zsbt_fill_missing_attribute_value(&btree_scan, &datum, &isnull);

				/* have to make a copy because we close the scan immediately. */
				if (!isnull)
					datum = zs_datumCopy(datum,
										 ZSBtreeScanGetAttInfo(&btree_scan)->attbyval,
										 ZSBtreeScanGetAttInfo(&btree_scan)->attlen);
				datums[n] = datum;
				isnulls[n] = isnull;
			}
			zsbt_end_scan(&btree_scan);
		}
	}
	scan->bmscan_nexttuple = 0;
	scan->bmscan_ntuples = ntuples;

	return ntuples > 0;
}

static bool
zedstoream_scan_bitmap_next_tuple(TableScanDesc sscan,
								  TBMIterateResult *tbmres,
								  TupleTableSlot *slot)
{
	ZedStoreDesc scan = (ZedStoreDesc) sscan;
	zstid		tid;

	if (scan->bmscan_nexttuple >= scan->bmscan_ntuples)
		return false;
	/*
	 * projection attributes were created based on Relation tuple descriptor
	 * it better match TupleTableSlot.
	 */
	Assert((scan->proj_data.num_proj_atts - 1) <= slot->tts_tupleDescriptor->natts);
	tid = scan->bmscan_tids[scan->bmscan_nexttuple];
	for (int i = 1; i < scan->proj_data.num_proj_atts; i++)
	{
		int			natt = scan->proj_data.proj_atts[i];
		Form_pg_attribute att = TupleDescAttr(slot->tts_tupleDescriptor, natt - 1);
		Datum		datum;
		bool        isnull;

		datum = (scan->bmscan_datums[i])[scan->bmscan_nexttuple];
		isnull = (scan->bmscan_isnulls[i])[scan->bmscan_nexttuple];

		/*
		 * flatten any ZS-TOASTed values, because the rest of the system
		 * doesn't know how to deal with them.
		 */
		if (!isnull && att->attlen == -1 &&
			VARATT_IS_EXTERNAL(datum) && VARTAG_EXTERNAL(datum) == VARTAG_ZEDSTORE)
		{
			datum = zedstore_toast_flatten(scan->rs_scan.rs_rd, natt, tid, datum);
		}

		slot->tts_values[natt - 1] = datum;
		slot->tts_isnull[natt - 1] = isnull;
	}
	slot->tts_tid = ItemPointerFromZSTid(tid);
	slot->tts_nvalid = slot->tts_tupleDescriptor->natts;
	slot->tts_flags &= ~TTS_FLAG_EMPTY;

	scan->bmscan_nexttuple++;

	pgstat_count_heap_fetch(scan->rs_scan.rs_rd);

	return true;
}

static bool
zedstoream_scan_sample_next_block(TableScanDesc sscan, SampleScanState *scanstate)
{
	ZedStoreDesc scan = (ZedStoreDesc) sscan;
	Relation	rel = scan->rs_scan.rs_rd;
	TsmRoutine *tsm = scanstate->tsmroutine;
	int			ntuples;
	ZSBtreeScan	btree_scan;
	zstid		tid;
	BlockNumber blockno;

	/* TODO: for now, assume that we need all columns */
	zs_initialize_proj_attributes_extended(scan, RelationGetDescr(rel));

	if (scan->max_tid_to_scan == InvalidZSTid)
	{
		/*
		 * get the max tid once and store it, used to calculate max blocks to
		 * scan either for SYSTEM or BERNOULLI sampling.
		 */
		scan->max_tid_to_scan = zsbt_get_last_tid(rel, ZS_META_ATTRIBUTE_NUM);
		/*
		 * TODO: should get lowest tid instead of starting from 0
		 */
		scan->next_tid_to_scan = ZSTidFromBlkOff(0, 1);
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

	ntuples = 0;
	zsbt_begin_scan(scan->rs_scan.rs_rd,
					RelationGetDescr(scan->rs_scan.rs_rd),
					ZS_META_ATTRIBUTE_NUM,
					ZSTidFromBlkOff(blockno, 1),
					ZSTidFromBlkOff(blockno + 1, 1),
					scan->rs_scan.rs_snapshot,
					&btree_scan);
	while ((tid = zsbt_scan_next_tid(&btree_scan)) != InvalidZSTid)
	{
		Assert(ZSTidGetBlockNumber(tid) == blockno);
		scan->bmscan_tids[ntuples] = tid;
		ntuples++;
	}
	zsbt_end_scan(&btree_scan);

	scan->bmscan_nexttuple = 0;
	scan->bmscan_ntuples = ntuples;

	return true;
}

static bool
zedstoream_scan_sample_next_tuple(TableScanDesc sscan, SampleScanState *scanstate,
								  TupleTableSlot *slot)
{
	ZedStoreDesc scan = (ZedStoreDesc) sscan;
	TsmRoutine *tsm = scanstate->tsmroutine;
	zstid		tid;
	BlockNumber blockno;
	OffsetNumber tupoffset;
	bool found;

	/* all tuples on this block are invisible */
	if (scan->bmscan_ntuples == 0)
		return false;

	blockno = ZSTidGetBlockNumber(scan->bmscan_tids[0]);

	/* find which visible tuple in this block to sample */
	for (;;)
	{
		zstid lasttid_for_block = scan->bmscan_tids[scan->bmscan_ntuples - 1];
		OffsetNumber maxoffset = ZSTidGetOffsetNumber(lasttid_for_block);
		/* Ask the tablesample method which tuples to check on this page. */
		tupoffset = tsm->NextSampleTuple(scanstate, blockno, maxoffset);

		if (!OffsetNumberIsValid(tupoffset))
			return false;

		tid = ZSTidFromBlkOff(blockno, tupoffset);

		found = false;
		for (int n = 0; n < scan->bmscan_ntuples; n++)
		{
			if (scan->bmscan_tids[n] == tid)
			{
				/* visible tuple */
				found = true;
				break;
			}
		}

		if (found)
			break;
		else
			continue;
	}

	/*
	 * projection attributes were created based on Relation tuple descriptor
	 * it better match TupleTableSlot.
	 */
	Assert((scan->proj_data.num_proj_atts - 1) <= slot->tts_tupleDescriptor->natts);
	/* fetch values for tuple pointed by tid to sample */
	for (int i = 1; i < scan->proj_data.num_proj_atts; i++)
	{
		int			natt = scan->proj_data.proj_atts[i];
		ZSBtreeScan btree_scan;
		Form_pg_attribute attr;
		Datum		datum;
		bool        isnull;

		zsbt_begin_scan(scan->rs_scan.rs_rd,
						slot->tts_tupleDescriptor,
						natt,
						tid, tid + 1,
						scan->rs_scan.rs_snapshot,
						&btree_scan);

		attr = ZSBtreeScanGetAttInfo(&btree_scan);
		if (zsbt_scan_next_fetch(&btree_scan, &datum, &isnull, tid))
		{
			Assert(ZSTidGetBlockNumber(tid) == blockno);
		}
		else
		{
			zsbt_fill_missing_attribute_value(&btree_scan, &datum, &isnull);
		}

		/*
		 * have to make a copy because we close the scan immediately.
		 * FIXME: I think this leaks into a too-long-lived context
		 */
		if (!isnull)
			datum = zs_datumCopy(datum, attr->attbyval, attr->attlen);

		slot->tts_values[natt - 1] = datum;
		slot->tts_isnull[natt - 1] = isnull;

		zsbt_end_scan(&btree_scan);
	}
	slot->tts_tid = ItemPointerFromZSTid(tid);
	slot->tts_nvalid = slot->tts_tupleDescriptor->natts;
	slot->tts_flags &= ~TTS_FLAG_EMPTY;

	return true;
}

static void
zedstoream_vacuum_rel(Relation onerel, VacuumParams *params,
					  BufferAccessStrategy bstrategy)
{
	zsundo_vacuum(onerel, params, bstrategy,
				  GetOldestXmin(onerel, PROCARRAY_FLAGS_VACUUM));
}

static const TableAmRoutine zedstoream_methods = {
	.type = T_TableAmRoutine,
	.uses_toast_table = false,
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
	.tuple_satisfies_snapshot = zedstoream_tuple_satisfies_snapshot,
	.compute_xid_horizon_for_tuples = zedstoream_compute_xid_horizon_for_tuples,

	.relation_set_new_filenode = zedstoream_relation_set_new_filenode,
	.relation_nontransactional_truncate = zedstoream_relation_nontransactional_truncate,
	.relation_copy_data = zedstoream_relation_copy_data,
	.relation_copy_for_cluster = zedstoream_relation_copy_for_cluster,
	.relation_vacuum = zedstoream_vacuum_rel,
	.scan_analyze_next_block = zedstoream_scan_analyze_next_block,
	.scan_analyze_next_tuple = zedstoream_scan_analyze_next_tuple,

	.index_build_range_scan = zedstoream_index_build_range_scan,
	.index_validate_scan = zedstoream_index_validate_scan,

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

typedef struct ParallelZSScanDescData
{
	ParallelTableScanDescData base;

	zstid		pzs_endtid;		/* last tid + 1 in relation at start of scan */
	pg_atomic_uint64 pzs_allocatedtid_blk;	/* TID space allocated to workers so far. (in  65536 increments) */
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
	zpscan->pzs_endtid = zsbt_get_last_tid(rel, ZS_META_ATTRIBUTE_NUM);
	pg_atomic_init_u64(&zpscan->pzs_allocatedtid_blk, 0);

	return sizeof(ParallelZSScanDescData);
}

static void
zs_parallelscan_reinitialize(Relation rel, ParallelTableScanDesc pscan)
{
	ParallelZSScanDesc bpscan = (ParallelZSScanDesc) pscan;

	pg_atomic_write_u64(&bpscan->pzs_allocatedtid_blk, 0);
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
	uint64		allocatedtid_blk;

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
	 *
	 * TODO: we divide the TID space into chunks of 2^16 TIDs each. That's
	 * pretty inefficient, there's a fair amount of overhead in re-starting
	 * the B-tree scans between each range. We probably should use much larger
	 * ranges. But this is good for testing.
	 */
	allocatedtid_blk = pg_atomic_fetch_add_u64(&pzscan->pzs_allocatedtid_blk, 1);
	*start = ZSTidFromBlkOff(allocatedtid_blk, 1);
	*end = ZSTidFromBlkOff(allocatedtid_blk + 1, 1);

	return *start < pzscan->pzs_endtid;
}

static void
zsbt_fill_missing_attribute_value(ZSBtreeScan *scan, Datum *datum, bool *isnull)
{
	int attno = scan->attno - 1;
	TupleDesc tupleDesc = scan->tupledesc;
	Form_pg_attribute attr = ZSBtreeScanGetAttInfo(scan);

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

		if (attrmiss[attno].am_present)
		{
			*isnull = false;
			if (attr->attbyval)
				*datum = fetch_att(&attrmiss[attno].am_value, attr->attbyval, attr->attlen);
			else
				*datum = zs_datumCopy(attrmiss[attno].am_value, attr->attbyval, attr->attlen);
		}
	}
}
