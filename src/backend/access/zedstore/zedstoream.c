/*-------------------------------------------------------------------------
 *
 * zedstoream.c
 *	  zedstore access method code
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/heap/heapam.c
 *
 * NOTES
 *	  This file contains the zedstore_ routines which implement
 *	  the POSTGRES zedstore access method.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/htup.h"
#include "access/htup_details.h"
#include "access/parallel.h"
#include "storage/itemptr.h"
#include "storage/predicate.h"
#include "access/xact.h"
#include "access/zedstore_internal.h"
#include "miscadmin.h"
/*
 * This routine is similar to heap_prepare_insert(). This sets the
 * tuple header fields, assigns an OID, and toasts the tuple if necessary.
 * Returns a toasted version of the tuple if it was toasted, or the original
 * tuple if not. Note that in any case, the header fields are also set in
 * the original tuple.
 */
void
zs_prepare_insert(Relation relation, HeapTupleHeader hdr, TransactionId xid,
					CommandId cid, int options)
{
	/*
	 * Parallel operations are required to be strictly read-only in a parallel
	 * worker.  Parallel inserts are not safe even in the leader in the
	 * general case, because group locking means that heavyweight locks for
	 * relation extension or GIN page locks will not conflict between members
	 * of a lock group, but we don't prohibit that case here because there are
	 * useful special cases that we can safely allow, such as CREATE TABLE AS.
	 */
	if (IsParallelWorker())
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TRANSACTION_STATE),
				 errmsg("cannot insert tuples in a parallel worker")));

	hdr->t_infomask &= ~(HEAP_XACT_MASK);
	hdr->t_infomask2 &= ~(HEAP2_XACT_MASK);
	hdr->t_infomask |= HEAP_XMAX_INVALID;
	HeapTupleHeaderSetXmin(hdr, xid);
	if (options & HEAP_INSERT_FROZEN)
		HeapTupleHeaderSetXminFrozen(hdr);

	HeapTupleHeaderSetCmin(hdr, cid);
	HeapTupleHeaderSetXmax(hdr, 0); /* for cleanliness */

	/* TODO: handle toasting. */
#if 0
	/*
	 * If the new tuple is too big for storage or contains already toasted
	 * out-of-line attributes from some other relation, invoke the toaster.
	 */
	if (relation->rd_rel->relkind != RELKIND_RELATION &&
		relation->rd_rel->relkind != RELKIND_MATVIEW)
	{
		/* toast table entries should never be recursively toasted */
		Assert(!HeapTupleHasExternal(tup));
		return tup;
	}
	else if (HeapTupleHasExternal(tup) || tup->t_len > TOAST_TUPLE_THRESHOLD)
		return toast_insert_or_update(relation, tup, NULL, options);
	else
		return tup;
#endif
}

bool
zs_tuple_satisfies_visibility(HeapTupleHeader hdr, ItemPointer tid, Snapshot snapshot, Buffer buffer)
{
	HeapTupleData tuple;
	bool valid;

	tuple.t_data = hdr;
	tuple.t_len = SizeofHeapTupleHeader;
	ItemPointerCopy(tid, &(tuple.t_self));

	/*
	 * if current tuple qualifies, return it.
	 */
	valid = HeapTupleSatisfiesVisibility(&tuple, snapshot, buffer);
	return valid;
}

/*
 * Given two versions of the same t_infomask for a tuple, compare them and
 * return whether the relevant status for a tuple Xmax has changed.  This is
 * used after a buffer lock has been released and reacquired: we want to ensure
 * that the tuple state continues to be the same it was when we previously
 * examined it.
 *
 * Note the Xmax field itself must be compared separately.
 */
static inline bool
xmax_infomask_changed(uint16 new_infomask, uint16 old_infomask)
{
	const uint16 interesting =
	HEAP_XMAX_IS_MULTI | HEAP_XMAX_LOCK_ONLY | HEAP_LOCK_MASK;

	if ((new_infomask & interesting) != (old_infomask & interesting))
		return true;

	return false;
}

void
zs_tuple_delete(ZSBtreeScanForTupleDelete *deldesc,
				HeapTupleHeader hdr, ItemPointer tid,
				Buffer buffer)
{
	Relation relation = deldesc->rel;
	CommandId cid = deldesc->cid;
	Snapshot crosscheck = deldesc->snapshot;
	bool wait = deldesc->wait;
	TM_FailureData *tmfd = deldesc->tmfd;

	TM_Result	result;
	TransactionId xid = GetCurrentTransactionId();
	HeapTupleData tp;
	TransactionId new_xmax;
	uint16		new_infomask,
				new_infomask2;
	bool		have_tuple_lock = false;
	bool		iscombo;
	HeapTuple	old_key_tuple = NULL;	/* replica identity of the tuple */
	bool		old_key_copied = false;

	tp.t_tableOid = RelationGetRelid(relation);
	tp.t_data = hdr;
	tp.t_len = SizeofHeapTupleHeader;
	tp.t_self = *tid;

l1:
	result = HeapTupleSatisfiesUpdate(&tp, cid, buffer);

	if (result == TM_Invisible)
	{
		deldesc->result = TM_Invisible;
		return;
	}

	if (result == TM_BeingModified && wait)
	{
		TransactionId xwait;
		uint16		infomask;

		/* must copy state data before unlocking buffer */
		xwait = HeapTupleHeaderGetRawXmax(tp.t_data);
		infomask = tp.t_data->t_infomask;

		/*
		 * Sleep until concurrent transaction ends -- except when there's a
		 * single locker and it's our own transaction.  Note we don't care
		 * which lock mode the locker has, because we need the strongest one.
		 *
		 * Before sleeping, we need to acquire tuple lock to establish our
		 * priority for the tuple (see heap_lock_tuple).  LockTuple will
		 * release us when we are next-in-line for the tuple.
		 *
		 * If we are forced to "start over" below, we keep the tuple lock;
		 * this arranges that we stay at the head of the line while rechecking
		 * tuple state.
		 */
		if (infomask & HEAP_XMAX_IS_MULTI)
		{
			/* wait for multixact */
			if (DoesMultiXactIdConflict((MultiXactId) xwait, infomask,
										LockTupleExclusive))
			{
				LockBuffer(buffer, BUFFER_LOCK_UNLOCK);

				/* acquire tuple lock, if necessary */
				heap_acquire_tuplock(relation, &(tp.t_self), LockTupleExclusive,
									 LockWaitBlock, &have_tuple_lock);

				/* wait for multixact */
				MultiXactIdWait((MultiXactId) xwait, MultiXactStatusUpdate, infomask,
								relation, &(tp.t_self), XLTW_Delete,
								NULL);
				LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

				/*
				 * If xwait had just locked the tuple then some other xact
				 * could update this tuple before we get to this point.  Check
				 * for xmax change, and start over if so.
				 */
				if (xmax_infomask_changed(tp.t_data->t_infomask, infomask) ||
					!TransactionIdEquals(HeapTupleHeaderGetRawXmax(tp.t_data),
										 xwait))
					goto l1;
			}

			/*
			 * You might think the multixact is necessarily done here, but not
			 * so: it could have surviving members, namely our own xact or
			 * other subxacts of this backend.  It is legal for us to delete
			 * the tuple in either case, however (the latter case is
			 * essentially a situation of upgrading our former shared lock to
			 * exclusive).  We don't bother changing the on-disk hint bits
			 * since we are about to overwrite the xmax altogether.
			 */
		}
		else if (!TransactionIdIsCurrentTransactionId(xwait))
		{
			/*
			 * Wait for regular transaction to end; but first, acquire tuple
			 * lock.
			 */
			LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
			heap_acquire_tuplock(relation, &(tp.t_self), LockTupleExclusive,
								 LockWaitBlock, &have_tuple_lock);
			XactLockTableWait(xwait, relation, &(tp.t_self), XLTW_Delete);
			LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

			/*
			 * xwait is done, but if xwait had just locked the tuple then some
			 * other xact could update this tuple before we get to this point.
			 * Check for xmax change, and start over if so.
			 */
			if (xmax_infomask_changed(tp.t_data->t_infomask, infomask) ||
				!TransactionIdEquals(HeapTupleHeaderGetRawXmax(tp.t_data),
									 xwait))
				goto l1;

			/* Otherwise check if it committed or aborted */
			UpdateXmaxHintBits(tp.t_data, buffer, xwait);
		}

		/*
		 * We may overwrite if previous xmax aborted, or if it committed but
		 * only locked the tuple without updating it.
		 */
		if ((tp.t_data->t_infomask & HEAP_XMAX_INVALID) ||
			HEAP_XMAX_IS_LOCKED_ONLY(tp.t_data->t_infomask) ||
			HeapTupleHeaderIsOnlyLocked(tp.t_data))
			result = TM_Ok;
		else if (!ItemPointerEquals(&tp.t_self, &tp.t_data->t_ctid) ||
				 HeapTupleHeaderIndicatesMovedPartitions(tp.t_data))
			result = TM_Updated;
		else
			result = TM_Deleted;
	}

	if (crosscheck != InvalidSnapshot && result == TM_Ok)
	{
		/* Perform additional check for transaction-snapshot mode RI updates */
		if (!HeapTupleSatisfiesVisibility(&tp, crosscheck, buffer))
			result = TM_Updated;
	}

	if (result != TM_Ok)
	{
		Assert(result == TM_SelfModified ||
			   result == TM_Updated ||
			   result == TM_Deleted ||
			   result == TM_BeingModified);
		Assert(!(tp.t_data->t_infomask & HEAP_XMAX_INVALID));
		Assert(result != TM_Updated ||
			   !ItemPointerEquals(&tp.t_self, &tp.t_data->t_ctid));
		tmfd->ctid = tp.t_data->t_ctid;
		tmfd->xmax = HeapTupleHeaderGetUpdateXid(tp.t_data);
		if (result == TM_SelfModified)
			tmfd->cmax = HeapTupleHeaderGetCmax(tp.t_data);
		else
			tmfd->cmax = InvalidCommandId;

		if (have_tuple_lock)
			UnlockTupleTuplock(relation, &(tp.t_self), LockTupleExclusive);

		deldesc->result = result;
		return;
	}

	/*
	 * We're about to do the actual delete -- check for conflict first, to
	 * avoid possibly having to roll back work we've just done.
	 *
	 * This is safe without a recheck as long as there is no possibility of
	 * another process scanning the page between this check and the delete
	 * being visible to the scan (i.e., an exclusive buffer content lock is
	 * continuously held from this point until the tuple delete is visible).
	 */
	CheckForSerializableConflictIn(relation, &tp, buffer);

	/* replace cid with a combo cid if necessary */
	HeapTupleHeaderAdjustCmax(tp.t_data, &cid, &iscombo);

	/*
	 * Compute replica identity tuple before entering the critical section so
	 * we don't PANIC upon a memory allocation failure.
	 */
	/* TODO: handle this later */
//	old_key_tuple = ExtractReplicaIdentity(relation, &tp, true, &old_key_copied);

	/*
	 * If this is the first possibly-multixact-able operation in the current
	 * transaction, set my per-backend OldestMemberMXactId setting. We can be
	 * certain that the transaction will never become a member of any older
	 * MultiXactIds than that.  (We have to do this even if we end up just
	 * using our own TransactionId below, since some other backend could
	 * incorporate our XID into a MultiXact immediately afterwards.)
	 */
	MultiXactIdSetOldestMember();

	compute_new_xmax_infomask(HeapTupleHeaderGetRawXmax(tp.t_data),
							  tp.t_data->t_infomask, tp.t_data->t_infomask2,
							  xid, LockTupleExclusive, true,
							  &new_xmax, &new_infomask, &new_infomask2);

	START_CRIT_SECTION();

#if 0
	/*
	 * If this transaction commits, the tuple will become DEAD sooner or
	 * later.  Set flag that this page is a candidate for pruning once our xid
	 * falls below the OldestXmin horizon.  If the transaction finally aborts,
	 * the subsequent page pruning will be a no-op and the hint will be
	 * cleared.
	 */
	PageSetPrunable(page, xid);

	if (PageIsAllVisible(page))
	{
		all_visible_cleared = true;
		PageClearAllVisible(page);
		visibilitymap_clear(relation, BufferGetBlockNumber(buffer),
							vmbuffer, VISIBILITYMAP_VALID_BITS);
	}
#endif

	/* store transaction information of xact deleting the tuple */
	tp.t_data->t_infomask &= ~(HEAP_XMAX_BITS | HEAP_MOVED);
	tp.t_data->t_infomask2 &= ~HEAP_KEYS_UPDATED;
	tp.t_data->t_infomask |= new_infomask;
	tp.t_data->t_infomask2 |= new_infomask2;
	HeapTupleHeaderClearHotUpdated(tp.t_data);
	HeapTupleHeaderSetXmax(tp.t_data, new_xmax);
	HeapTupleHeaderSetCmax(tp.t_data, cid, iscombo);
	/* Make sure there is no forward chain link in t_ctid */
	tp.t_data->t_ctid = tp.t_self;

#if 0
	/* Signal that this is actually a move into another partition */
	if (changingPart)
		HeapTupleHeaderSetMovedPartitions(tp.t_data);
#endif

	MarkBufferDirty(buffer);

	/*
	 * XLOG stuff
	 *
	 */
	END_CRIT_SECTION();

#if 0
	/*
	 * If the tuple has toasted out-of-line attributes, we need to delete
	 * those items too.  We have to do this before releasing the buffer
	 * because we need to look at the contents of the tuple, but it's OK to
	 * release the content lock on the buffer first.
	 */
	if (relation->rd_rel->relkind != RELKIND_RELATION &&
		relation->rd_rel->relkind != RELKIND_MATVIEW)
	{
		/* toast table entries should never be recursively toasted */
		Assert(!HeapTupleHasExternal(&tp));
	}
	else if (HeapTupleHasExternal(&tp))
		toast_delete(relation, &tp, false);

	/*
	 * Mark tuple for invalidation from system caches at next command
	 * boundary. We have to do this before releasing the buffer because we
	 * need to look at the contents of the tuple.
	 */
	CacheInvalidateHeapTuple(relation, &tp, NULL);
#endif

	/*
	 * Release the lmgr tuple lock, if we had it.
	 */
	if (have_tuple_lock)
		UnlockTupleTuplock(relation, &(tp.t_self), LockTupleExclusive);

	/* this should be done by top level function */
//	pgstat_count_heap_delete(relation);

	if (old_key_tuple != NULL && old_key_copied)
		heap_freetuple(old_key_tuple);

	deldesc->result = TM_Ok;
	return;
}
