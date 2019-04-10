/*
 * zedstore_visibility.c
 *		Routines for MVCC in Zedstore
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/zedstore/zedstore_visibility.c
 */
#include "postgres.h"

#include "access/tableam.h"
#include "access/xact.h"
#include "access/zedstore_internal.h"
#include "access/zedstore_undo.h"
#include "storage/procarray.h"

/*
 * Like HeapTupleSatisfiesUpdate.
 *
 * When returns TM_Ok, this also returns a flag in *undo_record_needed, to indicate
 * whether the old UNDO record is still of interest to anyone. If the old record
 * belonged to an aborted deleting transaction, for example, it can be ignored.
 */
TM_Result
zs_SatisfiesUpdate(ZSBtreeScan *scan, ZSUncompressedBtreeItem *item,
				   bool *undo_record_needed, TM_FailureData *tmfd)
{
	Relation	rel = scan->rel;
	Snapshot	snapshot = scan->snapshot;
	ZSUndoRecPtr recent_oldest_undo;
	ZSUndoRecPtr undo_ptr;
	bool		is_deleted;
	ZSUndoRec  *undorec;

	*undo_record_needed = true;

	if (scan->recent_oldest_undo.counter == 0)
		scan->recent_oldest_undo = zsundo_get_oldest_undo_ptr(scan->rel);
	recent_oldest_undo = scan->recent_oldest_undo;

	is_deleted = (item->t_flags & (ZSBT_UPDATED | ZSBT_DELETED)) != 0;
	undo_ptr = item->t_undo_ptr;

fetch_undo_record:

	/* Is it visible? */
	if (undo_ptr.counter < recent_oldest_undo.counter)
	{
		if (is_deleted)
		{
			/* this probably shouldn't happen.. */
			return  TM_Invisible;
		}
		else
		{
			/*
			 * the old UNDO record is no longer visible to anyone, so we don't
			 * need to keep it.
			 */
			*undo_record_needed = false;
			return  TM_Ok;
		}
	}

	/* have to fetch the UNDO record */
	undorec = zsundo_fetch(rel, undo_ptr);

	if (!is_deleted)
	{
		/* Inserted tuple */
		if (undorec->type == ZSUNDO_TYPE_INSERT)
		{
			if (TransactionIdIsCurrentTransactionId(undorec->xid))
			{
				if (undorec->cid >= snapshot->curcid)
					return TM_Invisible;	/* inserted after scan started */
				*undo_record_needed = true;
				return TM_Ok;
			}
			else if (TransactionIdIsInProgress(undorec->xid))
				return TM_Invisible;		/* inserter has not committed yet */
			else if (TransactionIdDidCommit(undorec->xid))
			{
				*undo_record_needed = true;
				return TM_Ok;
			}
			else
			{
				/* it must have aborted or crashed */
				return TM_Invisible;
			}
		}
		else if (undorec->type == ZSUNDO_TYPE_TUPLE_LOCK)
		{
			if (TransactionIdIsCurrentTransactionId(undorec->xid))
			{
				tmfd->ctid = ItemPointerFromZSTid(item->t_tid);
				tmfd->xmax = undorec->xid;
				tmfd->cmax = InvalidCommandId;
				return TM_BeingModified;
			}
			else if (TransactionIdIsInProgress(undorec->xid))
			{
				tmfd->ctid = ItemPointerFromZSTid(item->t_tid);
				tmfd->xmax = undorec->xid;
				tmfd->cmax = InvalidCommandId;
				return TM_BeingModified;
			}

			/* locking transaction committed, so lock is not held anymore. */
			/* look at the previous UNDO record */
			undo_ptr = ((ZSUndoRec_TupleLock *) undorec)->prevundorec;
			goto fetch_undo_record;
		}
		else
			elog(ERROR, "unexpected UNDO record type: %d", undorec->type);
	}
	else
	{
		/* deleted or updated-away tuple */
		Assert(undorec->type == ZSUNDO_TYPE_DELETE ||
			   undorec->type == ZSUNDO_TYPE_UPDATE);

		if (TransactionIdIsCurrentTransactionId(undorec->xid))
		{
			if (undorec->cid >= snapshot->curcid)
			{
				tmfd->ctid = ItemPointerFromZSTid(item->t_tid);
				tmfd->xmax = undorec->xid;
				tmfd->cmax = undorec->cid;
				return TM_SelfModified;	/* deleted/updated after scan started */
			}
			else
				return TM_Invisible;	/* deleted before scan started */
		}

		if (TransactionIdIsInProgress(undorec->xid))
		{
			tmfd->ctid = ItemPointerFromZSTid(item->t_tid);
			tmfd->xmax = undorec->xid;
			tmfd->cmax = InvalidCommandId;
			return TM_BeingModified;
		}

		if (!TransactionIdDidCommit(undorec->xid))
		{
			/* deleter must have aborted or crashed */
			*undo_record_needed = false;
			return TM_Ok;
		}

		if (undorec->type == ZSUNDO_TYPE_DELETE)
		{
			tmfd->ctid = ItemPointerFromZSTid(item->t_tid);
			tmfd->xmax = undorec->xid;
			tmfd->cmax = InvalidCommandId;
			return TM_Deleted;
		}
		else
		{
			tmfd->ctid = ItemPointerFromZSTid(((ZSUndoRec_Update *) undorec)->newtid);
			tmfd->xmax = undorec->xid;
			tmfd->cmax = InvalidCommandId;
			return TM_Updated;
		}
	}
}

/*
 * Like HeapTupleSatisfiesAny
 */
static bool
zs_SatisfiesAny(ZSBtreeScan *scan, ZSUncompressedBtreeItem *item)
{
	return true;
}

/*
 * helper function to zs_SatisfiesMVCC(), to check if the given XID
 * is visible to the snapshot.
 */
static bool
xid_is_visible(Snapshot snapshot, TransactionId xid, CommandId cid)
{
	if (TransactionIdIsCurrentTransactionId(xid))
	{
		if (cid >= snapshot->curcid)
			return false;
		else
			return true;
	}
	else if (XidInMVCCSnapshot(xid, snapshot))
		return false;
	else if (TransactionIdDidCommit(xid))
	{
		return true;
	}
	else
	{
		/* it must have aborted or crashed */
		return false;
	}
}

/*
 * Like HeapTupleSatisfiesMVCC
 */
static bool
zs_SatisfiesMVCC(ZSBtreeScan *scan, ZSUncompressedBtreeItem *item)
{
	Relation	rel = scan->rel;
	Snapshot	snapshot = scan->snapshot;
	ZSUndoRecPtr recent_oldest_undo = scan->recent_oldest_undo;
	ZSUndoRecPtr undo_ptr;
	ZSUndoRec  *undorec;
	bool		is_deleted;

	Assert (snapshot->snapshot_type == SNAPSHOT_MVCC);

	is_deleted = (item->t_flags & (ZSBT_UPDATED | ZSBT_DELETED)) != 0;
	undo_ptr = item->t_undo_ptr;

fetch_undo_record:
	if (undo_ptr.counter < recent_oldest_undo.counter)
	{
		if (!is_deleted)
			return true;
		else
			return false;
	}

	/* have to fetch the UNDO record */
	undorec = zsundo_fetch(rel, undo_ptr);

	if (!is_deleted)
	{
		/* Inserted tuple */
		if (undorec->type == ZSUNDO_TYPE_INSERT)
		{
			return xid_is_visible(snapshot, undorec->xid, undorec->cid);
		}
		else if (undorec->type == ZSUNDO_TYPE_TUPLE_LOCK)
		{
			/* we don't care about tuple locks here. Follow the link to the
			 * previous UNDO record for this tuple. */
			undo_ptr = ((ZSUndoRec_TupleLock *) undorec)->prevundorec;
			goto fetch_undo_record;
		}
		else
			elog(ERROR, "unexpected UNDO record type: %d", undorec->type);
	}
	else
	{
		/* deleted or updated-away tuple */
		Assert(undorec->type == ZSUNDO_TYPE_DELETE ||
			   undorec->type == ZSUNDO_TYPE_UPDATE);

		if (xid_is_visible(snapshot, undorec->xid, undorec->cid))
		{
			/* we can see the deletion */
			return false;
		}
		else
		{
			/*
			 * The deleting XID is not visible to us. But before concluding
			 * that the tuple is visible, we have to check if the inserting
			 * XID is visible to us.
			 */
			ZSUndoRecPtr	prevptr;

			do {
				if (undorec->type == ZSUNDO_TYPE_DELETE)
					prevptr = ((ZSUndoRec_Delete *) undorec)->prevundorec;
				else if (undorec->type == ZSUNDO_TYPE_UPDATE)
					prevptr = ((ZSUndoRec_Update *) undorec)->prevundorec;
				else if (undorec->type == ZSUNDO_TYPE_TUPLE_LOCK)
					prevptr = ((ZSUndoRec_TupleLock *) undorec)->prevundorec;
				else
					elog(ERROR, "unexpected UNDO record type: %d", undorec->type);

				if (prevptr.counter < recent_oldest_undo.counter)
					return true;

				undorec = zsundo_fetch(rel, prevptr);
			} while(undorec->type == ZSUNDO_TYPE_TUPLE_LOCK);

			Assert(undorec->type == ZSUNDO_TYPE_INSERT);
			if (xid_is_visible(snapshot, undorec->xid, undorec->cid))
				return true;	/* we can see the insert, but not the delete */
			else
				return false;	/* we cannot see the insert */
		}
	}
}

/*
 * Like HeapTupleSatisfiesSelf
 */
static bool
zs_SatisfiesSelf(ZSBtreeScan *scan, ZSUncompressedBtreeItem *item)
{
	Relation	rel = scan->rel;
	Snapshot	snapshot = scan->snapshot;
	ZSUndoRecPtr recent_oldest_undo = scan->recent_oldest_undo;
	ZSUndoRec  *undorec;
	bool		is_deleted;

	Assert (snapshot->snapshot_type == SNAPSHOT_SELF);

	is_deleted = (item->t_flags & (ZSBT_UPDATED | ZSBT_DELETED)) != 0;

	if (item->t_undo_ptr.counter < recent_oldest_undo.counter)
	{
		if (!is_deleted)
			return true;
		else
			return false;
	}

	/* have to fetch the UNDO record */
	undorec = zsundo_fetch(rel, item->t_undo_ptr);

	if (!is_deleted)
	{
		/* Inserted tuple */
		Assert(undorec->type == ZSUNDO_TYPE_INSERT);

		if (TransactionIdIsCurrentTransactionId(undorec->xid))
			return true;		/* inserted by me */
		else if (TransactionIdIsInProgress(undorec->xid))
			return false;
		else if (TransactionIdDidCommit(undorec->xid))
			return true;
		else
		{
			/* it must have aborted or crashed */
			return false;
		}
	}
	else
	{
		/* deleted or updated-away tuple */
		Assert(undorec->type == ZSUNDO_TYPE_DELETE ||
			   undorec->type == ZSUNDO_TYPE_UPDATE);

		if (TransactionIdIsCurrentTransactionId(undorec->xid))
		{
			/* deleted by me */
			return false;
		}

		if (TransactionIdIsInProgress(undorec->xid))
			return true;

		if (!TransactionIdDidCommit(undorec->xid))
		{
			/* deleter aborted or crashed */
			return true;
		}

		return false;
	}
}

/*
 * Like HeapTupleSatisfiesDirty
 */
static bool
zs_SatisfiesDirty(ZSBtreeScan *scan, ZSUncompressedBtreeItem *item)
{
	Relation	rel = scan->rel;
	Snapshot	snapshot = scan->snapshot;
	ZSUndoRecPtr recent_oldest_undo = scan->recent_oldest_undo;
	ZSUndoRecPtr undo_ptr;
	ZSUndoRec  *undorec;
	bool		is_deleted;

	Assert (snapshot->snapshot_type == SNAPSHOT_DIRTY);

	snapshot->xmin = snapshot->xmax = InvalidTransactionId;
	snapshot->speculativeToken = 0;

	is_deleted = (item->t_flags & (ZSBT_UPDATED | ZSBT_DELETED)) != 0;
	undo_ptr = item->t_undo_ptr;

fetch_undo_record:
	if (undo_ptr.counter < recent_oldest_undo.counter)
	{
		if (!is_deleted)
			return true;
		else
			return false;
	}

	/* have to fetch the UNDO record */
	undorec = zsundo_fetch(rel, undo_ptr);

	if (!is_deleted)
	{
		/* Inserted tuple */
		if (undorec->type == ZSUNDO_TYPE_INSERT)
		{
			if (TransactionIdIsCurrentTransactionId(undorec->xid))
				return true;		/* inserted by me */
			else if (TransactionIdIsInProgress(undorec->xid))
			{
				snapshot->xmin = undorec->xid;
				return true;
			}
			else if (TransactionIdDidCommit(undorec->xid))
			{
				return true;
			}
			else
			{
				/* it must have aborted or crashed */
				return false;
			}
		}
		else if (undorec->type == ZSUNDO_TYPE_TUPLE_LOCK)
		{
			/* locked tuple. */
			/* look at the previous UNDO record to find the insert record */
			undo_ptr = ((ZSUndoRec_TupleLock *) undorec)->prevundorec;
			goto fetch_undo_record;
		}
		else
			elog(ERROR, "unexpected UNDO record type: %d", undorec->type);
	}
	else
	{
		/* deleted or updated-away tuple */
		Assert(undorec->type == ZSUNDO_TYPE_DELETE ||
			   undorec->type == ZSUNDO_TYPE_UPDATE);

		if (TransactionIdIsCurrentTransactionId(undorec->xid))
		{
			/* deleted by me */
			return false;
		}

		if (TransactionIdIsInProgress(undorec->xid))
		{
			snapshot->xmax = undorec->xid;
			return true;
		}

		if (!TransactionIdDidCommit(undorec->xid))
		{
			/* deleter aborted or crashed */
			return true;
		}

		return false;
	}
}

/*
 * True if tuple might be visible to some transaction; false if it's
 * surely dead to everyone, ie, vacuumable.
 */
static bool
zs_SatisfiesNonVacuumable(ZSBtreeScan *scan, ZSUncompressedBtreeItem *item)
{
	Snapshot	snapshot = scan->snapshot;
	ZSUndoRecPtr recent_oldest_undo = scan->recent_oldest_undo;
	bool		is_deleted;

	Assert (snapshot->snapshot_type == SNAPSHOT_NON_VACUUMABLE);

	is_deleted = (item->t_flags & (ZSBT_UPDATED | ZSBT_DELETED)) != 0;

	if (item->t_undo_ptr.counter < recent_oldest_undo.counter)
	{
		if (!is_deleted)
			return true;
		else
			return false;
	}

	/* we could fetch the UNDO record and inspect closer, but we won't bother */
	return true;
}

/*
 * Like HeapTupleSatisfiesVisibility
 */
bool
zs_SatisfiesVisibility(ZSBtreeScan *scan, ZSUncompressedBtreeItem *item)
{
	/*
	 * If we don't have a cached oldest-undo-ptr value yet, fetch it
	 * from the metapage. (TODO: In the final EDB's UNDO-log implementation
	 * this will probably be just a global variable, like RecentGlobalXmin.)
	 */
	if (scan->recent_oldest_undo.counter == 0)
		scan->recent_oldest_undo = zsundo_get_oldest_undo_ptr(scan->rel);

	/* dead items are never considered visible. */
	if ((item->t_flags & ZSBT_DEAD) != 0)
		return false;

	switch (scan->snapshot->snapshot_type)
	{
		case SNAPSHOT_MVCC:
			return zs_SatisfiesMVCC(scan, item);

		case SNAPSHOT_SELF:
			return zs_SatisfiesSelf(scan, item);

		case SNAPSHOT_ANY:
			return zs_SatisfiesAny(scan, item);

		case SNAPSHOT_TOAST:
			elog(ERROR, "SnapshotToast not implemented in zedstore");
			break;

		case SNAPSHOT_DIRTY:
			return zs_SatisfiesDirty(scan, item);

		case SNAPSHOT_HISTORIC_MVCC:
			elog(ERROR, "SnapshotHistoricMVCC not implemented in zedstore yet");
			break;

		case SNAPSHOT_NON_VACUUMABLE:
			return zs_SatisfiesNonVacuumable(scan, item);
	}

	return false;				/* keep compiler quiet */
}
