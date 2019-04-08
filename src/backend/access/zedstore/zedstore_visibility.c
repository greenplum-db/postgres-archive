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
 */
TM_Result
zs_SatisfiesUpdate(ZSBtreeScan *scan, ZSUncompressedBtreeItem *item)
{
	Relation	rel = scan->rel;
	Snapshot	snapshot = scan->snapshot;
	ZSUndoRecPtr recent_oldest_undo;
	bool		is_deleted;
	ZSUndoRec  *undorec;

	if (scan->recent_oldest_undo.counter == 0)
		scan->recent_oldest_undo = zsundo_get_oldest_undo_ptr(scan->rel);
	recent_oldest_undo = scan->recent_oldest_undo;

	is_deleted = (item->t_flags & (ZSBT_UPDATED | ZSBT_DELETED)) != 0;

	/* Is it visible? */
	if (item->t_undo_ptr.counter < recent_oldest_undo.counter)
	{
		if (is_deleted)
		{
			/* this probably shouldn't happen.. */
			return  TM_Invisible;
		}
		else
			return  TM_Ok;
	}

	/* have to fetch the UNDO record */
	undorec = zsundo_fetch(rel, item->t_undo_ptr);

	if (!is_deleted)
	{
		/* Inserted tuple */
		Assert(undorec->type == ZSUNDO_TYPE_INSERT ||
			   undorec->type == ZSUNDO_TYPE_UPDATE);

		if (TransactionIdIsCurrentTransactionId(undorec->xid))
		{
			if (undorec->cid >= snapshot->curcid)
				return TM_Invisible;	/* inserted after scan started */
			return TM_Ok;
		}
		else if (TransactionIdIsInProgress(undorec->xid))
			return TM_Invisible;		/* inserter has not committed yet */
		else if (TransactionIdDidCommit(undorec->xid))
			return TM_Ok;
		else
		{
			/* it must have aborted or crashed */
			return TM_Invisible;
		}
	}
	else
	{
		/* deleted or updated-away tuple */
		Assert(undorec->type == ZSUNDO_TYPE_DELETE ||
			   undorec->type == ZSUNDO_TYPE_UPDATE);

		if (TransactionIdIsCurrentTransactionId(undorec->xid))
		{
			if (undorec->cid >= snapshot->curcid)
				return TM_SelfModified;	/* deleted/updated after scan started */
			else
				return TM_Invisible;	/* deleted before scan started */
		}

		if (TransactionIdIsInProgress(undorec->xid))
			return TM_BeingModified;

		if (!TransactionIdDidCommit(undorec->xid))
		{
			/* deleter must have aborted or crashed */
			return TM_Ok;
		}

		if (undorec->type == ZSUNDO_TYPE_DELETE)
			return TM_Deleted;
		else
			return TM_Updated;
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
	ZSUndoRec  *undorec;
	bool		is_deleted;

	Assert (snapshot->snapshot_type == SNAPSHOT_MVCC);

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
		Assert(undorec->type == ZSUNDO_TYPE_INSERT ||
			   undorec->type == ZSUNDO_TYPE_UPDATE);

		return xid_is_visible(snapshot, undorec->xid, undorec->cid);
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

			if (undorec->type == ZSUNDO_TYPE_DELETE)
				prevptr = ((ZSUndoRec_Delete *) undorec)->prevundorec;
			else
				prevptr = ((ZSUndoRec_Update *) undorec)->prevundorec;

			if (prevptr.counter < recent_oldest_undo.counter)
				return true;

			undorec = zsundo_fetch(rel, prevptr);

			Assert(undorec->type == ZSUNDO_TYPE_INSERT ||
				   undorec->type == ZSUNDO_TYPE_UPDATE);
			if (xid_is_visible(snapshot, undorec->xid, undorec->cid))
				return true;	/* we can see the insert, but not the delete */
			else
				return false;	/* we cannot see the insert */
		}
	}
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
			break;
		case SNAPSHOT_SELF:
			elog(ERROR, "SnapshotSelf not implemented in zedstore yet");
			break;
		case SNAPSHOT_ANY:
			return zs_SatisfiesAny(scan, item);
			break;
		case SNAPSHOT_TOAST:
			elog(ERROR, "SnapshotToast not implemented in zedstore");
			break;
		case SNAPSHOT_DIRTY:
			elog(ERROR, "SnapshotDirty not implemented in zedstore yet");
			break;
		case SNAPSHOT_HISTORIC_MVCC:
			elog(ERROR, "SnapshotHistoricMVCC not implemented in zedstore yet");
			break;
		case SNAPSHOT_NON_VACUUMABLE:
			elog(ERROR, "SnapshotNonVacuumable not implemented in zedstore yet");
			break;
	}

	return false;				/* keep compiler quiet */
}
