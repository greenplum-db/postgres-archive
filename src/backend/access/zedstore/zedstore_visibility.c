/*
 * zedstore_visibility.c
 *		Routines for MVCC in Zedstore
 *
 *
 * Copyright (c) 2019, PostgreSQL Global Development Group
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
 * This should be used kind of like RecentGlobalXmin, to determine
 * the cutoff point for old UNDO. UNDO records older than this are
 * assumed to be old enough to not be visible to anyone anymore.
 * This speeds up visibility checks for old-enough tuples, and
 * allows trimming old UNDO logs.
 *
 * TODO: but it's not implemented yet.
 */
static ZSUndoRecPtr recent_oldest_undo = {0};

/*
 * Like HeapTupleSatisfiesUpdate.
 */
TM_Result
zs_SatisfiesUpdate(Relation rel, ZSBtreeItem *item, Snapshot snapshot)
{
	/* Is it visible? */
	if (item->t_undo_ptr.counter < recent_oldest_undo.counter)
	{
		if ((item->t_flags & (ZSBT_DELETED | ZSBT_UPDATED)) == 0)
		{
			/* this probably shouldn't happen.. */
			return  TM_Invisible;
		}
		else
			return  TM_Ok;
	}
	else
	{
		/* have to fetch the UNDO record */
		ZSUndoRec *undorec;

		undorec = zsundo_fetch(rel, item->t_undo_ptr);

		if (undorec->type == ZSUNDO_TYPE_INSERT ||
			(undorec->type == ZSUNDO_TYPE_UPDATE && (item->t_flags & ZSBT_UPDATED) == 0))
		{
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
		else if (undorec->type == ZSUNDO_TYPE_DELETE ||
				 (undorec->type == ZSUNDO_TYPE_UPDATE && (item->t_flags & ZSBT_UPDATED) != 0))
		{
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
		else
			elog(ERROR, "unknown undo record type %d", undorec->type);
	}
}

/*
 * Like HeapTupleSatisfiesAny
 */
static bool
zs_SatisfiesAny(Relation rel, ZSBtreeItem *item, Snapshot snapshot)
{
	return true;
}

/*
 * Like HeapTupleSatisfiesMVCC
 */
static bool
zs_SatisfiesMVCC(Relation rel, ZSBtreeItem *item, Snapshot snapshot)
{
	ZSUndoRec *undorec;

	Assert (snapshot->snapshot_type == SNAPSHOT_MVCC);

	if (item->t_undo_ptr.counter < recent_oldest_undo.counter)
	{
		if ((item->t_flags & (ZSBT_DELETED | ZSBT_UPDATED)) == 0)
			return true;
		else
			return false;
	}

	/* have to fetch the UNDO record */
	undorec = zsundo_fetch(rel, item->t_undo_ptr);

	if (undorec->type == ZSUNDO_TYPE_INSERT ||
		(undorec->type == ZSUNDO_TYPE_UPDATE && (item->t_flags & ZSBT_UPDATED) == 0))
	{
		if (TransactionIdIsCurrentTransactionId(undorec->xid))
		{
			if (undorec->cid >= snapshot->curcid)
				return false;	/* inserted after scan started */
			return true;
		}
		else if (XidInMVCCSnapshot(undorec->xid, snapshot))
			return false;
		else if (TransactionIdDidCommit(undorec->xid))
			return true;
		else
		{
			/* it must have aborted or crashed */
			return false;
		}
	}
	else if (undorec->type == ZSUNDO_TYPE_DELETE ||
			 (undorec->type == ZSUNDO_TYPE_UPDATE && (item->t_flags & ZSBT_UPDATED) != 0))
	{
		if (TransactionIdIsCurrentTransactionId(undorec->xid))
		{
			if (undorec->cid >= snapshot->curcid)
				return true;	/* deleted after scan started */
			else
				return false;	/* deleted befor scan started */
		}
		else if (XidInMVCCSnapshot(undorec->xid, snapshot))
			return true;
		else if (!TransactionIdDidCommit(undorec->xid))
		{
			/* it must have aborted or crashed */
			return true;
		}
		/* deleting transaction committed and is visible to us */
		return false;
	}
	else
		elog(ERROR, "unknown undo record type");
}

/*
 * Like HeapTupleSatisfiesVisibility
 */
bool
zs_SatisfiesVisibility(Relation rel, ZSBtreeItem *item, Snapshot snapshot)
{
	switch (snapshot->snapshot_type)
	{
		case SNAPSHOT_MVCC:
			return zs_SatisfiesMVCC(rel, item, snapshot);
			break;
		case SNAPSHOT_SELF:
			elog(ERROR, "SnapshotSelf not implemented in zedstore yet");
			break;
		case SNAPSHOT_ANY:
			return zs_SatisfiesAny(rel, item, snapshot);
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
