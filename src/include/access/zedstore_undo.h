/*
 * zedstore_undo.h
 *		internal declarations for ZedStore undo logging
 *
 * Copyright (c) 2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/include/access/zedstore_undo.h
 */
#ifndef ZEDSTORE_UNDO_H
#define ZEDSTORE_UNDO_H

#include "commands/vacuum.h"
#include "utils/relcache.h"

/* this must match the definition in zedstore_internal.h */
typedef uint64	zstid;

/*
 * An UNDO-pointer.
 *
 * In the "real" UNDO-logging work from EDB, an UndoRecPtr is only 64 bits.
 * But we make life easier for us, by encoding more information in it.
 *
 * 'counter' is a number that's incremented every time a new undo record is
 * created. It can be used to determine if an undo pointer is too old to be
 * of interest to anyone.
 *
 * 'blkno' and 'offset' are the physical location of the UNDO record. They
 * can be used to easily fetch a given record.
 */
typedef struct
{
	uint64		counter;
	BlockNumber blkno;
	int32		offset;
} ZSUndoRecPtr;

/* TODO: assert that blkno and offset match, too, if counter matches */
#define ZSUndoRecPtrEquals(a, b) ((a).counter == (b).counter)

typedef struct
{
	int16		size;			/* size of this record, including header */
	uint8		type;			/* ZSUNDO_TYPE_* */
	ZSUndoRecPtr undorecptr;
	TransactionId xid;
	CommandId	cid;
	zstid		tid;
} ZSUndoRec;

#define ZSUNDO_TYPE_INSERT		1
#define ZSUNDO_TYPE_DELETE		2
#define ZSUNDO_TYPE_UPDATE		3
#define ZSUNDO_TYPE_TUPLE_LOCK	4

/*
 * Type-specific record formats.
 *
 * We store similar info as zheap for INSERT/UPDATE/DELETE. See zheap README.
 */
typedef struct
{
	ZSUndoRec	rec;
	zstid       endtid; /* inclusive */
} ZSUndoRec_Insert;

typedef struct
{
	ZSUndoRec	rec;

	/*
	 * UNDO-record of the inserter. This is needed if a row is inserted, and
	 * deleted, and there are some snapshots active don't don't consider even
	 * the insertion as visible.
	 */
	ZSUndoRecPtr prevundorec;

	/*
	 * TODO: It might be good to move the deleted tuple to the undo-log, so
	 * that the space can immediately be reused. But currently, we don't do
	 * that. (or even better, move the old tuple to the undo-log lazily, if
	 * the space is needed for a new insertion, before the old tuple becomes
	 * recyclable.
	 */
} ZSUndoRec_Delete;

/*
 * This is used for an UPDATE, to mark the old tuple version as updated.
 * It's the same as a deletion, except this stores the TID of the new tuple
 * version, so it can be followed in READ COMMITTED mode.
 *
 * The ZSUndoRec_Insert record is used for the insertion of the new tuple
 * version.
 */
typedef struct
{
	ZSUndoRec	rec;

	/* Like in ZSUndoRec_Delete. */
	ZSUndoRecPtr prevundorec;

	bool		key_update;		/* were key columns updated?
								 * (for conflicting with FOR KEY SHARE) */

	zstid		newtid;

} ZSUndoRec_Update;

/*
 * This is used when a tuple is locked e.g. with SELECT FOR UPDATE.
 * The tuple isn't really changed in any way, but the undo record gives
 * a place to store the XID of the locking transaction.
 *
 * In case of a FOR SHARE lock, there can be multiple lockers. Each locker
 * will create a new undo record with its own XID that points to the previous
 * record. So the records will form a chain, leading finally to the insertion
 * record (or beyond the UNDO horizon, meaning the tuple's insertion is visible
 * to everyone)
 */
typedef struct
{
	ZSUndoRec	rec;

	/*
	 * XXX: Is it OK to store this on disk? The enum values could change. Then
	 * again, no one should care about old locks that were acquired before
	 * last restart. Except with two-phase commit prepared transactions.
	 */
	LockTupleMode	lockmode;

	/* Like in ZSUndoRec_Delete. */
	ZSUndoRecPtr prevundorec;
} ZSUndoRec_TupleLock;

typedef struct
{
	BlockNumber	next;
	uint16		padding;			/* padding, to put zs_page_id last */
	uint16		zs_page_id; /* ZS_UNDO_PAGE_ID */
} ZSUndoPageOpaque;

static inline void
ZSUndoRecPtrInitialize(ZSUndoRecPtr *uptr)
{
	uptr->blkno = InvalidBlockNumber;
	uptr->offset = InvalidOffsetNumber;
	uptr->counter = 0;
}

static inline bool
IsZSUndoRecPtrValid(ZSUndoRecPtr *uptr)
{
	return (uptr->blkno != InvalidBlockNumber &&
			uptr->offset != InvalidOffsetNumber);
}

/* prototypes for functions in zstore_undo.c */
extern ZSUndoRecPtr zsundo_insert(Relation rel, ZSUndoRec *rec);
extern ZSUndoRec *zsundo_fetch(Relation rel, ZSUndoRecPtr undorecptr);
extern void zsundo_vacuum(Relation rel, VacuumParams *params, BufferAccessStrategy bstrategy,
			  TransactionId OldestXmin);
extern ZSUndoRecPtr zsundo_get_oldest_undo_ptr(Relation rel);

#endif							/* ZEDSTORE_UNDO_H */
