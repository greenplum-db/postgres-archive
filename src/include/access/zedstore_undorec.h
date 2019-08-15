/*
 * zedstore_undorec.h
 *		Declarations for different kinds of UNDO records in Zedstore.
 *
 * Copyright (c) 2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/include/access/zedstore_undorec.h
 */
#ifndef ZEDSTORE_UNDOREC_H
#define ZEDSTORE_UNDOREC_H

#include "access/zedstore_tid.h"
#include "nodes/lockoptions.h"
#include "storage/buf.h"
#include "storage/off.h"
#include "utils/relcache.h"

#define ZSUNDO_TYPE_INSERT		1
#define ZSUNDO_TYPE_DELETE		2
#define ZSUNDO_TYPE_UPDATE		3
#define ZSUNDO_TYPE_TUPLE_LOCK	4

struct ZSUndoRec
{
	int16		size;			/* size of this record, including header */
	uint8		type;			/* ZSUNDO_TYPE_* */
	ZSUndoRecPtr undorecptr;
	TransactionId xid;
	CommandId	cid;

	/*
	 * UNDO-record of the inserter. This is needed if a row is inserted, and
	 * deleted, and there are some snapshots active don't don't consider even
	 * the insertion as visible.
	 *
	 * This is also used in Insert records, if the record represents the
	 * new tuple version of an UPDATE, rather than an INSERT. It's needed to
	 * dig into possible KEY SHARE locks held on the row, which didn't prevent
	 * the tuple from being updated.
	 */
	ZSUndoRecPtr prevundorec;
};
typedef struct ZSUndoRec ZSUndoRec;

/*
 * Type-specific record formats.
 *
 * We store similar info as zheap for INSERT/UPDATE/DELETE. See zheap README.
 */
typedef struct
{
	ZSUndoRec	rec;
	zstid		firsttid;
	zstid       endtid; /* exclusive */
	uint32		speculative_token; /* Only used for INSERT records */

} ZSUndoRec_Insert;

#define ZSUNDO_NUM_TIDS_PER_DELETE	10

typedef struct
{
	ZSUndoRec	rec;

	bool		changedPart;	/* tuple was moved to a different partition by UPDATE */

	/*
	 * One deletion record can represent deleting up to
	 * ZSUNDO_NUM_TIDS_PER_DELETE tuples. The 'rec.tid' field is unused.
	 */
	uint16		num_tids;
	zstid		tids[ZSUNDO_NUM_TIDS_PER_DELETE];

	/*
	 * TODO: It might be good to move the deleted tuple to the undo-log, so
	 * that the space can immediately be reused. But currently, we don't do
	 * that. Or even better, move the old tuple to the undo-log lazily, if
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

	zstid		oldtid;
	zstid		newtid;

	bool		key_update;		/* were key columns updated?
								 * (for conflicting with FOR KEY SHARE) */

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
	zstid		tid;

	/*
	 * XXX: Is it OK to store this on disk? The enum values could change. Then
	 * again, no one should care about old locks that were acquired before
	 * last restart. Except with two-phase commit prepared transactions.
	 */
	LockTupleMode	lockmode;
} ZSUndoRec_TupleLock;

/*
 * zs_pending_undo_op encapsulates the insertion or modification of an UNDO
 * record. The zsundo_create_* functions don't insert UNDO records directly,
 * because the callers are not in a critical section yet, and may still need
 * to abort. For example, to inserting a new TID to the TID tree, we first
 * construct the UNDO record for the insertion, and then lock the correct
 * TID tree page to insert to. But if e.g. we need to split the TID page,
 * we might still have to error out.
 */
struct zs_pending_undo_op
{
	zs_undo_reservation	reservation;

	bool		is_update;
	/* more data follows (defined as uint64, to force alignment) */
	uint64		payload[FLEXIBLE_ARRAY_MEMBER];
};
typedef struct zs_pending_undo_op  zs_pending_undo_op;

/*
 * These are used in WAL records, to represent insertion or modification
 * of an UNDO record.
 *
 * We use this same record for all UNDO operations. It's a bit wasteful;
 * if an existing UNDO record is modified, we wouldn't need to overwrite
 * the whole record. Also, no need to WAL-log the command ids, because
 * they don't matter after crash/replay.
 */
typedef struct
{
	ZSUndoRecPtr undoptr;
	uint16		length;
	bool		is_update;

	char		payload[FLEXIBLE_ARRAY_MEMBER];
} zs_wal_undo_op;

#define SizeOfZSWalUndoOp	offsetof(zs_wal_undo_op, payload)

/* prototypes for functions in zedstore_undorec.c */
extern struct ZSUndoRec *zsundo_fetch_record(Relation rel, ZSUndoRecPtr undorecptr);

extern zs_pending_undo_op *zsundo_create_for_delete(Relation rel, TransactionId xid, CommandId cid, zstid tid,
													bool changedPart, ZSUndoRecPtr prev_undo_ptr);
extern zs_pending_undo_op *zsundo_create_for_insert(Relation rel, TransactionId xid, CommandId cid,
													zstid tid, int nitems,
													uint32 speculative_token, ZSUndoRecPtr prev_undo_ptr);
extern zs_pending_undo_op *zsundo_create_for_update(Relation rel, TransactionId xid, CommandId cid,
													zstid oldtid, zstid newtid, ZSUndoRecPtr prev_undo_ptr,
													bool key_update);
extern zs_pending_undo_op *zsundo_create_for_tuple_lock(Relation rel, TransactionId xid, CommandId cid,
														zstid tid, LockTupleMode lockmode,
														ZSUndoRecPtr prev_undo_ptr);
extern void zsundo_finish_pending_op(zs_pending_undo_op *pendingop, char *payload);
extern void zsundo_clear_speculative_token(Relation rel, ZSUndoRecPtr undoptr);

extern void XLogRegisterUndoOp(uint8 block_id, zs_pending_undo_op *undo_op);
extern Buffer XLogRedoUndoOp(XLogReaderState *record, uint8 block_id);

struct VacuumParams;
extern void zsundo_vacuum(Relation rel, struct VacuumParams *params, BufferAccessStrategy bstrategy,
			  TransactionId OldestXmin);
extern ZSUndoRecPtr zsundo_get_oldest_undo_ptr(Relation rel);

#endif							/* ZEDSTORE_UNDOREC_H */
