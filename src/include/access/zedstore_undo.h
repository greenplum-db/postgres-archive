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

#include "utils/relcache.h"

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

typedef struct
{
	int16		size;			/* size of this record, including header */
	uint8		type;			/* ZSUNDO_TYPE_* */
	AttrNumber	attno;
	ZSUndoRecPtr undorecptr;
	TransactionId xid;
	CommandId	cid;
	ItemPointerData tid;
} ZSUndoRec;

#define ZSUNDO_TYPE_INSERT		1
#define ZSUNDO_TYPE_DELETE		2
#define ZSUNDO_TYPE_UPDATE		3

/*
 * Type-specific record formats.
 *
 * We store similar info as zheap for INSERT/UPDATE/DELETE. See zheap README.
 */
typedef struct
{
	ZSUndoRec	rec;
} ZSUndoRec_Insert;

typedef struct
{
	ZSUndoRec	rec;

	/*
	 * TODO: It might be good to move the deleted tuple to the undo-log, so
	 * that the space can immediately be reused. But currently, we don't do
	 * that. (or even better, move the old tuple to the undo-log lazily, if
	 * the space is needed for a new insertion, before the old tuple becomes
	 * recyclable.
	 */
} ZSUndoRec_Delete;

typedef struct
{
	ZSUndoRec	rec;

	/* old version of the datum */
	/* TODO: currently, we only do "cold" updates, so the old tuple is
	 * left in the old place. Once we start supporting in-place updates,
	 * the old tuple should be stored here.
	 */
	ItemPointerData otid;
} ZSUndoRec_Update;

typedef struct
{
	BlockNumber	next;
	uint16		zs_page_id; /* ZS_UNDO_PAGE_ID */
} ZSUndoPageOpaque;

/* prototypes for functions in zstore_undo.c */
extern ZSUndoRecPtr zsundo_insert(Relation rel, ZSUndoRec *rec);
extern ZSUndoRec *zsundo_fetch(Relation rel, ZSUndoRecPtr undorecptr);

#endif							/* ZEDSTORE_UNDO_H */