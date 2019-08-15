/*
 * zedstore_undolog.h
 *		internal declarations for ZedStore undo logging
 *
 * Copyright (c) 2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/include/access/zedstore_undolog.h
 */
#ifndef ZEDSTORE_UNDOLOG_H
#define ZEDSTORE_UNDOLOG_H

#include "storage/buf.h"
#include "storage/off.h"
#include "utils/relcache.h"

// fixme: arbitrary
#define MaxUndoRecordSize		(BLCKSZ / 2)

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
	int32		offset;		/* int16 would suffice, but avoid padding */
} ZSUndoRecPtr;

/* TODO: assert that blkno and offset match, too, if counter matches */
#define ZSUndoRecPtrEquals(a, b) ((a).counter == (b).counter)

typedef struct
{
	BlockNumber	next;
	ZSUndoRecPtr first_undorecptr;	/* note: this is set even if the page is empty! */
	ZSUndoRecPtr last_undorecptr;
	uint16		padding0;			/* padding, to put zs_page_id last */
	uint16		padding1;			/* padding, to put zs_page_id last */
	uint16		padding2;			/* padding, to put zs_page_id last */
	uint16		zs_page_id; /* ZS_UNDO_PAGE_ID */
} ZSUndoPageOpaque;

/*
 * "invalid" undo pointer. The value is chosen so that an invalid pointer
 * is less than any real UNDO pointer value. Therefore, a tuple with an
 * invalid UNDO pointer is considered visible to everyone.
 */
static const ZSUndoRecPtr InvalidUndoPtr = {
	.counter = 0,
	.blkno = InvalidBlockNumber,
	.offset = 0
};

/*
 * A special value used on TID items, to mean that a tuple is not visible to
 * anyone
 */
static const ZSUndoRecPtr DeadUndoPtr = {
	.counter = 1,
	.blkno = InvalidBlockNumber,
	.offset = 0
};

static inline bool
IsZSUndoRecPtrValid(ZSUndoRecPtr *uptr)
{
	return uptr->counter != 0;
}

/*
 * zs_undo_reservation represents a piece of UNDO log that has been reserved for
 * inserting a new UNDO record, but the UNDO record hasn't been written yet.
 */
typedef struct
{
	Buffer		undobuf;
	ZSUndoRecPtr undorecptr;
	size_t		length;

	char	   *ptr;
} zs_undo_reservation;

/* prototypes for functions in zedstore_undolog.c */
extern void zsundo_insert_reserve(Relation rel, size_t size, zs_undo_reservation *reservation_p);
extern void zsundo_insert_finish(zs_undo_reservation *reservation);

extern char *zsundo_fetch(Relation rel, ZSUndoRecPtr undoptr, Buffer *buf_p, int lockmode, bool missing_ok);

extern void zsundo_discard(Relation rel, ZSUndoRecPtr oldest_undorecptr, BlockNumber oldest_undopage, List *unused_pages);

extern void zsundo_newpage_redo(XLogReaderState *record);
extern void zsundo_discard_redo(XLogReaderState *record);

#endif							/* ZEDSTORE_UNDO_H */
