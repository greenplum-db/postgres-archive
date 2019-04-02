/*
 * zedstore_toast.c
 *		Routines for Toasting oversized tuples in Zedstore
 *
 *
 * Copyright (c) 2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/access/zedstore/zedstore_toast.c
 */
#include "postgres.h"

#include "access/zedstore_compression.h"
#include "access/zedstore_internal.h"
#include "storage/bufmgr.h"
#include "utils/datum.h"
#include "utils/rel.h"

/*
 * Toast a datum, inside the ZedStore file.
 *
 * This is similar to regular toasting, but instead of using a separate index and
 * heap, the datum is stored within the same ZedStore file as all the btrees and
 * stuff. A chain of "toast-pages" is allocated for the datum, and each page is filled
 * with as much of the datum as possible.
 *
 *
 * Note: You must call zedstore_toast_finish() after this,
 * to set the TID in the toast-chain's first block. Otherwise, it's considered recyclable.
 */
Datum
zedstore_toast_datum(Relation rel, AttrNumber attno, Datum value)
{
	varatt_zs_toastptr *toastptr;
	BlockNumber firstblk = InvalidBlockNumber;
	Buffer		buf = InvalidBuffer;
	Page		page;
	ZSToastPageOpaque *opaque;
	Buffer		prevbuf = InvalidBuffer;
	ZSToastPageOpaque *prevopaque = NULL;
	char	   *ptr;
	int32		total_size;
	int32		offset;

	/* TODO: try to compress it in place first. Maybe just call toast_compress_datum? */

	/*
	 * If that doesn't reduce it enough, allocate a toast page
	 * for it.
	 */
	ptr = VARDATA_ANY(value);
	total_size = VARSIZE_ANY_EXHDR(value);
	offset = 0;

	while (total_size - offset > 0)
	{
		Size		thisbytes;

		buf = zs_getnewbuf(rel);
		if (prevbuf == InvalidBuffer)
			firstblk = BufferGetBlockNumber(buf);

		page = BufferGetPage(buf);
		PageInit(page, BLCKSZ, sizeof(ZSToastPageOpaque));

		thisbytes = Min(total_size - offset, PageGetExactFreeSpace(page));

		opaque = (ZSToastPageOpaque *) PageGetSpecialPointer(page);
		opaque->zs_attno = attno;
		ItemPointerSetInvalid(&opaque->zs_tid);
		opaque->zs_total_size = total_size;
		opaque->zs_slice_offset = offset;
		opaque->zs_prev = BufferIsValid(prevbuf) ? BufferGetBlockNumber(prevbuf) : InvalidBlockNumber;
		opaque->zs_next = InvalidBlockNumber;
		opaque->zs_flags = 0;
		opaque->zs_page_id = ZS_TOAST_PAGE_ID;

		memcpy((char *) page + SizeOfPageHeaderData, ptr, thisbytes);
		((PageHeader) page)->pd_lower += thisbytes;
		ptr += thisbytes;
		offset += thisbytes;

		if (prevbuf != InvalidBuffer)
		{
			prevopaque->zs_next = BufferGetBlockNumber(buf);
			MarkBufferDirty(prevbuf);
		}

		/* TODO: WAL-log */
		MarkBufferDirty(buf);

		if (prevbuf != InvalidBuffer)
			UnlockReleaseBuffer(prevbuf);
		prevbuf = buf;
		prevopaque = opaque;
	}

	UnlockReleaseBuffer(buf);

	toastptr = palloc0(sizeof(varatt_zs_toastptr));
	SET_VARTAG_1B_E(toastptr, VARTAG_ZEDSTORE);
	toastptr->zst_block = firstblk;

	return PointerGetDatum(toastptr);
}

void
zedstore_toast_finish(Relation rel, AttrNumber attno, Datum toasted, ItemPointerData tid)
{
	varatt_zs_toastptr *toastptr = (varatt_zs_toastptr *) DatumGetPointer(toasted);
	Buffer		buf;
	Page		page;
	ZSToastPageOpaque *opaque;

	Assert(toastptr->va_tag == VARTAG_ZEDSTORE);

	buf = ReadBuffer(rel, toastptr->zst_block);
	page = BufferGetPage(buf);
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
	opaque = (ZSToastPageOpaque *) PageGetSpecialPointer(page);

	Assert(!ItemPointerIsValid(&opaque->zs_tid));
	Assert(opaque->zs_attno == attno);
	Assert(opaque->zs_prev == InvalidBlockNumber);

	opaque->zs_tid = tid;

	/* TODO: WAL-log */
	MarkBufferDirty(buf);

	UnlockReleaseBuffer(buf);
}

Datum
zedstore_toast_flatten(Relation rel, AttrNumber attno, ItemPointerData tid, Datum toasted)
{
	varatt_zs_toastptr *toastptr = (varatt_zs_toastptr *) DatumGetPointer(toasted);
	BlockNumber	nextblk;
	BlockNumber	prevblk;
	char	   *result = NULL;
	char	   *ptr = NULL;
	int32		total_size = 0;

	Assert(toastptr->va_tag == VARTAG_ZEDSTORE);

	prevblk = InvalidBlockNumber;
	nextblk = toastptr->zst_block;

	while (nextblk != InvalidBlockNumber)
	{
		Buffer		buf;
		Page		page;
		ZSToastPageOpaque *opaque;
		uint32		size;

		buf = ReadBuffer(rel, nextblk);
		page = BufferGetPage(buf);
		LockBuffer(buf, BUFFER_LOCK_SHARE);

		opaque = (ZSToastPageOpaque *) PageGetSpecialPointer(page);

		Assert(opaque->zs_attno == attno);
		Assert(opaque->zs_prev == prevblk);

		if (prevblk == InvalidBlockNumber)
		{
			Assert(ItemPointerEquals(&opaque->zs_tid, &tid));

			total_size = opaque->zs_total_size;

			result = palloc(total_size + VARHDRSZ);
			SET_VARSIZE(result, total_size + VARHDRSZ);
			ptr = result + VARHDRSZ;
		}

		size = ((PageHeader) page)->pd_lower - SizeOfPageHeaderData;
		memcpy(ptr, (char *) page + SizeOfPageHeaderData, size);
		ptr += size;

		prevblk = nextblk;
		nextblk = opaque->zs_next;
		UnlockReleaseBuffer(buf);
	}
	Assert(total_size > 0);
	Assert(ptr == result + total_size + VARHDRSZ);

	return PointerGetDatum(result);
}
