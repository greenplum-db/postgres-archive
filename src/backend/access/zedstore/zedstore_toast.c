/*
 * zedstore_toast.c
 *		Routines for Toasting oversized tuples in Zedstore
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/zedstore/zedstore_toast.c
 */
#include "postgres.h"

#include "access/toast_internals.h"
#include "access/xlogutils.h"
#include "access/zedstore_internal.h"
#include "access/zedstore_wal.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "utils/datum.h"
#include "utils/rel.h"

static void zstoast_wal_log_newpage(Buffer prevbuf, Buffer buf, zstid tid, AttrNumber attno,
									int offset, int32 total_size);

/*
 * Toast a datum, inside the ZedStore file.
 *
 * This is similar to regular toasting, but instead of using a separate index and
 * heap, the datum is stored within the same ZedStore file as all the btrees and
 * stuff. A chain of "toast-pages" is allocated for the datum, and each page is filled
 * with as much of the datum as possible.
 */
Datum
zedstore_toast_datum(Relation rel, AttrNumber attno, Datum value, zstid tid)
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
	int32		decompressed_size = 0;
	int32		offset;
	bool		is_compressed;
	bool		is_first;
	Datum		toasted_datum;

	Assert(tid != InvalidZSTid);

	/*
	 * TID btree will always be inserted first, so there must be > 0 blocks
	 */
	Assert(RelationGetNumberOfBlocks(rel) != 0);

	if (VARATT_IS_COMPRESSED(value))
		toasted_datum = value;
	else
		toasted_datum = toast_compress_datum(value);
	if (DatumGetPointer(toasted_datum) != NULL)
	{
		/*
		 * If the compressed datum can be stored inline, return the datum
		 * directly.
		 */
		if (VARSIZE_ANY(toasted_datum) <= MaxZedStoreDatumSize)
		{
			return toasted_datum;
		}

		is_compressed     = true;
		decompressed_size = TOAST_COMPRESS_RAWSIZE(toasted_datum);
		ptr               = TOAST_COMPRESS_RAWDATA(toasted_datum);
		total_size = VARSIZE_ANY(toasted_datum) - TOAST_COMPRESS_HDRSZ;
	}
	else
	{
		/*
		 * If the compression doesn't reduce the size enough, allocate a
		 * toast page for it.
		 */
		is_compressed     = false;
		ptr = VARDATA_ANY(value);
		total_size = VARSIZE_ANY_EXHDR(value);
	}


	offset = 0;
	is_first = true;
	while (total_size - offset > 0)
	{
		Size		thisbytes;

		buf = zspage_getnewbuf(rel, ZS_INVALID_ATTRIBUTE_NUM);
		if (prevbuf == InvalidBuffer)
			firstblk = BufferGetBlockNumber(buf);

		START_CRIT_SECTION();

		page = BufferGetPage(buf);
		PageInit(page, BLCKSZ, sizeof(ZSToastPageOpaque));

		thisbytes = Min(total_size - offset, PageGetExactFreeSpace(page));

		opaque = (ZSToastPageOpaque *) PageGetSpecialPointer(page);
		opaque->zs_tid = tid;
		opaque->zs_attno = attno;
		opaque->zs_total_size = total_size;
		opaque->zs_decompressed_size = decompressed_size;
		opaque->zs_is_compressed = is_compressed;
		opaque->zs_slice_offset = offset;
		opaque->zs_prev = is_first ? InvalidBlockNumber : BufferGetBlockNumber(prevbuf);
		opaque->zs_next = InvalidBlockNumber;
		opaque->zs_flags = 0;
		opaque->zs_page_id = ZS_TOAST_PAGE_ID;

		memcpy((char *) page + SizeOfPageHeaderData, ptr, thisbytes);
		((PageHeader) page)->pd_lower += thisbytes;

		if (!is_first)
		{
			prevopaque->zs_next = BufferGetBlockNumber(buf);
			MarkBufferDirty(prevbuf);
		}

		MarkBufferDirty(buf);

		if (RelationNeedsWAL(rel))
			zstoast_wal_log_newpage(prevbuf, buf, tid, attno, offset, total_size);

		END_CRIT_SECTION();

		if (prevbuf != InvalidBuffer)
			UnlockReleaseBuffer(prevbuf);
		ptr += thisbytes;
		offset += thisbytes;
		prevbuf = buf;
		prevopaque = opaque;
		is_first = false;
	}

	UnlockReleaseBuffer(buf);

	toastptr = palloc0(sizeof(varatt_zs_toastptr));
	SET_VARTAG_1B_E(toastptr, VARTAG_ZEDSTORE);
	toastptr->zst_block = firstblk;

	return PointerGetDatum(toastptr);
}

Datum
zedstore_toast_flatten(Relation rel, AttrNumber attno, zstid tid, Datum toasted)
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
			Assert(opaque->zs_tid == tid);

			total_size = opaque->zs_total_size;

			if(opaque->zs_is_compressed)
			{
				result = palloc(total_size + TOAST_COMPRESS_HDRSZ);

				TOAST_COMPRESS_SET_RAWSIZE(result, opaque->zs_decompressed_size);
				SET_VARSIZE_COMPRESSED(result, total_size + TOAST_COMPRESS_HDRSZ);
				ptr = result + TOAST_COMPRESS_HDRSZ;
			}
			else
			{
				result = palloc(total_size + VARHDRSZ);
				SET_VARSIZE(result, total_size + VARHDRSZ);
				ptr = result + VARHDRSZ;
			}
		}

		size = ((PageHeader) page)->pd_lower - SizeOfPageHeaderData;
		memcpy(ptr, (char *) page + SizeOfPageHeaderData, size);
		ptr += size;

		prevblk = nextblk;
		nextblk = opaque->zs_next;
		UnlockReleaseBuffer(buf);
	}
	Assert(total_size > 0);
	Assert(ptr == result + VARSIZE_ANY(result));

	return PointerGetDatum(result);
}

void
zedstore_toast_delete(Relation rel, Form_pg_attribute attr, zstid tid, BlockNumber blkno)
{
	BlockNumber nextblk;

	nextblk = blkno;

	while (nextblk != InvalidBlockNumber)
	{
		Buffer		buf;
		Page		page;
		ZSToastPageOpaque *opaque;

		buf = ReadBuffer(rel, nextblk);
		page = BufferGetPage(buf);
		LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

		opaque = (ZSToastPageOpaque *) PageGetSpecialPointer(page);

		if (opaque->zs_tid != tid)
		{
			UnlockReleaseBuffer(buf);
			break;
		}

		Assert(opaque->zs_attno == attr->attnum);

		nextblk = opaque->zs_next;
		zspage_delete_page(rel, buf, InvalidBuffer, ZS_INVALID_ATTRIBUTE_NUM);
		UnlockReleaseBuffer(buf);
	}
}

static void
zstoast_wal_log_newpage(Buffer prevbuf, Buffer buf, zstid tid, AttrNumber attno,
						int offset, int32 total_size)
{
	wal_zedstore_toast_newpage xlrec;
	XLogRecPtr recptr;

	Assert(offset <= total_size);

	xlrec.tid = tid;
	xlrec.attno = attno;
	xlrec.offset = offset;
	xlrec.total_size = total_size;

	XLogBeginInsert();
	XLogRegisterData((char *) &xlrec, SizeOfZSWalToastNewPage);

	/*
	 * It is easier to just force a full-page image, than WAL-log data. That
	 * means that the information in the wal_zedstore_toast_newpage struct isn't
	 * really necessary, but keep it for now, for the benefit of debugging with
	 * pg_waldump.
	 */
	XLogRegisterBuffer(0, buf, REGBUF_FORCE_IMAGE | REGBUF_STANDARD);

	if (BufferIsValid(prevbuf))
		XLogRegisterBuffer(1, prevbuf, REGBUF_STANDARD);

	recptr = XLogInsert(RM_ZEDSTORE_ID, WAL_ZEDSTORE_TOAST_NEWPAGE);

	PageSetLSN(BufferGetPage(buf), recptr);
	if (BufferIsValid(prevbuf))
		PageSetLSN(BufferGetPage(prevbuf), recptr);
}

void
zstoast_newpage_redo(XLogReaderState *record)
{
	XLogRecPtr	lsn = record->EndRecPtr;
#if UNUSED
	wal_zedstore_toast_newpage *xlrec = (wal_zedstore_toast_newpage *) XLogRecGetData(record);
#endif
	BlockNumber	blkno;
	Buffer		buf;
	Buffer		prevbuf = InvalidBuffer;

	XLogRecGetBlockTag(record, 0, NULL, NULL, &blkno);

	if (XLogReadBufferForRedo(record, 0, &buf) != BLK_RESTORED)
		elog(ERROR, "zedstore toast newpage WAL record did not contain a full-page image");

	if (XLogRecHasBlockRef(record, 1))
	{
		if (XLogReadBufferForRedo(record, 1, &prevbuf) == BLK_NEEDS_REDO)
		{
			Page		prevpage = BufferGetPage(prevbuf);
			ZSToastPageOpaque *prevopaque;

			prevopaque = (ZSToastPageOpaque *) PageGetSpecialPointer(prevpage);
			prevopaque->zs_next = BufferGetBlockNumber(buf);

			PageSetLSN(prevpage, lsn);
			MarkBufferDirty(prevbuf);
		}
	}
	else
		prevbuf = InvalidBuffer;

	if (BufferIsValid(prevbuf))
		UnlockReleaseBuffer(prevbuf);
	UnlockReleaseBuffer(buf);
}
