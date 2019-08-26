/*
 * zedstoredesc.c
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/rmgrdesc/zedstoredesc.c
 */
#include "postgres.h"

#include "access/xlogreader.h"
#include "access/zedstore_tid.h"
#include "access/zedstore_wal.h"
#include "lib/stringinfo.h"

void
zedstore_desc(StringInfo buf, XLogReaderState *record)
{
	char	   *rec = XLogRecGetData(record);
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	if (info == WAL_ZEDSTORE_INIT_METAPAGE)
	{
		wal_zedstore_init_metapage *walrec = (wal_zedstore_init_metapage *) rec;

		appendStringInfo(buf, "natts %d", walrec->natts);
	}
	else if (info == WAL_ZEDSTORE_UNDO_NEWPAGE)
	{
		wal_zedstore_undo_newpage *walrec = (wal_zedstore_undo_newpage *) rec;

		appendStringInfo(buf, "first_counter " UINT64_FORMAT, walrec->first_counter);
	}
	else if (info == WAL_ZEDSTORE_UNDO_DISCARD)
	{
		wal_zedstore_undo_discard *walrec = (wal_zedstore_undo_discard *) rec;

		appendStringInfo(buf, "oldest_undorecptr " UINT64_FORMAT ", oldest_undopage %u",
						 walrec->oldest_undorecptr.counter,
						 walrec->oldest_undopage);
	}
	else if (info == WAL_ZEDSTORE_BTREE_NEW_ROOT)
	{
		wal_zedstore_btree_new_root *walrec = (wal_zedstore_btree_new_root *) rec;

		appendStringInfo(buf, "attno %d", walrec->attno);
	}
	else if (info == WAL_ZEDSTORE_TIDLEAF_ADD_ITEMS)
	{
		wal_zedstore_tidleaf_items *walrec = (wal_zedstore_tidleaf_items *) rec;

		appendStringInfo(buf, "%d items, off %d", walrec->nitems, walrec->off);
	}
	else if (info == WAL_ZEDSTORE_TIDLEAF_REPLACE_ITEM)
	{
		wal_zedstore_tidleaf_items *walrec = (wal_zedstore_tidleaf_items *) rec;

		appendStringInfo(buf, "%d items, off %d", walrec->nitems, walrec->off);
	}
	else if (info == WAL_ZEDSTORE_ATTSTREAM_CHANGE)
	{
		wal_zedstore_attstream_change *walrec = (wal_zedstore_attstream_change *) rec;

		if (walrec->is_upper)
			appendStringInfo(buf, "upper stream change");
		else
			appendStringInfo(buf, "lower stream change");
		appendStringInfo(buf, ", new size %d", walrec->new_attstream_size);
	}
	else if (info == WAL_ZEDSTORE_TOAST_NEWPAGE)
	{
		wal_zedstore_toast_newpage *walrec = (wal_zedstore_toast_newpage *) rec;

		appendStringInfo(buf, "tid (%u/%d), attno %d, offset %d/%d",
						 ZSTidGetBlockNumber(walrec->tid), ZSTidGetOffsetNumber(walrec->tid),
						 walrec->attno, walrec->offset, walrec->total_size);
	}
	else if (info == WAL_ZEDSTORE_FPM_DELETE_PAGE)
	{
		wal_zedstore_fpm_delete_page *walrec = (wal_zedstore_fpm_delete_page *) rec;

		appendStringInfo(buf, "nextblkno %u", walrec->next_free_blkno);
	}
	else if (info == WAL_ZEDSTORE_FPM_REUSE_PAGE)
	{
		wal_zedstore_fpm_reuse_page *walrec = (wal_zedstore_fpm_reuse_page *) rec;

		appendStringInfo(buf, "nextblkno %u", walrec->next_free_blkno);
	}
}

const char *
zedstore_identify(uint8 info)
{
	const char *id = NULL;

	switch (info & ~XLR_INFO_MASK)
	{
		case WAL_ZEDSTORE_INIT_METAPAGE:
			id = "INIT_METAPAGE";
			break;
		case WAL_ZEDSTORE_UNDO_NEWPAGE:
			id = "UNDO_NEWPAGE";
			break;
		case WAL_ZEDSTORE_UNDO_DISCARD:
			id = "UNDO_DISCARD";
			break;
		case WAL_ZEDSTORE_BTREE_NEW_ROOT:
			id = "BTREE_NEW_ROOT";
			break;
		case WAL_ZEDSTORE_TIDLEAF_ADD_ITEMS:
			id = "BTREE_TIDLEAF_ADD_ITEMS";
			break;
		case WAL_ZEDSTORE_TIDLEAF_REPLACE_ITEM:
			id = "BTREE_TIDLEAF_REPLACE_ITEM";
			break;
		case WAL_ZEDSTORE_BTREE_REWRITE_PAGES:
			id = "BTREE_REWRITE_PAGES";
			break;
		case WAL_ZEDSTORE_ATTSTREAM_CHANGE:
			id = "ATTSTREAM_CHANGE";
			break;
		case WAL_ZEDSTORE_TOAST_NEWPAGE:
			id = "ZSTOAST_NEWPAGE";
			break;
		case WAL_ZEDSTORE_FPM_DELETE_PAGE:
			id = "FPM_DELETE_PAGE";
			break;
		case WAL_ZEDSTORE_FPM_REUSE_PAGE:
			id = "FPM_REUSE_PAGE";
			break;
	}
	return id;
}
