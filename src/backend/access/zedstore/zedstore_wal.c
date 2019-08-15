/*
 * zedstore_wal.c
 *		WAL-logging for zedstore.
 *
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/zedstore/zedstore_wal.c
 */
#include "postgres.h"

#include "access/bufmask.h"
#include "access/xlogreader.h"
#include "access/zedstore_internal.h"
#include "access/zedstore_wal.h"

void
zedstore_redo(XLogReaderState *record)
{
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	switch (info)
	{
		case WAL_ZEDSTORE_INIT_METAPAGE:
			zsmeta_initmetapage_redo(record);
			break;
		case WAL_ZEDSTORE_UNDO_NEWPAGE:
			zsundo_newpage_redo(record);
			break;
		case WAL_ZEDSTORE_UNDO_TRIM:
			zsundo_trim_redo(record);
			break;
		case WAL_ZEDSTORE_BTREE_NEW_ROOT:
			zsmeta_new_btree_root_redo(record);
			break;
		case WAL_ZEDSTORE_BTREE_ADD_LEAF_ITEMS:
			zsbt_leaf_items_redo(record, false);
			break;
		case WAL_ZEDSTORE_BTREE_REPLACE_LEAF_ITEM:
			zsbt_leaf_items_redo(record, true);
			break;
		case WAL_ZEDSTORE_BTREE_REWRITE_PAGES:
			zsbt_rewrite_pages_redo(record);
			break;
		case WAL_ZEDSTORE_TOAST_NEWPAGE:
			zstoast_newpage_redo(record);
			break;
		default:
			elog(PANIC, "zedstore_redo: unknown op code %u", info);
	}
}

void
zedstore_mask(char *pagedata, BlockNumber blkno)
{
	Page		page = (Page) pagedata;

	mask_page_lsn_and_checksum(page);

	return;
}
