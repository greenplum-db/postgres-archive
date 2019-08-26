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
#include "access/zedstore_undolog.h"
#include "access/zedstore_undorec.h"
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
		case WAL_ZEDSTORE_UNDO_DISCARD:
			zsundo_discard_redo(record);
			break;
		case WAL_ZEDSTORE_BTREE_NEW_ROOT:
			zsmeta_new_btree_root_redo(record);
			break;
		case WAL_ZEDSTORE_BTREE_REWRITE_PAGES:
			zsbt_rewrite_pages_redo(record);
			break;
		case WAL_ZEDSTORE_TIDLEAF_ADD_ITEMS:
			zsbt_tidleaf_items_redo(record, false);
			break;
		case WAL_ZEDSTORE_TIDLEAF_REPLACE_ITEM:
			zsbt_tidleaf_items_redo(record, true);
			break;
		case WAL_ZEDSTORE_ATTSTREAM_CHANGE:
			zsbt_attstream_change_redo(record);
			break;
		case WAL_ZEDSTORE_TOAST_NEWPAGE:
			zstoast_newpage_redo(record);
			break;
		case WAL_ZEDSTORE_FPM_DELETE_PAGE:
			zspage_delete_page_redo(record);
			break;
		case WAL_ZEDSTORE_FPM_REUSE_PAGE:
			zspage_reuse_page_redo(record);
			break;
		default:
			elog(PANIC, "zedstore_redo: unknown op code %u", info);
	}
}

void
zedstore_mask(char *pagedata, BlockNumber blkno)
{
	Page		page = (Page) pagedata;
	uint16		page_id;

	mask_page_lsn_and_checksum(page);

	page_id = *(uint16 *) (pagedata + BLCKSZ - sizeof(uint16));

	if (blkno == ZS_META_BLK)
	{
	}
	else if (page_id == ZS_UNDO_PAGE_ID && PageGetSpecialSize(page) == sizeof(ZSUndoPageOpaque))
	{
		/*
		 * On INSERT undo records, mask out speculative insertion tokens.
		 */
		char	   *endptr = pagedata + ((PageHeader) pagedata)->pd_lower;
		char	   *ptr;

		ptr = pagedata + SizeOfPageHeaderData;

		while (ptr < endptr)
		{
			ZSUndoRec *undorec = (ZSUndoRec *) ptr;

			/* minimal validation */
			if (undorec->size < sizeof(ZSUndoRec) || ptr + undorec->size > endptr)
				break;

			if (undorec->type == ZSUNDO_TYPE_INSERT)
			{
				((ZSUndoRec_Insert *) undorec)->speculative_token = MASK_MARKER;
			}

			ptr += undorec->size;
		}
	}

	return;
}
