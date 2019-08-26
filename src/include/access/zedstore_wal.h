/*
 * zedstore_wal.h
 *		internal declarations for ZedStore wal logging
 *
 * Copyright (c) 2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/include/access/zedstore_wal.h
 */
#ifndef ZEDSTORE_WAL_H
#define ZEDSTORE_WAL_H

#include "access/attnum.h"
#include "access/xlogreader.h"
#include "access/zedstore_tid.h"
#include "access/zedstore_undolog.h"
#include "lib/stringinfo.h"
#include "storage/off.h"

#define WAL_ZEDSTORE_INIT_METAPAGE			0x00
#define WAL_ZEDSTORE_UNDO_NEWPAGE			0x10
#define WAL_ZEDSTORE_UNDO_DISCARD			0x20
#define WAL_ZEDSTORE_BTREE_NEW_ROOT			0x30
#define WAL_ZEDSTORE_BTREE_REWRITE_PAGES	0x40
#define WAL_ZEDSTORE_TIDLEAF_ADD_ITEMS		0x50
#define WAL_ZEDSTORE_TIDLEAF_REPLACE_ITEM	0x60
#define WAL_ZEDSTORE_ATTSTREAM_CHANGE		0x70
#define WAL_ZEDSTORE_TOAST_NEWPAGE			0x80
#define WAL_ZEDSTORE_FPM_DELETE_PAGE		0x90
#define WAL_ZEDSTORE_FPM_REUSE_PAGE			0xA0

/* in zedstore_wal.c */
extern void zedstore_redo(XLogReaderState *record);
extern void zedstore_mask(char *pagedata, BlockNumber blkno);

/* in zedstoredesc.c */
extern void zedstore_desc(StringInfo buf, XLogReaderState *record);
extern const char *zedstore_identify(uint8 info);

/*
 * WAL record for initializing zedstore metapage (WAL_ZEDSTORE_INIT_METAPAGE)
 *
 * These records always use a full-page image, so this data is really just
 * for debugging purposes.
 */
typedef struct wal_zedstore_init_metapage
{
	int32		natts;		/* number of attributes. */
} wal_zedstore_init_metapage;

#define SizeOfZSWalInitMetapage (offsetof(wal_zedstore_init_metapage, natts) + sizeof(int32))

/*
 * WAL record for extending the UNDO log with one page.
 */
typedef struct wal_zedstore_undo_newpage
{
	uint64		first_counter;
} wal_zedstore_undo_newpage;

#define SizeOfZSWalUndoNewPage (offsetof(wal_zedstore_undo_newpage, first_counter) + sizeof(uint64))

/*
 * WAL record for updating the oldest undo pointer on the metapage, after
 * discarding an old portion the UNDO log.
 *
 * blkref #0 is the metapage.
 *
 * If an old UNDO page was discarded away, advancing zs_undo_head, that page
 * is stored as blkref #1. The new block number to store in zs_undo_head is
 * stored as the data of blkref #0.
 */
typedef struct wal_zedstore_undo_discard
{
	ZSUndoRecPtr oldest_undorecptr;

	/*
	 * Next oldest remaining block in the UNDO chain. This is not the same as
	 * oldest_undorecptr.block, if we are discarding multiple UNDO blocks. We will
	 * update oldest_undorecptr in the first iteration already, so that visibility
	 * checks can use the latest value immediately. But we can't hold a potentially
	 * unlimited number of pages locked while we mark them as deleted, so they are
	 * deleted one by one, and each deletion is WAL-logged separately.
	 */
	BlockNumber	oldest_undopage;
} wal_zedstore_undo_discard;

#define SizeOfZSWalUndoDiscard (offsetof(wal_zedstore_undo_discard, oldest_undopage) + sizeof(BlockNumber))

/*
 * WAL record for creating a new, empty, root page for an attribute.
 */
typedef struct wal_zedstore_btree_new_root
{
	AttrNumber	attno;		/* 0 means TID tree */
} wal_zedstore_btree_new_root;

#define SizeOfZSWalBtreeNewRoot	(offsetof(wal_zedstore_btree_new_root, attno) + sizeof(AttrNumber))

/*
 * WAL record for replacing/adding items to the TID tree.
 */
typedef struct wal_zedstore_tidleaf_items
{
	int16		nitems;
	OffsetNumber off;

	/* the items follow */
} wal_zedstore_tidleaf_items;

#define SizeOfZSWalTidLeafItems (offsetof(wal_zedstore_tidleaf_items, off) + sizeof(OffsetNumber))

/*
 * WAL record for page splits, and other more complicated operations where
 * we just rewrite whole pages.
 *
 * block #0 is UNDO buffer, if any.
 * The rest are the b-tree pages (numpages).
 */
typedef struct wal_zedstore_btree_rewrite_pages
{
	AttrNumber	attno;		/* 0 means TID tree */
	int			numpages;
} wal_zedstore_btree_rewrite_pages;

#define SizeOfZSWalBtreeRewritePages (offsetof(wal_zedstore_btree_rewrite_pages, attno) + sizeof(AttrNumber))

/*
 * WAL record for a change to attribute leaf page.
 *
 * Modifies an attribute stream stored on an attribute leaf page.
 * If 'is_upper' is set, the change applies to the upper stream,
 * between pd_upper and pd_special, otherwise it applies to the
 * lower stream between page header and pd_lower.
 *
 * new_attstream_size is the new size of the attstream. At replay,
 * pd_lower or pd_upper is adjusted to match the new size. If
 * size of the upper stream changes, any existing data in the upper
 * area on the page conceptually moved to the beginning of the upper
 * area, before the replacement data in the record is applied.
 *
 * The block data 0 contains new data, which overwrites the data
 * between begin_offset and end_offset. Not all data in the stream
 * needs to be overwritten, that is, begin_offset and end_offset
 * don't need to cover the whole stream. That allows efficiently
 * appending data to an uncompressed stream. (It's also pretty
 * effective for the compressed stream: if a stream is
 * decompressed, some changes are made, and the stream is
 * recompressed, the part before the change will usually re-compress
 * to the same bytes.)
 */
typedef struct wal_zedstore_attstream_change
{
	bool		is_upper;

	/*
	 * These field correspond to the fields in ZSAttStream. But
	 * we use smaller fields to save on WAL volume. (ZSAttStream
	 * uses larger fields for the size, so that the same struct
	 * can be used for longer streams than fit on disk, when passed
	 * around in memory.)
	 */
	uint16		new_attstream_size;
	uint16		new_decompressed_size;
	uint16		new_decompressed_bufsize;
	zstid		new_lasttid;

	uint16		begin_offset;
	uint16		end_offset;
} wal_zedstore_attstream_change;

#define SizeOfZSWalAttstreamChange (offsetof(wal_zedstore_attstream_change, end_offset) + sizeof(uint16))

/*
 * WAL record for zedstore toasting. When a large datum spans multiple pages,
 * we write one of these for every page. The chain will appear valid between
 * every operation, except that the total size won't match the total size of
 * all the pages until the last page is written.
 *
 * blkref 0: the new page being added
 * blkref 1: the previous page in the chain
 */
typedef struct wal_zedstore_toast_newpage
{
	zstid		tid;
	AttrNumber	attno;
	int32		total_size;
	int32		offset;
} wal_zedstore_toast_newpage;

#define SizeOfZSWalToastNewPage (offsetof(wal_zedstore_toast_newpage, offset) + sizeof(int32))

typedef struct wal_zedstore_fpm_delete_page
{
	BlockNumber	next_free_blkno;
} wal_zedstore_fpm_delete_page;

#define SizeOfZSWalFpmDeletePage (offsetof(wal_zedstore_fpm_delete_page, next_free_blkno) + sizeof(BlockNumber))

typedef struct wal_zedstore_fpm_reuse_page
{
	BlockNumber	next_free_blkno;
} wal_zedstore_fpm_reuse_page;

#define SizeOfZSWalFpmReusePage (offsetof(wal_zedstore_fpm_reuse_page, next_free_blkno) + sizeof(BlockNumber))

extern void zsbt_tidleaf_items_redo(XLogReaderState *record, bool replace);
extern void zsmeta_new_btree_root_redo(XLogReaderState *record);
extern void zsbt_rewrite_pages_redo(XLogReaderState *record);
extern void zstoast_newpage_redo(XLogReaderState *record);
extern void zspage_delete_page_redo(XLogReaderState *record);
extern void zspage_reuse_page_redo(XLogReaderState *record);

#endif							/* ZEDSTORE_WAL_H */
