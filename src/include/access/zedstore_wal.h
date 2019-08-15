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
#define WAL_ZEDSTORE_BTREE_ADD_LEAF_ITEMS	0x40
#define WAL_ZEDSTORE_BTREE_REPLACE_LEAF_ITEM	0x50
#define WAL_ZEDSTORE_BTREE_REWRITE_PAGES	0x60
#define WAL_ZEDSTORE_TOAST_NEWPAGE			0x70

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
 * discarding an old portion the  UNDO log.
 */
typedef struct wal_zedstore_undo_discard
{
	ZSUndoRecPtr oldest_undorecptr;
	BlockNumber	oldest_undopage;		/* XXX: is this redundant with undorecptr.block? */
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
 * WAL record for replacing/adding items to the TID tree, or to an attribute tree.
 */
typedef struct wal_zedstore_btree_leaf_items
{
	AttrNumber	attno;		/* 0 means TID tree */
	int16		nitems;
	OffsetNumber off;

	/* the items follow */
} wal_zedstore_btree_leaf_items;

#define SizeOfZSWalBtreeLeafItems (offsetof(wal_zedstore_btree_leaf_items, off) + sizeof(OffsetNumber))

/*
 * WAL record for page splits, and other more complicated operations where
 * we just rewrite whole pages.
 *
 * block #0 is UNDO buffer, if any. The rest are the b-tree pages (numpages).
 */
typedef struct wal_zedstore_btree_rewrite_pages
{
	AttrNumber	attno;		/* 0 means TID tree */
	int			numpages;
} wal_zedstore_btree_rewrite_pages;

#define SizeOfZSWalBtreeRewritePages (offsetof(wal_zedstore_btree_rewrite_pages, attno) + sizeof(AttrNumber))

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

extern void zsbt_leaf_items_redo(XLogReaderState *record, bool replace);
extern void zsmeta_new_btree_root_redo(XLogReaderState *record);
extern void zsbt_rewrite_pages_redo(XLogReaderState *record);
extern void zstoast_newpage_redo(XLogReaderState *record);

#endif							/* ZEDSTORE_WAL_H */
