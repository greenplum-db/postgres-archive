/*
 * zedstore_internal.h
 *		internal declarations for ZedStore tables
 *
 * Copyright (c) 2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/include/access/zedstore_internal.h
 */
#ifndef ZEDSTORE_INTERNAL_H
#define ZEDSTORE_INTERNAL_H

#include "access/tableam.h"
#include "access/zedstore_compression.h"
#include "storage/bufmgr.h"

/*
 * Different page types:
 *
 * - metapage (block 0)
 * - Btree pages
 *   - root, internal, leaf
 * - FSM pages
 * - "toast" pages.
 *
 */
#define	ZS_META_PAGE_ID		0xF083
#define	ZS_BTREE_PAGE_ID	0xF084

/* like nbtree/gist FOLLOW_RIGHT flag, used to detect concurrent page splits */
#define ZS_FOLLOW_RIGHT		0x0002

typedef struct ZSBtreePageOpaque
{
	BlockNumber zs_next;
	ItemPointerData zs_lokey;		/* inclusive */
	ItemPointerData zs_hikey;		/* exclusive */
	uint16		zs_level;			/* 0 = leaf */
	uint16		zs_flags;
	uint16		zs_page_id;			/* always ZS_BTREE_PAGE_ID */
} ZSBtreePageOpaque;

#define ZSBtreePageGetOpaque(page) ((ZSBtreePageOpaque *) PageGetSpecialPointer(page))

/*
 * Internal page layout.
 *
 * The "contents" of the page is an array of ZSBtreeInternalPageItem. The number
 * of items can be deduced from pd_lower.
 */
typedef struct ZSBtreeInternalPageItem
{
	ItemPointerData tid;
	BlockIdData childblk;
} ZSBtreeInternalPageItem;

static inline ZSBtreeInternalPageItem *
ZSBtreeInternalPageGetItems(Page page)
{
	ZSBtreeInternalPageItem *items;

	items = (ZSBtreeInternalPageItem *) PageGetContents(page);

	return items;
}
static inline int
ZSBtreeInternalPageGetNumItems(Page page)
{
	ZSBtreeInternalPageItem *begin;
	ZSBtreeInternalPageItem *end;

	begin = (ZSBtreeInternalPageItem *) PageGetContents(page);
	end = (ZSBtreeInternalPageItem *) ((char *) page + ((PageHeader) page)->pd_lower);

	return end - begin;
}

static inline bool
ZSBtreeInternalPageIsFull(Page page)
{
	PageHeader phdr = (PageHeader) page;

	return phdr->pd_upper - phdr->pd_lower < sizeof(ZSBtreeInternalPageItem);
}

/*
 * Leaf page layout
 *
 * Leaf pages are packed with ZSBtreeItems. There are two kinds of items:
 *
 * 1. plain item, holds one tuple (or rather, one datum).
 *
 * 2. A "container item", which holds multiple plain items, compressed.
 */
typedef struct ZSBtreeItem
{
	uint16		t_size;
	uint16		t_flags;
	ItemPointerData t_tid;

	/* these are only used on compressed items */
	ItemPointerData t_lasttid;	/* inclusive */
	uint16		t_uncompressedsize;

	char		t_payload[FLEXIBLE_ARRAY_MEMBER];
} ZSBtreeItem;

#define		ZSBT_COMPRESSED		0x0001

/*
 * Block 0 on every ZedStore table is a metapage.
 *
 * It contains a directory of b-tree roots for each attribute.
 * Probably lots more in the future...
 */
#define ZS_META_BLK		0

typedef struct ZSMetaPage
{
	int			nattributes;
	BlockNumber	roots[FLEXIBLE_ARRAY_MEMBER];	/* one for each attribute */
} ZSMetaPage;

/*
 * it's not clear what we should store in the "opaque" special area, and what
 * as page contents, on a metapage. But have at least the page_id field here,
 * so that tools like pg_filedump can recognize it as a zedstore metapage.
 */
typedef struct ZSMetaPageOpaque
{
	uint16		zs_flags;
	uint16		zs_page_id;
} ZSMetaPageOpaque;


/*
 * Holds the state of an in-progress scan on a zedstore btree.
 */
typedef struct ZSBtreeScan
{
	Relation	rel;
	AttrNumber	attno;

	bool		active;
	Buffer		lastbuf;
	OffsetNumber lastoff;
	ItemPointerData nexttid;
	Snapshot snapshot;

	/*
	 * if we have remaining items from a compressed "container" tuple, they
	 * are kept in the decompressor context, and 'has_decompressed' is true.
	 */
	ZSDecompressContext decompressor;
	bool		has_decompressed;
} ZSBtreeScan;

typedef struct ZSBtreeScanForTupleDelete
{
	Relation rel;
	Snapshot snapshot;
	CommandId cid;
	bool wait;
	TM_FailureData *tmfd;
	TM_Result result;
} ZSBtreeScanForTupleDelete;

/*
 * Helper function to "increment" a TID by one.
 */
static inline void
ItemPointerIncrement(ItemPointer itemptr)
{
	if (itemptr->ip_posid == 0xffff)
		ItemPointerSet(itemptr, ItemPointerGetBlockNumber(itemptr) + 1, 1);
	else
		itemptr->ip_posid++;
}

/* prototypes for functions in zstore_btree.c */
extern ItemPointerData zsbt_insert(Relation rel, AttrNumber attno, Datum datum, HeapTupleHeader tupleheader);

extern void zsbt_begin_scan(Relation rel, AttrNumber attno, ItemPointerData starttid, Snapshot snapshot, ZSBtreeScan *scan);
extern bool zsbt_scan_next(ZSBtreeScan *scan, Datum *datum, ItemPointerData *tid, bool *visible);
extern void zsbt_end_scan(ZSBtreeScan *scan);
extern ItemPointerData zsbt_get_last_tid(Relation rel, AttrNumber attno);

extern bool zsbt_scan_for_tuple_delete(ZSBtreeScanForTupleDelete *deldesc, ItemPointerData tid);

/* prototypes for functions in zstore_meta.c */
extern Buffer zs_getnewbuf(Relation rel);
extern BlockNumber zsmeta_get_root_for_attribute(Relation rel, AttrNumber attno, bool for_update);
extern void zsmeta_update_root_for_attribute(Relation rel, AttrNumber attno, Buffer metabuf, BlockNumber rootblk);

extern void zs_prepare_insert(Relation relation, HeapTupleHeader hdr, TransactionId xid, CommandId cid, int options);

extern bool zs_tuple_satisfies_visibility(HeapTupleHeader tuple, ItemPointer tid, Snapshot snapshot, Buffer buffer);
extern void zs_tuple_delete(ZSBtreeScanForTupleDelete *deldesc, HeapTupleHeader hdr, ItemPointer tid, Buffer buffer);

#endif							/* ZEDSTORE_INTERNAL_H */
