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
#include "access/zedstore_undo.h"
#include "storage/bufmgr.h"
#include "utils/datum.h"

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
#define	ZS_UNDO_PAGE_ID		0xF085
#define	ZS_TOAST_PAGE_ID	0xF086

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
 *
 * TODO: some of the fields are only used on one or the other. Squeeze harder..
 */
typedef struct ZSBtreeItem
{
	uint16		t_size;
	uint16		t_flags;
	ItemPointerData t_tid;

	/* these are only used on compressed items */
	ItemPointerData t_lasttid;	/* inclusive */
	uint16		t_uncompressedsize;

	/* these are only used on uncompressed items. */
	ZSUndoRecPtr t_undo_ptr;

	char		t_payload[FLEXIBLE_ARRAY_MEMBER];
} ZSBtreeItem;

#define		ZSBT_COMPRESSED		0x0001
#define		ZSBT_DELETED		0x0002
#define		ZSBT_UPDATED		0x0004


/*
 * Maximum size of an individual untoasted Datum stored in ZedStore. Datums
 * larger than this need to be toasted.
 *
 * A datum needs to fit on a B-tree page, with page and item headers.
 *
 * XXX: 500 accounts for all the headers. Need to compute this correctly...
 */
#define		MaxZedStoreDatumSize		(BLCKSZ - 500)

typedef struct ZSToastPageOpaque
{
	AttrNumber	zs_attno;

	/* these are only set on the first page. */
	ItemPointerData zs_tid;
	uint32		zs_total_size;

	uint32		zs_slice_offset;
	BlockNumber	zs_prev;
	BlockNumber	zs_next;
	uint16		zs_flags;
	uint16		zs_page_id;
} ZSToastPageOpaque;

/*
 * This must look like varattrib_1b_e!
 */
typedef struct varatt_zs_toastptr
{
	/* varattrib_1b_e */
	uint8		va_header;
	uint8		va_tag;

	/* first block */
	BlockNumber	zst_block;
} varatt_zs_toastptr;

/*
 * va_tag value. this should be distinguishable from the values in
 * vartag_external
 */
#define		VARTAG_ZEDSTORE		10

/*
 * Versions of datumGetSize and datumCopy that know about ZedStore-toasted
 * datums.
 */
static inline Size
zs_datumGetSize(Datum value, bool typByVal, int typLen)
{
	if (typLen < 0 && VARATT_IS_EXTERNAL(value) && VARTAG_EXTERNAL(value) == VARTAG_ZEDSTORE)
		return sizeof(varatt_zs_toastptr);
	return datumGetSize(value, typByVal, typLen);
}

static inline Datum
zs_datumCopy(Datum value, bool typByVal, int typLen)
{
	if (typLen < 0 && VARATT_IS_EXTERNAL(value) && VARTAG_EXTERNAL(value) == VARTAG_ZEDSTORE)
	{
		char	   *result = palloc(sizeof(varatt_zs_toastptr));

		memcpy(result, DatumGetPointer(value), sizeof(varatt_zs_toastptr));

		return PointerGetDatum(result);
	}
	else
		return datumCopy(value, typByVal, typLen);
}

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
	uint64		zs_undo_counter;
	BlockNumber	zs_undo_head;
	BlockNumber	zs_undo_tail;
	ZSUndoRecPtr zs_undo_oldestptr;
	ZSUndoRecPtr zs_undo_curptr;

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

	bool		for_update;

	bool		active;
	Buffer		lastbuf;
	bool		lastbuf_is_locked;
	OffsetNumber lastoff;
	ItemPointerData nexttid;
	Snapshot	snapshot;
	ZSUndoRecPtr recent_oldest_undo;

	/*
	 * if we have remaining items from a compressed "container" tuple, they
	 * are kept in the decompressor context, and 'has_decompressed' is true.
	 */
	ZSDecompressContext decompressor;
	bool		has_decompressed;
} ZSBtreeScan;

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

/*
 * a <= x <= b
 */
static inline bool
ItemPointerBetween(ItemPointer a, ItemPointer x, ItemPointer b)
{
	return ItemPointerCompare(a, x) <= 0 &&
		ItemPointerCompare(x, b) <= 0;
}

/* prototypes for functions in zstore_btree.c */
extern ItemPointerData zsbt_insert(Relation rel, AttrNumber attno, Datum datum, TransactionId xmin, CommandId cmin, ItemPointerData tid);
extern TM_Result zsbt_delete(Relation rel, AttrNumber attno, ItemPointerData tid,
							 TransactionId xid, CommandId cid,
			Snapshot snapshot, Snapshot crosscheck, bool wait,
			TM_FailureData *hufd, bool changingPart);
extern TM_Result zsbt_update(Relation rel, AttrNumber attno, ItemPointerData otid, Datum newdatum,
							 TransactionId xid, CommandId cid, Snapshot snapshot, Snapshot crosscheck,
							 bool wait, TM_FailureData *hufd, ItemPointerData *newtid_p);

extern void zsbt_begin_scan(Relation rel, AttrNumber attno, ItemPointerData starttid, Snapshot snapshot, ZSBtreeScan *scan);
extern bool zsbt_scan_next(ZSBtreeScan *scan, Datum *datum, ItemPointerData *tid);
extern void zsbt_end_scan(ZSBtreeScan *scan);
extern ItemPointerData zsbt_get_last_tid(Relation rel, AttrNumber attno);

/* prototypes for functions in zstore_meta.c */
extern Buffer zs_getnewbuf(Relation rel);
extern BlockNumber zsmeta_get_root_for_attribute(Relation rel, AttrNumber attno, bool for_update);
extern void zsmeta_update_root_for_attribute(Relation rel, AttrNumber attno, Buffer metabuf, BlockNumber rootblk);

extern void zs_prepare_insert(Relation relation, HeapTupleHeader hdr, TransactionId xid, CommandId cid, int options);

/* prototypes for functions in zstore_visibility.c */
extern TM_Result zs_SatisfiesUpdate(Relation rel, ZSBtreeItem *item, Snapshot snapshot);
extern bool zs_SatisfiesVisibility(Relation rel, ZSBtreeItem *item, Snapshot snapshot);

/* prototypes for functions in zstore_toast.c */
extern Datum zedstore_toast_datum(Relation rel, AttrNumber attno, Datum value);
extern void zedstore_toast_finish(Relation rel, AttrNumber attno, Datum toasted, ItemPointerData tid);
extern Datum zedstore_toast_flatten(Relation rel, AttrNumber attno, ItemPointerData tid, Datum toasted);

#endif							/* ZEDSTORE_INTERNAL_H */