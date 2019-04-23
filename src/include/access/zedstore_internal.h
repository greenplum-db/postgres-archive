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

#define ZS_META_ATTRIBUTE_NUM 0

/*
 * Throughout ZedStore, we pass around TIDs as uint64's, rather than ItemPointers,
 * for speed.
 */
typedef uint64	zstid;

#define InvalidZSTid		0
#define MinZSTid			1		/* blk 0, off 1 */
#define MaxZSTid			((uint64) MaxBlockNumber << 16 | 0xffff)
/* note: if this is converted to ItemPointer, it is invalid */
#define MaxPlusOneZSTid		(MaxZSTid + 1)

#define MaxZSTidOffsetNumber	129

static inline zstid
ZSTidFromBlkOff(BlockNumber blk, OffsetNumber off)
{
	Assert(off != 0);

	return (uint64) blk * (MaxZSTidOffsetNumber - 1) + off;
}

static inline zstid
ZSTidFromItemPointer(ItemPointerData iptr)
{
	Assert(ItemPointerIsValid(&iptr));
	return ZSTidFromBlkOff(ItemPointerGetBlockNumber(&iptr),
						   ItemPointerGetOffsetNumber(&iptr));
}

static inline ItemPointerData
ItemPointerFromZSTid(zstid tid)
{
	ItemPointerData iptr;
	BlockNumber blk;
	OffsetNumber off;

	blk = (tid - 1) / (MaxZSTidOffsetNumber - 1);
	off = (tid - 1) % (MaxZSTidOffsetNumber - 1) + 1;

	ItemPointerSet(&iptr, blk, off);
	Assert(ItemPointerIsValid(&iptr));
	return iptr;
}

static inline BlockNumber
ZSTidGetBlockNumber(zstid tid)
{
	return (BlockNumber) ((tid - 1) / (MaxZSTidOffsetNumber - 1));
}

static inline OffsetNumber
ZSTidGetOffsetNumber(zstid tid)
{
	return (OffsetNumber) ((tid - 1) % (MaxZSTidOffsetNumber - 1) + 1);
}

/*
 * A ZedStore table contains different kinds of pages, all in the same file.
 *
 * Block 0 is always a metapage. It contains the block numbers of the other
 * data structures stored within the file, like the per-attribute B-trees,
 * and the UNDO log. In addition, if there are overly large datums in the
 * the table, they are chopped into separate "toast" pages.
 */
#define	ZS_META_PAGE_ID		0xF083
#define	ZS_BTREE_PAGE_ID	0xF084
#define	ZS_UNDO_PAGE_ID		0xF085
#define	ZS_TOAST_PAGE_ID	0xF086

/* like nbtree/gist FOLLOW_RIGHT flag, used to detect concurrent page splits */
#define ZS_FOLLOW_RIGHT		0x0002

typedef struct ZSBtreePageOpaque
{
	AttrNumber	zs_attno;
	BlockNumber zs_next;
	zstid		zs_lokey;		/* inclusive */
	zstid		zs_hikey;		/* exclusive */
	uint16		zs_level;			/* 0 = leaf */
	uint16		zs_flags;
	uint16		padding;			/* padding, to put zs_page_id last */
	uint16		zs_page_id;			/* always ZS_BTREE_PAGE_ID */
} ZSBtreePageOpaque;

#define ZSBtreePageGetOpaque(page) ((ZSBtreePageOpaque *) PageGetSpecialPointer(page))

/*
 * Internal B-tree page layout.
 *
 * The "contents" of the page is an array of ZSBtreeInternalPageItem. The number
 * of items can be deduced from pd_lower.
 */
typedef struct ZSBtreeInternalPageItem
{
	zstid		tid;
	BlockNumber childblk;
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
 * Leaf B-tree page layout
 *
 * Leaf pages are packed with ZSBtreeItems. There are three kinds of items:
 *
 * 1. Single item, holds one tuple (or rather, one datum).
 *
 * 2. "Array item", holds multiple datums, with consecutive TIDs and the same
 *    visibility information. An array item saves space compared to multiple
 *    single items, by leaving out repetitive UNDO and TID fields. An array
 *    item cannot mix NULLs and non-NULLs, so the ZSBT_NULL flag applies to
 *    all elements.
 *
 * 3. "Compressed item", which can hold multiple single or array items.
 *
 * A single or array item can furthermore be marked as DEAD. A dead item
 * prevents the TID (or TID range, for an array item) from being reused. It's
 * used during VACUUM, to mark items for which there are no index pointers
 * anymore. But it cannot be removed until the undo record has been trimmed
 * away, because if the TID was reused for a new record, vacuum might remove
 * the new tuple version instead. After t_undo_ptr becomes older than "oldest
 * undo ptr", the item can be removed and the TID recycled.
 *
 * TODO: squeeze harder: eliminate padding, use high bits of t_tid for flags or size
 */
typedef struct ZSBtreeItem
{
	zstid		t_tid;
	uint16		t_size;
	uint16		t_flags;
} ZSBtreeItem;

typedef struct ZSSingleBtreeItem
{
	/* these fields must match ZSBtreeItem */
	zstid		t_tid;
	uint16		t_size;
	uint16		t_flags;

	ZSUndoRecPtr t_undo_ptr;

	char		t_payload[FLEXIBLE_ARRAY_MEMBER];
} ZSSingleBtreeItem;

typedef struct ZSArrayBtreeItem
{
	/* these fields must match ZSBtreeItem */
	zstid		t_tid;
	uint16		t_size;
	uint16		t_flags;

	uint16		t_nelements;
	ZSUndoRecPtr t_undo_ptr;

	char		t_payload[FLEXIBLE_ARRAY_MEMBER];
} ZSArrayBtreeItem;

typedef struct ZSCompressedBtreeItem
{
	/* these fields must match ZSBtreeItem */
	zstid		t_tid;
	uint16		t_size;
	uint16		t_flags;

	uint16		t_uncompressedsize;
	zstid		t_lasttid;	/* inclusive */

	char		t_payload[FLEXIBLE_ARRAY_MEMBER];
} ZSCompressedBtreeItem;

#define ZSBT_COMPRESSED		0x0001
#define ZSBT_ARRAY			0x0002
#define ZSBT_DELETED		0x0004
#define ZSBT_UPDATED		0x0008
#define ZSBT_NULL			0x0010
#define ZSBT_DEAD			0x0020

/*
 * Get the last TID that the given item spans.
 *
 * For a single item, it's the TID of the item. For an array item, it's the
 * TID of the last element. For a compressed item, it's the last TID of the
 * last item it contains (which is stored explicitly in the item header).
 */
static inline zstid
zsbt_item_lasttid(ZSBtreeItem *item)
{
	if ((item->t_flags & ZSBT_COMPRESSED) != 0)
		return ((ZSCompressedBtreeItem *) item)->t_lasttid;
	else if ((item->t_flags & ZSBT_ARRAY) != 0)
	{
		ZSArrayBtreeItem *aitem = (ZSArrayBtreeItem *) item;
		return aitem->t_tid + aitem->t_nelements - 1;
	}
	else
		return item->t_tid;
}

static inline ZSUndoRecPtr
zsbt_item_undoptr(ZSBtreeItem *item)
{
	if ((item->t_flags & ZSBT_COMPRESSED) != 0)
		elog(ERROR, "cannot get undo pointer from compressed item");
	else if ((item->t_flags & ZSBT_ARRAY) != 0)
	{
		ZSArrayBtreeItem *aitem = (ZSArrayBtreeItem *) item;
		return aitem->t_undo_ptr;
	}
	else
	{
		ZSSingleBtreeItem *sitem = (ZSSingleBtreeItem *) item;
		return sitem->t_undo_ptr;
	}
}

/*
 * Toast page layout.
 *
 * When an overly large datum is stored, it is divided into chunks, and each
 * chunk is stored on a dedicated toast page. The toast pages of a datum form
 * list, each page has a next/prev pointer.
 */
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
	zstid		zs_tid;
	uint32		zs_total_size;

	uint32		zs_slice_offset;
	BlockNumber	zs_prev;
	BlockNumber	zs_next;
	uint16		zs_flags;
	uint16		padding1;			/* padding, to put zs_page_id last */
	uint16		padding2;			/* padding, to put zs_page_id last */
	uint16		zs_page_id;
} ZSToastPageOpaque;

/*
 * "Toast pointer" of a datum that's stored in zedstore toast pages.
 *
 * This looks somewhat like a normal TOAST pointer, but we mustn't let these
 * escape out of zedstore code, because the rest of the system doesn't know
 * how to deal with them.
 *
 * This must look like varattrib_1b_e!
 */
typedef struct varatt_zs_toastptr
{
	/* varattrib_1b_e */
	uint8		va_header;
	uint8		va_tag;			/* VARTAG_ZEDSTORE in zedstore toast datums */

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
	if (typLen > 0)
		return typLen;
	else if (typLen == -1)
	{
		if (VARATT_IS_EXTERNAL(value) && VARTAG_EXTERNAL(value) == VARTAG_ZEDSTORE)
			return sizeof(varatt_zs_toastptr);
		else
			return VARSIZE_ANY(value);
	}
	else
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
 * It contains a directory of b-tree roots for each attribute, and lots more.
 */
#define ZS_META_BLK		0

/*
 * The metapage stores one of these for each attribute.
 *
 * We have copy the atributes 'attlen' and 'attbyval' here, because we need
 * them when reading the rows. Normally, they match the values in the
 * relcache, of course, but there is a time during ALTER TABLE SET TYPE
 * where the tuple descriptor in the relcache entry already shows the new
 * type, but we're still reading the old relfile, which must happen using
 * the old type. We work around that by not relying on the relcache entry,
 * but on these copied values. And it seems like a good sanity check,
 * anyway.
 */
typedef struct ZSRootDirItem
{
	BlockNumber root;
	int16		attlen;
	bool		attbyval;
} ZSRootDirItem;

typedef struct ZSMetaPage
{
	int			nattributes;
	ZSRootDirItem tree_root_dir[FLEXIBLE_ARRAY_MEMBER];	/* one for each attribute */
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

	uint16		zs_flags;
	uint16		padding1;			/* padding, to put zs_page_id last */
	uint16		padding2;			/* padding, to put zs_page_id last */
	uint16		zs_page_id;
} ZSMetaPageOpaque;


/*
 * Holds the state of an in-progress scan on a zedstore btree.
 */
typedef struct ZSBtreeScan
{
	Relation	rel;
	AttrNumber	attno;
	int16		attlen;
	bool		attbyval;
	bool        atthasmissing;
	char		attalign;

	/*
	 * memory context that should be used for any allocations that go with the scan,
	 * like the decompression buffers. This isn't a dedicated context, you must still
	 * free everything to avoid leaking! We need this because the getnext function
	 * might be called in a short-lived memory context that is reset between calls.
	 */
	MemoryContext context;

	bool		active;
	Buffer		lastbuf;
	bool		lastbuf_is_locked;
	OffsetNumber lastoff;
	zstid		nexttid;
	zstid		endtid;
	Snapshot	snapshot;

	/* in the "real" UNDO-log, this would probably be a global variable */
	ZSUndoRecPtr recent_oldest_undo;

	/*
	 * if we have remaining items from a compressed container tuple, they
	 * are kept in the decompressor context, and 'has_decompressed' is true.
	 */
	ZSDecompressContext decompressor;
	bool		has_decompressed;

	/*
	 * These fields are used, if the scan is processing an array tuple.
	 */
	int			array_datums_allocated_size;
	Datum	   *array_datums;
	Datum	   *array_next_datum;
	int			array_elements_left;
	bool		array_isnull;

} ZSBtreeScan;

/* prototypes for functions in zedstore_btree.c */
extern void zsbt_multi_insert(Relation rel, AttrNumber attno,
						Datum *datums, bool *isnulls, zstid *tids, int ndatums,
						TransactionId xid, CommandId cid,
						ZSUndoRecPtr *undorecptr);
extern TM_Result zsbt_delete(Relation rel, AttrNumber attno, zstid tid,
							 TransactionId xid, CommandId cid,
							 Snapshot snapshot, Snapshot crosscheck, bool wait,
							 TM_FailureData *hufd, bool changingPart);
extern TM_Result zsbt_update(Relation rel, AttrNumber attno, zstid otid,
							 Datum newdatum, bool newisnull, TransactionId xid,
							 CommandId cid, Snapshot snapshot, Snapshot crosscheck,
							 bool wait, TM_FailureData *hufd, zstid *newtid_p);
extern void zsbt_mark_item_dead(Relation rel, AttrNumber attno, zstid tid, ZSUndoRecPtr);
extern TM_Result zsbt_lock_item(Relation rel, AttrNumber attno, zstid tid,
			   TransactionId xid, CommandId cid, Snapshot snapshot,
			   LockTupleMode lockmode, LockWaitPolicy wait_policy,
			   TM_FailureData *hufd);
extern void zsbt_undo_item_deletion(Relation rel, AttrNumber attno, zstid tid, ZSUndoRecPtr undoptr);
extern void zsbt_begin_scan(Relation rel, AttrNumber attno, zstid starttid, zstid endtid, Snapshot snapshot, ZSBtreeScan *scan);
extern bool zsbt_scan_next(ZSBtreeScan *scan, Datum *datum, bool *isnull, zstid *tid);
extern void zsbt_end_scan(ZSBtreeScan *scan);
extern zstid zsbt_get_last_tid(Relation rel, AttrNumber attno);

/*
 * A fast-path version of zsbt_scan_next(), where the common case where we just
 * return the next element from an array item is inlined.
 *
 * Keep this fast-path in sync with zsbt_scan_next().
 */
static inline bool
zsbt_scan_next_fast(ZSBtreeScan *scan, Datum *datum, bool *isnull, zstid *tid)
{
	if (!scan->active)
		return false;

	if (scan->array_elements_left > 0)
	{
		*isnull = scan->array_isnull;
		*datum = *(scan->array_next_datum++);
		*tid = (scan->nexttid++);
		scan->array_elements_left--;
		return true;
	}

	/* slow path */
	return zsbt_scan_next(scan, datum, isnull, tid);
}


/* prototypes for functions in zedstore_meta.c */
extern void zsmeta_initmetapage(Relation rel);
extern Buffer zs_getnewbuf(Relation rel);
extern BlockNumber zsmeta_get_root_for_attribute(Relation rel, AttrNumber attno, bool for_update, int16 *attlen_p, bool *attbyval_p);
extern void zsmeta_update_root_for_attribute(Relation rel, AttrNumber attno, Buffer metabuf, BlockNumber rootblk);
extern void zsmeta_add_root_for_new_attributes(Relation rel, Page page);

/* prototypes for functions in zedstore_visibility.c */
extern TM_Result zs_SatisfiesUpdate(Relation rel, Snapshot snapshot,
				   ZSUndoRecPtr recent_oldest_undo, ZSBtreeItem *item,
				   bool *undo_record_needed, TM_FailureData *tmfd);
extern bool zs_SatisfiesVisibility(ZSBtreeScan *scan, ZSBtreeItem *item);

/* prototypes for functions in zedstore_toast.c */
extern Datum zedstore_toast_datum(Relation rel, AttrNumber attno, Datum value);
extern void zedstore_toast_finish(Relation rel, AttrNumber attno, Datum toasted, zstid tid);
extern Datum zedstore_toast_flatten(Relation rel, AttrNumber attno, zstid tid, Datum toasted);

#endif							/* ZEDSTORE_INTERNAL_H */
