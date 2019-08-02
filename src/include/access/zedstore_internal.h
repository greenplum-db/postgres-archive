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
#include "lib/integerset.h"
#include "storage/bufmgr.h"
#include "storage/smgr.h"
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
#define	ZS_FREE_PAGE_ID		0xF087

/* flags for zedstore b-tree pages */
#define ZSBT_ROOT				0x0001

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
 * Attribute B-tree leaf page layout
 *
 * Leaf pages in the attribute trees are packed with ZSAttributeItems. There are two
 * kinds of items:
 *
 * 1. "Array item", holds multiple datums, with consecutive TIDs and the same
 *    visibility information. An array item saves space compared to multiple
 *    single items, by leaving out repetitive UNDO and TID fields.
 *
 * 2. "Compressed item", which can hold multiple single or array items.
 *
 * TODO: squeeze harder: eliminate padding, use high bits of t_tid for flags or size
 */
typedef struct ZSAttributeItem
{
	zstid		t_tid;
	uint16		t_size;
	uint16		t_flags;
} ZSAttributeItem;

#define ZSBT_ATTR_COMPRESSED		0x0001

typedef struct ZSAttributeArrayItem
{
	/* these fields must match ZSAttributeItem */
	zstid		t_tid;
	uint16		t_size;
	uint16		t_flags;

	uint16		t_nelements;

	uint8		t_bitmap[FLEXIBLE_ARRAY_MEMBER];	/* null bitmap */
	/* payload follows at next MAXALIGN boundary after the bitmap */
} ZSAttributeArrayItem;

#define ZSBT_ATTR_BITMAPLEN(nelems)		(((int) (nelems) + 7) / 8)

static inline char *
zsbt_attr_item_payload(ZSAttributeArrayItem *aitem)
{
	return (char *) MAXALIGN(aitem->t_bitmap + ZSBT_ATTR_BITMAPLEN(aitem->t_nelements));
}

static inline bool
zsbt_attr_item_isnull(ZSAttributeArrayItem *aitem, int n)
{
	return (aitem->t_bitmap[n / 8] & (1 << (n % 8))) != 0;
}

static inline void
zsbt_attr_item_setnull(ZSAttributeArrayItem *aitem, int n)
{
	aitem->t_bitmap[n / 8] |= (1 << (n % 8));
}

typedef struct ZSAttributeCompressedItem
{
	/* these fields must match ZSAttributeItem */
	zstid		t_tid;
	uint16		t_size;
	uint16		t_flags;

	uint16		t_uncompressedsize;
	zstid		t_lasttid;	/* inclusive */

	char		t_payload[FLEXIBLE_ARRAY_MEMBER];
} ZSAttributeCompressedItem;

/*
 * Get the last TID that the given item spans.
 *
 * For an uncompressed array item, it's the TID of the last element. For
 * a compressed item, it's the last TID of the last item it contains (which
 * is stored explicitly in the item header).
 */
static inline zstid
zsbt_attr_item_lasttid(ZSAttributeItem *item)
{
	if ((item->t_flags & ZSBT_ATTR_COMPRESSED) != 0)
		return ((ZSAttributeCompressedItem *) item)->t_lasttid;
	else
	{
		ZSAttributeArrayItem *aitem = (ZSAttributeArrayItem *) item;
		return aitem->t_tid + aitem->t_nelements - 1;
	}
}



/*
 * TID B-tree leaf page layout
 *
 * Leaf pages are packed with ZSTidArrayItems. Each ZSTidArrayItem represents
 * a range of tuples, starting at 't_firsttid', up to 't_endtid' - 1. For each
 * tuple, we its TID and the UNDO pointer. The TIDs and UNDO pointers are specially
 * encoded, so that they take less space.
 *
 * Item format:
 *
 * We make use of some assumptions / observations on the TIDs and UNDO pointers
 * to pack them tightly:
 *
 * - TIDs are kept in ascending order, and the gap between two TIDs
 *   is usually very small. On a newly loaded table, all TIDs are
 *   consecutive.
 *
 * - It's common for the UNDO pointer to be old so that the tuple is
 *   visible to everyone. In that case we don't need to keep the exact value.
 *
 * - Nearby TIDs are likely to have only a few distinct UNDO pointer values.
 *
 *
 * Each item looks like this:
 *
 *  Header  |  1-16 TID codewords | 0-2 UNDO pointers | UNDO "slotwords"
 *
 * The fixed-size header contains the start and end of the TID range that
 * this item represents, and information on how many UNDO slots and codewords
 * follow in the variable-size part.
 *
 * After the fixed-size header comes the list of TIDs. They are encoded in
 * Simple-8b codewords. Simple-8b is an encoding scheme to pack multiple
 * integers in 64-bit codewords. A single codeword can pack e.g. three 20-bit
 * integers, or 20 3-bit integers, or a number of different combinations.
 * Therefore, small integers pack more tightly than larger integers. We encode
 * the difference between each TID, so in the common case that there are few
 * gaps between the TIDs, we only need a few bits per tuple. The first encoded
 * integer is always 0, because the first TID is stored explicitly in
 * t_firsttid. (TODO: storing the first constant 0 is obviously a waste of
 * space. Also, since there cannot be duplicates, we could store "delta - 1",
 * which would allow a more tight representation in some cases.)
 *
 * After the TID codeword, are so called "UNDO slots". They represent all the
 * distinct UNDO pointers in the group of TIDs that this item covers.
 * Logically, there are 4 slots. Slots 0 and 1 are special, representing
 * all-visible "old" TIDs, and "dead" TIDs. They are not stored in the item
 * itself, to save space, but logically, they can be thought to be part of
 * every item. They are included in 't_num_undo_slots', so the number of UNDO
 * pointers physically stored on an item is actually 't_num_undo_slots - 2'.
 *
 * With the 4 UNDO slots, we can represent an UNDO pointer using a 2-bit
 * slot number. If you update a tuple with a new UNDO pointer, and all four
 * slots are already in use, the item needs to be split. Hopefully that doesn't
 * happen too often (see assumptions above).
 *
 * After the UNDO slots come "UNDO slotwords". The slotwords contain the slot
 * number of each tuple in the item. The slot numbers are packed in 64 bit
 * integers, with 2 bits for each tuple.
 *
 * Representing UNDO pointers as distinct slots also has the advantage that
 * when we're scanning the TID array, we can check the few UNDO pointers in
 * the slots against the current snapshot, and remember the visibility of
 * each slot, instead of checking every UNDO pointer separately. That
 * considerably speeds up visibility checks when reading. That's one
 * advantage of this special encoding scheme, compared to e.g. using a
 * general-purpose compression algorithm on an array of TIDs and UNDO pointers.
 *
 * The physical size of an item depends on how many tuples it covers, the
 * number of codewords needed to encode the TIDs, and many distinct UNDO
 * pointers they have.
 */
typedef struct
{
	uint16		t_size;
	uint16		t_num_tids;
	uint16		t_num_codewords;
	uint16		t_num_undo_slots;

	zstid		t_firsttid;
	zstid		t_endtid;

	/* Followed by UNDO slots, and then followed by codewords */
	uint64		t_payload[FLEXIBLE_ARRAY_MEMBER];

} ZSTidArrayItem;

/*
 * We use 2 bits for the UNDO slot number for every tuple. We can therefore
 * fit 32 slot numbers in each 64-bit "slotword".
 */
#define ZSBT_ITEM_UNDO_SLOT_BITS	2
#define ZSBT_MAX_ITEM_UNDO_SLOTS	(1 << (ZSBT_ITEM_UNDO_SLOT_BITS))
#define ZSBT_ITEM_UNDO_SLOT_MASK	(ZSBT_MAX_ITEM_UNDO_SLOTS - 1)
#define ZSBT_SLOTNOS_PER_WORD		(64 / ZSBT_ITEM_UNDO_SLOT_BITS)

/*
 * To keep the item size and time needed to work with them reasonable,
 * limit the size of an item to max 16 codewords and 128 TIDs.
 */
#define ZSBT_MAX_ITEM_CODEWORDS		16
#define ZSBT_MAX_ITEM_TIDS			128

#define ZSBT_OLD_UNDO_SLOT			0
#define ZSBT_DEAD_UNDO_SLOT			1
#define ZSBT_FIRST_NORMAL_UNDO_SLOT	2

/* Number of UNDO slotwords needed for a given number of tuples */
#define ZSBT_NUM_SLOTWORDS(num_tids) ((num_tids + ZSBT_SLOTNOS_PER_WORD - 1) / ZSBT_SLOTNOS_PER_WORD)

static inline size_t
SizeOfZSTidArrayItem(int num_tids, int num_undo_slots, int num_codewords)
{
	Size		sz;

	sz = offsetof(ZSTidArrayItem, t_payload);
	sz += num_codewords * sizeof(uint64);
	sz += (num_undo_slots - ZSBT_FIRST_NORMAL_UNDO_SLOT) * sizeof(ZSUndoRecPtr);
	sz += ZSBT_NUM_SLOTWORDS(num_tids) * sizeof(uint64);

	return sz;
}

/*
 * Get pointers to the TID codewords, UNDO slots, and slotwords from an item.
 *
 * Note: this is also used to get the pointers when constructing a new item, so
 * don't assert here that the data is valid!
 */
static inline void
ZSTidArrayItemDecode(ZSTidArrayItem *item, uint64 **codewords,
					 ZSUndoRecPtr **slots, uint64 **slotwords)
{
	char		*p = (char *) item->t_payload;

	*codewords = (uint64 *) p;
	p += item->t_num_codewords * sizeof(uint64);
	*slots = (ZSUndoRecPtr *) p;
	p += (item->t_num_undo_slots - ZSBT_FIRST_NORMAL_UNDO_SLOT) * sizeof(ZSUndoRecPtr);
	*slotwords = (uint64 *) p;
}

/*
 * Get the last TID that the given item spans.
 */
static inline zstid
zsbt_tid_item_lasttid(ZSTidArrayItem *item)
{
	return item->t_endtid - 1;
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
 */
typedef struct ZSRootDirItem
{
	BlockNumber root;
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

	BlockNumber zs_fpm_head;		/* head of the Free Page Map list */

	uint16		zs_flags;
	uint16		zs_page_id;
} ZSMetaPageOpaque;

/*
 * Codes populated by zs_SatisfiesNonVacuumable. This has minimum values
 * defined based on what's needed. Heap equivalent has more states.
 */
typedef enum
{
	ZSNV_NONE,
	ZSNV_RECENTLY_DEAD	/* tuple is dead, but not deletable yet */
} ZSNV_Result;

typedef struct ZSUndoSlotVisibility
{
	TransactionId xmin;
	TransactionId xmax;
	CommandId cmin;
	uint32		speculativeToken;
	ZSNV_Result nonvacuumable_status;
} ZSUndoSlotVisibility;

static const ZSUndoSlotVisibility InvalidUndoSlotVisibility = {
	.xmin = InvalidTransactionId,
	.xmax = InvalidTransactionId,
	.cmin = InvalidCommandId,
	.speculativeToken = INVALID_SPECULATIVE_TOKEN,
	.nonvacuumable_status = ZSNV_NONE
};

typedef struct ZSTidItemIterator
{
	int			tids_allocated_size;
	zstid	   *tids;
	uint8	   *tid_undoslotnos;
	int			next_idx;
	int			num_tids;
	MemoryContext context;

	ZSUndoRecPtr  undoslots[ZSBT_MAX_ITEM_UNDO_SLOTS];
	ZSUndoSlotVisibility undoslot_visibility[ZSBT_MAX_ITEM_UNDO_SLOTS];
} ZSTidItemIterator;

/*
 * Holds the state of an in-progress scan on a zedstore Tid tree.
 */
typedef struct ZSTidTreeScan
{
	Relation	rel;

	/*
	 * memory context that should be used for any allocations that go with the scan,
	 * like the decompression buffers. This isn't a dedicated context, you must still
	 * free everything to avoid leaking! We need this because the getnext function
	 * might be called in a short-lived memory context that is reset between calls.
	 */
	MemoryContext context;

	bool		active;
	Buffer		lastbuf;
	OffsetNumber lastoff;
	zstid		nexttid;
	zstid		endtid;
	Snapshot	snapshot;

	/* in the "real" UNDO-log, this would probably be a global variable */
	ZSUndoRecPtr recent_oldest_undo;

	/* should this scan do predicate locking? Or check for conflicts? */
	bool		serializable;
	bool		acquire_predicate_tuple_locks;

	/*
	 * These fields are used, if the scan is processing an array tuple.
	 * And also for a single-item tuple - it works just like a single-element
	 * array tuple.
	 */
	ZSTidItemIterator array_iter;
} ZSTidTreeScan;

/*
 * This is convenience function to get the index aka slot number for undo and
 * visibility array. Important to note this performs "next_idx - 1" means
 * works after returning from TID scan function when the next_idx has been
 * incremented.
 */
static inline uint8
ZSTidScanCurUndoSlotNo(ZSTidTreeScan *scan)
{
	Assert(scan->array_iter.next_idx > 0);
	Assert(scan->array_iter.tid_undoslotnos != NULL);
	return (scan->array_iter.tid_undoslotnos[scan->array_iter.next_idx - 1]);
}

/*
 * Holds the state of an in-progress scan on a zedstore attribute tree.
 */
typedef struct ZSAttrTreeScan
{
	Relation	rel;
	AttrNumber	attno;
	Form_pg_attribute attdesc;

	/*
	 * memory context that should be used for any allocations that go with the scan,
	 * like the decompression buffers. This isn't a dedicated context, you must still
	 * free everything to avoid leaking! We need this because the getnext function
	 * might be called in a short-lived memory context that is reset between calls.
	 */
	MemoryContext context;

	bool		active;
	Buffer		lastbuf;
	OffsetNumber lastoff;
	zstid		nexttid;
	zstid		endtid;

	/*
	 * if we have remaining items from a compressed container tuple, they
	 * are kept in the decompressor context, and 'has_decompressed' is true.
	 */
	ZSDecompressContext decompressor;
	bool		has_decompressed;

	/*
	 * These fields are used, if the scan is processing an array tuple.
	 * And also for a single-item tuple - it works just like a single-element
	 * array tuple.
	 */
	int			array_datums_allocated_size;
	Datum	   *array_datums;
	bool	   *array_isnulls;
	int			array_next_datum;
	int			array_num_elements;

} ZSAttrTreeScan;

/*
 * We keep a this cached copy of the information in the metapage in
 * backend-private memory. In RelationData->rd_amcache.
 *
 * The cache contains the block numbers of the roots of all the tree
 * structures, for quick searches, as well as the rightmost leaf page, for
 * quick insertions to the end.
 *
 * Use zsmeta_get_cache() to get the cached struct.
 *
 * This is used together with smgr_targblock. smgr_targblock tracks the
 * physical size of the relation file. This struct is only considered valid
 * when smgr_targblock is valid. So in effect, we invalidate this whenever
 * a smgr invalidation happens. Logically, the lifetime of this is the same
 * as smgr_targblocks/smgr_fsm_nblocks/smgr_vm_nblocks, but there's no way
 * to attach an AM-specific struct directly to SmgrRelation.
 */
typedef struct ZSMetaCacheData
{
	int			cache_nattributes;

	/* For each attribute */
	struct {
		BlockNumber root;				/* root of the b-tree */
		BlockNumber rightmost; 			/* right most leaf page */
		zstid		rightmost_lokey;	/* lokey of rightmost leaf */
	} cache_attrs[FLEXIBLE_ARRAY_MEMBER];

} ZSMetaCacheData;

extern ZSMetaCacheData *zsmeta_populate_cache(Relation rel);

static inline ZSMetaCacheData *
zsmeta_get_cache(Relation rel)
{
	if (rel->rd_amcache == NULL || RelationGetTargetBlock(rel) == InvalidBlockNumber)
		zsmeta_populate_cache(rel);
	return (ZSMetaCacheData *) rel->rd_amcache;
}

/*
 * Blow away the cached ZSMetaCacheData struct. Next call to zsmeta_get_cache()
 * will reload it from the metapage.
 */
static inline void
zsmeta_invalidate_cache(Relation rel)
{
	if (rel->rd_amcache != NULL)
	{
		pfree(rel->rd_amcache);
		rel->rd_amcache = NULL;
	}
}

/*
 * zs_split_stack is used during page split, or page merge, to keep track
 * of all the modified pages. The page split (or merge) routines don't
 * modify pages directly, but they construct a list of 'zs_split_stack'
 * entries. Each entry holds a buffer, and a temporary in-memory copy of
 * a page that should be written to the buffer, once everything is completed.
 * All the buffers are exclusively-locked.
 */
typedef struct zs_split_stack zs_split_stack;

struct zs_split_stack
{
	zs_split_stack *next;

	Buffer		buf;
	Page		page;		/* temp in-memory copy of page */
	bool		recycle;	/* should the page be added to the FPM? */
};

/* prototypes for functions in zedstore_tidpage.c */
extern void zsbt_tid_begin_scan(Relation rel,
								zstid starttid, zstid endtid, Snapshot snapshot, ZSTidTreeScan *scan);
extern void zsbt_tid_reset_scan(ZSTidTreeScan *scan, zstid starttid);
extern void zsbt_tid_end_scan(ZSTidTreeScan *scan);
extern zstid zsbt_tid_scan_next(ZSTidTreeScan *scan);

extern void zsbt_tid_multi_insert(Relation rel,
								  zstid *tids, int ntuples,
								  TransactionId xid, CommandId cid,
								  uint32 speculative_token, ZSUndoRecPtr prevundoptr);
extern TM_Result zsbt_tid_delete(Relation rel, zstid tid,
								 TransactionId xid, CommandId cid,
								 Snapshot snapshot, Snapshot crosscheck, bool wait,
								 TM_FailureData *hufd, bool changingPart);
extern TM_Result zsbt_tid_update(Relation rel, zstid otid,
								 TransactionId xid,
								 CommandId cid, bool key_update, Snapshot snapshot, Snapshot crosscheck,
								 bool wait, TM_FailureData *hufd, zstid *newtid_p);
extern void zsbt_tid_clear_speculative_token(Relation rel, zstid tid, uint32 spectoken, bool forcomplete);
extern void zsbt_tid_mark_dead(Relation rel, zstid tid, ZSUndoRecPtr recent_oldest_undo);
extern IntegerSet *zsbt_collect_dead_tids(Relation rel, zstid starttid, zstid *endtid);
extern void zsbt_tid_remove(Relation rel, IntegerSet *tids);
extern TM_Result zsbt_tid_lock(Relation rel, zstid tid,
							   TransactionId xid, CommandId cid,
							   LockTupleMode lockmode, bool follow_updates,
							   Snapshot snapshot, TM_FailureData *hufd,
							   zstid *next_tid, ZSUndoSlotVisibility *visi_info);
extern void zsbt_tid_undo_deletion(Relation rel, zstid tid, ZSUndoRecPtr undoptr, ZSUndoRecPtr recent_oldest_undo);
extern zstid zsbt_get_last_tid(Relation rel);
extern void zsbt_find_latest_tid(Relation rel, zstid *tid, Snapshot snapshot);

/* prototypes for functions in zedstore_tiditem.c */
extern List *zsbt_tid_item_create_for_range(zstid tid, int nelements, ZSUndoRecPtr undo_ptr);
extern List *zsbt_tid_item_add_tids(ZSTidArrayItem *orig, zstid firsttid, int nelements,
									ZSUndoRecPtr undo_ptr, bool *modified_orig);
extern void zsbt_tid_item_unpack(ZSTidArrayItem *item, ZSTidItemIterator *iter);
extern List *zsbt_tid_item_change_undoptr(ZSTidArrayItem *orig, zstid target_tid, ZSUndoRecPtr undoptr, ZSUndoRecPtr recent_oldest_undo);
extern List *zsbt_tid_item_remove_tids(ZSTidArrayItem *orig, zstid *nexttid, IntegerSet *remove_tids,
									   ZSUndoRecPtr recent_oldest_undo);


/* prototypes for functions in zedstore_attrpage.c */
extern void zsbt_attr_begin_scan(Relation rel, TupleDesc tdesc, AttrNumber attno,
								zstid starttid, zstid endtid, ZSAttrTreeScan *scan);
extern void zsbt_attr_reset_scan(ZSAttrTreeScan *scan, zstid starttid);
extern void zsbt_attr_end_scan(ZSAttrTreeScan *scan);
extern bool zsbt_attr_scan_next(ZSAttrTreeScan *scan);

extern void zsbt_attr_multi_insert(Relation rel, AttrNumber attno,
							  Datum *datums, bool *isnulls, zstid *tids, int ndatums);

/* prototypes for functions in zedstore_btree.c */
extern zs_split_stack *zsbt_newroot(Relation rel, AttrNumber attno, int level, List *downlinks);
extern zs_split_stack *zsbt_insert_downlinks(Relation rel, AttrNumber attno,
					  zstid leftlokey, BlockNumber leftblkno, int level,
					  List *downlinks);
extern void zsbt_attr_remove(Relation rel, AttrNumber attno, IntegerSet *tids);
extern zs_split_stack *zsbt_unlink_page(Relation rel, AttrNumber attno, Buffer buf, int level);
extern Buffer zsbt_descend(Relation rel, AttrNumber attno, zstid key, int level, bool readonly);
extern bool zsbt_page_is_expected(Relation rel, AttrNumber attno, zstid key, int level, Buffer buf);

static inline void
zsbt_attr_scan_skip(ZSAttrTreeScan *scan, zstid tid)
{
	if (tid > scan->nexttid)
	{
		if (scan->array_next_datum < scan->array_num_elements)
		{
			int64		skip = tid - scan->nexttid;

			if (skip < scan->array_num_elements - scan->array_next_datum)
			{
				scan->array_next_datum += skip;
			}
			else
			{
				scan->array_next_datum = scan->array_num_elements;
			}
		}
		scan->nexttid = tid;
	}
}

/*
 * Return the value of row identified with 'tid' in a scan.
 *
 * 'tid' must be greater than any previously returned item.
 *
 * Returns true if a matching item is found, false otherwise. After
 * a false return, it's OK to call this again with another greater TID.
 */
static inline bool
zsbt_attr_fetch(ZSAttrTreeScan *scan, Datum *datum, bool *isnull, zstid tid)
{
	if (!scan->active)
		return false;

	/* skip to the given tid. */
	zsbt_attr_scan_skip(scan, tid);

	/*
	 * Fetch the next item from the scan. The item we're looking for might
	 * already be in scan->array_*.
	 */
	do
	{
		if (tid < scan->nexttid)
		{
			/* The next item from this scan is beyond the TID we're looking for. */
			return false;
		}

		if (scan->array_next_datum < scan->array_num_elements)
		{
			*isnull = scan->array_isnulls[scan->array_next_datum];
			*datum = scan->array_datums[scan->array_next_datum];
			scan->array_next_datum++;
			scan->nexttid++;
			return true;
		}
		/* Advance the scan, and check again. */
	} while (zsbt_attr_scan_next(scan));

	return false;
}

extern PGDLLIMPORT const TupleTableSlotOps TTSOpsZedstore;

/* prototypes for functions in zedstore_meta.c */
extern void zsmeta_initmetapage(Relation rel);
extern void zsmeta_initmetapage_redo(XLogReaderState *record);
extern BlockNumber zsmeta_get_root_for_attribute(Relation rel, AttrNumber attno, bool for_update);
extern void zsmeta_update_root_for_attribute(Relation rel, AttrNumber attno, Buffer metabuf, BlockNumber rootblk);
extern void zsmeta_add_root_for_new_attributes(Relation rel, Page page);

/* prototypes for functions in zedstore_visibility.c */
extern TM_Result zs_SatisfiesUpdate(Relation rel, Snapshot snapshot,
									ZSUndoRecPtr recent_oldest_undo,
									zstid item_tid, ZSUndoRecPtr item_undoptr,
									LockTupleMode mode,
									bool *undo_record_needed,
									TM_FailureData *tmfd, zstid *next_tid,
									ZSUndoSlotVisibility *visi_info);
extern bool zs_SatisfiesVisibility(ZSTidTreeScan *scan, ZSUndoRecPtr item_undoptr,
								   TransactionId *obsoleting_xid, zstid *next_tid,
								   ZSUndoSlotVisibility *visi_info);

/* prototypes for functions in zedstore_toast.c */
extern Datum zedstore_toast_datum(Relation rel, AttrNumber attno, Datum value, zstid tid);
extern Datum zedstore_toast_flatten(Relation rel, AttrNumber attno, zstid tid, Datum toasted);

/* prototypes for functions in zedstore_freepagemap.c */
extern Buffer zspage_getnewbuf(Relation rel, Buffer metabuf);
extern Buffer zspage_extendrel_newbuf(Relation rel);
extern void zspage_delete_page(Relation rel, Buffer buf);

/* prototypes for functions in zedstore_utils.c */
extern zs_split_stack *zs_new_split_stack_entry(Buffer buf, Page page);
extern void zs_apply_split_changes(Relation rel, zs_split_stack *stack);

typedef struct ZedstoreTupleTableSlot
{
	TupleTableSlot base;
	TransactionId xmin;
	CommandId cmin;

	char	   *data;		/* data for materialized slots */
} ZedstoreTupleTableSlot;

#endif							/* ZEDSTORE_INTERNAL_H */
