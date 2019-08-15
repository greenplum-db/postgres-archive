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
#include "access/zedstore_tid.h"
#include "access/zedstore_undo.h"
#include "lib/integerset.h"
#include "storage/bufmgr.h"
#include "storage/smgr.h"
#include "utils/datum.h"

#define ZS_META_ATTRIBUTE_NUM 0

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
 * Leaf pages in the attribute trees are packed with "array items", which
 * contain the actual user data for the column, in a compact format. Each
 * array item contains the datums for a range of TIDs. The ranges of two
 * items never overlap, but there can be gaps, if a row has been deleted
 * or updated.
 *
 * Each array item consists of a fixed header, a list of TIDs of the rows
 * contained in it, a NULL bitmap (if there are any NULLs), and the actual
 * Datum data. The TIDs are encoded using Simple-8b encoding, like in the
 * TID tree.
 *
 * The data (including the TID codewords) can be compressed. In that case,
 * ZSAttributeCompressedItem is used. The fields are mostly the same as in
 * ZSAttributeArrayItem, and we cast between the two liberally.
 *
 * The datums are packed in a custom format. Fixed-width datatypes are
 * stored as is, but without any alignment padding. Variable-length
 * datatypes are *not* stored in the usual Postgres varlen format; the
 * following encoding is used instead:
 *
 * Each varlen datum begins with a one or two byte header, to store the
 * size. If the size of the datum, excluding the varlen header, is <=
 * 128, then a one byte header is used. Otherwise, the high bit of the
 * first byte is set, and two bytes are used to represent the size.
 * Two bytes is always enough, because if a datum is larger than a page,
 * it must be toasted.
 *
 * Traditional Postgres toasted datums should not be seen on-disk in
 * zedstore. However, "zedstore-toasted" datums, i.e. datums that have been
 * stored on separate toast blocks within zedstore, are possible. They
 * are stored with magic 0xFF 0xFF as the two header bytes, followed by
 * the block number of the first toast block.
 *
 * 0xxxxxxx [up to 128 bytes of data follows]
 * 1xxxxxxx xxxxxxxx [data]
 * 11111111 11111111 toast pointer.
 *
 * XXX Heikki: I'm not sure if this special encoding makes sense. Perhaps
 * just storing normal Postgres varlenas would be better. Having a custom
 * encoding felt like a good idea, but I'm not sure we're actually gaining
 * anything. If we also did alignment padding, like the rest of Postgres
 * does, then we could avoid some memory copies when decoding the array.
 *
 * TODO: squeeze harder: eliminate padding, use high bits of t_tid for flags or size
 */
typedef struct ZSAttributeArrayItem
{
	uint16		t_size;
	uint16		t_flags;

	uint16		t_num_elements;
	uint16		t_num_codewords;

	zstid		t_firsttid;
	zstid		t_endtid;

	uint64		t_tid_codewords[FLEXIBLE_ARRAY_MEMBER];

	/* NULL bitmap follows, if ZSBT_HAS_NULLS is set */

	/* The Datum data follows */
} ZSAttributeArrayItem;

typedef struct ZSAttributeCompressedItem
{
	uint16		t_size;
	uint16		t_flags;

	uint16		t_num_elements;
	uint16		t_num_codewords;

	zstid		t_firsttid;
	zstid		t_endtid;

	uint16		t_uncompressed_size;

	/* compressed data follows */
	char		t_payload[FLEXIBLE_ARRAY_MEMBER];

} ZSAttributeCompressedItem;

/*
 * The two structs above are stored on disk. ZSExplodedItem is a third
 * representation of an array item that is only used in memory, when
 * repacking items on a page. It is distinguished by t_size == 0.
 */
typedef struct ZSExplodedItem
{
	uint16		t_size; /* dummy 0 */
	uint16		t_flags;

	uint16		t_num_elements;

	zstid	   *tids;

	bits8	   *nullbitmap;

	char	   *datumdata;
	int			datumdatasz;
} ZSExplodedItem;

#define ZSBT_ATTR_COMPRESSED		0x0001
#define ZSBT_HAS_NULLS				0x0002

#define ZSBT_ATTR_BITMAPLEN(nelems)		(((int) (nelems) + 7) / 8)

static inline void
zsbt_attr_item_setnull(bits8 *nullbitmap, int n)
{
	nullbitmap[n / 8] |= (1 << (n % 8));
}

static inline bool
zsbt_attr_item_isnull(bits8 *nullbitmap, int n)
{
	return (nullbitmap[n / 8] & (1 << (n % 8))) != 0;
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
	/*
	 * Head and tail page of the UNDO log.
	 *
	 * 'zs_undo_tail' is the newest page, where new UNDO records will be
	 * inserted, and 'zs_undo_head' is the oldest page.
	 * 'zs_undo_tail_first_counter' is the UNDO counter value of the first
	 * record on the tail page (or if the tail page is empty, the counter
	 * value the first trecord on the tail page will have, when it's
	 * inserted). If there is no UNDO log at all,
	 *  'zs_undo_tail_first_counter' is the new counter value to use. It's
	 * actually redundant, except when there is no UNDO log at all, but it's
	 * a nice cross-check at other times.
	 */
	BlockNumber	zs_undo_head;
	BlockNumber	zs_undo_tail;
	uint64		zs_undo_tail_first_counter;

	/*
	 * Oldest UNDO record that is still needed. Anything older than this can
	 * be discarded, and considered as visible to everyone.
	 */
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
	Snapshot	snapshot;

	/*
	 * starttid and endtid define a range of TIDs to scan. currtid is the previous
	 * TID that was returned from the scan. They determine what zsbt_tid_scan_next()
	 * will return.
	 */
	zstid		starttid;
	zstid		endtid;
	zstid		currtid;

	/* in the "real" UNDO-log, this would probably be a global variable */
	ZSUndoRecPtr recent_oldest_undo;

	/* should this scan do predicate locking? Or check for conflicts? */
	bool		serializable;
	bool		acquire_predicate_tuple_locks;

	/*
	 * These fields are used, when the scan is processing an array item.
	 */
	ZSTidItemIterator array_iter;
	int			array_curr_idx;
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
	Assert(scan->array_curr_idx >= 0 && scan->array_curr_idx < scan->array_iter.num_tids);
	Assert(scan->array_iter.tid_undoslotnos != NULL);
	return (scan->array_iter.tid_undoslotnos[scan->array_curr_idx]);
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

	/*
	 * These fields are used, when the scan is processing an array tuple.
	 * They are filled in by zsbt_attr_item_extract().
	 */
	int			array_datums_allocated_size;
	Datum	   *array_datums;
	bool	   *array_isnulls;
	zstid	   *array_tids;
	int			array_num_elements;

	int			array_curr_idx;

	/* working areas for zsbt_attr_item_extract() */
	char	   *decompress_buf;
	int			decompress_buf_size;
	char	   *attr_buf;
	int			attr_buf_size;

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
extern void zsbt_tid_begin_scan(Relation rel, zstid starttid, zstid endtid,
								Snapshot snapshot, ZSTidTreeScan *scan);
extern void zsbt_tid_reset_scan(ZSTidTreeScan *scan, zstid starttid, zstid endtid, zstid currtid);
extern void zsbt_tid_end_scan(ZSTidTreeScan *scan);
extern bool zsbt_tid_scan_next_array(ZSTidTreeScan *scan, zstid nexttid, ScanDirection direction);

/*
 * Return the next TID in the scan.
 *
 * The next TID means the first TID > scan->currtid. Each call moves
 * scan->currtid to the last returned TID. You can call zsbt_tid_reset_scan()
 * to change the position, scan->starttid and scan->endtid define the
 * boundaries of the search.
 */
static inline zstid
zsbt_tid_scan_next(ZSTidTreeScan *scan, ScanDirection direction)
{
	zstid		nexttid;
	int			idx;

	Assert(scan->active);

	if (direction == ForwardScanDirection)
		nexttid = scan->currtid + 1;
	else if (direction == BackwardScanDirection)
		nexttid = scan->currtid - 1;
	else
		nexttid = scan->currtid;

	if (scan->array_iter.num_tids == 0 ||
		nexttid < scan->array_iter.tids[0] ||
		nexttid > scan->array_iter.tids[scan->array_iter.num_tids - 1])
	{
		scan->array_curr_idx = -1;
		if (!zsbt_tid_scan_next_array(scan, nexttid, direction))
		{
			scan->currtid = nexttid;
			return InvalidZSTid;
		}
	}

	/*
	 * Optimize for the common case that we're scanning forward from the previous
	 * TID.
	 */
	if (scan->array_curr_idx >= 0 && scan->array_iter.tids[scan->array_curr_idx] < nexttid)
		idx = scan->array_curr_idx + 1;
	else
		idx = 0;

	for (; idx < scan->array_iter.num_tids; idx++)
	{
		zstid		this_tid = scan->array_iter.tids[idx];

		if (this_tid >= scan->endtid)
		{
			scan->currtid = nexttid;
			return InvalidZSTid;
		}

		if (this_tid >= nexttid)
		{
			/*
			 * Callers using SnapshotDirty need some extra visibility information.
			 */
			if (scan->snapshot->snapshot_type == SNAPSHOT_DIRTY)
			{
				int			slotno = scan->array_iter.tid_undoslotnos[idx];
				ZSUndoSlotVisibility *visi_info = &scan->array_iter.undoslot_visibility[slotno];

				if (visi_info->xmin != FrozenTransactionId)
					scan->snapshot->xmin = visi_info->xmin;
				scan->snapshot->xmax = visi_info->xmax;
				scan->snapshot->speculativeToken = visi_info->speculativeToken;
			}

			/* on next call, continue the scan at the next TID */
			scan->currtid = this_tid;
			scan->array_curr_idx = idx;
			return this_tid;
		}
	}

	/*
	 * unreachable, because zsbt_tid_scan_next_array() should never return an array
	 * that doesn't contain a matching TID.
	 */
	Assert(false);
	return InvalidZSTid;
}


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


/* prototypes for functions in zedstore_attpage.c */
extern void zsbt_attr_begin_scan(Relation rel, TupleDesc tdesc, AttrNumber attno,
								 ZSAttrTreeScan *scan);
extern void zsbt_attr_end_scan(ZSAttrTreeScan *scan);
extern bool zsbt_attr_scan_fetch_array(ZSAttrTreeScan *scan, zstid tid);

extern void zsbt_attr_multi_insert(Relation rel, AttrNumber attno,
							  Datum *datums, bool *isnulls, zstid *tids, int ndatums);

/* prototypes for functions in zedstore_attitem.c */
extern List *zsbt_attr_create_items(Form_pg_attribute att,
									Datum *datums, bool *isnulls, zstid *tids, int nelements);
extern void zsbt_split_item(Form_pg_attribute attr, ZSExplodedItem *origitem, zstid first_right_tid,
					 ZSExplodedItem **leftitem_p, ZSExplodedItem **rightitem_p);
extern ZSExplodedItem *zsbt_attr_remove_from_item(Form_pg_attribute attr,
												  ZSAttributeArrayItem *olditem,
												  zstid *removetids);
extern List *zsbt_attr_recompress_items(Form_pg_attribute attr, List *olditems);

extern void zsbt_attr_item_extract(ZSAttrTreeScan *scan, ZSAttributeArrayItem *item);


/* prototypes for functions in zedstore_btree.c */
extern zs_split_stack *zsbt_newroot(Relation rel, AttrNumber attno, int level, List *downlinks);
extern zs_split_stack *zsbt_insert_downlinks(Relation rel, AttrNumber attno,
					  zstid leftlokey, BlockNumber leftblkno, int level,
					  List *downlinks);
extern void zsbt_attr_remove(Relation rel, AttrNumber attno, IntegerSet *tids);
extern zs_split_stack *zsbt_unlink_page(Relation rel, AttrNumber attno, Buffer buf, int level);
extern zs_split_stack *zs_new_split_stack_entry(Buffer buf, Page page);
extern void zs_apply_split_changes(Relation rel, zs_split_stack *stack);
extern Buffer zsbt_descend(Relation rel, AttrNumber attno, zstid key, int level, bool readonly);
extern Buffer zsbt_find_and_lock_leaf_containing_tid(Relation rel, AttrNumber attno,
													 Buffer buf, zstid nexttid, int lockmode);
extern bool zsbt_page_is_expected(Relation rel, AttrNumber attno, zstid key, int level, Buffer buf);

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
	int			idx;

	/*
	 * Fetch the next item from the scan. The item we're looking for might
	 * already be in scan->array_*.
	 */
	if (scan->array_num_elements == 0 ||
		tid < scan->array_tids[0] ||
		scan->array_tids[scan->array_num_elements - 1] < tid)
	{
		if (!zsbt_attr_scan_fetch_array(scan, tid))
			return false;
		scan->array_curr_idx = -1;
	}
	Assert(scan->array_num_elements > 0 &&
		   scan->array_tids[0] <= tid &&
		   scan->array_tids[scan->array_num_elements - 1] >= tid);

	/*
	 * Optimize for the common case that we're scanning forward from the previous
	 * TID.
	 */
	if (scan->array_curr_idx != -1 && scan->array_tids[scan->array_curr_idx] < tid)
		idx = scan->array_curr_idx + 1;
	else
		idx = 0;

	for (; idx < scan->array_num_elements; idx++)
	{
		zstid		this_tid = scan->array_tids[idx];

		if (this_tid == tid)
		{
			*isnull = scan->array_isnulls[idx];
			*datum = scan->array_datums[idx];
			scan->array_curr_idx = idx;
			return true;
		}
		if (this_tid > tid)
			return false;
	}

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

typedef struct ZedstoreTupleTableSlot
{
	TupleTableSlot base;
	TransactionId xmin;
	CommandId cmin;

	char	   *data;		/* data for materialized slots */
} ZedstoreTupleTableSlot;

#endif							/* ZEDSTORE_INTERNAL_H */
