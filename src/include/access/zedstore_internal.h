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
#include "access/zedstore_undolog.h"
#include "lib/integerset.h"
#include "storage/bufmgr.h"
#include "storage/smgr.h"
#include "utils/datum.h"

struct zs_pending_undo_op;

#define ZS_META_ATTRIBUTE_NUM 0

#define INVALID_SPECULATIVE_TOKEN 0

/*
 * attstream_buffer is an in-memory representation of an attribute stream. It is used
 * by the operations that construct and manipulate attribute streams.
 */
typedef struct
{
	/*
	 * Enlargeable buffer. The chunks are stored in 'data', between the
	 * 'cursor' and 'len' positions. So if cursor > 0, there is some unused
	 * space before the chunks, and if data < maxlen, there is unused space
	 * after the chunks.
	 */
	char	   *data;		/* contains raw chunks */
	int			len;
	int			maxlen;
	int			cursor;		/* beginning of remaining chunks */

	/*
	 * First and last TID (inclusive) stored in the chunks.
	 */
	zstid		firsttid;
	zstid		lasttid;

	/*
	 * meta-data of the attribute, so that we don't need to pass these along
	 * as separate arguments everywhere.
	 */
	int16		attlen;
	bool		attbyval;
} attstream_buffer;

/*
 * attstream_decoder is used to unpack an attstream into tids/datums/isnulls.
 */
typedef struct
{
	/* memory context holding the buffer */
	MemoryContext cxt;

	/* this is for holding decoded element data in the arrays, reset between decoder_attstream_cont calls */
	MemoryContext tmpcxt;

	/*
	 * meta-data of the attribute, so that we don't need to pass these along
	 * as separate arguments everywhere.
	 */
	int16		attlen;
	bool		attbyval;

	/* buffer and its allocated size */
	char	   *chunks_buf;
	int			chunks_buf_size;

	/* information about the current attstream in the buffer */
	int			chunks_len;
	zstid		firsttid;
	zstid		lasttid;

	/* next position within the attstream */
	int			pos;
	zstid		prevtid;

	/*
	 * currently decoded batch of elements
	 */
/* must be >= the max number of items in one codeword (that is, >= 60)*/
#define DECODER_MAX_ELEMS	90
	zstid		tids[DECODER_MAX_ELEMS];
	Datum		datums[DECODER_MAX_ELEMS];
	bool		isnulls[DECODER_MAX_ELEMS];
	int			num_elements;
} attstream_decoder;

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
	uint16		zs_level;			/* 0 = leaf */
	BlockNumber zs_next;
	zstid		zs_lokey;		/* inclusive */
	zstid		zs_hikey;		/* exclusive */
	uint16		zs_flags;

	uint16		padding1;
	uint16		padding2;

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
 * Leaf pages in the attribute trees don't follow the normal page layout
 * with line pointers and items. They use the standard page header,
 * with pd_lower and pd_upper, but the data stored in the lower and upper
 * parts are different from the normal usage.
 *
 * The upper and lower parts of the page contain one "attribute stream"
 * each. An attibute stream contains attribute data for a range of rows.
 * Logically, it contains a list of TIDs, and their Datums and isnull
 * flags. The ranges of TIDs stored in the streams never overlap, but
 * there can be gaps, if rows have been deleted or updated.
 *
 * Physically, the stream consists of "chunks", where one chunk contains
 * the TIDs of 1-60 datums, packed in a compact form, and their datums.
 * Finally, the whole stream can be compressed. See comments in
 * zedstore_attstream.c for a more detailed description of the chunk
 * format.
 *
 * By convention, the attribute stream stored in the upper part of the
 * page, between pd_upper and pd_special, is compressed, and the lower
 * stream, stored between the page header and pd_lower, is uncompressed:
 *
 * +--------------------+
 * | PageHeaderData     |
 * +--------------------+
 * | lower attstream    |
 * | (uncompressed) ... |
 * | .................. |
 * | .................. |
 * +--------------------+ <-pd_lower
 * |                    |
 * |    (free space)    |
 * |                    |
 * +--------------------+ <-pd_upper
 * | upper attstream    |
 * | (compressed) ....  |
 * | .................. |
 * | .................. |
 * | .................. |
 * +--------------------+ <-pd_special
 * | ZSBtreePageOpaque  |
 * +--------------------+
 *
 * The point of having two streams is to allow fast appending of
 * data to a page, without having to decompress and recompress
 * the whole page. When new data is inserted, it is added to
 * the uncompressed stream, if it fits. When a page comes full,
 * the uncompressed stream is merged with the compressed stream,
 * replacing both with one larger compressed stream.
 *
 * The names "lower" and "upper" refer to the physical location of
 * the stream on the page. The data in the lower attstream
 * have higher-numbered TIDs than the data in the upper attstream.
 * No overlap is allowed. This works well with the usual usage
 * pattern that new data is added to the end (i.e. with increasing
 * sequence of TIDs), and old data is archived in compressed form
 * when a page fills up.
 */

/*
 * ZSAttStream represents one attribute stream, stored in the lower
 * or upper part of an attribute leaf page. It is also used to
 * pass around data in memory, in which case a stream can be
 * arbitrarily long.
 *
 *
 * Attstreams are compressed by feeding the stream to the compressor, until
 * all the space available on the page. However, the compressor doesn't know
 * about chunk boundaries within the stream, so it may stop the compression
 * in the middle of a chunk. As an artifact of that, a compressed stream
 * often contains an incomplete chunk at the end. That space goes wasted, and
 * is ignored. 't_decompressed_size' is the total size of all complete chunks
 * in a compressed stream, while 't_decompressed_bufsize' includes the wasted
 * bytes at the end.
 *
 * XXX: We could avoid the waste by using a compressor that knows about the
 * chunk boundaries. Or we could compress twice, first to get the size that
 * fits, and second time to compress just what fits. But that would be twice
 * as slow. In practice, the wasted space doesn't matter much. We try to
 * keep each chunk relatively small, to minimize the waste. And because we
 * know the next chunk wouldn't fit on the page anyway, there isn't much else
 * we could do with the wasted space, anyway.
 */
typedef struct
{
	uint32		t_size;			/* physical size of the stream. */
	uint32		t_flags;
	uint32		t_decompressed_size;	/* payload size, excludes waste */
	uint32		t_decompressed_bufsize;	/* payload size, includes waste */
	zstid		t_lasttid;		/* last TID stored in this stream */

	char		t_payload[FLEXIBLE_ARRAY_MEMBER];
} ZSAttStream;

#define SizeOfZSAttStreamHeader	offsetof(ZSAttStream, t_payload)

#define ATTSTREAM_COMPRESSED	1


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
	uint32      zs_decompressed_size;
	bool        zs_is_compressed;

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
	 * 'zs_undo_tail' is the newest page, where new UNDO records will be inserted,
	 * and 'zs_undo_head' is the oldest page. 'zs_undo_tail_first_counter' is the
	 * UNDO counter value of the first record on the tail page (or if the tail
	 * page is empty, the counter value the first record on the tail page will
	 * have, when it's inserted.) If there is no UNDO log at all,
	 * 'zs_undo_tail_first_counter' is the new counter value to use. It's actually
	 * redundant, except when there is no UNDO log at all, but it's a nice
	 * cross-check at other times.
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
	CommandId	cmin;
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
	 * They are filled in by zsbt_attr_scan_fetch_array().
	 */
	attstream_decoder decoder;

	/* last index into attr_decoder arrays */
	int			decoder_last_idx;

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
	bool		special_only; /* if set, only the "special" area was changed, (the
							   * rest of the page won't need to be WAL-logged */
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


extern zstid zsbt_tid_multi_insert(Relation rel, int ntuples,
								   TransactionId xid, CommandId cid,
								   uint32 speculative_token, ZSUndoRecPtr prevundoptr);
extern TM_Result zsbt_tid_delete(Relation rel, zstid tid,
								 TransactionId xid, CommandId cid,
								 Snapshot snapshot, Snapshot crosscheck, bool wait,
								 TM_FailureData *hufd, bool changingPart, bool *this_xact_has_lock);
extern TM_Result zsbt_tid_update(Relation rel, zstid otid,
								 TransactionId xid,
								 CommandId cid, bool key_update, Snapshot snapshot, Snapshot crosscheck,
								 bool wait, TM_FailureData *hufd, zstid *newtid_p, bool *this_xact_has_lock);
extern void zsbt_tid_clear_speculative_token(Relation rel, zstid tid, uint32 spectoken, bool forcomplete);
extern void zsbt_tid_mark_dead(Relation rel, zstid tid, ZSUndoRecPtr recent_oldest_undo);
extern IntegerSet *zsbt_collect_dead_tids(Relation rel, zstid starttid, zstid *endtid, uint64 *num_live_tuples);
extern void zsbt_tid_remove(Relation rel, IntegerSet *tids);
extern TM_Result zsbt_tid_lock(Relation rel, zstid tid,
							   TransactionId xid, CommandId cid,
							   LockTupleMode lockmode, bool follow_updates,
							   Snapshot snapshot, TM_FailureData *hufd,
							   zstid *next_tid, bool *this_xact_has_lock,
							   ZSUndoSlotVisibility *visi_info);
extern void zsbt_tid_undo_deletion(Relation rel, zstid tid, ZSUndoRecPtr undoptr, ZSUndoRecPtr recent_oldest_undo);
extern zstid zsbt_get_first_tid(Relation rel);
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

extern void zsbt_attr_add(Relation rel, AttrNumber attno, attstream_buffer *newstream);
extern void zsbt_attstream_change_redo(XLogReaderState *record);

/* prototypes for functions in zedstore_attstream.c */
extern void create_attstream(attstream_buffer *buffer, bool attbyval, int16 attlen,
							 int nelems, zstid *tids, Datum *datums, bool *isnulls);
extern void init_attstream_buffer(attstream_buffer *buf, bool attbyval, int16 attlen);
extern void init_attstream_buffer_from_stream(attstream_buffer *buf, bool attbyval, int16 attlen,
											  ZSAttStream *attstream, MemoryContext memcontext);
extern int append_attstream(attstream_buffer *buffer, bool all, int nelems,
							zstid *tids, Datum *datums, bool *isnulls);
extern void vacuum_attstream(Relation rel, AttrNumber attno, attstream_buffer *buffer,
							 ZSAttStream *attstream,
							 zstid *tids_to_remove, int num_tids_to_remove);

extern void merge_attstream(Form_pg_attribute attr, attstream_buffer *buffer, ZSAttStream *attstream2);
extern void merge_attstream_buffer(Form_pg_attribute attr, attstream_buffer *buffer, attstream_buffer *buffer2);

extern bool append_attstream_inplace(Form_pg_attribute att, ZSAttStream *oldstream, int freespace, attstream_buffer *newstream);

extern int find_attstream_chop_pos(Form_pg_attribute att, char *chunks, int len, zstid *lasttid);
extern void chop_attstream(attstream_buffer *buffer, int pos, zstid lasttid);

extern void print_attstream(int attlen, char *chunk, int len);

extern void init_attstream_decoder(attstream_decoder *decoder, bool attbyval, int16 attlen);
extern void destroy_attstream_decoder(attstream_decoder *decoder);
extern void decode_attstream_begin(attstream_decoder *decoder, ZSAttStream *attstream);
extern bool decode_attstream_cont(attstream_decoder *decoder);

/* prototypes for functions in zedstore_tuplebuffer.c */
extern zstid zsbt_tuplebuffer_allocate_tid(Relation rel, TransactionId xid, CommandId cid);
extern void zsbt_tuplebuffer_flush(Relation rel);
extern void zsbt_tuplebuffer_spool_tuple(Relation rel, zstid tid, Datum *datums, bool *isnulls);
extern void zsbt_tuplebuffer_spool_slots(Relation rel, zstid *tids, TupleTableSlot **slots, int ntuples);

extern void AtEOXact_zedstream_tuplebuffers(bool isCommit);


/* prototypes for functions in zedstore_btree.c */
extern zs_split_stack *zsbt_newroot(Relation rel, AttrNumber attno, int level, List *downlinks);
extern zs_split_stack *zsbt_insert_downlinks(Relation rel, AttrNumber attno,
					  zstid leftlokey, BlockNumber leftblkno, int level,
					  List *downlinks);
extern void zsbt_attr_remove(Relation rel, AttrNumber attno, IntegerSet *tids);
extern zs_split_stack *zsbt_unlink_page(Relation rel, AttrNumber attno, Buffer buf, int level);
extern zs_split_stack *zs_new_split_stack_entry(Buffer buf, Page page);
extern void zs_apply_split_changes(Relation rel, zs_split_stack *stack, struct zs_pending_undo_op *undo_op);
extern Buffer zsbt_descend(Relation rel, AttrNumber attno, zstid key, int level, bool readonly);
extern Buffer zsbt_find_and_lock_leaf_containing_tid(Relation rel, AttrNumber attno,
													 Buffer buf, zstid nexttid, int lockmode);
extern bool zsbt_page_is_expected(Relation rel, AttrNumber attno, zstid key, int level, Buffer buf);
extern void zsbt_wal_log_leaf_items(Relation rel, AttrNumber attno, Buffer buf, OffsetNumber off, bool replace, List *items, struct zs_pending_undo_op *undo_op);
extern void zsbt_wal_log_rewrite_pages(Relation rel, AttrNumber attno, List *buffers, struct zs_pending_undo_op *undo_op);

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
	if (scan->decoder.num_elements == 0 ||
		tid < scan->decoder.tids[0] ||
		tid > scan->decoder.tids[scan->decoder.num_elements - 1])
	{
		if (!zsbt_attr_scan_fetch_array(scan, tid))
			return false;
		scan->decoder_last_idx = -1;
	}
	Assert(scan->decoder.num_elements > 0 &&
		   tid >= scan->decoder.tids[0] &&
		   tid <= scan->decoder.tids[scan->decoder.num_elements - 1]);

	/*
	 * Optimize for the common case that we're scanning forward from the previous
	 * TID.
	 */
	if (scan->decoder_last_idx != -1 && scan->decoder.tids[scan->decoder_last_idx] < tid)
		idx = scan->decoder_last_idx + 1;
	else
		idx = 0;

	for (; idx < scan->decoder.num_elements; idx++)
	{
		zstid		this_tid = scan->decoder.tids[idx];

		if (this_tid == tid)
		{
			*isnull = scan->decoder.isnulls[idx];
			*datum = scan->decoder.datums[idx];
			scan->decoder_last_idx = idx;
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
extern void zsmeta_add_root_for_new_attributes(Relation rel, Page page);

/* prototypes for functions in zedstore_visibility.c */
extern TM_Result zs_SatisfiesUpdate(Relation rel, Snapshot snapshot,
									ZSUndoRecPtr recent_oldest_undo,
									zstid item_tid, ZSUndoRecPtr item_undoptr,
									LockTupleMode mode,
									bool *undo_record_needed, bool *this_xact_has_lock,
									TM_FailureData *tmfd, zstid *next_tid,
									ZSUndoSlotVisibility *visi_info);
extern bool zs_SatisfiesVisibility(ZSTidTreeScan *scan, ZSUndoRecPtr item_undoptr,
								   TransactionId *obsoleting_xid, zstid *next_tid,
								   ZSUndoSlotVisibility *visi_info);

/* prototypes for functions in zedstore_toast.c */
extern Datum zedstore_toast_datum(Relation rel, AttrNumber attno, Datum value, zstid tid);
extern Datum zedstore_toast_flatten(Relation rel, AttrNumber attno, zstid tid, Datum toasted);
extern void zedstore_toast_delete(Relation rel, Form_pg_attribute attr, zstid tid, BlockNumber blkno);

/* prototypes for functions in zedstore_freepagemap.c */
extern Buffer zspage_getnewbuf(Relation rel);
extern void zspage_mark_page_deleted(Page page, BlockNumber next_free_blk);
extern void zspage_delete_page(Relation rel, Buffer buf, Buffer metabuf);

typedef struct ZedstoreTupleTableSlot
{
	TupleTableSlot base;

	char	   *data;		/* data for materialized slots */

	/*
	 * Extra visibility information. The tuple's xmin and cmin can be extracted
	 * from here, used e.g. for triggers (XXX is that true?). There's also
	 * a flag to indicate if a tuple is vacuumable or not, which can be useful
	 * if you're scanning with SnapshotAny. That's currently used in index
	 * build.
	 */
	ZSUndoSlotVisibility *visi_info;

	/*
	 * Normally, when a tuple is retrieved from a table, 'visi_info' points to
	 * TID tree scan's data structures. But sometimes it's useful to keep the
	 * information together with the slot, e.g. whe a slot is copied, so that
	 * it doesn't depend on any data outside the slot. In that case, you can
	 * fill in 'visi_info_buf', and set visi_info = &visi_info_buf.
	 */
	ZSUndoSlotVisibility visi_info_buf;
} ZedstoreTupleTableSlot;

#endif							/* ZEDSTORE_INTERNAL_H */
