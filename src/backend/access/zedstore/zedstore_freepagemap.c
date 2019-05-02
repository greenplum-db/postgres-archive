/*-------------------------------------------------------------------------
 *
 * zedstore_freepagemap.c
 *	  ZedStore free space management
 *
 * The Free Page Map keeps track of unused pages in the relation.
 *
 * The FPM is a b-tree, indexed by physical block number.  To be more compact,
 * it stores "extents", i.e. block ranges, rather than just blocks, when
 * possible.

 * Design principles:
 *
 * - it's ok to have a block incorrectly stored in the FPM. Before actually
 *   reusing a page, we must check that it's safe.
 *
 * - a deletable page must be simple to detect just by looking at the page,
 *   and perhaps a few other pages. It should *not* require scanning the
 *   whole table, or even a whole b-tree. For example, if a column is dropped,
 *   we can detect if a b-tree page belongs to the dropped column just by
 *   looking at the information (the attribute number) stored in the page
 *   header.
 *
 * - if a page is deletable, it should become immediately reusable. No
 *   "wait out all possible readers that might be about to follow a link
 *   to it" business. All code that reads pages need to keep pages locked
 *   while following a link, or be prepared to retry if they land on an
 *   unexpected page.
 *
 *
 * TODO:
 *
 * - Avoid fragmentation. If B-tree page is split, try to hand out a page
 *   that's close to the old page. When the relation is extended, allocate
 *   a larger chunk at once.
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/zedstore/zedstore_freepagemap.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "access/zedstore_internal.h"
#include "miscadmin.h"
#include "storage/bufpage.h"
#include "storage/lmgr.h"
#include "utils/rel.h"

/*
 * On-disk format of the Free Page Map.
 *
 * The FPM is a b-tree, indexed by block number. Each page contains a
 * ZSFreePageMapOpaque in the "special area", and an array of
 * ZSFreePageMapItems as the content (ie. after the normal page header,
 * up to pd_lower). On an internal page, each item contains the starting
 * block number, and a pointer to the child FPM page. On a leaf page,
 * each entry contains the start and end of the block range that the item
 * represents.
 *
 * The block ranges stored on leaf pages must not overlap!
 */
typedef struct
{
	BlockNumber	zs_lokey;		/* inclusive */
	BlockNumber	zs_hikey;		/* exclusive */
	uint16		zs_level;			/* 0 = leaf */
	uint16		zs_flags;
	char		padding[2];			/* padding, to put zs_page_id last */
	uint16		zs_page_id;			/* always ZS_FPM_PAGE_ID */
} ZSFreePageMapOpaque;

typedef struct
{
	BlockNumber	zs_startblk;	/* inclusive */
	union {
		BlockNumber	zs_endblk;		/* on a leaf page, end of extent, exclusive */
		BlockNumber	zs_downlink;	/* on an internal page, pointer to child */
	} u;
} ZSFreePageMapItem;

#define ZSFreePageMapGetOpaque(page) ((ZSFreePageMapOpaque *) PageGetSpecialPointer(page))

/* overlap, or touch? */
static inline bool
zsextent_overlap(BlockNumber start1, BlockNumber end1, BlockNumber start2, BlockNumber end2)
{
	if (start2 < end1)
		return false;
	if (start1 < end2)
		return false;
	return true;
}

static inline ZSFreePageMapItem *
ZSFreePageMapPageGetItems(Page page)
{
	ZSFreePageMapItem *items;

	items = (ZSFreePageMapItem *) PageGetContents(page);

	return items;
}
static inline int
ZSFreePageMapPageGetNumItems(Page page)
{
	ZSFreePageMapItem *begin;
	ZSFreePageMapItem *end;

	begin = (ZSFreePageMapItem *) PageGetContents(page);
	end = (ZSFreePageMapItem *) ((char *) page + ((PageHeader) page)->pd_lower);

	return end - begin;
}

/*
 * zsfpm_split_stack is used during page split, or page merge, to keep track
 * of all the modified pages.
 */
typedef struct zsfpm_split_stack zsfpm_split_stack;

struct zsfpm_split_stack
{
	zsfpm_split_stack *next;

	Buffer		buf;
	Page		page;
};

static zsfpm_split_stack *zsfpm_unlink_page(Relation rel, Buffer buf, int level, Buffer metabuf);
static zsfpm_split_stack *zsfpm_merge_pages(Relation rel, Buffer leftbuf, Buffer rightbuf, bool target_is_left, Buffer metabuf);
static BlockNumber zsfpm_consume_page(Relation rel, Buffer metabuf);
static void zsfpm_insert(Relation rel, BlockNumber startblk, BlockNumber endblk);
static zsfpm_split_stack *zsfpm_split(Relation rel, Buffer leftbuf,
						int newpos, ZSFreePageMapItem *newitem);
static zsfpm_split_stack *zsfpm_insert_downlink(Relation rel, Buffer leftbuf,
								  BlockNumber rightlokey, BlockNumber rightblkno);
static zsfpm_split_stack *zsfpm_newroot(Relation rel, Buffer metabuf, int level,
			  ZSFreePageMapItem *item1, ZSFreePageMapItem *item2);
static Buffer zsfpm_descend(Relation rel, Buffer metabuf, BlockNumber key, int level);
static int zsfpm_binsrch_blkno(BlockNumber key, ZSFreePageMapItem *arr, int arr_elems);

/*
 * zspage_is_recyclable()
 *
 * Is the current page recyclable?
 *
 * It can be:
 *
 * - an empty, all-zeros page,
 * - explicitly marked as deleted,
 * - an UNDO page older than oldest_undo_ptr
 * - a b-tree page belonging to a deleted attribute
 * - a TOAST page belonging to a dead item
 *
 */
static bool
zspage_is_recyclable(Buffer buf)
{
	if (PageIsNew(BufferGetPage(buf)))
		return true;
	return false;
}


static void
zsfpm_delete_leaf(Relation rel, Buffer buf, Buffer metabuf)
{
	Page		page = BufferGetPage(buf);
	ZSFreePageMapOpaque *opaque = ZSFreePageMapGetOpaque(page);

	if (opaque->zs_lokey == 0 && opaque->zs_hikey == MaxBlockNumber + 1)
	{
		/* Don't delete the last leaf page. Just mark it empty */
		START_CRIT_SECTION();

		((PageHeader) page)->pd_lower = SizeOfPageHeaderData;

		MarkBufferDirty(buf);

		/* TODO: WAL-log */

		END_CRIT_SECTION();

		UnlockReleaseBuffer(buf);

		return;
	}
	else
	{
		zsfpm_split_stack *stack;

		stack = zsfpm_unlink_page(rel, buf, 0, metabuf);

		START_CRIT_SECTION();
		while (stack)
		{
			zsfpm_split_stack *next;

			PageRestoreTempPage(stack->page, BufferGetPage(stack->buf));
			MarkBufferDirty(stack->buf);
			UnlockReleaseBuffer(stack->buf);

			next = stack->next;
			pfree(stack);
			stack = next;
		}

		/* TODO: WAL-log */
		END_CRIT_SECTION();
	}
}

/*
 * Removes the last item from page, and unlinks the page from the tree.
 *
 *
 * NOTE: you cannot remove the only leaf.
 */
static zsfpm_split_stack *
zsfpm_unlink_page(Relation rel, Buffer buf, int level, Buffer metabuf)
{
	Page		page = BufferGetPage(buf);
	ZSFreePageMapOpaque *opaque = ZSFreePageMapGetOpaque(page);
	Buffer		leftbuf;
	Buffer		rightbuf;
	bool		target_is_left;

	Assert(opaque->zs_lokey != 0 || opaque->zs_hikey != MaxBlockNumber + 1);
	Assert(ZSFreePageMapPageGetNumItems(page) == 1);

	/*
	 * Find left sibling.
	 * or if this is leftmost page, find right sibling.
	 */
	if (opaque->zs_lokey != 0)
	{
		rightbuf = buf;
		leftbuf = zsfpm_descend(rel, metabuf, opaque->zs_lokey, level);
		target_is_left = false;
	}
	else
	{
		rightbuf = zsfpm_descend(rel, metabuf, opaque->zs_hikey, level);
		leftbuf = buf;
		target_is_left = true;
	}

	return zsfpm_merge_pages(rel, leftbuf, rightbuf, target_is_left, metabuf);
}

/*
 * Page deletion:
 *
 * Mark page empty, remove downlink. If parent becomes empty, recursively delete it.
 *
 * Unlike in the nbtree index, we don't need to worry about concurrent scans. They
 * will simply retry if they land on an unexpected page.
 */
static zsfpm_split_stack *
zsfpm_merge_pages(Relation rel, Buffer leftbuf, Buffer rightbuf, bool target_is_left, Buffer metabuf)
{
	Buffer		parentbuf;
	Page		origleftpage;
	Page		leftpage;
	Page		rightpage;
	ZSFreePageMapOpaque *leftopaque;
	ZSFreePageMapOpaque *rightopaque;
	ZSFreePageMapItem *leftitems;
	ZSFreePageMapItem *origleftitems;
	ZSFreePageMapItem *rightitems;
	ZSFreePageMapItem *parentitems;
	int			origleftnitems;
	int			rightnitems;
	int			parentnitems;
	Page		parentpage;
	int			itemno;
	zsfpm_split_stack *stack;
	zsfpm_split_stack *stack_head;
	zsfpm_split_stack *stack_tail;

	origleftpage = BufferGetPage(leftbuf);
	leftpage = PageGetTempPageCopySpecial(origleftpage);
	leftopaque = ZSFreePageMapGetOpaque(leftpage);

	origleftitems = ZSFreePageMapPageGetItems(origleftpage);
	origleftnitems = ZSFreePageMapPageGetNumItems(origleftpage);

	leftitems = ZSFreePageMapPageGetItems(leftpage);

	rightpage = BufferGetPage(rightbuf);
	rightopaque = ZSFreePageMapGetOpaque(rightpage);
	rightitems = ZSFreePageMapPageGetItems(rightpage);
	rightnitems = ZSFreePageMapPageGetNumItems(rightpage);

	/* move all items from right to left */

	if (target_is_left)
	{
		Assert(origleftnitems == 1);

		memcpy(leftitems,
			   rightitems,
			   rightnitems * sizeof(ZSFreePageMapItem));
		((PageHeader) leftpage)->pd_lower += rightnitems * sizeof(ZSFreePageMapItem);
	}
	else
	{
		origleftitems = ZSFreePageMapPageGetItems(origleftpage);
		leftitems = ZSFreePageMapPageGetItems(leftpage);

		Assert(rightnitems == 1);

		memcpy(leftitems,
			   origleftitems,
			   origleftnitems * sizeof(ZSFreePageMapItem));
	}

	/* update left hikey */
	leftopaque->zs_hikey = ZSFreePageMapGetOpaque(rightpage)->zs_hikey;

	Assert(ZSFreePageMapGetOpaque(leftpage)->zs_level == ZSFreePageMapGetOpaque(rightpage)->zs_level);

	stack = palloc(sizeof(zsfpm_split_stack));
	stack->next = NULL;
	stack->buf = leftbuf;
	stack->page = leftpage;
	stack_head = stack_tail = stack;

	/* Mark right page as empty/unused */
	rightpage = palloc0(BLCKSZ);

	stack = palloc(sizeof(zsfpm_split_stack));
	stack->next = NULL;
	stack->buf = rightbuf;
	stack->page = rightpage;
	stack_tail->next = stack;
	stack_tail = stack;

	/* find downlink for 'rightbuf' in the parent */
	parentbuf = zsfpm_descend(rel, metabuf, rightopaque->zs_lokey, leftopaque->zs_level + 1);
	parentpage = BufferGetPage(parentbuf);

	parentitems = ZSFreePageMapPageGetItems(parentpage);
	parentnitems = ZSFreePageMapPageGetNumItems(parentpage);
	itemno = zsfpm_binsrch_blkno(rightopaque->zs_lokey, parentitems, parentnitems);
	if (itemno < 0 || parentitems[itemno].u.zs_downlink != BufferGetBlockNumber(rightbuf))
		elog(ERROR, "could not find downlink to FPM page %u", BufferGetBlockNumber(rightbuf));

	/* remove downlink from parent */
	if (parentnitems > 1)
	{
		Page		newpage = PageGetTempPageCopySpecial(parentpage);
		ZSFreePageMapItem *newitems = ZSFreePageMapPageGetItems(newpage);

		memcpy(newitems, parentitems, itemno * sizeof(ZSFreePageMapItem));
		memcpy(&newitems[itemno], &parentitems[itemno + 1], (parentnitems - itemno -1) * sizeof(ZSFreePageMapItem));

		((PageHeader) newpage)->pd_lower += (parentnitems - 1) * sizeof(ZSFreePageMapItem);

		stack = palloc(sizeof(zsfpm_split_stack));
		stack->next = NULL;
		stack->buf = parentbuf;
		stack->page = newpage;
		stack_tail->next = stack;
		stack_tail = stack;
	}
	else
	{
		/* the parent becomes empty as well. Recursively remove it. */
		stack_tail->next = zsfpm_unlink_page(rel, parentbuf, leftopaque->zs_level + 1, metabuf);
	}
	return stack_head;
}

/*
 * Allocate a new page.
 *
 * The page is exclusive-locked, but not initialized.
 */
Buffer
zspage_getnewbuf(Relation rel, Buffer metabuf)
{
	bool		release_metabuf;
	Buffer		buf;
	BlockNumber blk;

	if (metabuf == InvalidBuffer)
	{
		metabuf = ReadBuffer(rel, ZS_META_BLK);
		LockBuffer(metabuf, BUFFER_LOCK_SHARE);
		release_metabuf = true;
	}
	else
		release_metabuf = false;

retry:
	/* Get a block from the FPM. */
	blk = zsfpm_consume_page(rel, metabuf);
	if (blk == 0)
	{
		/* metapage, not expected */
		elog(ERROR, "could not find valid page in FPM");
	}
	if (blk == InvalidBlockNumber)
	{
		/* No free pages. Have to extend the relation. */
		buf = zspage_extendrel_newbuf(rel);
		blk = BufferGetBlockNumber(buf);
	}
	else
	{
		buf = ReadBuffer(rel, blk);
		LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

		/* Check that the page really is unused. */
		if (!zspage_is_recyclable(buf))
		{
			UnlockReleaseBuffer(buf);
			goto retry;
		}
	}

	if (release_metabuf)
		UnlockReleaseBuffer(metabuf);
	return buf;
}

/*
 * Extend the relation.
 *
 * Returns the new page, exclusive-locked.
 */
Buffer
zspage_extendrel_newbuf(Relation rel)
{
	Buffer		buf;
	bool		needLock;

	/*
	 * Extend the relation by one page.
	 *
	 * We have to use a lock to ensure no one else is extending the rel at
	 * the same time, else we will both try to initialize the same new
	 * page.  We can skip locking for new or temp relations, however,
	 * since no one else could be accessing them.
	 */
	needLock = !RELATION_IS_LOCAL(rel);

	if (needLock)
		LockRelationForExtension(rel, ExclusiveLock);

	buf = ReadBuffer(rel, P_NEW);

	/* Acquire buffer lock on new page */
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

	/*
	 * Release the file-extension lock; it's now OK for someone else to
	 * extend the relation some more.  Note that we cannot release this
	 * lock before we have buffer lock on the new page, or we risk a race
	 * condition against btvacuumscan --- see comments therein.
	 */
	if (needLock)
		UnlockRelationForExtension(rel, ExclusiveLock);

	return buf;
}


/*
 * Explictly mark a page as deleted and recyclable, and add it to the FPM.
 *
 * The caller must hold an exclusive-lock on the page.
 */
void
zspage_delete_page(Relation rel, Buffer buf)
{
	BlockNumber blk = BufferGetBlockNumber(buf);
	Page		page;

	page = BufferGetPage(buf);
	memset(page, 0, BLCKSZ);

	zsfpm_insert(rel, blk, blk + 1);
}

/*
 * Remove and return a page from the FPM.
 */
static BlockNumber
zsfpm_consume_page(Relation rel, Buffer metabuf)
{
	/* TODO: add some smarts, to allocate the page nearby old page, etc. */
	/* currently, we just pick the first available page. */
	Page		metapage;
	ZSMetaPageOpaque *metaopaque;
	BlockNumber rootblk;
	Buffer		buf;
	Page		page;
	ZSFreePageMapItem *items;
	int			nitems;
	BlockNumber result;

	metapage = BufferGetPage(metabuf);
	metaopaque = (ZSMetaPageOpaque *) PageGetSpecialPointer(metapage);
	rootblk = metaopaque->zs_fpm_root;

	if (rootblk == InvalidBlockNumber)
		return InvalidBlockNumber;

	buf = zsfpm_descend(rel, metabuf, 0, 0);
	page = BufferGetPage(buf);

	items = ZSFreePageMapPageGetItems(page);
	nitems = ZSFreePageMapPageGetNumItems(page);

	if (nitems == 0)
	{
		UnlockReleaseBuffer(buf);
		return InvalidBlockNumber;
	}

	result = items[0].zs_startblk;
	items[0].zs_startblk++;
	if (items[0].u.zs_endblk == items[0].zs_startblk)
	{
		if (nitems > 1)
		{
			memmove(&items[0],
					&items[1],
					(nitems - 1) * sizeof(ZSFreePageMapItem));
			((PageHeader) page)->pd_lower -= sizeof(ZSFreePageMapItem);

			UnlockReleaseBuffer(buf);
		}
		else
		{
			zsfpm_delete_leaf(rel, buf, metabuf);
			/* zsfpm_delete_leaf() released 'buf' */
		}
	}
	else
	{
		UnlockReleaseBuffer(buf);
	}
	return result;
}

/*
 * Add a block range to the FPM.
 */
static void
zsfpm_insert(Relation rel, BlockNumber startblk, BlockNumber endblk)
{
	Buffer		metabuf;
	Page		metapage;
	ZSMetaPageOpaque *metaopaque;
	BlockNumber rootblk;
	Buffer		buf;
	Page		page;
	ZSFreePageMapItem *items;
	int			nitems;
	int			pos;
	int			replacepos_first;
	int			replacepos_last;

	metabuf = ReadBuffer(rel, ZS_META_BLK);
	metapage = BufferGetPage(metabuf);
	LockBuffer(metabuf, BUFFER_LOCK_EXCLUSIVE);	/* TODO: get shared lock first */
	metaopaque = (ZSMetaPageOpaque *) PageGetSpecialPointer(metapage);
	rootblk = metaopaque->zs_fpm_root;

	if (rootblk == InvalidBlockNumber)
	{
		/* Create a new FPM root page */
		ZSFreePageMapOpaque *opaque;

		buf = zspage_extendrel_newbuf(rel);
		page = BufferGetPage(buf);
		rootblk = BufferGetBlockNumber(buf);

		PageInit(page, BLCKSZ, sizeof(ZSFreePageMapOpaque));
		opaque = ZSFreePageMapGetOpaque(page);
		opaque->zs_lokey = 0;
		opaque->zs_hikey = MaxBlockNumber + 1;
		opaque->zs_level = 0;
		opaque->zs_flags = 0;
		opaque->zs_page_id = ZS_FPM_PAGE_ID;

		metaopaque->zs_fpm_root = rootblk;

		items = ZSFreePageMapPageGetItems(page);
		Assert(ZSFreePageMapPageGetNumItems(page) == 0);
		items[0].zs_startblk = startblk;
		items[0].u.zs_endblk = endblk;

		/* TODO: WAL-logging */

		MarkBufferDirty(metabuf);
		MarkBufferDirty(buf);

		UnlockReleaseBuffer(metabuf);
		UnlockReleaseBuffer(buf);
		return;
	}

	/* Descend to the correct leaf page for this block */

	buf = zsfpm_descend(rel, metabuf, startblk, 0);

	UnlockReleaseBuffer(metabuf);

	page = BufferGetPage(buf);
	items = ZSFreePageMapPageGetItems(page);
	nitems = ZSFreePageMapPageGetNumItems(page);

	pos = zsfpm_binsrch_blkno(startblk, items, nitems);

	/* FIXME: this merging business won't work correctly if the range crosses
	 * a b-tree page boundary. Not a problem currently, when we only insert
	 * individual pages.
	 */

	/* Check if this item can be merged with the previous item */
	replacepos_first = -1;
	if (pos >= 0 && items[pos].u.zs_endblk >= startblk)
	{
		replacepos_first = pos;
	}
	/* If not, can this be merged with the next item? */
	else if (pos + 1 < nitems && endblk >= items[pos + 1].zs_startblk)
	{
		/* yes, merge */
		replacepos_first = pos + 1;
	}

	if (replacepos_first >= 0)
	{
		/* adjust the start block of this item */
		if (startblk < items[replacepos_first].zs_startblk)
		{
			items[replacepos_first].zs_startblk = startblk;
		}

		/*
		 * The new end block might overlap with any number of existing
		 * ranges. Replace all overlapping ranges with one range that
		 * covers them all.
		 */
		replacepos_last = replacepos_first;
		if (endblk > items[replacepos_first].u.zs_endblk)
		{
			int			j;
			BlockNumber replace_end;

			replace_end = endblk;

			for (j = replacepos_first + 1; j < nitems; j++)
			{
				if (items[j].zs_startblk > replace_end)
					break;

				/*
				 * This item will be replaced. Check the end, to see
				 * if this is the last one that can be replaced.
				 */
				replacepos_last = j;

				if (items[j].u.zs_endblk > replace_end)
				{
					replace_end = items[j].u.zs_endblk;
					break;
				}
			}

			items[replacepos_first].u.zs_endblk = replace_end;
		}

		/* we already adjusted the item at 'replacepos_first'. Remove the rest. */
		if (replacepos_last > replacepos_first)
		{
			int			move_items = (replacepos_last + 1 - nitems);
			int			remain_items = nitems - (replacepos_last - replacepos_first);

			if (move_items > 0)
				memmove(&items[replacepos_first + 1],
						&items[replacepos_last + 1],
						move_items * sizeof(ZSFreePageMapItem));

			((PageHeader) page)->pd_lower = remain_items * sizeof(ZSFreePageMapItem);

		}

		MarkBufferDirty(buf);
		UnlockReleaseBuffer(buf);

		return;
	}

	/*
	 * No overlap with any existing ranges. Add a new one. This might require
	 * splitting the page.
	 */
	pos = pos + 1;

	if (PageGetExactFreeSpace(page) >= sizeof(ZSFreePageMapItem))
	{
		START_CRIT_SECTION();

		memmove(&items[pos],
				&items[pos + 1],
				(nitems - pos) * sizeof(ZSFreePageMapItem));

		items[pos].zs_startblk = startblk;
		items[pos].u.zs_endblk = endblk;

		((PageHeader) page)->pd_lower += sizeof(ZSFreePageMapItem);

		/* TODO: WAL-log */

		MarkBufferDirty(buf);

		END_CRIT_SECTION();

		UnlockReleaseBuffer(buf);
		return;
	}
	else
	{
		/* last resort: split the page */
		zsfpm_split_stack *split_stack;
		ZSFreePageMapItem newitem;

		newitem.zs_startblk = startblk;
		newitem.u.zs_endblk = endblk;
		split_stack = zsfpm_split(rel, buf, pos, &newitem);

		/* write out the temporary page copies */
		START_CRIT_SECTION();

		/* TODO: WAL-log */

		while (split_stack)
		{
			zsfpm_split_stack *next;

			PageRestoreTempPage(split_stack->page, BufferGetPage(split_stack->buf));
			MarkBufferDirty(split_stack->buf);
			UnlockReleaseBuffer(split_stack->buf);

			next = split_stack->next;
			pfree(split_stack);
			split_stack = next;
		}
		END_CRIT_SECTION();
	}
}

/*
 * Insert a downlink for right page, after splitting 'leftbuf' FPM page.
 */
static zsfpm_split_stack *
zsfpm_insert_downlink(Relation rel, Buffer leftbuf,
					  BlockNumber rightlokey, BlockNumber rightblkno)
{
	Buffer		parentbuf;
	Page		leftpage = BufferGetPage(leftbuf);
	BlockNumber leftblkno = BufferGetBlockNumber(leftbuf);
	ZSFreePageMapOpaque *leftopaque = ZSFreePageMapGetOpaque(leftpage);
	zstid		leftlokey = leftopaque->zs_lokey;
	ZSFreePageMapItem downlink;
	Buffer		metabuf;
	Page		metapage;
	ZSMetaPageOpaque *metaopaque;
	BlockNumber rootblk;
	Page		parentpage;
	ZSFreePageMapItem *items;
	int			nitems;
	int			pos;
	zsfpm_split_stack *split_stack;

	/*
	 * First, find the parent of 'leftbuf'.
	 *
	 * TODO: this is a bit inefficient. Usually, we have just descended the
	 * tree, and if we just remembered the path we descended, we could just
	 * walk back up.
	 */
	metabuf = ReadBuffer(rel, ZS_META_BLK);
	metapage = BufferGetPage(metabuf);
	LockBuffer(metabuf, BUFFER_LOCK_SHARE);
	metapage = BufferGetPage(metabuf);
	metaopaque = (ZSMetaPageOpaque *) PageGetSpecialPointer(metapage);
	rootblk = metaopaque->zs_fpm_root;

	if (rootblk == BufferGetBlockNumber(leftbuf))
	{
		/* Root split. Create new root with downlinks for the left and right page. */
		ZSFreePageMapItem downlink1;
		ZSFreePageMapItem downlink2;

		/* re-acquire the lock on metapage in exclusive mode */
		LockBuffer(metabuf, BUFFER_LOCK_UNLOCK);
		LockBuffer(metabuf, BUFFER_LOCK_EXCLUSIVE);
		metaopaque = (ZSMetaPageOpaque *) PageGetSpecialPointer(metapage);

		/*
		 * No one should have been able to change the root pointer, because we were
		 * holding a lock on the root page
		 */
		Assert(metaopaque->zs_fpm_root == BufferGetBlockNumber(leftbuf));

		downlink1.zs_startblk = leftlokey;
		downlink1.u.zs_downlink = leftblkno;
		downlink2.zs_startblk = rightlokey;
		downlink2.u.zs_downlink = rightblkno;

		return zsfpm_newroot(rel, metabuf, leftopaque->zs_level + 1,
							 &downlink1, &downlink2);
	}

	UnlockReleaseBuffer(metabuf);

	parentbuf = zsfpm_descend(rel, metabuf, leftlokey, leftopaque->zs_level + 1);
	parentpage = BufferGetPage(parentbuf);

	downlink.zs_startblk = rightlokey;
	downlink.u.zs_downlink = rightblkno;

	/* insert the item */
	items = ZSFreePageMapPageGetItems(parentpage);
	nitems = ZSFreePageMapPageGetNumItems(parentpage);

	pos = zsfpm_binsrch_blkno(rightlokey, items, nitems);
	pos = pos + 1;

	if (PageGetExactFreeSpace(parentpage) >= sizeof(ZSFreePageMapItem))
	{
		ZSFreePageMapItem *newitems;
		Page		newpage;

		newpage = PageGetTempPageCopySpecial(parentpage);

		split_stack = palloc(sizeof(zsfpm_split_stack));
		split_stack->next = NULL;
		split_stack->buf = parentbuf;
		split_stack->page = newpage;

		newitems = ZSFreePageMapPageGetItems(newpage);
		memcpy(newitems, items, pos * sizeof(ZSFreePageMapItem));

		newitems[pos] = downlink;

		memcpy(&newitems[pos + 1], &items[pos], (nitems - pos) * sizeof(ZSFreePageMapItem));

		((PageHeader) newpage)->pd_lower += (nitems + 1) * sizeof(ZSFreePageMapItem);
	}
	else
	{
		/* have to split the page. */
		split_stack = zsfpm_split(rel, parentbuf, pos, &downlink);
	}
	return split_stack;
}

/*
 * Split a page for insertion of 'newitem', at 'newpos'.
 *
 * A page split needs to modify the page being split, the block allocated for
 * the new page, and also the downlink in the parent. If the parent needs to
 * be split as well, its parent also needs to be recursively updated, all the
 * way up to the root page, in the worst case. zsfpm_split() doesn't modify
 * any pages directly, but locks them exclusively, and returns a list of
 * zsfpm_split_stack structs to represent the modifications. The caller must
 * WAL-log and apply all the changes represented by the list.
 */
static zsfpm_split_stack *
zsfpm_split(Relation rel, Buffer leftbuf, int newpos, ZSFreePageMapItem *newitem)
{
	Buffer		rightbuf;
	Page		origpage = BufferGetPage(leftbuf);
	Page		leftpage;
	Page		rightpage;
	BlockNumber rightblkno;
	ZSFreePageMapOpaque *leftopaque;
	ZSFreePageMapOpaque *rightopaque;
	ZSFreePageMapItem *origitems;
	ZSFreePageMapItem *leftitems;
	ZSFreePageMapItem *rightitems;
	int			orignitems;
	int			leftnitems;
	int			rightnitems;
	int			splitpoint;
	BlockNumber splitkey;
	bool		newitemonleft;
	int			i;
	zsfpm_split_stack *stack1;
	zsfpm_split_stack *stack2;

	leftpage = PageGetTempPageCopySpecial(origpage);
	leftopaque = ZSFreePageMapGetOpaque(leftpage);

	/*
	 * FIXME: can't use the FPM to get a page, because we might deadlock with
	 * ourself. We could steal a block from the page we're splitting...
	 */
	rightbuf = zspage_extendrel_newbuf(rel);
	rightblkno = BufferGetBlockNumber(rightbuf);

	rightpage = palloc(BLCKSZ);
	PageInit(rightpage, BLCKSZ, sizeof(ZSFreePageMapOpaque));
	rightopaque = ZSFreePageMapGetOpaque(rightpage);

	/*
	 * Figure out the split point.
	 *
	 * TODO: currently, always do 90/10 split.
	 */
	origitems = ZSFreePageMapPageGetItems(origpage);
	orignitems = ZSFreePageMapPageGetNumItems(origpage);
	splitpoint = orignitems * 0.9;
	splitkey = origitems[splitpoint].zs_startblk;
	newitemonleft = (newitem->zs_startblk < splitkey);

	/* Set up the page headers */
	rightopaque->zs_lokey = splitkey;
	rightopaque->zs_hikey = leftopaque->zs_hikey;
	rightopaque->zs_level = leftopaque->zs_level;
	rightopaque->zs_flags = 0;
	rightopaque->zs_page_id = ZS_FPM_PAGE_ID;

	leftopaque->zs_hikey = splitkey;

	/* copy the items */
	leftitems = ZSFreePageMapPageGetItems(leftpage);
	leftnitems = 0;
	rightitems = ZSFreePageMapPageGetItems(rightpage);
	rightnitems = 0;

	for (i = 0; i < orignitems; i++)
	{
		if (i == newpos)
		{
			if (newitemonleft)
				leftitems[leftnitems++] = *newitem;
			else
				rightitems[rightnitems++] = *newitem;
		}

		if (i < splitpoint)
			leftitems[leftnitems++] = origitems[i];
		else
			rightitems[rightnitems++] = origitems[i];
	}
	/* cope with possibility that newitem goes at the end */
	if (i <= newpos)
	{
		Assert(!newitemonleft);
		rightitems[rightnitems++] = *newitem;
	}
	((PageHeader) leftpage)->pd_lower += leftnitems * sizeof(ZSFreePageMapItem);
	((PageHeader) rightpage)->pd_lower += rightnitems * sizeof(ZSFreePageMapItem);

	Assert(leftnitems + rightnitems == orignitems + 1);

	stack1 = palloc(sizeof(zsfpm_split_stack));
	stack1->buf = leftbuf;
	stack1->page = leftpage;

	stack2 = palloc(sizeof(zsfpm_split_stack));
	stack2->buf = rightbuf;
	stack2->page = rightpage;

	/* recurse to insert downlink. */
	stack1->next = stack2;
	stack2->next = zsfpm_insert_downlink(rel, leftbuf, splitkey, rightblkno);

	return stack1;
}

static zsfpm_split_stack *
zsfpm_newroot(Relation rel, Buffer metabuf, int level,
			  ZSFreePageMapItem *item1, ZSFreePageMapItem *item2)
{
	/* Create a new FPM root page */
	Page		metapage;
	ZSMetaPageOpaque *metaopaque;
	ZSFreePageMapOpaque *opaque;
	Buffer		buf;
	Page		page;
	BlockNumber rootblk;
	ZSFreePageMapItem *items;
	zsfpm_split_stack *stack1;
	zsfpm_split_stack *stack2;

	metapage = PageGetTempPageCopy(BufferGetPage(metabuf));
	metaopaque = (ZSMetaPageOpaque *) PageGetSpecialPointer(metapage);

	/* TODO: get the page from the FPM */
	buf = zspage_extendrel_newbuf(rel);
	rootblk = BufferGetBlockNumber(buf);

	page = palloc(BLCKSZ);
	PageInit(page, BLCKSZ, sizeof(ZSFreePageMapOpaque));
	opaque = ZSFreePageMapGetOpaque(page);
	opaque->zs_lokey = 0;
	opaque->zs_hikey = MaxBlockNumber + 1;
	opaque->zs_level = level;
	opaque->zs_flags = 0;
	opaque->zs_page_id = ZS_FPM_PAGE_ID;

	items = ZSFreePageMapPageGetItems(page);
	items[0] = *item1;
	items[1] = *item2;
	((PageHeader) page)->pd_lower += 2 * sizeof(ZSFreePageMapItem);
	Assert(ZSFreePageMapPageGetNumItems(page) == 2);

	metaopaque->zs_fpm_root = rootblk;

	stack1 = palloc(sizeof(zsfpm_split_stack));
	stack1->next = NULL;
	stack1->buf = metabuf;
	stack1->page = metapage;

	stack2 = palloc(sizeof(zsfpm_split_stack));
	stack2->next = stack1;
	stack2->buf = buf;
	stack2->page = page;

	return stack2;
}

static Buffer
zsfpm_descend(Relation rel, Buffer metabuf, BlockNumber key, int level)
{
	BlockNumber next;
	Buffer		buf;
	Page		page;
	ZSFreePageMapOpaque *opaque;
	ZSFreePageMapItem *items;
	int			nitems;
	int			itemno;
	int			nextlevel = -1;
	BlockNumber failblk = InvalidBlockNumber;
	Page		metapage;
	ZSMetaPageOpaque *metaopaque;
	BlockNumber rootblk;

	metapage = BufferGetPage(metabuf);
	metaopaque = (ZSMetaPageOpaque *) PageGetSpecialPointer(metapage);
	rootblk = metaopaque->zs_fpm_root;

	next = rootblk;
	for (;;)
	{
		/*
		 * If we arrive again to a block that was a dead-end earlier, it seems
		 * that the tree is corrupt.
		 *
		 * XXX: It's theoretically possible that the block was removed, but then
		 * added back at the same location, and removed again. So perhaps retry
		 * a few times?
		 */
		if (next == failblk)
			elog(ERROR, "could not descend to block %u in FPM", key);

		buf = ReadBuffer(rel, next);
		LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);		/* TODO: shared */
		page = BufferGetPage(buf);
		opaque = ZSFreePageMapGetOpaque(page);

		if (nextlevel == -1)
			nextlevel = opaque->zs_level;
		else if (opaque->zs_level != nextlevel)
			elog(ERROR, "unexpected level encountered when descending FPM tree");

		if (opaque->zs_level < level)
			elog(ERROR, "unexpected page level encountered");

		/*
		 * Do we need to walk right? This could happen if the page was concurrently split.
		 *
		 * XXX: actually, we restart from root. We're holding a lock on the metapage,
		 * so the root cannot change.
		 */
		if (key >= opaque->zs_hikey)
		{
			/* Restart from the root */
			failblk = next;
			next = rootblk;
			nextlevel = -1;
		}
		else
		{
			if (opaque->zs_level == level)
				return buf;

			/* Find the downlink and follow it */
			items = ZSFreePageMapPageGetItems(page);
			nitems = ZSFreePageMapPageGetNumItems(page);

			itemno = zsfpm_binsrch_blkno(key, items, nitems);

			if (itemno < 0)
				elog(ERROR, "could not descend FPM tree for key blk %u", key);

			next = items[itemno].u.zs_downlink;
			nextlevel--;
		}
		UnlockReleaseBuffer(buf);
	}
}


static int
zsfpm_binsrch_blkno(BlockNumber key, ZSFreePageMapItem *arr, int arr_elems)
{
	int			low,
				high,
				mid;

	low = 0;
	high = arr_elems;
	while (high > low)
	{
		mid = low + (high - low) / 2;

		if (key >= arr[mid].zs_startblk)
			low = mid + 1;
		else
			high = mid;
	}
	return low - 1;
}
