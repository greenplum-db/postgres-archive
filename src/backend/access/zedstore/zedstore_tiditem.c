/*
 * zedstore_tiditem.c
 *		Routines for packing TIDs into "items"
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/zedstore/zedstore_tiditem.c
 */
#include "postgres.h"

#include "access/zedstore_internal.h"

/*
 * Maximum number of integers that can be encoded in a single Simple-8b
 * codeword. (Defined here before anything else, so that we can size arrays
 * using this.)
 */
#define SIMPLE8B_MAX_VALUES_PER_CODEWORD 240


static ZSTidArrayItem *remap_slots(zstid firsttid, uint64 *vals, int num_vals,
								   ZSUndoRecPtr *orig_slots, int num_orig_slots,
								   int target_idx, ZSUndoRecPtr target_ptr,
								   ZSUndoRecPtr recent_oldest_undo);

static uint64 simple8b_encode(const uint64 *ints, int num_ints, int *num_encoded);
static int simple8b_decode(uint64 codeword, uint64 *decoded);


List *
zsbt_tid_pack_item(zstid tid, ZSUndoRecPtr undo_ptr, int nelements)
{
	uint64		*vals;
	uint64		elemno;
	List	   *newitems = NIL;
	uint64		codewords[MAX_ITEM_CODEWORDS];
	int			num_slots;
	int			slotno;

	Assert(undo_ptr.counter != DeadUndoPtr.counter);
	if (IsZSUndoRecPtrValid(&undo_ptr))
	{
		slotno = ZSBT_FIRST_NORMAL_UNDO_SLOT;
		num_slots = ZSBT_FIRST_NORMAL_UNDO_SLOT + 1;
	}
	else
	{
		slotno = ZSBT_OLD_UNDO_SLOT;
		num_slots = ZSBT_FIRST_NORMAL_UNDO_SLOT;
	}

	vals = palloc(sizeof(uint64) * nelements);
	for (int i = 0; i < nelements; i++)
		vals[i] = (1 << ZSBT_ITEM_UNDO_SLOT_BITS) | slotno;

	elemno = 0;
	while (elemno < nelements)
	{
		ZSTidArrayItem *newitem;
		Size		itemsz;
		int			num_encoded;
		int			num_codewords;
		zstid		firsttid = tid + elemno;

		/* clear the 'diff' from the first value, because it's 'starttid' */
		vals[elemno] = 0 | slotno;
		for (num_codewords = 0; num_codewords < MAX_ITEM_CODEWORDS && elemno < nelements; num_codewords++)
		{
			uint64		codeword;

			codeword = simple8b_encode(&vals[elemno], nelements - elemno, &num_encoded);

			if (num_encoded == 0)
				break;

			codewords[num_codewords] = codeword;
			elemno += num_encoded;
		}

		itemsz = SizeOfZSTidArrayItem(num_slots, num_codewords);
		newitem = palloc(itemsz);
		newitem->t_size = itemsz;
		newitem->t_num_undo_slots = num_slots;
		newitem->t_num_codewords = num_codewords;
		newitem->t_firsttid = firsttid;
		newitem->t_endtid = tid + elemno;
		newitem->t_num_tids = newitem->t_endtid - newitem->t_firsttid;

		if (slotno == ZSBT_FIRST_NORMAL_UNDO_SLOT)
			ZSTidArrayItemGetUndoSlots(newitem)[0] = undo_ptr;

		memcpy(ZSTidArrayItemGetCodewords(newitem), codewords, num_codewords * sizeof(uint64));

		newitems = lappend(newitems, newitem);
	}

	pfree(vals);

	return newitems;
}

/*
 * Extract TIDs from an item into iterator.
 */
void
zsbt_tid_item_unpack(ZSTidArrayItem *item, ZSTidItemIterator *iter)
{
	int			total_decoded = 0;
	zstid		prev_tid;
	uint64	   *codewords;
	ZSUndoRecPtr *slotptr;

	if (iter->tids_allocated_size < item->t_num_tids)
	{
		if (iter->tids)
			pfree(iter->tids);
		if (iter->tid_undoslotnos)
			pfree(iter->tid_undoslotnos);
		iter->tids = MemoryContextAlloc(iter->context, item->t_num_tids * sizeof(zstid));
		iter->tid_undoslotnos = MemoryContextAlloc(iter->context, item->t_num_tids * sizeof(uint8));
		iter->tids_allocated_size = item->t_num_tids;
	}

	/* decode all the codewords */
	codewords = ZSTidArrayItemGetCodewords(item);
	for (int i = 0; i < item->t_num_codewords; i++)
	{
		int			num_decoded;

		num_decoded = simple8b_decode(codewords[i],
									  &iter->tids[total_decoded]);
		total_decoded += num_decoded;
	}
	Assert(total_decoded == item->t_num_tids);

	/* convert the deltas to TIDs and undo slot numbers */
	prev_tid = item->t_firsttid;
	for (int i = 0; i < total_decoded; i++)
	{
		uint64		val = iter->tids[i];

		iter->tid_undoslotnos[i] = val & ZSBT_ITEM_UNDO_SLOT_MASK;
		prev_tid = iter->tids[i] = (val >> ZSBT_ITEM_UNDO_SLOT_BITS) + prev_tid;
	}
	iter->num_tids = total_decoded;

	Assert(iter->tids[total_decoded - 1] == item->t_endtid - 1);

	/* also copy out the slots to the iterator */
	iter->undoslots[ZSBT_OLD_UNDO_SLOT] = InvalidUndoPtr;
	iter->undoslots[ZSBT_DEAD_UNDO_SLOT] = DeadUndoPtr;

	slotptr = ZSTidArrayItemGetUndoSlots(item);
	for (int i = ZSBT_FIRST_NORMAL_UNDO_SLOT; i < item->t_num_undo_slots; i++)
		iter->undoslots[i] = slotptr[i - ZSBT_FIRST_NORMAL_UNDO_SLOT];
}

List *
zsbt_tid_item_change_undoptr(ZSTidArrayItem *orig, zstid target_tid, ZSUndoRecPtr undoptr,
							 ZSUndoRecPtr recent_oldest_undo)
{
	int			total_decoded = 0;
	uint64	   *vals;
	zstid	   *tids;
	int			nelements = orig->t_num_tids;
	int			target_idx = -1;
	ZSUndoRecPtr orig_slots[ZSBT_MAX_ITEM_UNDO_SLOTS];
	List	   *newitems = NIL;
	zstid		tid;
	int			idx;
	uint64	   *codewords;
	ZSUndoRecPtr *slotptr;

	vals = palloc(sizeof(uint64) * nelements);
	tids = palloc(sizeof(zstid) * nelements);

	/* decode all the codewords */
	codewords = ZSTidArrayItemGetCodewords(orig);
	for (int i = 0; i < orig->t_num_codewords; i++)
	{
		int			num_decoded;

		num_decoded = simple8b_decode(codewords[i], &vals[total_decoded]);
		total_decoded += num_decoded;
	}
	Assert(total_decoded == orig->t_num_tids);

	/* also decode the slots */
	orig_slots[ZSBT_OLD_UNDO_SLOT] = InvalidUndoPtr;
	orig_slots[ZSBT_DEAD_UNDO_SLOT] = DeadUndoPtr;
	slotptr = ZSTidArrayItemGetUndoSlots(orig);
	for (int i = ZSBT_FIRST_NORMAL_UNDO_SLOT; i < orig->t_num_undo_slots; i++)
		orig_slots[i] = slotptr[i - ZSBT_FIRST_NORMAL_UNDO_SLOT];

	/* Find the target TID. */
	tid = orig->t_firsttid;
	for (int i = 0; i < total_decoded; i++)
	{
		uint64		val = vals[i];

		tid += (val >> ZSBT_ITEM_UNDO_SLOT_BITS);

		tids[i] = tid;
		if (tid == target_tid)
			target_idx = i;
	}
	Assert(target_idx != -1);

	/*
	 * Ok, we have the decoded tids and undo slotnos in vals and undoslotnos now.
	 *
	 * First, just output everything up to the target_tid.
	 */
	idx = 0;
	while (idx < total_decoded)
	{
		ZSTidArrayItem *newitem;

		vals[idx] &= ZSBT_ITEM_UNDO_SLOT_MASK;
		newitem = remap_slots(tids[idx], &vals[idx], total_decoded - idx,
							  orig_slots, orig->t_num_undo_slots,
							  target_idx - idx, undoptr,
							  recent_oldest_undo);
		idx += newitem->t_num_tids;
		newitem->t_endtid = tids[idx - 1] + 1;

		newitems = lappend(newitems, newitem);
	}

	pfree(vals);
	pfree(tids);

	return newitems;
}


List *
zsbt_tid_item_remove_tids(ZSTidArrayItem *orig, zstid *nexttid, IntegerSet *remove_tids,
						  ZSUndoRecPtr recent_oldest_undo)
{
	uint64	   *codewords;
	ZSUndoRecPtr *slotptr;
	int			total_decoded = 0;
	int			total_remain;
	uint64	   *vals;
	zstid	   *tids;
	int			nelements = orig->t_num_tids;
	ZSUndoRecPtr orig_slots[4];
	List	   *newitems = NIL;
	zstid		tid;
	zstid		prev_tid;
	int			idx;

	vals = palloc(sizeof(uint64) * nelements);
	tids = palloc(sizeof(zstid) * nelements);

	/* decode all the codewords */
	codewords = ZSTidArrayItemGetCodewords(orig);
	for (int i = 0; i < orig->t_num_codewords; i++)
	{
		int			num_decoded;

		num_decoded = simple8b_decode(codewords[i], &vals[total_decoded]);
		total_decoded += num_decoded;
	}
	Assert(total_decoded == orig->t_num_tids);

	/* also decode the slots */
	orig_slots[ZSBT_OLD_UNDO_SLOT] = InvalidUndoPtr;
	orig_slots[ZSBT_DEAD_UNDO_SLOT] = DeadUndoPtr;
	slotptr = ZSTidArrayItemGetUndoSlots(orig);
	for (int i = ZSBT_FIRST_NORMAL_UNDO_SLOT; i < orig->t_num_undo_slots; i++)
		orig_slots[i] = slotptr[i - ZSBT_FIRST_NORMAL_UNDO_SLOT];

	/*
	 * Remove all the TIDs we can
	 */
	total_remain = 0;
	tid = orig->t_firsttid;
	for (int i = 0; i < total_decoded; i++)
	{
		uint64		val = vals[i];
		uint64		diff = (val >> ZSBT_ITEM_UNDO_SLOT_BITS);
		uint64		slotno = (val & ZSBT_ITEM_UNDO_SLOT_MASK);

		tid += diff;

		while(*nexttid < tid)
		{
			if (!intset_iterate_next(remove_tids, nexttid))
				*nexttid = MaxPlusOneZSTid;
		}
		if (tid > *nexttid)
		{
			vals[total_remain] = (tid - prev_tid) << ZSBT_ITEM_UNDO_SLOT_BITS | slotno;
			tids[total_remain] = tid;
			prev_tid = tid;
			total_remain++;
		}
	}
	if (total_remain > 0)
	{
		/*
		 * Ok, we have the decoded tids and undo slotnos in vals and undoslotnos now.
		 *
		 * Time to re-encode.
		 */
		ZSTidArrayItem *newitem;

		idx = 0;

		vals[idx] &= ZSBT_ITEM_UNDO_SLOT_MASK;
		newitem = remap_slots(tids[idx], &vals[idx], total_decoded - idx,
							  orig_slots, orig->t_num_undo_slots,
							  -1, InvalidUndoPtr,
							  recent_oldest_undo);
		idx += newitem->t_num_tids;
		newitem->t_endtid = tids[idx - 1] + 1;

		newitems = lappend(newitems, newitem);
	}

	pfree(vals);
	pfree(tids);

	return newitems;
}

static ZSTidArrayItem *
remap_slots(zstid firsttid, uint64 *vals, int num_vals,
			ZSUndoRecPtr *orig_slots, int num_orig_slots,
			int target_idx, ZSUndoRecPtr target_ptr, ZSUndoRecPtr recent_oldest_undo)
{
	ZSUndoRecPtr curr_slots[ZSBT_MAX_ITEM_UNDO_SLOTS];
	int			curr_numslots;
	int			i;
	ZSTidArrayItem *newitem;
	uint64	   *vals_mapped;
	ZSUndoRecPtr *slotptr;
	int			num_mapped;
	int			num_encoded;
	int			num_codewords;
	uint64		codewords[MAX_ITEM_CODEWORDS];
	Size		itemsz;
	int			new_slotno;
	bool		remap_needed;

	vals_mapped = palloc(num_vals * sizeof(uint64));

	curr_slots[ZSBT_OLD_UNDO_SLOT] = InvalidUndoPtr;
	curr_slots[ZSBT_DEAD_UNDO_SLOT] = DeadUndoPtr;
	curr_numslots = ZSBT_FIRST_NORMAL_UNDO_SLOT;

	/*
	 * Is the new UNDO pointer equal to an existing UNDO slot on the item? Or is
	 * is there a free slot where we can stick the new UNDO pointer? If so, we
	 * can avoid remapping all the values.
	 *
	 * TODO: if we co-operated with the caller, we could also avoid doing the
	 * Simple-8b encoding again in many common cases.
	 */
	new_slotno = -1;
	remap_needed = true;

	if (target_idx < 0 || target_idx >= num_vals)
	{
		/*
		 * There are no changes. We can just use the original slots as is.
		 *
		 * We get here, if we changed the target in a previous call already,
		 * and we're now just creating an item for the remaining TIDs from
		 * the original item. TODO: We might not need all of the slots anymore,
		 * if the remanining TIDs don't reference them, but we don't bother
		 * to check for that here. We could save a little bit of space by
		 * leaving out any unused undo slots.
		 */
		for (int j = ZSBT_FIRST_NORMAL_UNDO_SLOT; j < num_orig_slots; j++)
			curr_slots[j] = orig_slots[j];
		curr_numslots = num_orig_slots;
		remap_needed = false;
	}
	for (i = 0; i < num_orig_slots; i++)
	{
		if (orig_slots[i].counter == target_ptr.counter)
		{
			/* We can reuse this existing slot for the target. */
			new_slotno = i;
			for (int j = ZSBT_FIRST_NORMAL_UNDO_SLOT; j < num_orig_slots; j++)
				curr_slots[j] = orig_slots[j];
			curr_numslots = num_orig_slots;
			remap_needed = false;
			break;
		}
	}
	if (remap_needed && num_orig_slots < ZSBT_MAX_ITEM_UNDO_SLOTS)
	{
		/* There's a free slot we can use for the target */
		for (int j = ZSBT_FIRST_NORMAL_UNDO_SLOT; j < num_orig_slots; j++)
			curr_slots[j] = orig_slots[j];
		new_slotno = num_orig_slots;
		curr_slots[new_slotno] = target_ptr;
		curr_numslots = num_orig_slots + 1;
	}

	if (!remap_needed)
	{
		/* We can take the fast path. */
		memcpy(vals_mapped, vals, num_vals * sizeof(uint64));
		if (target_idx >= 0 && target_idx < num_vals)
		{
			vals_mapped[target_idx] &= ~ZSBT_ITEM_UNDO_SLOT_MASK;
			vals_mapped[target_idx] |= new_slotno;
		}
		num_mapped = num_vals;
	}
	else
	{
		/*
		 * Have to remap the UNDO slots.
		 *
		 * We start with empty UNDO slots, and walk through the items,
		 * filling a slot whenever we encounter an UNDO pointer that we
		 * haven't assigned a slot for yet. If we run out of slots, stop.
		 */
		int8		slot_mapping[ZSBT_MAX_ITEM_UNDO_SLOTS];

		slot_mapping[ZSBT_OLD_UNDO_SLOT] = ZSBT_OLD_UNDO_SLOT;
		slot_mapping[ZSBT_DEAD_UNDO_SLOT] = ZSBT_DEAD_UNDO_SLOT;
		for (i = ZSBT_FIRST_NORMAL_UNDO_SLOT; i < ZSBT_MAX_ITEM_UNDO_SLOTS; i++)
			slot_mapping[i] = -1;

		for (i = 0; i < num_vals; i++)
		{
			uint64		val = vals[i];
			uint64		shifted_delta = val & ~ZSBT_ITEM_UNDO_SLOT_MASK;
			int			orig_slotno = val & ZSBT_ITEM_UNDO_SLOT_MASK;
			int			new_slotno;

			if (i == target_idx)
				new_slotno = -1;
			else
				new_slotno = slot_mapping[orig_slotno];
			if (new_slotno == -1)
			{
				/* assign new slot for this. */
				ZSUndoRecPtr this_undoptr;

				if (i == target_idx)
					this_undoptr = target_ptr;
				else
					this_undoptr = orig_slots[orig_slotno];

				if (this_undoptr.counter == DeadUndoPtr.counter)
					new_slotno = ZSBT_DEAD_UNDO_SLOT;
				else if (this_undoptr.counter < recent_oldest_undo.counter)
					new_slotno = ZSBT_OLD_UNDO_SLOT;
				else
				{
					for (int j = 0; j < curr_numslots; j++)
					{
						if (curr_slots[j].counter == this_undoptr.counter)
						{
							/* We already had a slot for this undo pointer. Reuse it. */
							new_slotno = j;
							break;
						}
					}
					if (new_slotno == -1)
					{
						if (curr_numslots >= ZSBT_MAX_ITEM_UNDO_SLOTS)
							break; /* out of slots */
						else
						{
							/* assign to free slot */
							curr_slots[curr_numslots] = this_undoptr;
							new_slotno = curr_numslots;
							curr_numslots++;
						}
					}
				}

				if (i != target_idx)
					slot_mapping[orig_slotno] = new_slotno;
			}

			vals_mapped[i] = shifted_delta | new_slotno;
		}
		num_mapped = i;
	}

	/*
	 * Create codewords.
	 */
	num_codewords = 0;
	num_encoded = 0;
	while (num_encoded < num_mapped && num_codewords < MAX_ITEM_CODEWORDS)
	{
		int			n;
		uint64		codeword;

		codeword = simple8b_encode(&vals_mapped[num_encoded], num_mapped - num_encoded, &n);
		num_encoded += n;

		codewords[num_codewords++] = codeword;
	}

	/*
	 * Construct the item to represent these.
	 */
	itemsz = SizeOfZSTidArrayItem(curr_numslots, num_codewords);
	newitem = palloc(itemsz);
	newitem->t_size = itemsz;
	newitem->t_num_tids = num_encoded;
	newitem->t_num_undo_slots = curr_numslots;
	newitem->t_num_codewords = num_codewords;

	newitem->t_firsttid = firsttid;
	/* endtid must be set by caller */

	/*
	 * Write undo slots.
	 */
	slotptr = ZSTidArrayItemGetUndoSlots(newitem);
	for (int i = ZSBT_FIRST_NORMAL_UNDO_SLOT; i < curr_numslots; i++)
		slotptr[i - ZSBT_FIRST_NORMAL_UNDO_SLOT] = curr_slots[i];

	memcpy(ZSTidArrayItemGetCodewords(newitem), codewords, num_codewords * sizeof(uint64));

	pfree(vals_mapped);

	return newitem;
}

/*
 * Simple-8b encoding.
 *
 * The simple-8b algorithm packs between 1 and 240 integers into 64-bit words,
 * called "codewords".  The number of integers packed into a single codeword
 * depends on the integers being packed; small integers are encoded using
 * fewer bits than large integers.  A single codeword can store a single
 * 60-bit integer, or two 30-bit integers, for example.
 *
 * Since we're storing a unique, sorted, set of integers, we actually encode
 * the *differences* between consecutive integers.  That way, clusters of
 * integers that are close to each other are packed efficiently, regardless
 * of their absolute values.
 *
 * In Simple-8b, each codeword consists of a 4-bit selector, which indicates
 * how many integers are encoded in the codeword, and the encoded integers are
 * packed into the remaining 60 bits.  The selector allows for 16 different
 * ways of using the remaining 60 bits, called "modes".  The number of integers
 * packed into a single codeword in each mode is listed in the simple8b_modes
 * table below.  For example, consider the following codeword:
 *
 *      20-bit integer       20-bit integer       20-bit integer
 * 1101 00000000000000010010 01111010000100100000 00000000000000010100
 * ^
 * selector
 *
 * The selector 1101 is 13 in decimal.  From the modes table below, we see
 * that it means that the codeword encodes three 20-bit integers.  In decimal,
 * those integers are 18, 500000 and 20.  Because we encode deltas rather than
 * absolute values, the actual values that they represent are 18, 500018 and
 * 500038.
 *
 * Modes 0 and 1 are a bit special; they encode a run of 240 or 120 zeroes
 * (which means 240 or 120 consecutive integers, since we're encoding the
 * deltas between integers), without using the rest of the codeword bits
 * for anything.
 *
 * Simple-8b cannot encode integers larger than 60 bits.  Values larger than
 * that are always stored in the 'first' field of a leaf item, never in the
 * packed codeword.  If there is a sequence of integers that are more than
 * 2^60 apart, the codeword will go unused on those items.  To represent that,
 * we use a magic EMPTY_CODEWORD codeword value.
 */
static const struct simple8b_mode
{
	uint8		bits_per_int;
	uint8		num_ints;
} simple8b_modes[17] =

{
	{0, 240},					/* mode  0: 240 zeroes */
	{0, 120},					/* mode  1: 120 zeroes */
	{1, 60},					/* mode  2: sixty 1-bit integers */
	{2, 30},					/* mode  3: thirty 2-bit integers */
	{3, 20},					/* mode  4: twenty 3-bit integers */
	{4, 15},					/* mode  5: fifteen 4-bit integers */
	{5, 12},					/* mode  6: twelve 5-bit integers */
	{6, 10},					/* mode  7: ten 6-bit integers */
	{7, 8},						/* mode  8: eight 7-bit integers (four bits
								 * are wasted) */
	{8, 7},						/* mode  9: seven 8-bit integers (four bits
								 * are wasted) */
	{10, 6},					/* mode 10: six 10-bit integers */
	{12, 5},					/* mode 11: five 12-bit integers */
	{15, 4},					/* mode 12: four 15-bit integers */
	{20, 3},					/* mode 13: three 20-bit integers */
	{30, 2},					/* mode 14: two 30-bit integers */
	{60, 1},					/* mode 15: one 60-bit integer */

	{0, 0}						/* sentinel value */
};

/*
 * EMPTY_CODEWORD is a special value, used to indicate "no values".
 * It is used if the next value is too large to be encoded with Simple-8b.
 *
 * This value looks like a mode-0 codeword, but we can distinguish it
 * because a regular mode-0 codeword would have zeroes in the unused bits.
 */
#define EMPTY_CODEWORD		UINT64CONST(0x0FFFFFFFFFFFFFFF)

/*
 * Encode a number of integers into a Simple-8b codeword.
 *
 * Returns the encoded codeword, and sets *num_encoded to the number of
 * input integers that were encoded.  That can be zero, if the first delta
 * is too large to be encoded.
 */
static uint64
simple8b_encode(const uint64 *ints, int num_ints, int *num_encoded)
{
	int			selector;
	int			nints;
	int			bits;
	uint64		val;
	uint64		codeword;
	int			i;

	/*
	 * Select the "mode" to use for this codeword.
	 *
	 * In each iteration, check if the next value can be represented in the
	 * current mode we're considering.  If it's too large, then step up the
	 * mode to a wider one, and repeat.  If it fits, move on to the next
	 * integer.  Repeat until the codeword is full, given the current mode.
	 *
	 * Note that we don't have any way to represent unused slots in the
	 * codeword, so we require each codeword to be "full".  It is always
	 * possible to produce a full codeword unless the very first delta is too
	 * large to be encoded.  For example, if the first delta is small but the
	 * second is too large to be encoded, we'll end up using the last "mode",
	 * which has nints == 1.
	 */
	selector = 0;
	nints = simple8b_modes[0].num_ints;
	bits = simple8b_modes[0].bits_per_int;
	val = ints[0];
	i = 0;						/* number of deltas we have accepted */
	for (;;)
	{
		if (val >= (UINT64CONST(1) << bits))
		{
			/* too large, step up to next mode */
			selector++;
			nints = simple8b_modes[selector].num_ints;
			bits = simple8b_modes[selector].bits_per_int;
			/* we might already have accepted enough deltas for this mode */
			if (i >= nints)
				break;
		}
		else
		{
			/* accept this delta; then done if codeword is full */
			i++;
			if (i >= nints)
				break;
			/* examine next delta */
			if (i < num_ints)
				val = ints[i];
			else
				val = PG_UINT64_MAX;
		}
	}

	if (nints == 0)
	{
		/*
		 * The first delta is too large to be encoded with Simple-8b.
		 *
		 * If there is at least one not-too-large integer in the input, we
		 * will encode it using mode 15 (or a more compact mode).  Hence, we
		 * can only get here if the *first* delta is >= 2^60.
		 */
		Assert(i == 0);
		*num_encoded = 0;
		return EMPTY_CODEWORD;
	}

	/*
	 * Encode the integers using the selected mode.  Note that we shift them
	 * into the codeword in reverse order, so that they will come out in the
	 * correct order in the decoder.
	 */
	codeword = 0;
	if (bits > 0)
	{
		for (i = nints - 1; i > 0; i--)
		{
			val = ints[i];
			codeword |= val;
			codeword <<= bits;
		}
		val = ints[0];
		codeword |= val;
	}

	/* add selector to the codeword, and return */
	codeword |= (uint64) selector << 60;

	*num_encoded = nints;
	return codeword;
}

/*
 * Decode a codeword into an array of integers.
 * Returns the number of integers decoded.
 */
static int
simple8b_decode(uint64 codeword, uint64 *decoded)
{
	int			selector = (codeword >> 60);
	int			nints = simple8b_modes[selector].num_ints;
	int			bits = simple8b_modes[selector].bits_per_int;
	uint64		mask = (UINT64CONST(1) << bits) - 1;

	if (codeword == EMPTY_CODEWORD)
		return 0;

	for (int i = 0; i < nints; i++)
	{
		uint64		val = codeword & mask;

		decoded[i] = val;
		codeword >>= bits;
	}
	return nints;
}
