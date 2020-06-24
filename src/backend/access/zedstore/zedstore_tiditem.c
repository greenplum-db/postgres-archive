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
#include "access/zedstore_simple8b.h"

static int remap_slots(uint8 *slotnos, int num_tids,
					   ZSUndoRecPtr *orig_slots, int num_orig_slots,
					   int target_idx, ZSUndoRecPtr target_ptr,
					   ZSUndoRecPtr *new_slots,
					   int *new_num_slots,
					   uint8 *new_slotnos,
					   ZSUndoRecPtr recent_oldest_undo);
static ZSTidArrayItem *build_item(zstid *tids, uint64 *deltas, uint8 *slotnos, int num_tids,
								  ZSUndoRecPtr *slots, int num_slots);

static void deltas_to_tids(zstid firsttid, uint64 *deltas, int num_tids, zstid *tids);
static void slotwords_to_slotnos(uint64 *slotwords, int num_tids, uint8 *slotnos);
static int binsrch_tid_array(zstid key, zstid *arr, int arr_elems);

/*
 * Extract TIDs from an item into iterator.
 */
void
zsbt_tid_item_unpack(ZSTidArrayItem *item, ZSTidItemIterator *iter)
{
	ZSUndoRecPtr *slots;
	int			num_tids;
	uint64	   *slotwords;
	uint64	   *codewords;

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

	ZSTidArrayItemDecode(item, &codewords, &slots, &slotwords);
	num_tids = item->t_num_tids;

	/* decode all the codewords */
	simple8b_decode_words(codewords, item->t_num_codewords, iter->tids, num_tids);

	/* convert the deltas to TIDs */
	deltas_to_tids(item->t_firsttid, iter->tids, num_tids, iter->tids);
	iter->num_tids = num_tids;
	Assert(iter->tids[num_tids - 1] == item->t_endtid - 1);

	/* Expand slotwords to slotnos */
	slotwords_to_slotnos(slotwords, num_tids, iter->tid_undoslotnos);

	/* also copy out the slots to the iterator */
	InvalidateUndoVisibility(&iter->visi_infos[ZSBT_OLD_UNDO_SLOT]);
	InvalidateUndoVisibility(&iter->visi_infos[ZSBT_DEAD_UNDO_SLOT]);
	iter->visi_infos[ZSBT_DEAD_UNDO_SLOT].undoptr = DeadUndoPtr;
	for (int i = ZSBT_FIRST_NORMAL_UNDO_SLOT; i < item->t_num_undo_slots; i++)
	{
		InvalidateUndoVisibility(&iter->visi_infos[i]);
		iter->visi_infos[i].undoptr = slots[i - ZSBT_FIRST_NORMAL_UNDO_SLOT];
	}
}

/*
 * Create a ZSTidArrayItem (or items), to represent a range of contiguous TIDs,
 * all with the same UNDO pointer.
 */
List *
zsbt_tid_item_create_for_range(zstid tid, int nelements, ZSUndoRecPtr undo_ptr)
{
	uint64		total_encoded;
	List	   *newitems = NIL;
	uint64		codewords[ZSBT_MAX_ITEM_CODEWORDS];
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

	total_encoded = 0;
	while (total_encoded < nelements)
	{
		ZSTidArrayItem *newitem;
		Size		itemsz;
		int			num_codewords;
		int			num_tids;
		zstid		firsttid = tid + total_encoded;
		uint64		first_delta;
		uint64		second_delta;
		ZSUndoRecPtr *newitem_slots;
		uint64	   *slotword_p;
		uint64	   *newitem_slotwords;
		uint64	   *newitem_codewords;
		int			i;

		/*
		 * The first 'diff' is 0, because the first TID is implicitly 'starttid'.
		 * The rest have distance of 1 to the previous TID.
		 */
		first_delta = 0;
		second_delta = 1;
		num_tids = 0;
		for (num_codewords = 0;
			 num_codewords < ZSBT_MAX_ITEM_CODEWORDS && total_encoded < nelements && num_tids < ZSBT_MAX_ITEM_TIDS;
			 num_codewords++)
		{
			uint64		codeword;
			int			num_encoded;

			codeword = simple8b_encode_consecutive(first_delta, second_delta,
												   nelements - total_encoded,
												   &num_encoded);
			if (num_encoded == 0)
				break;

			codewords[num_codewords] = codeword;
			total_encoded += num_encoded;
			num_tids += num_encoded;
			first_delta = 1;
		}

		itemsz = SizeOfZSTidArrayItem(num_tids, num_slots, num_codewords);
		newitem = palloc(itemsz);
		newitem->t_size = itemsz;
		newitem->t_num_tids = num_tids;
		newitem->t_num_undo_slots = num_slots;
		newitem->t_num_codewords = num_codewords;
		newitem->t_firsttid = firsttid;
		newitem->t_endtid = tid + total_encoded;

		ZSTidArrayItemDecode(newitem, &newitem_codewords, &newitem_slots, &newitem_slotwords);

		/* Fill in undo slots */
		if (slotno == ZSBT_FIRST_NORMAL_UNDO_SLOT)
		{
			Assert(num_slots == ZSBT_FIRST_NORMAL_UNDO_SLOT + 1);
			newitem_slots[0] = undo_ptr;
		}

		/* Fill in slotwords */
		i = 0;
		slotword_p = newitem_slotwords;
		while (i < num_tids)
		{
			uint64		slotword;

			slotword = 0;
			for (int j = 0; j < ZSBT_SLOTNOS_PER_WORD && i < num_tids; j++)
			{
				slotword |= (uint64) slotno << (j * ZSBT_ITEM_UNDO_SLOT_BITS);
				i++;
			}
			*(slotword_p++) = slotword;
		}

		/* Fill in TID codewords */
		for (int i = 0; i < num_codewords; i++)
			newitem_codewords[i] = codewords[i];

		newitems = lappend(newitems, newitem);
	}

	return newitems;
}

/*
 * Add a range of contiguous TIDs to an existing item.
 *
 * If all the new TIDs can be merged with the existing item, returns a List
 * with a single element, containing the new combined item that covers all
 * the existing TIDs, and the new TIDs. *modified_orig is set to true.
 *
 * If some of the new TIDs can be merged with the existing item, returns a
 * List with more than one item. The returned items together replace the
 * original item, such that all the existing TIDs and all the new TIDs are
 * covered. *modified_orig is set to true in that case, too.
 *
 * If the new TIDs could not be merged with the existing item, returns a list
 * of new items to represent the new TIDs, just like
 * zsbt_tid_item_create_for_range(), and *modified_orig is set to false.
 */
List *
zsbt_tid_item_add_tids(ZSTidArrayItem *orig, zstid firsttid, int nelements,
					   ZSUndoRecPtr undo_ptr, bool *modified_orig)
{
	int			num_slots;
	int			num_new_codewords;
	uint64		new_codewords[ZSBT_MAX_ITEM_CODEWORDS];
	ZSUndoRecPtr *orig_slots;
	uint64	   *orig_slotwords;
	uint64	   *orig_codewords;
	int			slotno;
	uint64		first_delta;
	uint64		second_delta;
	int			total_new_encoded;
	Size		itemsz;
	ZSTidArrayItem *newitem;
	ZSUndoRecPtr *newitem_slots;
	uint64	   *newitem_slotwords;
	uint64	   *newitem_codewords;
	List	   *newitems;
	int			num_tids;
	ZSUndoRecPtr *dst_slot;
	uint64	   *dst_slotword;
	uint64	   *dst_codeword;
	int			i;
	int			j;

	if (orig == NULL)
	{
		*modified_orig = false;
		return zsbt_tid_item_create_for_range(firsttid, nelements, undo_ptr);
	}

	/* Quick check to see if we can add the new TIDs to the previous item */
	Assert(orig->t_endtid <= firsttid);

	/*
	 * Is there room for a new codeword? Currently, we don't try to add tids to the
	 * last existing codeword, even if we perhaps could.
	 */
	if (orig->t_num_codewords >= ZSBT_MAX_ITEM_CODEWORDS)
	{
		*modified_orig = false;
		return zsbt_tid_item_create_for_range(firsttid, nelements, undo_ptr);
	}

	ZSTidArrayItemDecode(orig, &orig_codewords, &orig_slots, &orig_slotwords);

	/* Is there an UNDO slot we can use? */
	Assert(undo_ptr.counter != DeadUndoPtr.counter);
	if (!IsZSUndoRecPtrValid(&undo_ptr))
	{
		slotno = ZSBT_OLD_UNDO_SLOT;
		num_slots = orig->t_num_undo_slots;
	}
	else
	{
		for (slotno = ZSBT_FIRST_NORMAL_UNDO_SLOT; slotno < orig->t_num_undo_slots; slotno++)
		{
			if (orig_slots[slotno - ZSBT_FIRST_NORMAL_UNDO_SLOT].counter == undo_ptr.counter)
				break;
		}
		if (slotno >= ZSBT_MAX_ITEM_UNDO_SLOTS)
		{
			*modified_orig = false;
			return zsbt_tid_item_create_for_range(firsttid, nelements, undo_ptr);
		}

		if (slotno >= orig->t_num_undo_slots)
			num_slots = orig->t_num_undo_slots + 1;
		else
			num_slots = orig->t_num_undo_slots;
	}

	/* ok, go ahead, create as many new codewords as fits, or is needed. */
	first_delta = firsttid - orig->t_endtid + 1;
	second_delta = 1;
	total_new_encoded = 0;
	num_new_codewords = 0;
	while (num_new_codewords < ZSBT_MAX_ITEM_CODEWORDS - orig->t_num_codewords &&
		   total_new_encoded < nelements && orig->t_num_tids + total_new_encoded < ZSBT_MAX_ITEM_TIDS)
	{
		uint64		codeword;
		int			num_encoded;

		codeword = simple8b_encode_consecutive(first_delta,
											   second_delta,
											   nelements - total_new_encoded,
											   &num_encoded);
		if (num_encoded == 0)
			break;

		new_codewords[num_new_codewords] = codeword;
		first_delta = 1;
		num_new_codewords++;
		total_new_encoded += num_encoded;
	}

	if (num_new_codewords == 0)
	{
		*modified_orig = false;
		return zsbt_tid_item_create_for_range(firsttid, nelements, undo_ptr);
	}

	num_tids = orig->t_num_tids + total_new_encoded;

	itemsz = SizeOfZSTidArrayItem(num_tids, num_slots, orig->t_num_codewords + num_new_codewords);
	newitem = palloc(itemsz);
	newitem->t_size = itemsz;
	newitem->t_num_undo_slots = num_slots;
	newitem->t_num_codewords = orig->t_num_codewords + num_new_codewords;
	newitem->t_firsttid = orig->t_firsttid;
	newitem->t_endtid = firsttid + total_new_encoded;
	newitem->t_num_tids = newitem->t_endtid - newitem->t_firsttid;

	ZSTidArrayItemDecode(newitem, &newitem_codewords, &newitem_slots, &newitem_slotwords);

	/* copy existing codewords, followed by new ones */
	dst_codeword  = newitem_codewords;
	for (int i = 0; i < orig->t_num_codewords; i++)
		*(dst_codeword++) = orig_codewords[i];
	for (int i = 0; i < num_new_codewords; i++)
		*(dst_codeword++) = new_codewords[i];

	/* copy existing UNDO slots, followed by new slot, if any */
	dst_slot = newitem_slots;
	for (i = ZSBT_FIRST_NORMAL_UNDO_SLOT; i < orig->t_num_undo_slots; i++)
		*(dst_slot++) = orig_slots[i - ZSBT_FIRST_NORMAL_UNDO_SLOT];
	if (num_slots > orig->t_num_undo_slots)
		*(dst_slot++) = undo_ptr;

	/*
	 * Copy and build slotwords
	 */
	dst_slotword = newitem_slotwords;
	/* copy full original slotwords as is */
	for (i = 0; i < orig->t_num_tids / ZSBT_SLOTNOS_PER_WORD; i++)
		*(dst_slotword++) = orig_slotwords[i];

	/* add to the last, partial slotword. */
	i = orig->t_num_tids;
	j = orig->t_num_tids % ZSBT_SLOTNOS_PER_WORD;
	if (j != 0)
	{
		uint64		slotword = orig_slotwords[orig->t_num_tids / ZSBT_SLOTNOS_PER_WORD];

		for (; j < ZSBT_SLOTNOS_PER_WORD && i < num_tids; j++)
		{
			slotword |= (uint64) slotno << (j * ZSBT_ITEM_UNDO_SLOT_BITS);
			i++;
		}
		*(dst_slotword++) = slotword;
	}

	/* new slotwords */
	while (i < num_tids)
	{
		uint64		slotword = 0;

		for (j = 0; j < ZSBT_SLOTNOS_PER_WORD && i < num_tids; j++)
		{
			slotword |= (uint64) slotno << (j * ZSBT_ITEM_UNDO_SLOT_BITS);
			i++;
		}
		*(dst_slotword++) = slotword;
	}
	Assert(dst_slotword == newitem_slotwords + ZSBT_NUM_SLOTWORDS(num_tids));

	/* Create more items for the remainder, if needed */
	*modified_orig = true;
	if (total_new_encoded < nelements)
		newitems = zsbt_tid_item_create_for_range(newitem->t_endtid,
												  nelements - total_new_encoded,
												  undo_ptr);
	else
		newitems = NIL;
	newitems = lcons(newitem, newitems);
	return newitems;
}

/*
 * Change the UNDO pointer of a tuple with TID 'target_tid', inside an item.
 *
 * Returns an item, or multiple items, to replace the original one.
 */
List *
zsbt_tid_item_change_undoptr(ZSTidArrayItem *orig, zstid target_tid, ZSUndoRecPtr undoptr,
							 ZSUndoRecPtr recent_oldest_undo)
{
	uint64	   *deltas;
	zstid	   *tids;
	int			num_tids = orig->t_num_tids;
	int			target_idx = -1;
	ZSUndoRecPtr *orig_slots_partial;
	ZSUndoRecPtr orig_slots[ZSBT_MAX_ITEM_UNDO_SLOTS];
	uint64	   *orig_slotwords;
	uint64	   *orig_codewords;
	List	   *newitems;
	int			new_slotno;

	deltas = palloc(sizeof(uint64) * num_tids);
	tids = palloc(sizeof(zstid) * num_tids);

	ZSTidArrayItemDecode(orig, &orig_codewords, &orig_slots_partial, &orig_slotwords);

	/* decode the codewords, to find the target TID */
	simple8b_decode_words(orig_codewords, orig->t_num_codewords, deltas, num_tids);

	deltas_to_tids(orig->t_firsttid, deltas, num_tids, tids);

	target_idx = binsrch_tid_array(target_tid, tids, num_tids);
	Assert(tids[target_idx] == target_tid);

	/* Ok, we know the target TID now. Can we use one of the existing UNDO slots? */
	new_slotno = -1;
	if (undoptr.counter == DeadUndoPtr.counter)
		new_slotno = ZSBT_DEAD_UNDO_SLOT;
	if (new_slotno == -1 && undoptr.counter < recent_oldest_undo.counter)
		new_slotno = ZSBT_OLD_UNDO_SLOT;

	orig_slots[ZSBT_OLD_UNDO_SLOT] = InvalidUndoPtr;
	orig_slots[ZSBT_DEAD_UNDO_SLOT] = DeadUndoPtr;
	for (int i = ZSBT_FIRST_NORMAL_UNDO_SLOT; i < orig->t_num_undo_slots; i++)
		orig_slots[i] = orig_slots_partial[i - ZSBT_FIRST_NORMAL_UNDO_SLOT];

	if (new_slotno == -1)
	{
		for (int i = 0; i < orig->t_num_undo_slots; i++)
		{
			if (orig_slots[i].counter == undoptr.counter)
			{
				/* We can reuse this existing slot for the target. */
				new_slotno = i;
			}
		}
	}
	if (new_slotno == -1 && orig->t_num_undo_slots < ZSBT_MAX_ITEM_UNDO_SLOTS)
	{
		/* There's a free slot we can use for the target */
		new_slotno = orig->t_num_undo_slots;
	}

	if (new_slotno != -1)
	{
		int			num_slots;
		Size		itemsz;
		ZSTidArrayItem *newitem;
		ZSUndoRecPtr *newitem_slots;
		uint64	   *newitem_slotwords;
		uint64	   *newitem_codewords;

		num_slots = orig->t_num_undo_slots;
		if (new_slotno == orig->t_num_undo_slots)
			num_slots++;

		/* Simple case */
		itemsz = SizeOfZSTidArrayItem(orig->t_num_tids, num_slots, orig->t_num_codewords);
		newitem = palloc(itemsz);
		newitem->t_size = itemsz;
		newitem->t_num_undo_slots = num_slots;
		newitem->t_num_codewords = orig->t_num_codewords;
		newitem->t_firsttid = orig->t_firsttid;
		newitem->t_endtid = orig->t_endtid;
		newitem->t_num_tids = orig->t_num_tids;

		ZSTidArrayItemDecode(newitem, &newitem_codewords, &newitem_slots, &newitem_slotwords);

		/* copy codewords. They're unmodified. */
		for (int i = 0; i < orig->t_num_codewords; i++)
			newitem_codewords[i] = orig_codewords[i];

		/* copy existing slots, followed by new slot, if any */
		for (int i = ZSBT_FIRST_NORMAL_UNDO_SLOT; i < orig->t_num_undo_slots; i++)
			newitem_slots[i - ZSBT_FIRST_NORMAL_UNDO_SLOT] = orig_slots[i];
		if (new_slotno == orig->t_num_undo_slots)
			newitem_slots[new_slotno - ZSBT_FIRST_NORMAL_UNDO_SLOT] = undoptr;

		/* copy slotwords */
		for (int i = 0; i < ZSBT_NUM_SLOTWORDS(orig->t_num_tids); i++)
		{
			uint64		slotword;

			slotword = orig_slotwords[i];

			if (target_idx / ZSBT_SLOTNOS_PER_WORD == i)
			{
				/* this slotword contains the target TID */
				int			shift = (target_idx % ZSBT_SLOTNOS_PER_WORD) * ZSBT_ITEM_UNDO_SLOT_BITS;
				uint64		mask;

				mask = ((UINT64CONST(1) << ZSBT_ITEM_UNDO_SLOT_BITS) - 1) << shift;

				slotword &= ~mask;
				slotword |= (uint64) new_slotno << shift;
			}

			newitem_slotwords[i] = slotword;
		}

		newitems = list_make1(newitem);
	}
	else
	{
		/* Have to remap the slots. */
		uint8		*slotnos;
		ZSUndoRecPtr tmp_slots[ZSBT_MAX_ITEM_UNDO_SLOTS];
		uint8		*tmp_slotnos;
		int			idx;

		slotnos = palloc(orig->t_num_tids * sizeof(uint8));
		slotwords_to_slotnos(orig_slotwords, orig->t_num_tids, slotnos);

		tmp_slotnos = palloc(orig->t_num_tids * sizeof(uint8));

		/* reconstruct items */
		idx = 0;
		newitems = NIL;
		while (idx < orig->t_num_tids)
		{
			ZSTidArrayItem *newitem;
			int			num_remapped;
			int			num_tmp_slots;

			num_remapped = remap_slots(&slotnos[idx], orig->t_num_tids - idx,
									   orig_slots, orig->t_num_undo_slots,
									   target_idx - idx, undoptr,
									   tmp_slots, &num_tmp_slots,
									   tmp_slotnos,
									   recent_oldest_undo);

			deltas[idx] = 0;
			newitem = build_item(&tids[idx], &deltas[idx], tmp_slotnos, num_remapped,
								 tmp_slots, num_tmp_slots);

			newitems = lappend(newitems, newitem);
			idx += newitem->t_num_tids;
		}

		pfree(slotnos);
		pfree(tmp_slotnos);
	}

	pfree(deltas);
	pfree(tids);

	return newitems;
}

/*
 * Completely remove a number of TIDs from an item. (for vacuum)
 */
List *
zsbt_tid_item_remove_tids(ZSTidArrayItem *orig, zstid *nexttid, IntegerSet *remove_tids,
						  ZSUndoRecPtr recent_oldest_undo)
{
	ZSUndoRecPtr *orig_slots_partial;
	ZSUndoRecPtr orig_slots[ZSBT_MAX_ITEM_UNDO_SLOTS];
	uint64	   *orig_slotwords;
	uint64	   *orig_codewords;
	int			total_remain;
	uint64	   *deltas;
	zstid	   *tids;
	int			nelements = orig->t_num_tids;
	List	   *newitems = NIL;
	zstid		tid;
	zstid		prev_tid;
	int			idx;
	uint8	   *slotnos;

	deltas = palloc(sizeof(uint64) * nelements);
	tids = palloc(sizeof(zstid) * nelements);
	slotnos = palloc(sizeof(uint8) * nelements);

	ZSTidArrayItemDecode(orig, &orig_codewords, &orig_slots_partial, &orig_slotwords);

	/* decode all the codewords */
	simple8b_decode_words(orig_codewords, orig->t_num_codewords, deltas, orig->t_num_tids);

	/* also decode the slotwords */
	orig_slots[ZSBT_OLD_UNDO_SLOT] = InvalidUndoPtr;
	orig_slots[ZSBT_DEAD_UNDO_SLOT] = DeadUndoPtr;
	for (int i = ZSBT_FIRST_NORMAL_UNDO_SLOT; i < orig->t_num_undo_slots; i++)
		orig_slots[i] = orig_slots_partial[i - ZSBT_FIRST_NORMAL_UNDO_SLOT];

	idx = 0;
	while (idx < orig->t_num_tids)
	{
		uint64		slotword = orig_slotwords[idx / ZSBT_SLOTNOS_PER_WORD];

		for (int j = 0; j < ZSBT_SLOTNOS_PER_WORD && idx < orig->t_num_tids; j++)
		{
			slotnos[idx++] = slotword & ((UINT64CONST(1) << ZSBT_ITEM_UNDO_SLOT_BITS) - 1);
			slotword >>= ZSBT_ITEM_UNDO_SLOT_BITS;
		}
	}

	/*
	 * Remove all the TIDs we can
	 */
	total_remain = 0;
	tid = orig->t_firsttid;
	prev_tid = tid;
	for (int i = 0; i < orig->t_num_tids; i++)
	{
		uint64		delta = deltas[i];

		tid += delta;

		while (*nexttid < tid)
		{
			if (!intset_iterate_next(remove_tids, nexttid))
				*nexttid = MaxPlusOneZSTid;
		}
		if (tid < *nexttid)
		{
			deltas[total_remain] = tid - prev_tid;
			tids[total_remain] = tid;
			slotnos[total_remain] = slotnos[i];
			total_remain++;
			prev_tid = tid;
		}
	}

	if (total_remain > 0)
	{
		ZSUndoRecPtr tmp_slots[ZSBT_MAX_ITEM_UNDO_SLOTS];
		uint8		*tmp_slotnos;
		int			idx;

		tmp_slotnos = palloc(total_remain * sizeof(uint8));

		/*
		 * Ok, we have the decoded tids and undo slotnos in vals and undoslotnos now.
		 *
		 * Time to re-encode.
		 */
		idx = 0;
		while (idx < total_remain)
		{
			ZSTidArrayItem *newitem;
			int			num_remapped;
			int			num_tmp_slots;

			num_remapped = remap_slots(&slotnos[idx], total_remain - idx,
									   orig_slots, orig->t_num_undo_slots,
									   -1, InvalidUndoPtr,
									   tmp_slots, &num_tmp_slots,
									   tmp_slotnos,
									   recent_oldest_undo);

			deltas[idx] = 0;
			newitem = build_item(&tids[idx], &deltas[idx], tmp_slotnos, num_remapped,
								 tmp_slots, num_tmp_slots);

			newitems = lappend(newitems, newitem);
			idx += newitem->t_num_tids;
		}
		pfree(tmp_slotnos);
	}

	pfree(deltas);
	pfree(tids);
	pfree(slotnos);

	return newitems;
}


/*
 * Convert an array of deltas to tids.
 *
 * Note: the input and output may point to the same array!
 */
static void
deltas_to_tids(zstid firsttid, uint64 *deltas, int num_tids, zstid *tids)
{
	zstid		prev_tid = firsttid;

	for (int i = 0; i < num_tids; i++)
	{
		zstid		tid;

		tid = prev_tid + deltas[i];
		tids[i] = tid;
		prev_tid = tid;
	}
}

/*
 * Expand the slot numbers packed in slotwords, 2 bits per slotno, into
 * a regular C array.
 */
static void
slotwords_to_slotnos(uint64 *slotwords, int num_tids, uint8 *slotnos)
{
	uint64	   *slotword_p;
	const uint64 mask = (UINT64CONST(1) << ZSBT_ITEM_UNDO_SLOT_BITS) - 1;
	int			i;

	i = 0;
	slotword_p = slotwords;
	while (i < num_tids)
	{
		uint64		slotword = *(slotword_p++);
		int			j;

		/*
		 * process four elements at a time, for speed (this is an
		 * unrolled version of the loop below
		 */
		j = 0;
		while (j < ZSBT_SLOTNOS_PER_WORD && num_tids - i > 3)
		{
			slotnos[i] = slotword & mask;
			slotnos[i + 1] = (slotword >> 2) & mask;
			slotnos[i + 2] = (slotword >> 4) & mask;
			slotnos[i + 3] = (slotword >> 6) & mask;
			slotword = slotword >> 8;
			i += 4;
			j += 4;
		}
		/* handle the 0-3 elements at the end */
		while (j < ZSBT_SLOTNOS_PER_WORD && num_tids - i > 0)
		{
			slotnos[i] = slotword & mask;
			slotword = slotword >> 2;
			i++;
			j++;
		}
	}
}

/*
 * Remap undo slots.
 *
 * We start with empty UNDO slots, and walk through the items,
 * filling a slot whenever we encounter an UNDO pointer that we
 * haven't assigned a slot for yet. If we run out of slots, stop.
 */
static int
remap_slots(uint8 *slotnos, int num_tids,
			ZSUndoRecPtr *orig_slots, int num_orig_slots,
			int target_idx, ZSUndoRecPtr target_ptr,
			ZSUndoRecPtr *new_slots,
			int *new_num_slots,
			uint8 *new_slotnos,
			ZSUndoRecPtr recent_oldest_undo)
{
	int			num_slots;
	int8		slot_mapping[ZSBT_MAX_ITEM_UNDO_SLOTS + 1];
	int			idx;

	new_slots[ZSBT_OLD_UNDO_SLOT] = InvalidUndoPtr;
	new_slots[ZSBT_DEAD_UNDO_SLOT] = DeadUndoPtr;
	num_slots = ZSBT_FIRST_NORMAL_UNDO_SLOT;

	/*
	 * Have to remap the UNDO slots.
-	 *
	 * We start with empty UNDO slots, and walk through the items,
	 * filling a slot whenever we encounter an UNDO pointer that we
	 * haven't assigned a slot for yet. If we run out of slots, stop.
	 */

	slot_mapping[ZSBT_OLD_UNDO_SLOT] = ZSBT_OLD_UNDO_SLOT;
	slot_mapping[ZSBT_DEAD_UNDO_SLOT] = ZSBT_DEAD_UNDO_SLOT;
	for (int i = ZSBT_FIRST_NORMAL_UNDO_SLOT; i < num_orig_slots; i++)
		slot_mapping[i] = -1;

	for (idx = 0; idx < num_tids; idx++)
	{
		int			orig_slotno = slotnos[idx];
		int			new_slotno;

		if (idx == target_idx)
			new_slotno = -1;
		else
			new_slotno = slot_mapping[orig_slotno];
		if (new_slotno == -1)
		{
			/* assign new slot for this. */
			ZSUndoRecPtr this_undoptr;

			if (idx == target_idx)
				this_undoptr = target_ptr;
			else
				this_undoptr = orig_slots[orig_slotno];

			if (this_undoptr.counter == DeadUndoPtr.counter)
				new_slotno = ZSBT_DEAD_UNDO_SLOT;
			else if (this_undoptr.counter < recent_oldest_undo.counter)
				new_slotno = ZSBT_OLD_UNDO_SLOT;
			else
			{
				for (int j = 0; j < num_slots; j++)
				{
					if (new_slots[j].counter == this_undoptr.counter)
					{
						/* We already had a slot for this undo pointer. Reuse it. */
						new_slotno = j;
						break;
					}
				}
				if (new_slotno == -1)
				{
					if (num_slots >= ZSBT_MAX_ITEM_UNDO_SLOTS)
						break; /* out of slots */
					else
					{
						/* assign to free slot */
						new_slots[num_slots] = this_undoptr;
						new_slotno = num_slots;
						num_slots++;
					}
				}
			}

			if (idx != target_idx)
				slot_mapping[orig_slotno] = new_slotno;
		}

		new_slotnos[idx] = new_slotno;
	}

	*new_num_slots = num_slots;
	return idx;
}

/*
 * Construct a ZSTidArrayItem.
 *
 * 'tids' is the list of TIDs to be packed in the item.
 *
 * 'deltas' contain the difference between each TID. They could be computed
 * from the 'tids', but since the caller has them lready, we can save some
 * effort by passing them down.
 *
 * 'slots' contains the UNDO slots to be stored. NOTE: it contains the
 * special 0 and 1 slots too, but they won't be stored in the item that's
 * created.
 *
 * 'slotnos' contains the UNDO slot numbers corresponding to each tuple
 */
static ZSTidArrayItem *
build_item(zstid *tids, uint64 *deltas, uint8 *slotnos, int num_tids,
		   ZSUndoRecPtr *slots, int num_slots)
{
	int			num_codewords;
	Size		itemsz;
	ZSTidArrayItem *newitem;
	int			num_encoded;
	uint64		codewords[ZSBT_MAX_ITEM_CODEWORDS];
	ZSUndoRecPtr *newitem_slots;
	uint64	   *newitem_slotwords;
	uint64	   *newitem_codewords;
	uint64	   *dst_slotword;
	int			idx;

	/*
	 * Create codewords.
	 */
	num_codewords = 0;
	num_encoded = 0;
	while (num_encoded < num_tids && num_codewords < ZSBT_MAX_ITEM_CODEWORDS)
	{
		int			n;
		uint64		codeword;

		codeword = simple8b_encode(&deltas[num_encoded], num_tids - num_encoded, &n);
		if (n == 0)
			break;

		num_encoded += n;

		codewords[num_codewords++] = codeword;
	}

	itemsz = SizeOfZSTidArrayItem(num_encoded, num_slots, num_codewords);
	newitem = palloc(itemsz);
	newitem->t_size = itemsz;
	newitem->t_num_tids = num_encoded;
	newitem->t_num_undo_slots = num_slots;
	newitem->t_num_codewords = num_codewords;
	newitem->t_firsttid = tids[0];
	newitem->t_endtid = tids[num_encoded - 1] + 1;

	ZSTidArrayItemDecode(newitem, &newitem_codewords, &newitem_slots, &newitem_slotwords);

	/* Copy in the TID codewords */
	for (int i = 0; i < num_codewords; i++)
		newitem_codewords[i] = codewords[i];

	/* Copy in undo slots */
	for (int i = ZSBT_FIRST_NORMAL_UNDO_SLOT; i < num_slots; i++)
		newitem_slots[i - ZSBT_FIRST_NORMAL_UNDO_SLOT] = slots[i];

	/* Create slotwords */
	dst_slotword = newitem_slotwords;
	idx = 0;
	while (idx < num_encoded)
	{
		uint64		slotword = 0;

		for (int j = 0; j < ZSBT_SLOTNOS_PER_WORD && idx < num_encoded; j++)
			slotword |= (uint64) slotnos[idx++] << (j * ZSBT_ITEM_UNDO_SLOT_BITS);

		*(dst_slotword++) = slotword;
	}
	Assert(dst_slotword == newitem_slotwords + ZSBT_NUM_SLOTWORDS(num_tids));

	return newitem;
}

static int
binsrch_tid_array(zstid key, zstid *arr, int arr_elems)
{
	int			low,
		high,
		mid;

	low = 0;
	high = arr_elems;
	while (high > low)
	{
		mid = low + (high - low) / 2;

		if (key >= arr[mid])
			low = mid + 1;
		else
			high = mid;
	}
	return low - 1;
}
