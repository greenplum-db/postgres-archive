/*
 * zedstore_tupslot.c
 *		Implementation of a TupleTableSlot for zedstore.
 *
 * This implementation is identical to a Virtual tuple slot
 * (TTSOpsVirtual), but it has a slot_getsysattr() implementation
 * that can fetch and compute the 'xmin' for the tuple.
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/zedstore/zedstore_tupslot.c
 */
#include "postgres.h"

#include "access/table.h"
#include "access/zedstore_internal.h"
#include "executor/tuptable.h"
#include "utils/expandeddatum.h"

const TupleTableSlotOps TTSOpsZedstore;


typedef struct ZedstoreTupleTableSlot
{
	TupleTableSlot base;

	char	   *data;		/* data for materialized slots */
} ZedstoreTupleTableSlot;


static void
tts_zedstore_init(TupleTableSlot *slot)
{
}

static void
tts_zedstore_release(TupleTableSlot *slot)
{
}

static void
tts_zedstore_clear(TupleTableSlot *slot)
{
	if (unlikely(TTS_SHOULDFREE(slot)))
	{
		ZedstoreTupleTableSlot *vslot = (ZedstoreTupleTableSlot *) slot;

		pfree(vslot->data);
		vslot->data = NULL;

		slot->tts_flags &= ~TTS_FLAG_SHOULDFREE;
	}

	slot->tts_nvalid = 0;
	slot->tts_flags |= TTS_FLAG_EMPTY;
	ItemPointerSetInvalid(&slot->tts_tid);
}

/*
 * Attribute values are readily available in tts_values and tts_isnull array
 * in a ZedstoreTupleTableSlot. So there should be no need to call either of the
 * following two functions.
 */
static void
tts_zedstore_getsomeattrs(TupleTableSlot *slot, int natts)
{
	elog(ERROR, "getsomeattrs is not required to be called on a zedstore tuple table slot");
}

static void
zs_get_xmin_cmin(Relation rel, ZSUndoRecPtr recent_oldest_undo, zstid tid, ZSUndoRecPtr undo_ptr,
				 TransactionId *xmin, CommandId *cmin)
{
	TransactionId this_xmin;
	CommandId	this_cmin;
	ZSUndoRec  *undorec;

	/*
	 * Follow the chain of UNDO records for this tuple, to find the
	 * transaction that originally inserted the row  (xmin/cmin).
	 *
	 * XXX: this is similar logic to zs_cluster_process_tuple(). Can
	 * we merge it?
	 */
	this_xmin = FrozenTransactionId;
	this_cmin = InvalidCommandId;

	for (;;)
	{
		if (undo_ptr.counter < recent_oldest_undo.counter)
		{
			/* This tuple version is visible to everyone. */
			break;
		}

		/* Fetch the next UNDO record. */
		undorec = zsundo_fetch(rel, undo_ptr);

		if (undorec->type == ZSUNDO_TYPE_INSERT)
		{
			this_xmin = undorec->xid;
			this_cmin = undorec->cid;
			break;
		}
		else if (undorec->type == ZSUNDO_TYPE_TUPLE_LOCK ||
				 undorec->type == ZSUNDO_TYPE_DELETE ||
				 undorec->type == ZSUNDO_TYPE_UPDATE)
		{
			undo_ptr = undorec->prevundorec;
			continue;
		}
	}

	*xmin = this_xmin;
	*cmin = this_cmin;
}

/*
 * We only support fetching 'xmin', currently. It's needed for referential
 * integrity triggers (i.e. foreign keys).
 */
static Datum
tts_zedstore_getsysattr(TupleTableSlot *slot, int attnum, bool *isnull)
{
	if (attnum == MinTransactionIdAttributeNumber ||
		attnum == MinCommandIdAttributeNumber)
	{
		zstid		tid = ZSTidFromItemPointer(slot->tts_tid);
		ZSBtreeScan btree_scan;
		bool		found;
		Relation	rel;
		ZSUndoRecPtr recent_oldest_undo;
		TransactionId xmin;
		CommandId cmin;

		/*
		 * We assume that the table OID and TID in the slot are set. We
		 * fetch the tuple from the table, and follow its UNDO chain to
		 * find the transaction that inserted it.
		 *
		 * XXX: This is very slow compared to e.g. the heap, where we
		 * always store the xmin in tuple itself. We should probably do
		 * the same in zedstore, and add extra fields in the slot to hold
		 * xmin/cmin and fill them in when we fetch the tuple and check its
		 * visibility for the first time.
		 */
		if (!OidIsValid(slot->tts_tableOid))
			elog(ERROR, "zedstore tuple table slot does not have a table oid");

		/* assume the caller is already holding a suitable lock on the table */
		rel = table_open(slot->tts_tableOid, NoLock);
		recent_oldest_undo = zsundo_get_oldest_undo_ptr(rel);

		/* Use the meta-data tree for the visibility information. */
		zsbt_begin_scan(rel, slot->tts_tupleDescriptor, ZS_META_ATTRIBUTE_NUM, tid,
						tid + 1, SnapshotAny, &btree_scan);

		found = zsbt_scan_next_tid(&btree_scan) != InvalidZSTid;
		if (!found)
			elog(ERROR, "could not find zedstore tuple (%u, %u)",
				 ZSTidGetBlockNumber(tid), ZSTidGetOffsetNumber(tid));

		zs_get_xmin_cmin(rel, recent_oldest_undo, tid, btree_scan.array_undoptr, &xmin, &cmin);

		zsbt_end_scan(&btree_scan);

		table_close(rel, NoLock);

		*isnull = false;
		if (attnum == MinTransactionIdAttributeNumber)
			return TransactionIdGetDatum(xmin);
		else
		{
			Assert(attnum == MinCommandIdAttributeNumber);
			return CommandIdGetDatum(cmin);
		}
	}	
	elog(ERROR, "zedstore tuple table slot does not have system attributes (except xmin and cmin)");

	return 0; /* silence compiler warnings */
}

/*
 * To materialize a zedstore slot all the datums that aren't passed by value
 * have to be copied into the slot's memory context.  To do so, compute the
 * required size, and allocate enough memory to store all attributes.  That's
 * good for cache hit ratio, but more importantly requires only memory
 * allocation/deallocation.
 */
static void
tts_zedstore_materialize(TupleTableSlot *slot)
{
	ZedstoreTupleTableSlot *vslot = (ZedstoreTupleTableSlot *) slot;
	TupleDesc	desc = slot->tts_tupleDescriptor;
	Size		sz = 0;
	char	   *data;

	/* already materialized */
	if (TTS_SHOULDFREE(slot))
		return;

	/* compute size of memory required */
	for (int natt = 0; natt < desc->natts; natt++)
	{
		Form_pg_attribute att = TupleDescAttr(desc, natt);
		Datum val;

		if (att->attbyval || slot->tts_isnull[natt])
			continue;

		val = slot->tts_values[natt];

		if (att->attlen == -1 &&
			VARATT_IS_EXTERNAL_EXPANDED(DatumGetPointer(val)))
		{
			/*
			 * We want to flatten the expanded value so that the materialized
			 * slot doesn't depend on it.
			 */
			sz = att_align_nominal(sz, att->attalign);
			sz += EOH_get_flat_size(DatumGetEOHP(val));
		}
		else
		{
			sz = att_align_nominal(sz, att->attalign);
			sz = att_addlength_datum(sz, att->attlen, val);
		}
	}

	/* all data is byval */
	if (sz == 0)
		return;

	/* allocate memory */
	vslot->data = data = MemoryContextAlloc(slot->tts_mcxt, sz);
	slot->tts_flags |= TTS_FLAG_SHOULDFREE;

	/* and copy all attributes into the pre-allocated space */
	for (int natt = 0; natt < desc->natts; natt++)
	{
		Form_pg_attribute att = TupleDescAttr(desc, natt);
		Datum val;

		if (att->attbyval || slot->tts_isnull[natt])
			continue;

		val = slot->tts_values[natt];

		if (att->attlen == -1 &&
			VARATT_IS_EXTERNAL_EXPANDED(DatumGetPointer(val)))
		{
			Size data_length;

			/*
			 * We want to flatten the expanded value so that the materialized
			 * slot doesn't depend on it.
			 */
			ExpandedObjectHeader *eoh = DatumGetEOHP(val);

			data = (char *) att_align_nominal(data,
											  att->attalign);
			data_length = EOH_get_flat_size(eoh);
			EOH_flatten_into(eoh, data, data_length);

			slot->tts_values[natt] = PointerGetDatum(data);
			data += data_length;
		}
		else
		{
			Size data_length = 0;

			data = (char *) att_align_nominal(data, att->attalign);
			data_length = att_addlength_datum(data_length, att->attlen, val);

			memcpy(data, DatumGetPointer(val), data_length);

			slot->tts_values[natt] = PointerGetDatum(data);
			data += data_length;
		}
	}
}

static void
tts_zedstore_copyslot(TupleTableSlot *dstslot, TupleTableSlot *srcslot)
{
	TupleDesc	srcdesc = dstslot->tts_tupleDescriptor;

	Assert(srcdesc->natts <= dstslot->tts_tupleDescriptor->natts);

	tts_zedstore_clear(dstslot);

	slot_getallattrs(srcslot);

	for (int natt = 0; natt < srcdesc->natts; natt++)
	{
		dstslot->tts_values[natt] = srcslot->tts_values[natt];
		dstslot->tts_isnull[natt] = srcslot->tts_isnull[natt];
	}

	dstslot->tts_nvalid = srcdesc->natts;
	dstslot->tts_flags &= ~TTS_FLAG_EMPTY;

	/* make sure storage doesn't depend on external memory */
	tts_zedstore_materialize(dstslot);
}

static HeapTuple
tts_zedstore_copy_heap_tuple(TupleTableSlot *slot)
{
	Assert(!TTS_EMPTY(slot));

	return heap_form_tuple(slot->tts_tupleDescriptor,
						   slot->tts_values,
						   slot->tts_isnull);

}

static MinimalTuple
tts_zedstore_copy_minimal_tuple(TupleTableSlot *slot)
{
	Assert(!TTS_EMPTY(slot));

	return heap_form_minimal_tuple(slot->tts_tupleDescriptor,
								   slot->tts_values,
								   slot->tts_isnull);
}


const TupleTableSlotOps TTSOpsZedstore = {
	.base_slot_size = sizeof(ZedstoreTupleTableSlot),
	.init = tts_zedstore_init,
	.release = tts_zedstore_release,
	.clear = tts_zedstore_clear,
	.getsomeattrs = tts_zedstore_getsomeattrs,
	.getsysattr = tts_zedstore_getsysattr,
	.materialize = tts_zedstore_materialize,
	.copyslot = tts_zedstore_copyslot,

	/*
	 * A zedstore tuple table slot can not "own" a heap tuple or a minimal
	 * tuple.
	 */
	.get_heap_tuple = NULL,
	.get_minimal_tuple = NULL,
	.copy_heap_tuple = tts_zedstore_copy_heap_tuple,
	.copy_minimal_tuple = tts_zedstore_copy_minimal_tuple
};
