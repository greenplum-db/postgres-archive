/*
B-tree over TIDs.

Operations:

- Sequential scan in TID order
  - efficient with scanning multiple trees in sync

- random lookups, by TID (for index scan)

- range scans by TID (for bitmap index scan)


- leafs are compressed

 *
 */
#include "postgres.h"

#include "access/itup.h"
#include "access/zedstore_internal.h"
#include "storage/bufmgr.h"
#include "utils/datum.h"
#include "utils/rel.h"

/* internal operations */
/*
- split

- insert tuple. Choose TID based on where the tuple landed.

- FSM

 */

typedef struct ZSStack
{
	BlockNumber blk;
	struct ZSStack *parent;
} ZSStack;

typedef struct ZSInsertState
{
	Relation	rel;
	AttrNumber	attno;
	Datum		datum;
} ZSInsertState;

static ItemPointerData zsbt_insert_to_leaf(Buffer buf, ZSInsertState *state);

ItemPointerData
zsbt_insert(Relation rel, AttrNumber attno, Datum datum)
{
	ZSInsertState state;
	Buffer		buf;
	BlockNumber	rootblk;

	rootblk = zsmeta_get_root_for_attribute(rel, attno, true);

	state.rel = rel;
	state.attno = attno;
	state.datum = datum;

	buf = ReadBuffer(rel, rootblk);
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

	/* TODO: Find a leaf page with free space */

	return zsbt_insert_to_leaf(buf, &state);
}

static ItemPointerData
zsbt_insert_to_leaf(Buffer buf, ZSInsertState *state)
{
	/* If there's space, add here */
	TupleDesc	desc = RelationGetDescr(state->rel);
	Form_pg_attribute attr = &desc->attrs[state->attno - 1];
	Page		page = BufferGetPage(buf);
	ZSBtreePageOpaque *opaque = ZSBtreePageGetOpaque(page);
	Size		datumsz;
	Size		itemsz;
	IndexTuple	itup;
	char	   *dataptr;
	ItemPointerData tid;
	OffsetNumber maxoff;

	/*
	 * Look at the last item, for its tid.
	 */
	maxoff = PageGetMaxOffsetNumber(page);
	if (maxoff > FirstOffsetNumber)
	{
		ItemId		iid = PageGetItemId(page, maxoff);
		IndexTuple	hitup = (IndexTuple) PageGetItem(page, iid);

		tid = hitup->t_tid;
	}
	else
		tid = opaque->zs_lokey;

	datumsz = datumGetSize(state->datum, attr->attbyval, attr->attlen);
	itemsz = sizeof(IndexTupleData) + datumsz;

	/* FIXME: should we detoast or deal with "expanded" datums here? */

	itup = palloc(itemsz);
	itup->t_tid = tid;
	itup->t_info = 0;
	itup->t_info |= itemsz;

	dataptr = ((char *) itup) + sizeof(IndexTupleData);

	if (attr->attbyval)
		store_att_byval(dataptr, state->datum, attr->attlen);
	else
		memcpy(dataptr, DatumGetPointer(state->datum), datumsz);

	if (PageGetFreeSpace(page) >= itemsz)
	{
		if (!PageAddItemExtended(page, (Item) itup, itemsz, maxoff, 0))
			elog(ERROR, "didn't fit, after all?");

		MarkBufferDirty(buf);
		/* TODO: WAL-log */

		UnlockReleaseBuffer(buf);

		return tid;
	}
	else
	{
		/* split the page */
		//zsbt_split(buf, nkeys, datums);
		elog(ERROR, "not implemented");
	}
}

#if 0
static void
zsbt_update_parent(BlockNumber rootblk, ItemPointer tid, int nkeys, Datum *datums)
{

}


void
zsbt_split(Buffer buf, int nkeys, Datum *datums)
{

}
#endif

void
zsbt_begin_scan(Relation rel, AttrNumber attno, ItemPointer starttid, ZSBtreeScan *scan)
{
	TupleDesc	tupdesc = RelationGetDescr(rel);
	Buffer		buf;
	Page		page;
	ZSBtreePageOpaque *opaque;
	BlockNumber	rootblk;

	rootblk = zsmeta_get_root_for_attribute(rel, attno, false);
	
	if (rootblk != InvalidBlockNumber)
	{
		buf = ReadBuffer(rel, rootblk);
		page = BufferGetPage(buf);
		opaque = ZSBtreePageGetOpaque(page);
		(void) opaque;
		LockBuffer(buf, BUFFER_LOCK_SHARE);

		/* TODO: descend the tree to leaf */

		LockBuffer(buf, BUFFER_LOCK_UNLOCK);

		scan->desc = tupdesc;
		scan->attno = attno;
		
		scan->active = true;
		scan->lastbuf = buf;
		scan->lastoff = InvalidOffsetNumber;
		ItemPointerSetInvalid(&scan->lasttid);
	}
	else
	{
		scan->desc = NULL;
		scan->attno = InvalidAttrNumber;
		scan->active = false;
		scan->lastbuf = InvalidBuffer;
		scan->lastoff = InvalidOffsetNumber;
		ItemPointerSetInvalid(&scan->lasttid);
	}
}

void
zsbt_end_scan(ZSBtreeScan *scan)
{
	if (scan->lastbuf)
		ReleaseBuffer(scan->lastbuf);
	scan->active = false;
}

/*
 * Return true if there was another tuple. The datum is returned in *datum,
 * and its TID in *tid. For a pass-by-ref datum, it's a palloc'd copy.
 */
bool
zsbt_scan_next(ZSBtreeScan *scan, Datum *datum, ItemPointerData *tid)
{
	Buffer		buf;
	Page		page;
	OffsetNumber off;
	OffsetNumber maxoff;

	if (!scan->active)
		return false;

	buf = scan->lastbuf;
	page = BufferGetPage(buf);

	LockBuffer(buf, BUFFER_LOCK_SHARE);

	/* TODO: check the last offset first */
	maxoff = PageGetMaxOffsetNumber(page);
	for (off = FirstOffsetNumber; off <= maxoff; off++)
	{
		ItemId		iid = PageGetItemId(page, off);
		IndexTuple	itup = (IndexTuple) PageGetItem(page, iid);
		char	   *ptr;

		if (!ItemPointerIsValid(&scan->lasttid) ||
			ItemPointerCompare(&itup->t_tid, &scan->lasttid) > 0)
		{
			Form_pg_attribute attr = &scan->desc->attrs[scan->attno - 1];

			ptr = ((char *) itup) + sizeof(IndexTupleData);

			*datum = fetchatt(attr, ptr);
			*datum = datumCopy(*datum, attr->attbyval, attr->attlen);
			*tid = itup->t_tid;
			LockBuffer(buf, BUFFER_LOCK_UNLOCK);

			scan->lastbuf = buf;
			scan->lasttid = *tid;
			scan->lastoff = off;

			return true;
		}
	}

	LockBuffer(buf, BUFFER_LOCK_UNLOCK);
	scan->active = false;
	return false;
}
