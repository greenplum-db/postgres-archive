/*
 * zedstore_tuplebuffer.c
 *		Buffering insertions into a zedstore table
 *
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/zedstore/zedstore_tuplebuffer.c
 */
#include "postgres.h"

#include "access/table.h"
#include "access/tuptoaster.h"
#include "access/zedstoream.h"
#include "access/zedstore_internal.h"
#include "miscadmin.h"
#include "utils/datum.h"
#include "utils/hashutils.h"

#define TID_RESERVATION_SIZE		100

#define ATTBUFFER_SIZE				(1024 * 1024)

typedef struct
{
	zstid		buffered_tids[60];
	Datum		buffered_datums[60];
	bool		buffered_isnulls[60];
	int			num_buffered_rows;

	attstream_buffer chunks;

} attbuffer;

typedef struct
{
	Oid			relid;			/* table's OID (hash key) */
	char		status;			/* hash entry status */

	int			natts;			/* # of attributes on table might change, if it's ALTERed */
	attbuffer	*attbuffers;

} tuplebuffer;


/* define hashtable mapping block numbers to PagetableEntry's */
#define SH_PREFIX tuplebuffers
#define SH_ELEMENT_TYPE tuplebuffer
#define SH_KEY_TYPE Oid
#define SH_KEY relid
#define SH_HASH_KEY(tb, key) murmurhash32(key)
#define SH_EQUAL(tb, a, b) a == b
#define SH_SCOPE static inline
#define SH_DEFINE
#define SH_DECLARE
#include "lib/simplehash.h"


/* prototypes for internal functions */
static void zsbt_attbuffer_spool(Relation rel, AttrNumber attno, attbuffer *attbuffer, int ntuples, zstid *tids, Datum *datums, bool *isnulls);
static void zsbt_attbuffer_init(Form_pg_attribute attr, attbuffer *attbuffer);
static void zsbt_attbuffer_flush(Relation rel, AttrNumber attno, attbuffer *attbuffer, bool all);


static MemoryContext tuplebuffers_cxt = NULL;
static struct tuplebuffers_hash *tuplebuffers = NULL;

static tuplebuffer *
get_tuplebuffer(Relation rel)
{
	bool		found;
	tuplebuffer *tupbuffer;

	if (tuplebuffers_cxt == NULL)
	{
		tuplebuffers_cxt = AllocSetContextCreate(TopTransactionContext,
												 "ZedstoreAMTupleBuffers",
												 ALLOCSET_DEFAULT_SIZES);
		tuplebuffers = tuplebuffers_create(tuplebuffers_cxt, 10, NULL);
	}
retry:
	tupbuffer = tuplebuffers_insert(tuplebuffers, RelationGetRelid(rel), &found);
	if (!found)
	{
		MemoryContext oldcxt;
		AttrNumber	attno;
		int			natts;

		oldcxt = MemoryContextSwitchTo(tuplebuffers_cxt);
		natts = rel->rd_att->natts;
		tupbuffer->attbuffers = palloc(natts * sizeof(attbuffer));
		tupbuffer->natts = natts;

		for (attno = 1; attno <= natts; attno++)
		{
			Form_pg_attribute attr = TupleDescAttr(rel->rd_att, attno - 1);
			attbuffer *attbuffer = &tupbuffer->attbuffers[attno - 1];

			zsbt_attbuffer_init(attr, attbuffer);
		}
		MemoryContextSwitchTo(oldcxt);
	}
	else if (rel->rd_att->natts > tupbuffer->natts)
	{
		zsbt_tuplebuffer_flush(rel);
		goto retry;
	}

	return tupbuffer;
}

/* buffer more data */
void
zsbt_tuplebuffer_spool_tuple(Relation rel, zstid tid, Datum *datums, bool *isnulls)
{
	AttrNumber	attno;
	tuplebuffer *tupbuffer;

	tupbuffer = get_tuplebuffer(rel);

	for (attno = 1; attno <= rel->rd_att->natts; attno++)
	{
		Form_pg_attribute attr = TupleDescAttr(rel->rd_att, attno - 1);
		attbuffer *attbuffer = &tupbuffer->attbuffers[attno - 1];
		Datum		datum;
		bool		isnull;

		datum = datums[attno - 1];
		isnull = isnulls[attno - 1];

		if (!isnull && attr->attlen < 0 && VARATT_IS_EXTERNAL(datum))
			datum = PointerGetDatum(heap_tuple_fetch_attr((struct varlena *) DatumGetPointer(datum)));

		/* If this datum is too large, toast it */
		if (!isnull && attr->attlen < 0 &&
			VARSIZE_ANY_EXHDR(datum) > MaxZedStoreDatumSize)
		{
			datum = zedstore_toast_datum(rel, attno, datum, tid);
		}

		zsbt_attbuffer_spool(rel, attno, attbuffer, 1, &tid, &datum, &isnull);
	}
}

void
zsbt_tuplebuffer_spool_slots(Relation rel, zstid *tids, TupleTableSlot **slots, int ntuples)
{
	AttrNumber	attno;
	tuplebuffer *tupbuffer;
	Datum	   *datums;
	bool	   *isnulls;

	tupbuffer = get_tuplebuffer(rel);

	datums = palloc(ntuples * sizeof(Datum));
	isnulls = palloc(ntuples * sizeof(bool));

	for (attno = 1; attno <= rel->rd_att->natts; attno++)
	{
		Form_pg_attribute attr = TupleDescAttr(rel->rd_att, attno - 1);
		attbuffer *attbuffer = &tupbuffer->attbuffers[attno - 1];

		for (int i = 0; i < ntuples; i++)
		{
			Datum		datum = slots[i]->tts_values[attno - 1];
			bool		isnull = slots[i]->tts_isnull[attno - 1];

			if (attno == 1)
				slot_getallattrs(slots[i]);

			if (!isnull && attr->attlen < 0 && VARATT_IS_EXTERNAL(datum))
				datum = PointerGetDatum(heap_tuple_fetch_attr((struct varlena *) DatumGetPointer(datum)));

			/* If this datum is too large, toast it */
			if (!isnull && attr->attlen < 0 &&
				VARSIZE_ANY_EXHDR(datum) > MaxZedStoreDatumSize)
			{
				datum = zedstore_toast_datum(rel, attno, datum, tids[i]);
			}
			datums[i] = datum;
			isnulls[i] = isnull;
		}

		zsbt_attbuffer_spool(rel, attno, attbuffer, ntuples, tids, datums, isnulls);
	}

	pfree(datums);
	pfree(isnulls);
}


static void
zsbt_attbuffer_init(Form_pg_attribute attr, attbuffer *attbuffer)
{
	attstream_buffer *attbuf = &attbuffer->chunks;

#define ATTBUF_INIT_SIZE 1024
	attbuf->data = palloc(ATTBUF_INIT_SIZE);
	attbuf->len = 0;
	attbuf->maxlen = ATTBUF_INIT_SIZE;
	attbuf->cursor = 0;

	attbuf->firsttid = 0;
	attbuf->lasttid = 0;

	attbuf->attlen = attr->attlen;
	attbuf->attbyval = attr->attbyval;

	attbuffer->num_buffered_rows = 0;
}

static void
zsbt_attbuffer_spool(Relation rel, AttrNumber attno, attbuffer *attbuffer,
					 int ntuples, zstid *tids, Datum *datums, bool *isnulls)
{
	int			i;
	attstream_buffer *chunks = &attbuffer->chunks;

	for (i = 0; i < ntuples; i++)
	{
		Datum		datum;

		if (attbuffer->num_buffered_rows >= 60)
			zsbt_attbuffer_flush(rel, attno, attbuffer, false);

		if (!chunks->attbyval && !isnulls[i])
		{
			/* XXX: have to make a copy of pass-by ref values, because we
			 * need it to live until the end-of-xact, where we flush the buffers.
			 * That's pretty inefficient!
			 */
			MemoryContext oldcxt = MemoryContextSwitchTo(tuplebuffers_cxt);
			datum = zs_datumCopy(datums[i], chunks->attbyval, chunks->attlen);
			MemoryContextSwitchTo(oldcxt);
		}
		else
			datum = datums[i];

		attbuffer->buffered_tids[attbuffer->num_buffered_rows] = tids[i];
		attbuffer->buffered_datums[attbuffer->num_buffered_rows] = datum;
		attbuffer->buffered_isnulls[attbuffer->num_buffered_rows] = isnulls[i];
		attbuffer->num_buffered_rows++;
	}
}

/* flush */

static void
zsbt_attbuffer_flush(Relation rel, AttrNumber attno, attbuffer *attbuffer, bool all)
{
	int			num_encoded;
	int			num_remain;
	attstream_buffer *chunks = &attbuffer->chunks;

	/* First encode more */
	if (attbuffer->num_buffered_rows >= 60 ||
		(all && attbuffer->num_buffered_rows > 0))
	{
		num_encoded = append_attstream(chunks, all, attbuffer->num_buffered_rows,
									   attbuffer->buffered_tids,
									   attbuffer->buffered_datums,
									   attbuffer->buffered_isnulls);
		num_remain = attbuffer->num_buffered_rows - num_encoded;

		if (!chunks->attbyval)
		{
			for (int i = 0; i < num_encoded; i++)
			{
				if (!attbuffer->buffered_isnulls[i])
					pfree(DatumGetPointer(attbuffer->buffered_datums[i]));
			}
		}

		memmove(attbuffer->buffered_tids, &attbuffer->buffered_tids[num_encoded], num_remain * sizeof(zstid));
		memmove(attbuffer->buffered_datums, &attbuffer->buffered_datums[num_encoded], num_remain * sizeof(Datum));
		memmove(attbuffer->buffered_isnulls, &attbuffer->buffered_isnulls[num_encoded], num_remain * sizeof(bool));
		attbuffer->num_buffered_rows = num_remain;
	}

	while ((all && chunks->len - chunks->cursor > 0) ||
		   chunks->len - chunks->cursor > ATTBUFFER_SIZE)
	{
		zsbt_attr_add(rel, attno, &attbuffer->chunks);
	}
}

void
zsbt_tuplebuffer_flush(Relation rel)
{
	tuplebuffer *tupbuffer;
	AttrNumber	attno;

	if (!tuplebuffers)
		return;
	tupbuffer = tuplebuffers_lookup(tuplebuffers, RelationGetRelid(rel));
	if (!tupbuffer)
		return;

	for (attno = 1; attno <= tupbuffer->natts; attno++)
	{
		attbuffer *attbuffer = &tupbuffer->attbuffers[attno - 1];

		zsbt_attbuffer_flush(rel, attno, attbuffer, true);
	}
	tuplebuffers_delete(tuplebuffers, RelationGetRelid(rel));
}

static void
zsbt_tuplebuffers_flush(void)
{
	tuplebuffers_iterator iter;
	tuplebuffer *tupbuffer;

	tuplebuffers_start_iterate(tuplebuffers, &iter);
	while ((tupbuffer = tuplebuffers_iterate(tuplebuffers, &iter)) != NULL)
	{
		Relation	rel;
		AttrNumber	attno;

		rel = table_open(tupbuffer->relid, NoLock);

		for (attno = 1; attno <= tupbuffer->natts; attno++)
		{
			attbuffer *attbuffer = &tupbuffer->attbuffers[attno - 1];

			zsbt_attbuffer_flush(rel, attno, attbuffer, true);
		}

		table_close(rel, NoLock);
	}
}


/* check in a scan */


/*
 * End-of-transaction cleanup for zedstore.
 *
 * Flush tuple buffers in zedstore.
 *
 * We must flush everything before the top transaction commit becomes
 * visible to others, so that they can see the data. On abort, we can drop
 * everything we had buffered at top transaction abort. That's fortunate,
 * because we couldn't access the table during abort processing anyway.
 *
 * Subtransactions:
 *
 * After a subtransaction has been marked as aborted, we mustn't write
 * out any attribute data belonging to the aborted subtransaction. Two
 * reasons for that. Firstly, the TIDs belonging to an aborted
 * subtransaction might be vacuumed away at any point. We mustn't write
 * out attribute data for a TID that's already been vacuumed away in the
 * TID tree. Secondly, subtransaction abort releases locks acquired in
 * the subtransaction, and we cannot write out data if we're not holding
 * a lock on the table. So we must throw our buffers away at subtransaction
 * abort.
 *
 * Since we throw away our buffers at subtransaction abort, we must take
 * care that the buffers are empty when a subtransaction begins. If there
 * was any leftover buffered data for other subtransactions, we would
 * throw away that data too, if the new subtransaction aborts.
 *
 * Writing out the buffers at subtransaction commit probably isn't necessary,
 * but might as well play it safe and do it.
 */
void
AtEOXact_zedstore_tuplebuffers(bool isCommit)
{
	if (tuplebuffers_cxt)
	{
		if (isCommit)
			zsbt_tuplebuffers_flush();
		MemoryContextDelete(tuplebuffers_cxt);
		tuplebuffers_cxt = NULL;
		tuplebuffers = NULL;
	}
}

void
AtSubStart_zedstore_tuplebuffers(void)
{
	if (tuplebuffers_cxt)
	{
		zsbt_tuplebuffers_flush();
		MemoryContextDelete(tuplebuffers_cxt);
		tuplebuffers_cxt = NULL;
		tuplebuffers = NULL;
	}
}

void
AtEOSubXact_zedstore_tuplebuffers(bool isCommit)
{
	if (tuplebuffers_cxt)
	{
		if (isCommit)
			zsbt_tuplebuffers_flush();
		MemoryContextDelete(tuplebuffers_cxt);
		tuplebuffers_cxt = NULL;
		tuplebuffers = NULL;
	}
}
