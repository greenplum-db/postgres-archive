/*
 * zedstore_attstream.c
 *		Routines for packing datums into "attribute streams", to be stored
 *      on attribute tree leaf pages.
 *
 * An attribute stream consists of "chunks", where one chunk contains
 * the TIDs of 1-60 datums, packed in a compact form, and their datums.
 * Each chunk begins with a 64-bit codeword, which contains the TIDs
 * in the chunk. The TIDs are delta-encoded, so we store the difference of
 * each TID to the previous TID in the stream, and the differences are
 * packed in 64-bit codewords using a variant of Simple-8b encoding.
 *
 * For the first TID in a stream, the "previous" TID is thought to be 0,
 * so the first TID in the stream's first chunk actually stores the
 * absolute TID.
 *
 * The encoding of TIDs in the codeword is a variant of the Simple-8b
 * algorithm. 4 bits in each codeword determine a "mode", and the remaining
 * 60 bits encode the TIDs in a format that depends on the mode. But we also
 * use the codeword to encode the presence of NULLs, and in the case of
 * variable-width attributes, the length of each datum in the chunk.
 * Therefore, fixed- and variable-length attributes use different "modes".
 *
 * This chunked format has a few desireable properties:
 *
 * - It is compact for the common case of no or few gaps between TIDs.
 *   In the best case, one codeword can pack 60 consecutive TIDs in
 *   one 64-bit codeword. It also "degrades gracefully", as TIDs are
 *   removed, so deleting a few TIDs here and there doesn't usually make
 *   the overall stream larger.
 *
 * - It is relatively fast to encode and decode.
 *
 * - A long stream can be split easily. You can cut the stream at any chunk,
 *   having to re-encode only the first chunk after the split point. Also,
 *   each chunk is relatively small, which minimizes waste when a large
 *   stream needs to be chopped into page-sized pieces.
 *
 * - Two streams can easily be appended to each other, without having to
 *   re-encode the chunks (although it might not result in the most compact
 *   possible codewords.)
 *
 * Some drawbacks:
 *
 * - Random access is not possible. To access a particular TID, the stream
 *   must be read starting from the beginning.
 *
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/zedstore/zedstore_attstream.c
 */
#include "postgres.h"

#include "access/zedstore_internal.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "utils/datum.h"
#include "utils/memutils.h"

#define TARGET_CHUNK_SIZE	128

/*
 * Internal functions that operate on a single chunk.
 */
static bool replace_first_tid(int attlen, zstid newtid, char *chunk);
static int skip_chunk(int attlen, char *chunk, zstid *lasttid);
static int get_chunk_length(int attlen, char *chunk);
static zstid get_chunk_first_tid(int attlen, char *chunk);
static int decode_chunk(bool attbyval, int attlen, zstid *lasttid, char *chunk,
						int *num_elems, zstid *tids, Datum *datums, bool *isnulls);
static int encode_chunk(bool attbyval, int attlen, zstid prevtid, int ntids,
						zstid *tids, Datum *datums, bool *isnulls,
						StringInfo dst);
#ifdef USE_ASSERT_CHECKING
static void verify_attstream(Form_pg_attribute att, ZSAttStream *attstream);
#endif

/* Other internal functions. */
static ZSAttStream *decompress_attstream(ZSAttStream *attstream);
static ZSAttStream *append_attstreams(Form_pg_attribute attr,
									  ZSAttStream *attstream1, ZSAttStream *attstream2);

/*
 * Pack given datums into an attstream.
 */
ZSAttStream *
create_attstream(Form_pg_attribute att, int nelems, zstid *tids,
				   Datum *datums, bool *isnulls)
{
	StringInfoData result;
	int			num_encoded;
	zstid		prevtid;
	char		zeros[offsetof(ZSAttStream, t_payload)];
	ZSAttStream *hdr;

	initStringInfo(&result);

	appendBinaryStringInfo(&result, zeros, offsetof(ZSAttStream, t_payload));

	prevtid = 0;
	while (nelems > 0)
	{
		num_encoded = encode_chunk(att->attbyval, att->attlen,
								   prevtid, nelems, tids, datums, isnulls, &result);
		Assert(num_encoded > 0);
		prevtid = tids[num_encoded - 1];
		datums += num_encoded;
		isnulls += num_encoded;
		tids += num_encoded;
		nelems -= num_encoded;
	}

	/* Fill in the header */
	hdr = (ZSAttStream *) result.data;
	hdr->t_size = result.len;
	hdr->t_flags = 0;
	hdr->t_decompressed_size = 0;
	hdr->t_decompressed_bufsize = 0;
	hdr->t_lasttid = tids[nelems - 1];

#ifdef USE_ASSERT_CHECKING
	verify_attstream(att, hdr);
#endif

	return hdr;
}


/*
 * Extract TID and Datum/isnull arrays an attstream.
 *
 * The arrays are stored directly into the scan->array_* fields.
 *
 * TODO: avoid extracting elements we're not interested in, by passing
 * starttid/endtid.
 */
void
decode_attstream(ZSAttrTreeScan *scan, ZSAttStream *chunks)
{
	Form_pg_attribute attr = scan->attdesc;
	zstid		lasttid;
	int			total_decoded;
	char	   *p;
	char	   *pend;
	MemoryContext oldcxt;

	if (scan->array_datums_allocated_size < 200)
	{
		/* initial size */
		int			newsize = 200;

		/*
		 * Note: we don't allocate these in 'array_cxt'. 'array_cxt' is reset
		 * that between every invocation of decode_attstream(), but we want to
		 * reuse these arrays.
		 */
		if (scan->array_datums)
			pfree(scan->array_datums);
		if (scan->array_isnulls)
			pfree(scan->array_isnulls);
		if (scan->array_tids)
			pfree(scan->array_tids);
		scan->array_datums = MemoryContextAlloc(scan->context, newsize * sizeof(Datum));
		scan->array_isnulls = MemoryContextAlloc(scan->context, newsize * sizeof(bool));
		scan->array_tids = MemoryContextAlloc(scan->context, newsize * sizeof(zstid));
		scan->array_datums_allocated_size = newsize;
	}

	if (scan->array_cxt == NULL)
	{
		scan->array_cxt = AllocSetContextCreate(scan->context,
												"ZedstoreAMScanContext",
												ALLOCSET_DEFAULT_SIZES);
	}
	else
		MemoryContextReset(scan->array_cxt);
	oldcxt = MemoryContextSwitchTo(scan->array_cxt);

	if ((chunks->t_flags & ATTSTREAM_COMPRESSED) != 0)
		chunks = decompress_attstream(chunks);

	p = chunks->t_payload;
	pend = ((char *) chunks) + chunks->t_size;

	total_decoded = 0;
	lasttid = 0;
	while (p < pend)
	{
		int			num_decoded;

		if (scan->array_datums_allocated_size < total_decoded + 60)
		{
			/* initial size */
			int			newsize = (total_decoded * 2) + 60;

			scan->array_datums = repalloc(scan->array_datums, newsize * sizeof(Datum));
			scan->array_isnulls = repalloc(scan->array_isnulls, newsize * sizeof(bool));
			scan->array_tids = repalloc(scan->array_tids, newsize * sizeof(zstid));
			scan->array_datums_allocated_size = newsize;
		}

		p += decode_chunk(attr->attbyval, attr->attlen, &lasttid, p,
						  &num_decoded,
						  &scan->array_tids[total_decoded],
						  &scan->array_datums[total_decoded],
						  &scan->array_isnulls[total_decoded]);
		total_decoded += num_decoded;
	}
	Assert(p == pend);
	scan->array_num_elements = total_decoded;

	MemoryContextSwitchTo(oldcxt);
}

/*
 * Get the first TID of an attream.
 */
zstid
get_attstream_first_tid(int attlen, ZSAttStream *attstream)
{
	if ((attstream->t_flags & ATTSTREAM_COMPRESSED) != 0)
		elog(ERROR, "cannot get first tid of compressed chunk");
	return get_chunk_first_tid(attlen, attstream->t_payload);
}

/*
 * Split 'chunk' at 'pos'. 'lasttid' is the TID of the item,
 * 'pos'
 *
 * The current chunk begins at chunks->cursor. The 'cursor' will
 * be moved to the new starting position.
 *
 * NOTE: this modifies the chunk in place.
 *
 * Returns the firsttid after the splitpoint.
 */
zstid
chop_attstream(Form_pg_attribute att, chopper_state *chunks, int pos, zstid lasttid)
{
	char	   *first_chunk;
	int			first_chunk_len;
	zstid		first_chunk_tids[60];
	Datum		first_chunk_datums[60];
	bool		first_chunk_isnulls[60];
	int			first_chunk_num_elems;
	zstid		xtid;
	StringInfoData buf;
	int			total_encoded;
	zstid		prevtid;
	zstid		newfirsttid;

	chunks->cursor += pos;
	Assert(chunks->cursor <= chunks->len);
	if (chunks->cursor >= chunks->len)
	{
		Assert(chunks->cursor == chunks->len);
		return InvalidZSTid;
	}

	/* FIXME: arbitrary limit. We need some space before the split point, for the
	 * new attstream header. Compute this correctly, and perhaps reallocate a
	 * bigger buffer if needed. ATM, though, this is only used to chop large
	 * attstreams to page-sized parts, so this never gets called with a very
	 * small 'pos'.
	 */
	if (chunks->cursor < 500)
		elog(ERROR, "cannot split");

	/*
	 * Try to modify the first codeword in place. It just might work out if
	 * we're lucky.
	 */
	first_chunk = chunks->data + chunks->cursor;

	newfirsttid = lasttid + get_chunk_first_tid(att->attlen, first_chunk);
	if (replace_first_tid(att->attlen, newfirsttid, first_chunk))
	{
		return newfirsttid;
	}

	/* Try to split the first chunk */
	xtid = lasttid;
	first_chunk_len = decode_chunk(att->attbyval, att->attlen, &xtid,
								   first_chunk,
								   &first_chunk_num_elems,
								   first_chunk_tids,
								   first_chunk_datums,
								   first_chunk_isnulls);
	initStringInfo(&buf);

	/* re-encode */
	total_encoded = 0;
	prevtid = 0;
	while (total_encoded < first_chunk_num_elems)
	{
		int		num_encoded;

		num_encoded = encode_chunk(att->attbyval, att->attlen, prevtid,
								   first_chunk_num_elems - total_encoded,
								   &first_chunk_tids[total_encoded],
								   &first_chunk_datums[total_encoded],
								   &first_chunk_isnulls[total_encoded],
								   &buf);
		total_encoded += num_encoded;
		prevtid = first_chunk_tids[total_encoded - 1];
	}

	/* replace the chunk in the original stream with the new chunks */
	chunks->cursor += first_chunk_len;
	if (chunks->cursor < buf.len)
		elog(ERROR, "not enough work space to split");
	chunks->cursor -= buf.len;
	memcpy(&chunks->data[chunks->cursor], buf.data, buf.len);

	return first_chunk_tids[0];
}

/*
 * Find the beginning offset of last chunk that fits in 'len'.
 *
 * Returns -1 if there are no full chunks.
 */
int
truncate_attstream(Form_pg_attribute att, char *chunks, int len, zstid *lasttid)
{
	char	   *p = chunks;
	char	   *pend = p + len;

	*lasttid = 0;
	while (p + sizeof(uint64) <= pend)
	{
		int			this_chunk_len;

		this_chunk_len = get_chunk_length(att->attlen, p);

		if (p + this_chunk_len > pend)
			break;		/* this one is not complete */

		p += skip_chunk(att->attlen, p, lasttid);
	}
	/* 'p' now points to the first incomplete chunk */
	return p - (char *) chunks;
}

#ifdef USE_ASSERT_CHECKING
static void
verify_attstream(Form_pg_attribute att, ZSAttStream *attstream)
{
	char	   *p = attstream->t_payload;
	char	   *pend = ((char *) attstream) + attstream->t_size;
	zstid		tid;

	tid = 0;

	while (p < pend)
	{
		p += skip_chunk(att->attlen, p, &tid);
	}
	Assert(tid == attstream->t_lasttid);
	Assert(p == pend);
}
#endif

void
print_attstream(Form_pg_attribute att, char *chunk, int len)
{
	char	   *p = chunk;
	char	   *pend = chunk + len;
	zstid		tid;

	tid = 0;
	while (p < pend)
	{
		uint64		codeword;

		memcpy(&codeword, p, sizeof(uint64));

		p += skip_chunk(att->attlen, p, &tid);
		elog(NOTICE, "%016lX: TID %lu", codeword, tid);
	}
}

static ZSAttStream *
decompress_attstream(ZSAttStream *attstream)
{
	ZSAttStream *result;

	Assert(attstream->t_flags & ATTSTREAM_COMPRESSED);

	result = palloc(SizeOfZSAttStreamHeader + attstream->t_decompressed_bufsize);
	zs_decompress(attstream->t_payload, result->t_payload,
				  attstream->t_size - SizeOfZSAttStreamHeader,
				  attstream->t_decompressed_bufsize);

	result->t_size = SizeOfZSAttStreamHeader + attstream->t_decompressed_size;
	result->t_flags = 0;
	result->t_decompressed_size = 0;
	result->t_decompressed_bufsize = 0;
	result->t_lasttid = attstream->t_lasttid;

	return result;
}

static ZSAttStream *
append_attstreams(Form_pg_attribute attr,
				  ZSAttStream *attstream1, ZSAttStream *attstream2)
{
	int			resultbufsize;
	ZSAttStream *result;

	if ((attstream1->t_flags & ATTSTREAM_COMPRESSED) != 0)
		elog(ERROR, "cannot append compressed attstream");
	if ((attstream2->t_flags & ATTSTREAM_COMPRESSED) != 0)
		elog(ERROR, "cannot append compressed attstream");

	resultbufsize = attstream1->t_size + attstream2->t_size;

	result = palloc(resultbufsize);
	memcpy(result, attstream1, attstream1->t_size);

	if (!append_attstreams_inplace(attr, result, resultbufsize - result->t_size, attstream2))
		elog(ERROR, "splicing two attstreams failed");

	return result;
}

/*
 * Merge two attstreams together.
 *
 * This is the workhorse of repacking and re-encoding data, when
 * new attribute data is added to a page (INSERT/UPDATE), or when
 * some data is removed (VACUUM after a DELETE).
 *
 * 'attstream1' and 'attstream2' are the two streams to merge.
 * Either one can be NULL, if you just want to re-encode and
 * recompress an existing stream.
 *
 * 'tids_to_remove' is an optional array of TIDs to remove from
 * the stream(s).
 *
 * There are some heuristics here:
 *
 * - A compressed attstream is assumed to already be in a "dense"
 *   form, using maximally compact codewords. An uncompressed
 *   stream, however, might not be, so uncompressed streams are
 *   always decoded into constituent datums, and re-encoded.
 *
 */
ZSAttStream *
merge_attstreams(Relation rel, Form_pg_attribute attr,
				 ZSAttStream *attstream1, ZSAttStream *attstream2,
				 zstid *tids_to_remove, int num_tids_to_remove)
{
	ZSAttStream *result;
	ZSAttrTreeScan scan1;
	ZSAttrTreeScan scan2;
	Datum	   *result_datums;
	bool	   *result_isnulls;
	zstid	   *result_tids;
	int			total_elems;
	int			remain_elems;
	int			removeidx = 0;
	bool		attstream1_was_compressed = false;
	bool		attstream2_was_compressed = false;
	zstid		firsttid1 = InvalidZSTid;
	zstid		firsttid2 = InvalidZSTid;

	/*
	 * If either input is compressed, decompress them.
	 */
	if (attstream1 && (attstream1->t_flags & ATTSTREAM_COMPRESSED) != 0)
	{
		attstream1 = decompress_attstream(attstream1);
		attstream1_was_compressed = true;
	}

	if (attstream2 && (attstream2->t_flags & ATTSTREAM_COMPRESSED) != 0)
	{
		attstream2 = decompress_attstream(attstream2);
		attstream2_was_compressed = true;
	}

	if (attstream1)
		firsttid1 = get_attstream_first_tid(attr->attlen, attstream1);
	if (attstream2)
		firsttid2 = get_attstream_first_tid(attr->attlen, attstream2);

	/*
	 * Swap the inputs, so that the stream with smaller starting TID is
	 * 'attstream1'.
	 */
	if (attstream1 && attstream2 && firsttid1 > firsttid2)
	{
		ZSAttStream *attstream_tmp;
		bool		attstream_was_compressed_tmp;
		zstid		firsttid_tmp;

		attstream_tmp = attstream1;
		attstream_was_compressed_tmp = attstream1_was_compressed;
		firsttid_tmp = firsttid1;

		attstream1 = attstream2;
		attstream1_was_compressed = attstream2_was_compressed;
		firsttid1 = firsttid2;

		attstream2 = attstream_tmp;
		attstream2_was_compressed = attstream_was_compressed_tmp;
		firsttid2 = firsttid_tmp;
	}

	/*
	 * Fast path:
	 *
	 * If we have nothing to remove, and the two streams don't overlap, then
	 * we can avoid re-encoding and just append one stream after the other.
	 * We only do this if the stream that comes first was compressed:
	 * otherwise it may not be optimally packed, and we want to re-encode it
	 * to make sure it's using densest possible codewords.
	 */
	if (tids_to_remove == 0 &&
		attstream1 && attstream2 &&
		firsttid2 > attstream1->t_lasttid &&
		(attstream1_was_compressed || attstream1_was_compressed))
	{
		/* repack inputs that were not previously compressed */
		if (!attstream1_was_compressed)
			attstream1 = merge_attstreams(rel, attr, attstream1, NULL, NULL, 0);
		if (!attstream2_was_compressed)
			attstream2 = merge_attstreams(rel, attr, attstream2, NULL, NULL, 0);

		return append_attstreams(attr, attstream1, attstream2);
	}

	/*
	 * naive implementation: decode everything, merge arrays, and re-encode.
	 */
	memset(&scan1, 0, sizeof(scan1));
	memset(&scan2, 0, sizeof(scan1));
	scan1.context = CurrentMemoryContext;
	scan1.attdesc = attr;
	scan2.context = CurrentMemoryContext;
	scan2.attdesc = attr;

	if (attstream1)
		decode_attstream(&scan1, attstream1);
	if (attstream2)
		decode_attstream(&scan2, attstream2);
	total_elems = scan1.array_num_elements + scan2.array_num_elements;
	result_datums = palloc(total_elems * sizeof(Datum));
	result_isnulls = palloc(total_elems * sizeof(bool));
	result_tids = palloc(total_elems * sizeof(zstid));

	remain_elems = 0;
	for (;;)
	{
		ZSAttrTreeScan *scannext;
		zstid		tid;
		Datum		datum;
		bool		isnull;

		if (scan1.array_curr_idx < scan1.array_num_elements &&
			scan2.array_curr_idx < scan2.array_num_elements)
		{
			if (scan1.array_tids[scan1.array_curr_idx] < scan2.array_tids[scan2.array_curr_idx])
				scannext = &scan1;
			else if (scan1.array_tids[scan1.array_curr_idx] > scan2.array_tids[scan2.array_curr_idx])
				scannext = &scan2;
			else
				elog(ERROR, "attstream with duplicate TIDs");
		}
		else if (scan1.array_curr_idx < scan1.array_num_elements)
		{
			scannext = &scan1;
		}
		else if (scan2.array_curr_idx < scan2.array_num_elements)
		{
			scannext = &scan2;
		}
		else
		{
			break;	/* all done */
		}

		tid = scannext->array_tids[scannext->array_curr_idx];
		datum = scannext->array_datums[scannext->array_curr_idx];
		isnull = scannext->array_isnulls[scannext->array_curr_idx];
		scannext->array_curr_idx++;

		/* also "merge" in the list of tids to remove */
		while (removeidx < num_tids_to_remove && tid > tids_to_remove[removeidx])
			removeidx++;
		if (removeidx < num_tids_to_remove && tid == tids_to_remove[removeidx])
		{
			/*
			 * This datum needs to be removd. Leave it out from the result.
			 *
			 * If it's a toasted datum, also remove the toast blocks.
			 */
			if (attr->attlen == -1 && !isnull &&
				VARATT_IS_EXTERNAL(datum) && VARTAG_EXTERNAL(datum) == VARTAG_ZEDSTORE)
			{
				varatt_zs_toastptr *toastptr = (varatt_zs_toastptr *) DatumGetPointer(datum);
				BlockNumber toast_blkno = toastptr->zst_block;

				zedstore_toast_delete(rel, attr, tid, toast_blkno);
			}
		}
		else
		{
			result_tids[remain_elems] = tid;
			result_datums[remain_elems] = datum;
			result_isnulls[remain_elems] = isnull;
			remain_elems++;
		}
	}

	if (remain_elems != 0)
	{
		for (int i = 1; i < remain_elems; i++)
			Assert(result_tids[i] > result_tids[i-1]);

		result = create_attstream(attr, remain_elems, result_tids, result_datums, result_isnulls);
	}
	else
		result = NULL;

	pfree(result_datums);
	pfree(result_isnulls);
	pfree(result_tids);
	if (scan1.array_datums)
		pfree(scan1.array_datums);
	if (scan1.array_isnulls)
		pfree(scan1.array_isnulls);
	if (scan1.array_tids)
		pfree(scan1.array_tids);
	if (scan1.array_cxt)
		MemoryContextDelete(scan1.array_cxt);
	if (scan2.array_datums)
		pfree(scan2.array_datums);
	if (scan2.array_isnulls)
		pfree(scan2.array_isnulls);
	if (scan2.array_tids)
		pfree(scan2.array_tids);
	if (scan2.array_cxt)
		MemoryContextDelete(scan2.array_cxt);

	return result;
}

/*
 * Append 'newstream' to 'oldstream' in place, modifying 'oldstream'.
 *
 * There is assumed to be 'freespace' bytes after 'oldstream', where we can
 * write the new data.
 *
 * If the new data doesn't fit in the available space, does nothing and
 * returns false.
 *
 * NB: This is used within a critical section, so keep it simple. No ereport
 * or pallocs!
 */
bool
append_attstreams_inplace(Form_pg_attribute att, ZSAttStream *oldstream, int freespace, ZSAttStream *newstream)
{
	zstid		firstnewtid;
	char		*pos_new;
	zstid		delta;

	/*
	 * fast path requirements:
	 *
	 * - the new stream goes after the old one
	 * - there is enough space to append newstream
	 * - neither stream is compressed
	 */
	if (oldstream->t_flags & ATTSTREAM_COMPRESSED)
		return false;
	if (newstream->t_flags & ATTSTREAM_COMPRESSED)
		return false;

	if (freespace < newstream->t_size - SizeOfZSAttStreamHeader)
		return false;	/* no space */

	firstnewtid = get_chunk_first_tid(att->attlen, newstream->t_payload);
	if (firstnewtid <= oldstream->t_lasttid)
	{
		/* overlap */
		return false;
	}

	/*
	 * We can do it!
	 *
	 * The trivial way is to just append the new stream to the new stream,
	 * adjusting the first TID at the seam, so that it's a delta from the last
	 * old tid.
	 *
	 * TODO A better way: try to re-code the last old item, and first new item
	 * together. For example, if new data is added one row at a time, we currently
	 * generate a stream of single-datum chunks, with the 8-byte codeword for
	 * every datum. It would be better to combine the chunks at the seam, using
	 * more compact codewords. But if you implement that, make sure the callers
	 * are happy with that! At the moment, the caller WAL-logs the change, and
	 * doesn't expect us to change the existing data.
	 */
	pos_new = ((char *) oldstream) + oldstream->t_size;
	memcpy(pos_new,
		   newstream->t_payload,
		   newstream->t_size - SizeOfZSAttStreamHeader);

	delta = firstnewtid - oldstream->t_lasttid;
	replace_first_tid(att->attlen, delta, pos_new);
	oldstream->t_size += newstream->t_size - SizeOfZSAttStreamHeader;
	oldstream->t_lasttid = newstream->t_lasttid;

	return true;
}

/* ----------------------------------------------------------------------------
 * Functions work with individual chunks in an attstream.
 * ----------------------------------------------------------------------------
 */

/*
 * FIXED-LENGTH CODEWORD MODES
 * ---------------------------
 *
 * These modes are used with fixed-length attributes (attlen > 0). Each codeword
 * includes a 4-bit mode selector, and between 1-60 TIDs, and in some modes, a NULL
 * bitmap. To avoid creating too large chunks, which might not fit conveniently on
 * a page, we avoid using the most dense modes when the resulting chunk would exceed
 * TARGET_CHUNK_SIZE.
 *
 * Glossary:
 *
 *  x       Bit positions representing TIDs (or rather, deltas between TIDs.
 *  0..9    In the lower modes that encode a lot of TIDs, the boundaries between TIDs
 *          are not shown and 'x' is used to represent all of them. In higher modes,
 *          the numbers are used to indicate which bit position encodes which TID.)
 *
 *  N       Bit positions used for a NULL bitmap
 *
 *  w       unused, wasted, bits
 *
 * mode  0: 0000 xxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx
 *
 *          60 TIDs, 1 bit each
 *
 * mode  1: 0001 NNNN NNNNNNNN NNNNNNNN NNNNNNNN NNxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx
 *
 *          30 TIDs, 1 bit each
 *          30 NULL bits
 *
 * mode  2: 0010 xxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx
 *
 *          30 TIDs, 2 bits each
 *
 * mode  3: 0011 NNNN NNNNNNNN NNNNNNNN xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx
 *
 *          20 TIDs, 2 bits each
 *          20 NULL bits
 *
 * mode  4: 0100 xxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx
 *
 *          20 TIDs, 3 bits each
 *
 * mode  5: 0101 NNNN NNNNNNNN NNNxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx
 *
 *          15 TIDs, 3 bits each
 *          15 NULL bits
 *
 * mode  6: 0110 xxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx
 *
 *          15 TIDs, 4 bits each
 *
 * mode  7: 0111 NNNN NNNNNNNN xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx
 *
 *          12 TIDs, 4 bits each
 *          12 NULL bits
 *
 * mode  8: 1000 xxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx
 *
 *          12 TIDs, 5 bits each
 *
 * mode  9: 1001 NNNN NNNNNNxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx
 *
 *          10 TIDs, 5 bits each
 *          10 NULL bits
 *
 * mode 10: 1010 wwww NNNNNNNN 88888877 77776666 66555555 44444433 33332222 22111111
 *
 *          8 TIDs, 6 bits each
 *          8 NULL bits
 *          (four bits are wasted)
 *
 * mode 11: 1011 NNNN NN666666 66655555 55554444 44444333 33333322 22222221 11111111
 *
 *          6 TIDs, 9 bits each
 *          6 NULL bits
 *
 * mode 12: 1100 NNNN 44444444 44444433 33333333 33332222 22222222 22111111 11111111
 *          4 TIDs, 14 bits each
 *          4 NULL bits
 *
 * mode 13: 1101 NNN3 33333333 33333333 33222222 22222222 22222111 11111111 11111111
 *
 *          three TIDs, 19 bits each
 *          3 NULL bits
 *
 * mode 14: 1110 NN22 22222222 22222222 22222222 22211111 11111111 11111111 11111111
 *
 *          two TIDs, 29 bits each
 *          two NULL bits
 *
 * mode 15: 1111 0000 Nxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx
 *
 *          one TID, 59 bits
 *          NULL bit
 *
 * XXX: we store the first TID in the low bits, and subsequent TIDs in higher bits. Not
 * sure if that's how it's usually done...
 *
 * XXX: We could use delta 0 to mark unused slots. That way, we wouldn't need to shift
 * to a higher mode when we're running out of TIDs to encode. Or we could subtract one
 * from each distance, so that value 0 didn't go wasted, and we could sometimes use
 * more compact modes.
 */
static const struct codeword_mode
{
	uint8		bits_per_int;
	uint8		num_ints;
	bool		nullbitmap;
} fixed_width_modes[17] =
{
	{1, 60, false},				/* mode  0 */
	{1, 30, true},				/* mode  1 */
	{2, 30, false},				/* mode  2 */
	{2, 20, true},				/* mode  3 */
	{3, 20, false},				/* mode  4 */
	{3, 15, true},				/* mode  5 */
	{4, 15, false},				/* mode  6 */
	{4, 12, true},				/* mode  7 */
	{5, 12, false},				/* mode  8 */
	{5, 10, true},				/* mode  9 */
	{6,  8, true},				/* mode 10 */
	{9,  6, true},				/* mode 11 */
	{14, 4, true},				/* mode 12 */
	{19, 3, true},				/* mode 13 */
	{29, 2, true},				/* mode 14 */
	{55, 1, true},				/* mode 15 */
	{0, 0, false}				/* sentinel */
};

static int
get_chunk_length_fixed(int attlen, char *chunk)
{
	uint64		codeword;

	memcpy(&codeword, chunk, sizeof(uint64));

	{
		int			selector = (codeword >> 60);
		int			nints = fixed_width_modes[selector].num_ints;
		int			bits = fixed_width_modes[selector].bits_per_int;
		bool		has_nulls = fixed_width_modes[selector].nullbitmap;
		int			num_nulls;

		/* skip over the TIDs */
		codeword >>= bits * nints;

		num_nulls = 0;
		if (has_nulls)
		{
			/* count set bits in the NULL bitmap */
			for (int i = 0; i < nints; i++)
			{
				 if (codeword & 1)
					 num_nulls++;
				 codeword >>= 1;
			 }
		 }
		return sizeof(uint64) + (nints - num_nulls) * attlen;
	}
}

static zstid
get_chunk_first_tid_fixed(int attlen, char *chunk)
{
	uint64		codeword;

	memcpy(&codeword, chunk, sizeof(uint64));

	{
		int			selector = (codeword >> 60);
		int			bits = fixed_width_modes[selector].bits_per_int;
		uint64		mask = (UINT64CONST(1) << bits) - 1;

		/* get first tid */
		return (codeword & mask);
	}
}

static bool
replace_first_tid_fixed(int attlen, zstid newtid, char *chunk)
{
	uint64		codeword;

	memcpy(&codeword, chunk, sizeof(uint64));

	{
		int			selector = (codeword >> 60);
		int			bits = fixed_width_modes[selector].bits_per_int;
		uint64		mask = (UINT64CONST(1) << bits) - 1;

		/* get first tid */
		if (newtid >= (1 << bits))
			return false;

		/* zero out the TID */
		codeword &= ~mask;
		codeword |= newtid;

		memcpy(chunk, &codeword, sizeof(uint64));

		return true;
	}
}

static int
skip_chunk_fixed(int attlen, char *chunk, zstid *lasttid)
{
	zstid		prevtid = *lasttid;
	char	   *p = chunk;
	uint64		codeword;

	memcpy(&codeword, p, sizeof(uint64));
	p += sizeof(uint64);

	{
		int			selector = (codeword >> 60);
		int			nints = fixed_width_modes[selector].num_ints;
		int			bits = fixed_width_modes[selector].bits_per_int;
		bool		has_nulls = fixed_width_modes[selector].nullbitmap;
		uint64		mask = (UINT64CONST(1) << bits) - 1;
		int			num_nulls;
		zstid		tid = prevtid;

		for (int i = 0; i < nints; i++)
		{
			uint64		val = codeword & mask;

			tid += val;
			codeword >>= bits;
		}

		num_nulls = 0;
		if (has_nulls)
		{
			/* count set bits in the NULL bitmap */
			for (int i = 0; i < nints; i++)
			{
				if (codeword & 1)
					num_nulls++;
				codeword >>= 1;
			}
		 }

		/* ignore the datums */
		*lasttid = tid;
		return sizeof(uint64) + (nints - num_nulls) * attlen;
	 }
}

static int
decode_chunk_fixed(bool attbyval, int attlen, zstid *lasttid, char *chunk,
				   int *num_elems, zstid *tids, Datum *datums, bool *isnulls)
{
	char	   *p = chunk;
	uint64		codeword;

	memcpy(&codeword, p, sizeof(uint64));
	p += sizeof(uint64);

	{
		int			selector = (codeword >> 60);
		int			bits = fixed_width_modes[selector].bits_per_int;
		bool		has_nulls = fixed_width_modes[selector].nullbitmap;
		int			nints = fixed_width_modes[selector].num_ints;
		uint64		mask = (UINT64CONST(1) << bits) - 1;
		zstid		tid = *lasttid;
		uint64		nullbitmap;

		for (int i = 0; i < nints; i++)
		{
			uint64		val = codeword & mask;

			tid = tid + val;
			tids[i] = tid;
			codeword >>= bits;
		}
		*lasttid = tid;

		if (has_nulls)
			nullbitmap = codeword & UINT64CONST(0x0FFFFFFFFFFFFF);
		else
			nullbitmap = 0;

		/* datums follow */
		if (attbyval)
		{
			if (nullbitmap == 0)
			{
				/* FIXME: the loops below ignore alignment. 'p' might not be aligned */
				if (attlen == sizeof(Datum))
				{
					for (int i = 0; i < nints; i++)
					{
						datums[i] = *((Datum *) p);
						isnulls[i] = false;
						p += sizeof(Datum);
					}
				}
				else if (attlen == sizeof(int32))
				{
					for (int i = 0; i < nints; i++)
					{
						datums[i] = Int32GetDatum(*(int32 *) p);
						isnulls[i] = false;
						p += sizeof(int32);
					}
				}
				else if (attlen == sizeof(int16))
				{
					for (int i = 0; i < nints; i++)
					{
						datums[i] = DatumGetInt16(*(int16 *) p);
						isnulls[i] = false;
						p += sizeof(int16);
					}
				}
				else if (attlen == sizeof(char))
				{
					for (int i = 0; i < nints; i++)
					{
						datums[i] = CharGetDatum(*p);
						isnulls[i] = false;
						p++;
					}
				}
				else
					elog(ERROR, "unsupported byval length: %d", attlen);
			}
			else
			{
				/* FIXME: the loops below ignore alignment. 'p' might not be aligned */
				if (attlen == sizeof(Datum))
				{
					for (int i = 0; i < nints; i++)
					{
						if ((nullbitmap & (1 << i)) == 0)
						{
							datums[i] = *((Datum *) p);
							isnulls[i] = false;
							p += sizeof(Datum);
						}
						else
						{
							datums[i] = (Datum) 0;
							isnulls[i] = true;
						}
					}
				}
				else if (attlen == sizeof(int32))
				{
					for (int i = 0; i < nints; i++)
					{
						if ((nullbitmap & (1 << i)) == 0)
						{
							datums[i] = Int32GetDatum(*(int32 *) p);
							isnulls[i] = false;
							p += sizeof(int32);
						}
						else
						{
							datums[i] = (Datum) 0;
							isnulls[i] = true;
						}
					}
				}
				else if (attlen == sizeof(int16))
				{
					for (int i = 0; i < nints; i++)
					{
						if ((nullbitmap & (1 << i)) == 0)
						{
							datums[i] = DatumGetInt16(*(int16 *) p);
							isnulls[i] = false;
							p += sizeof(int16);
						}
						else
						{
							datums[i] = (Datum) 0;
							isnulls[i] = true;
						}
					}
				}
				else if (attlen == sizeof(char))
				{
					for (int i = 0; i < nints; i++)
					{
						if ((nullbitmap & (1 << i)) == 0)
						{
							datums[i] = CharGetDatum(*p);
							isnulls[i] = false;
							p++;
						}
						else
						{
							datums[i] = (Datum) 0;
							isnulls[i] = true;
						}
					}
				}
				else
					elog(ERROR, "unsupported byval length: %d", attlen);
			}
		}
		else
		{
			char	   *datumbuf = palloc(MAXALIGN(attlen) * nints); /* XXX: attalign */
			char	   *datump = datumbuf;

			for (int i = 0; i < nints; i++)
			{
				if ((nullbitmap & (1 << i)) == 0)
				{
					memcpy(datump, p, attlen);
					datums[i] = PointerGetDatum(datump);
					isnulls[i] = false;
					p += attlen;
					datump += MAXALIGN(attlen);
				}
				else
				{
					datums[i] = (Datum) 0;
					isnulls[i] = true;
				}
			}
		}

		*num_elems = nints;
		return p - chunk;
	}
}

static int
encode_chunk_fixed(bool attbyval, int attlen, zstid prevtid, int ntids,
				   zstid *tids, Datum *datums, bool *isnulls,
				   StringInfo dst)
{
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
	int			selector;
	int			this_nints;
	int			this_bits;
	bool		this_supports_nulls;
	uint64		val;
	int			i;
	bool		has_nulls;
	int			size = sizeof(uint64);
	uint64		codeword;
	uint64		deltas[60];
	char		*p;

	selector = 0;
	this_nints = fixed_width_modes[0].num_ints;
	this_bits = fixed_width_modes[0].bits_per_int;
	this_supports_nulls = fixed_width_modes[0].nullbitmap;

	val = tids[0] - prevtid;
	has_nulls = isnulls[0];
	i = 0;
	for (;;)
	{
		if (val >= (UINT64CONST(1) << this_bits) ||
			(has_nulls && !this_supports_nulls))
		{
			/* Too large, or need NULL bitmap. Step up to next mode */
			selector++;
			this_nints = fixed_width_modes[selector].num_ints;
			this_bits = fixed_width_modes[selector].bits_per_int;
			this_supports_nulls = fixed_width_modes[selector].nullbitmap;

			/* we might already have accepted enough deltas for this mode */
			if (i >= this_nints)
				break;
		}
		else
		{
			/* accept this delta; then done if codeword is full */
			deltas[i] = val;
			if (!isnulls[i])
				size += attlen;
			i++;
			if (i >= this_nints)
				break;
			/* examine next delta */
			has_nulls |= isnulls[i];
			if (i < ntids && size + attlen <= TARGET_CHUNK_SIZE)
			{
				val = tids[i] - tids[i - 1];
			}
			else
			{
				/*
				 * Reached end of input. Pretend that the next integer is a
				 * value that's too large to represent in Simple-8b, so that
				 * we fall out.
				 */
				val = PG_UINT64_MAX;
			}
		}
	}

	Assert(i > 0);

	/*
	 * Encode the integers using the selected mode.
	 */
	codeword = 0;
	if (has_nulls)
	{
		for (int i = 0; i < this_nints; i++)
			codeword |= isnulls[i] ? (1 << i) : 0;
		codeword <<= this_nints * this_bits;
	}
	for (int i = 0; i < this_nints; i++)
		codeword |= deltas[i] << (i * this_bits);

	/* add selector to the codeword, and return */
	codeword |= (uint64) selector << 60;

	/*
	 * Note: 'size' is too large at this point, if we had to "back down" to a
	 * less dense mode. That's fine for sizing the destination buffer, but we
	 * can't rely on it for the final size of the chunk.
	 */
	enlargeStringInfo(dst, size);
	appendBinaryStringInfo(dst, (char *) &codeword, sizeof(uint64));

	/*
	 * Now, the data
	 */

	/* FIXME: the loops below ignore alignment. 'p' might not be aligned */
	p = &dst->data[dst->len];
	if (attbyval)
	{
		if (attlen == sizeof(Datum))
		{
			for (int i = 0; i < this_nints; i++)
			{
				if (!isnulls[i])
				{
					*((Datum *) p) = datums[i];
					p += sizeof(Datum);
				}
			}
		}
		else if (attlen == sizeof(int32))
		{
			for (int i = 0; i < this_nints; i++)
			{
				if (!isnulls[i])
				{
					*((int32 *) p) = DatumGetInt32(datums[i]);
					p += sizeof(int32);
				}
			}
		}
		else if (attlen == sizeof(int16))
		{
			for (int i = 0; i < this_nints; i++)
			{
				if (!isnulls[i])
				{
					*((int16 *) p) = DatumGetInt16(datums[i]);
					p += sizeof(int16);
				}
			}
		}
		else if (attlen == sizeof(char))
		{
			for (int i = 0; i < this_nints; i++)
			{
				if (!isnulls[i])
					*(p++) = DatumGetChar(datums[i]);
			}
		}
		else
			elog(ERROR, "unsupported byval length: %d", attlen);
	}
	else
	{
		for (int i = 0; i < this_nints; i++)
		{
			if (!isnulls[i])
			{
				memcpy(p, DatumGetPointer(datums[i]), attlen);
				p += attlen;
			}
		}
	}
	dst->len = p - dst->data;
	Assert(dst->len < dst->maxlen);

	return this_nints;
}

/*
 * VARIABLE-SIZE MODES
 * -------------------
 *
 * These are used with varlenas. With varlenas, we encode not only the
 * TIDs and NULLness of each datum, but also its length, in the same
 * codeword. The value 0 stored in the length field is used to represent
 * a NULL; there is no separate NULL bitmap. For non-NULLs, the value
 * stored in the length is the real length + 1.
 *
 * We don't track a maximum size for the chunk during encoding, but the
 * fact that we use a smallish number of bits to store the length, depending
 * on the mode, puts a cap on the max chunk size. For example, in mode 4,
 * we encode 10 TIDs in a codeword with 4 bits to store the length. With four
 * bits, each datum can be max 14 bytes long. That limits the max size of a
 * chunk in mode 4 to 4*14 = 140 bytes. Below table shows the modes with the
 * number of bits use for the TID and length of each datum, and the maximum
 * chunk size they give (not including the size of the codeword itself)
 *
 * mode    tidbits lenbits wasted  ntids   maxsize
 * 0       1       1       0       30      30
 * 1       1       2       0       20      40
 * 2       1       3       0       15      90
 * 3       2       3       0       12      72
 * 4       2       4       0       10      140
 * 5       3       4       4       8       112
 * 6       4       4       4       7       98
 * 7       5       5       0       6       180
 * 8       6       6       0       5       310
 * 9       8       7       0       4       504
 * 10      13      7       0       3       378
 * 11      23      7       0       2       252
 * 12      45      15      0       1       32766
 * 13 unused
 * 14 toast pointer
 * 15 unused
 *
 * Modes 13 and 15 are currently unused. (The idea is that 15 could be
 * used for various extended modes with special handling, using more
 * bits to indicate which extended mode it is. And it seems logical to
 * have special modes, like the toast pointer, at the end. We could use
 * 13 for another "regular" mode.. )
 *
 * Mode 14 is special: It is used to encode a toast pointer. The TID of
 * the datum is stored in the codeword as is, and after the codeword
 * comes the block number of the first toast block, as a 32-bit integer.
 *
 * FIXME: Mode 12 is the widest mode, but it only uses up to 45 bits for
 * the TID. That's not enough to cover the whole range of valid zstids.
 * I think we need one more special mode, where we use full 60 bits for
 * the TID, with the length stored separately after the codeword, for
 * the odd case that you have a very large datum with a very high TID.
 */
static const struct
{
	uint8		bits_per_tid;
	uint8		lenbits;
	uint8		num_ints;
} varlen_modes[17] =
{
	{ 1,   1,   30 },	/* mode 0 */
	{ 1,   2,   20 },	/* mode 1 */
	{ 1,   3,   15 },	/* mode 2 */
	{ 2,   3,   12 },	/* mode 3 */
	{ 2,   4,   10 },	/* mode 4 */
	{ 3,   4,   8  },	/* mode 5 */
	{ 4,   4,   7  },	/* mode 6 */
	{ 5,   5,   6  },	/* mode 7 */
	{ 6,   6,   5  },	/* mode 8 */
	{ 8,   7,   4  },	/* mode 9 */
	{ 13,  7,   3  },	/* mode 10 */
	{ 23,  7,   2  },	/* mode 11 */
	{ 45,  15,  1  },	/* mode 12 */

	/* special modes */
	{ 0, 0, 0 },	/* mode 13 (unused) */
	{ 48, 0, 1 },	/* mode 14: toast pointer) */
	{ 0, 0, 0 },	/* mode 15 */

	{ 0, 0, 0 }		/* sentinel */
};

static int
get_chunk_length_varlen(char *chunk)
{
	uint64		codeword;

	memcpy(&codeword, chunk, sizeof(uint64));

	{
		int			selector = (codeword >> 60);
		int			nints = varlen_modes[selector].num_ints;
		int			tidbits = varlen_modes[selector].bits_per_tid;
		int			lenbits = varlen_modes[selector].lenbits;
		uint64		lenmask = (UINT64CONST(1) << lenbits) - 1;
		int			total_len;

		if (selector == 14)
		{
			/* toast pointer */
			return sizeof(uint64) + sizeof(BlockNumber);
		}

		/* skip over the TIDs */
		codeword >>= tidbits * nints;

		/* Sum up the lengths */
		total_len = 0;
		for (int i = 0; i < nints; i++)
		{
			uint64		len = codeword & lenmask;

			if (len > 0)
				total_len += len - 1;
			codeword >>= lenbits;
		}
		return sizeof(uint64) + total_len;
	}
}

static zstid
get_chunk_first_tid_varlen(char *chunk)
{
	uint64		codeword;

	memcpy(&codeword, chunk, sizeof(uint64));

	{
		int			selector = (codeword >> 60);
		int			bits = varlen_modes[selector].bits_per_tid;
		uint64		mask = (UINT64CONST(1) << bits) - 1;

		/* get first tid */
		return (codeword & mask);
	}
}

static bool
replace_first_tid_varlen(zstid newtid, char *chunk)
{
	uint64		codeword;

	memcpy(&codeword, chunk, sizeof(uint64));

	{
		int			selector = (codeword >> 60);
		int			bits = varlen_modes[selector].bits_per_tid;
		uint64		mask = (UINT64CONST(1) << bits) - 1;

		if (newtid >= (1 << bits))
			return false;

		/* zero out the TID */
		codeword &= ~mask;
		codeword |= newtid;

		memcpy(chunk, &codeword, sizeof(uint64));

		return true;
	}
}

static int
skip_chunk_varlen(char *chunk, zstid *lasttid)
{
	zstid		prevtid = *lasttid;
	char	   *p = chunk;
	uint64		codeword;

	memcpy(&codeword, p, sizeof(uint64));
	p += sizeof(uint64);

	{
		int			selector = (codeword >> 60);
		int			nints = varlen_modes[selector].num_ints;
		int			tidbits = varlen_modes[selector].bits_per_tid;
		int			lenbits = varlen_modes[selector].lenbits;
		uint64		mask = (UINT64CONST(1) << tidbits) - 1;
		uint64		lenmask = (UINT64CONST(1) << lenbits) - 1;
		int			total_len;
		zstid		tid = prevtid;

		if (selector == 14)
		{
			/* toast pointer */
			*lasttid = tid + (codeword & mask);
			return sizeof(uint64) + sizeof(BlockNumber);
		}

		for (int i = 0; i < nints; i++)
		{
			uint64		val = codeword & mask;

			tid += val;
			codeword >>= tidbits;
		}

		/* Sum up the lengths */
		total_len = 0;
		for (int i = 0; i < nints; i++)
		{
			uint64		len = codeword & lenmask;

			if (len > 0)
				total_len += len - 1;
			codeword >>= lenbits;
		}

		/* ignore the datums */
		*lasttid = tid;
		return sizeof(uint64) + total_len;
	 }
}

static int
decode_chunk_varlen(zstid *lasttid, char *chunk,
					int *num_elems, zstid *tids, Datum *datums, bool *isnulls)
{
	char	   *p = chunk;
	uint64		codeword;

	memcpy(&codeword, p, sizeof(uint64));
	p += sizeof(uint64);

	{
		int			selector = (codeword >> 60);
		int			nints = varlen_modes[selector].num_ints;
		int			tidbits = varlen_modes[selector].bits_per_tid;
		int			lenbits = varlen_modes[selector].lenbits;
		uint64		tidmask = (UINT64CONST(1) << tidbits) - 1;
		uint64		lenmask = (UINT64CONST(1) << lenbits) - 1;
		zstid		tid = *lasttid;
		char	   *datump;

		if (selector == 14)
		{
			/* toast pointer */
			BlockNumber toastblkno;
			varatt_zs_toastptr *toastptr;

			tid += (codeword & tidmask);

			memcpy(&toastblkno, p, sizeof(BlockNumber));
			p += sizeof(BlockNumber);

			toastptr = palloc0(sizeof(varatt_zs_toastptr));
			SET_VARTAG_1B_E(toastptr, VARTAG_ZEDSTORE);
			toastptr->zst_block = toastblkno;

			tids[0] = tid;
			datums[0] = PointerGetDatum(toastptr);
			isnulls[0] = false;
			*num_elems = 1;

			*lasttid = tid;
			return p - chunk;
		}

		for (int i = 0; i < nints; i++)
		{
			uint64		val = codeword & tidmask;

			tid = tid + val;
			tids[i] = tid;
			codeword >>= tidbits;
		}
		*lasttid = tid;

		/* Decode the datums / isnulls */
		datump = palloc(MAXALIGN(VARHDRSZ + ((1 << lenbits))) * nints);

		for (int i = 0; i < nints; i++)
		{
			uint64		len = codeword & lenmask;

			if (len == 0)
			{
				datums[i] = (Datum) 0;
				isnulls[i] = true;
			}
			else
			{
				memcpy(VARDATA(datump), p, len - 1);
				SET_VARSIZE(datump, len - 1 + VARHDRSZ);

				datums[i] = PointerGetDatum(datump);
				isnulls[i] = false;

				datump += MAXALIGN(VARHDRSZ + len - 1);
				p += (len - 1);
			}
			codeword >>= lenbits;
		}

		*num_elems = nints;
		return p - chunk;
	}
}

static int
encode_chunk_varlen(zstid prevtid, int ntids,
					zstid *tids, Datum *datums, bool *isnulls,
					StringInfo dst)
{
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
	int			selector;
	int			this_nints;
	int			this_tidbits;
	int			this_lenbits;
	uint64		val;
	int			len;
	int			i;
	uint64		codeword;
	uint64		deltas[60];

	/* special case for toast pointers */
	if (!isnulls[0] && VARATT_IS_EXTERNAL(datums[0]) && VARTAG_EXTERNAL(datums[0]) == VARTAG_ZEDSTORE)
	{
		varatt_zs_toastptr *toastptr = (varatt_zs_toastptr *) DatumGetPointer(datums[0]);
		BlockNumber toastblkno = toastptr->zst_block;

		codeword = UINT64CONST(14) << 60 | (tids[0] - prevtid);
		appendBinaryStringInfo(dst, (char *) &codeword, sizeof(uint64));
		appendBinaryStringInfo(dst, (char *) &toastblkno, sizeof(BlockNumber));
		return 1;
	}

	selector = 0;
	this_nints = varlen_modes[0].num_ints;
	this_tidbits = varlen_modes[0].bits_per_tid;
	this_lenbits = varlen_modes[0].lenbits;

	val = tids[0] - prevtid;
	if (isnulls[0])
		len = 0;
	else
		len = VARSIZE_ANY_EXHDR(datums[0]) + 1;
	i = 0;
	for (;;)
	{
		if (val >= (UINT64CONST(1) << this_tidbits) ||
			len >= (UINT64CONST(1) << this_lenbits))
		{
			/* Too large TID distance, or length. Step up to next mode */
			selector++;
			this_nints = varlen_modes[selector].num_ints;
			this_tidbits = varlen_modes[selector].bits_per_tid;
			this_lenbits = varlen_modes[selector].lenbits;

			/* we might already have accepted enough deltas for this mode */
			if (i >= this_nints)
				break;
		}
		else
		{
			/* accept this delta; then done if codeword is full */
			deltas[i] = val;
			i++;

			if (i >= this_nints)
				break;

			/* examine next delta and length */
			if (i < ntids)
			{
				val = tids[i] - tids[i - 1];
				if (isnulls[i])
					len = 0;
				else
				{
					if (VARATT_IS_EXTERNAL(datums[i]) && VARTAG_EXTERNAL(datums[i]) == VARTAG_ZEDSTORE)
					{
						/* toast pointer, bail out */
						val = PG_UINT64_MAX;
						len = PG_INT32_MAX;
					}
					else
						len = VARSIZE_ANY_EXHDR(datums[i]) + 1;
				}
			}
			else
			{
				/*
				 * Reached end of input. Pretend that the next integer is a
				 * value that's too large to represent in Simple-8b, so that
				 * we fall out.
				 */
				val = PG_UINT64_MAX;
				len = PG_INT32_MAX;
			}
		}
	}

	Assert(i > 0);

	/*
	 * Encode the length and TID deltas using the selected mode.
	 */
	codeword = 0;
	for (int i = 0; i < this_nints; i++)
	{
		int			len;

		if (isnulls[i])
			len = 0;
		else
			len = VARSIZE_ANY_EXHDR(datums[i]) + 1;
		codeword |= (uint64) len << (i * this_lenbits);
	}
	codeword <<= this_nints * this_tidbits;

	for (int i = 0; i < this_nints; i++)
		codeword |= deltas[i] << (i * this_tidbits);

	/* add selector to the codeword, and return */
	codeword |= (uint64) selector << 60;

	appendBinaryStringInfo(dst, (char *) &codeword, sizeof(uint64));

	/*
	 * Now, the data
	 */
	{
		char		*p;

		enlargeStringInfo(dst, (1 << this_lenbits) * this_nints );
		p = &dst->data[dst->len];

		for (int i = 0; i < this_nints; i++)
		{
			if (!isnulls[i])
			{
				int			len = VARSIZE_ANY_EXHDR(datums[i]);

				memcpy(p, VARDATA_ANY(datums[i]), len);
				p += len;
			}
		}

		Assert(p - dst->data  < dst->maxlen);

		dst->len = p - dst->data;
	}

	return this_nints;
}




/*
 * Wrapper functions over the fixed-length and varlen variants.
 */

static bool
replace_first_tid(int attlen, zstid newtid, char *chunk)
{
	if (attlen > 0)
		return replace_first_tid_fixed(attlen, newtid, chunk);
	else
		return replace_first_tid_varlen(newtid, chunk);
}

static int
skip_chunk(int attlen, char *chunk, zstid *lasttid)
{
	if (attlen > 0)
		return skip_chunk_fixed(attlen, chunk, lasttid);
	else
		return skip_chunk_varlen(chunk, lasttid);
}

static int
get_chunk_length(int attlen, char *chunk)
{
	if (attlen > 0)
		return get_chunk_length_fixed(attlen, chunk);
	else
		return get_chunk_length_varlen(chunk);
}

static zstid
get_chunk_first_tid(int attlen, char *chunk)
{
	if (attlen > 0)
		return get_chunk_first_tid_fixed(attlen, chunk);
	else
		return get_chunk_first_tid_varlen(chunk);
}

static int
decode_chunk(bool attbyval, int attlen, zstid *lasttid, char *chunk,
			 int *num_elems, zstid *tids, Datum *datums, bool *isnulls)
{
	if (attlen > 0)
		return decode_chunk_fixed(attbyval, attlen, lasttid, chunk, num_elems,
								  tids, datums, isnulls);
	else
		return decode_chunk_varlen(lasttid, chunk, num_elems,
								   tids, datums, isnulls);
}

static int
encode_chunk(bool attbyval, int attlen, zstid prevtid, int ntids,
			 zstid *tids, Datum *datums, bool *isnulls,
			 StringInfo dst)
{
	if (attlen > 0)
		return encode_chunk_fixed(attbyval, attlen, prevtid, ntids,
								  tids, datums, isnulls, dst);
	else
		return encode_chunk_varlen(prevtid, ntids,
								   tids, datums, isnulls, dst);
}
