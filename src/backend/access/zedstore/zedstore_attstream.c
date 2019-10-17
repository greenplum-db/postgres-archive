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
 * Most of the functions in this file deal with 'attstream_buffers'. An
 * attstream_buffer is an in-memory representation of an attribute stream.
 * It is a resizeable buffer, without the ZSAttStream header, but enough
 * information in the attstream_buffer struct to construct the ZSAttStream
 * header when needed.
 *
 * Another in-memory representation is 'attstream_decoder'. It holds state
 * when reading an attribute stream.
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/zedstore/zedstore_attstream.c
 */
#include "postgres.h"

#include "access/detoast.h"
#include "access/toast_internals.h"
#include "access/zedstore_internal.h"
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
static int encode_chunk(attstream_buffer *dst, zstid prevtid, int ntids,
						zstid *tids, Datum *datums, bool *isnulls);

/* Other internal functions. */
static void merge_attstream_guts(Form_pg_attribute attr, attstream_buffer *buffer, char *chunks2, int chunks2len, zstid lasttid2);
#ifdef USE_ASSERT_CHECKING
static void verify_attstream(attstream_buffer *buffer);
#endif


/* ----------------------------------------------------------------------------
 * Decoder routines
 *
 * To read an attribute stream, initialize a "decoder" by calling
 * init_attstream_decoder(). Then call decode_attstream_begin()
 * to load the decoder with data. Read the data, one chunk at a time,
 * by calling decode_attstream_cont(), until it returns false. Each
 * call to decode_attstream_cont() fills the arrays in the decoder
 * struct with the TIDs, Datums and isnull-flags in current chunk.
 * ----------------------------------------------------------------------------
 */

/*
 * Initialize a decoder.
 */
void
init_attstream_decoder(attstream_decoder *decoder, bool attbyval, int16 attlen)
{
	decoder->cxt = CurrentMemoryContext;
	decoder->tmpcxt = NULL;		/* can be set by caller */

	decoder->attbyval = attbyval;
	decoder->attlen = attlen;

	decoder->chunks_buf = NULL;
	decoder->chunks_buf_size = 0;
	decoder->chunks_len = 0;
	decoder->lasttid = InvalidZSTid;

	decoder->pos = 0;
	decoder->prevtid = InvalidZSTid;

	decoder->num_elements = 0;
}

void
destroy_attstream_decoder(attstream_decoder *decoder)
{
	if (decoder->chunks_buf)
		pfree(decoder->chunks_buf);
	decoder->chunks_buf = NULL;
	decoder->chunks_buf_size = 0;
	decoder->chunks_len = 0;
	decoder->num_elements = 0;
}

/*
 * Begin reading an attribute stream.
 */
void
decode_attstream_begin(attstream_decoder *decoder, ZSAttStream *attstream)
{
	int			buf_size_needed;

	if ((attstream->t_flags & ATTSTREAM_COMPRESSED) != 0)
		buf_size_needed = attstream->t_decompressed_bufsize;
	else
		buf_size_needed = attstream->t_size - SizeOfZSAttStreamHeader;

	if (decoder->chunks_buf_size < buf_size_needed)
	{
		if (decoder->chunks_buf)
			pfree(decoder->chunks_buf);

		decoder->chunks_buf = MemoryContextAlloc(decoder->cxt, buf_size_needed);
		decoder->chunks_buf_size = buf_size_needed;
	}

	if ((attstream->t_flags & ATTSTREAM_COMPRESSED) != 0)
	{
		/* decompress */
		zs_decompress(attstream->t_payload, decoder->chunks_buf,
					  attstream->t_size - SizeOfZSAttStreamHeader,
					  attstream->t_decompressed_bufsize);
		decoder->chunks_len = attstream->t_decompressed_size;
	}
	else
	{
		memcpy(decoder->chunks_buf,
			   ((char *) attstream) + SizeOfZSAttStreamHeader,
			   attstream->t_size - SizeOfZSAttStreamHeader);
		decoder->chunks_len = attstream->t_size - SizeOfZSAttStreamHeader;
	}
	decoder->firsttid = get_chunk_first_tid(decoder->attlen, decoder->chunks_buf);
	decoder->lasttid = attstream->t_lasttid;

	decoder->pos = 0;
	decoder->prevtid = 0;

	decoder->num_elements = 0;
}

/*
 * internal routine like decode_attstream_begin(), for reading chunks without the
 * ZSAttStream header.
 */
static void
decode_chunks_begin(attstream_decoder *decoder, char *chunks, int chunkslen, zstid lasttid)
{
	if (decoder->chunks_buf_size < chunkslen)
	{
		if (decoder->chunks_buf)
			pfree(decoder->chunks_buf);

		decoder->chunks_buf = MemoryContextAlloc(decoder->cxt, chunkslen);
		decoder->chunks_buf_size = chunkslen;
	}

	/* XXX: do we really need to make a copy? */
	memcpy(decoder->chunks_buf, chunks, chunkslen);
	decoder->chunks_len = chunkslen;
	decoder->lasttid = lasttid;

	decoder->pos = 0;
	decoder->prevtid = 0;

	decoder->num_elements = 0;
}

/*
 * Decode the next chunk in an attribute steam.
 *
 * The TIDs, Datums and isnull flags in 'decoder' are filled in with
 * data from the next chunk. Returns true if there was more data,
 * false if the end of chunk was reached.
 *
 * TODO: avoid extracting elements we're not interested in, by passing
 * starttid/endtid. Or provide a separate "fast forward" function.
 */
bool
decode_attstream_cont(attstream_decoder *decoder)
{
	zstid		lasttid;
	int			total_decoded;
	char	   *p;
	char	   *pend;
	MemoryContext oldcxt;

	oldcxt = CurrentMemoryContext;
	if (decoder->tmpcxt)
	{
		MemoryContextReset(decoder->tmpcxt);
		MemoryContextSwitchTo(decoder->tmpcxt);
	}

	p = decoder->chunks_buf + decoder->pos;
	pend = decoder->chunks_buf + decoder->chunks_len;

	total_decoded = 0;
	lasttid = decoder->prevtid;

	if (p >= pend)
		return false;

	while (p < pend && total_decoded + 60 < DECODER_MAX_ELEMS)
	{
		int			num_decoded;

		p += decode_chunk(decoder->attbyval, decoder->attlen, &lasttid, p,
						  &num_decoded,
						  &decoder->tids[total_decoded],
						  &decoder->datums[total_decoded],
						  &decoder->isnulls[total_decoded]);
		total_decoded += num_decoded;
	}

	MemoryContextSwitchTo(oldcxt);

	Assert(p <= pend);
	decoder->num_elements = total_decoded;
	decoder->pos = p - decoder->chunks_buf;
	if (total_decoded > 0)
	{
		decoder->prevtid = decoder->tids[total_decoded - 1];
		return true;
	}
	else
		return false;
}

/* ----------------------------------------------------------------------------
 * Functions for constructing and manipulating attribute streams.
 * ----------------------------------------------------------------------------
 */

static void
enlarge_attstream_buffer_slow(attstream_buffer *buf, int needed)
{
	/* copied from StringInfo */
	int			newlen;

	if (((Size) needed) >= (MaxAllocSize - (Size) buf->len))
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("out of memory"),
				 errdetail("Cannot enlarge attstream buffer containing %d bytes by %d more bytes.",
						   buf->len, needed)));

	needed += buf->len;		/* total space required now */

	/* Because of the above test, we now have needed <= MaxAllocSize */

	if (needed <= buf->maxlen)
		return;					/* got enough space already */

	/*
	 * We don't want to allocate just a little more space with each append;
	 * for efficiency, double the buffer size each time it overflows.
	 * Actually, we might need to more than double it if 'needed' is big...
	 */
	newlen = 2 * buf->maxlen;
	while (needed > newlen)
		newlen = 2 * newlen;

	/*
	 * Clamp to MaxAllocSize in case we went past it.  Note we are assuming
	 * here that MaxAllocSize <= INT_MAX/2, else the above loop could
	 * overflow.  We will still have newlen >= needed.
	 */
	if (newlen > (int) MaxAllocSize)
		newlen = (int) MaxAllocSize;

	buf->data = (char *) repalloc(buf->data, newlen);

	buf->maxlen = newlen;
}

static inline void
enlarge_attstream_buffer(attstream_buffer *buf, int needed)
{
	if (needed > buf->maxlen - buf->len)
		enlarge_attstream_buffer_slow(buf, needed);
}

/*
 * Pack given datums into an attstream.
 */
void
create_attstream(attstream_buffer *dst, bool attbyval, int16 attlen,
				 int nelems, zstid *tids, Datum *datums, bool *isnulls)
{
	int			num_encoded;
	int			elems_remain;
	zstid		prevtid;

	Assert(nelems > 0);

#define INIT_ATTREAM_BUF_SIZE 1024
	dst->data = palloc(INIT_ATTREAM_BUF_SIZE);
	dst->len = 0;
	dst->maxlen = INIT_ATTREAM_BUF_SIZE;
	dst->cursor = 0;
	dst->attlen = attlen;
	dst->attbyval = attbyval;

	dst->firsttid = tids[0];
	dst->lasttid = tids[nelems - 1];

	prevtid = 0;
	elems_remain = nelems;
	while (elems_remain > 0)
	{
		num_encoded = encode_chunk(dst, prevtid, elems_remain, tids, datums, isnulls);
		Assert(num_encoded > 0);
		prevtid = tids[num_encoded - 1];
		datums += num_encoded;
		isnulls += num_encoded;
		tids += num_encoded;
		elems_remain -= num_encoded;
	}
}

int
append_attstream(attstream_buffer *buf, bool all, int nelems,
				 zstid *tids, Datum *datums, bool *isnulls)
{
	int			num_encoded;
	int			elems_remain;
	zstid		prevtid;

	/* Can we avoid enlarging the buffer by moving the existing data? */
	if (buf->cursor > 128 * 1024 && buf->cursor > buf->len / 2)
	{
		memcpy(buf->data, buf->data + buf->cursor, buf->len - buf->cursor);
		buf->len -= buf->cursor;
		buf->cursor = 0;
	}

	Assert(nelems > 0);
	Assert(tids[0] > buf->lasttid);

	if (buf->len - buf->cursor == 0)
	{
		buf->firsttid = tids[0];
		prevtid = 0;
	}
	else
		prevtid = buf->lasttid;
	elems_remain = nelems;
	while (elems_remain > (all ? 0 : 59))
	{
		num_encoded = encode_chunk(buf, prevtid, elems_remain, tids, datums, isnulls);
		Assert(num_encoded > 0);
		prevtid = tids[num_encoded - 1];
		datums += num_encoded;
		isnulls += num_encoded;
		tids += num_encoded;
		elems_remain -= num_encoded;
	}

	buf->lasttid = prevtid;

	return nelems - elems_remain;
}

/*
 * Split 'chunk' at 'pos'. 'lasttid' is the TID of the item,
 * 'pos'
 *
 * The current chunk begins at chunks->cursor. The 'cursor' will
 * be moved to the new starting position.
 */
void
chop_attstream(attstream_buffer *buf, int pos, zstid lasttid)
{
	char	   *first_chunk;
	int			first_chunk_len;
	zstid		first_chunk_tids[60];
	Datum		first_chunk_datums[60];
	bool		first_chunk_isnulls[60];
	int			first_chunk_num_elems;
	zstid		xtid;
	attstream_buffer tmpbuf;
	zstid		newfirsttid;

	buf->cursor += pos;
	Assert(buf->cursor <= buf->len);
	if (buf->cursor >= buf->len)
	{
		Assert(buf->cursor == buf->len);
		return;
	}

	/* FIXME: arbitrary limit. We need some space before the split point, in
	 * case we need to re-encode the first new chunk. Compute this correctly,
	 * and perhaps reallocate a bigger buffer if needed. ATM, though, this is
	 * only used to chop large attstreams to page-sized parts, so this never
	 * gets called with a very small 'pos'.
	 */
	if (buf->cursor < 500)
		elog(ERROR, "cannot split");

	/*
	 * Try to modify the first codeword in place. It just might work out if
	 * we're lucky.
	 */
	first_chunk = buf->data + buf->cursor;

	newfirsttid = lasttid + get_chunk_first_tid(buf->attlen, first_chunk);
	if (!replace_first_tid(buf->attlen, newfirsttid, first_chunk))
	{

		/* Try to split the first chunk */
		xtid = lasttid;
		first_chunk_len = decode_chunk(buf->attbyval, buf->attlen, &xtid,
									   first_chunk,
									   &first_chunk_num_elems,
									   first_chunk_tids,
									   first_chunk_datums,
									   first_chunk_isnulls);

		/* re-encode the first chunk */
		create_attstream(&tmpbuf, buf->attbyval, buf->attlen,
						 first_chunk_num_elems,
						 first_chunk_tids,
						 first_chunk_datums,
						 first_chunk_isnulls);

		/* replace the chunk in the original stream with the new chunks */
		buf->cursor += first_chunk_len;
		if (buf->cursor < tmpbuf.len - tmpbuf.cursor)
			elog(ERROR, "not enough work space to split");
		buf->cursor -= (tmpbuf.len - tmpbuf.cursor);
		memcpy(&buf->data[buf->cursor],
			   tmpbuf.data + tmpbuf.cursor,
			   tmpbuf.len - tmpbuf.cursor);

		pfree(tmpbuf.data);

	}
	buf->firsttid = newfirsttid;
#ifdef USE_ASSERT_CHECKING
	verify_attstream(buf);
#endif
}

/*
 * Find the beginning offset of last chunk that fits in 'len'.
 *
 * Returns -1 if there are no full chunks. (FIXME: no it doesn't currently)
 */
int
find_attstream_chop_pos(Form_pg_attribute att, char *chunks, int len, zstid *lasttid)
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

void
init_attstream_buffer_from_stream(attstream_buffer *buf, bool attbyval, int16 attlen,
								  ZSAttStream *attstream, MemoryContext memcontext)
{
	int			buf_size;

	if ((attstream->t_flags & ATTSTREAM_COMPRESSED) != 0)
		buf_size = attstream->t_decompressed_bufsize;
	else
		buf_size = attstream->t_size - SizeOfZSAttStreamHeader;

	buf->data = MemoryContextAlloc(memcontext, buf_size);
	buf->len = 0;
	buf->maxlen = buf_size;
	buf->cursor = 0;

	buf->attlen = attlen;
	buf->attbyval = attbyval;

	if ((attstream->t_flags & ATTSTREAM_COMPRESSED) != 0)
	{
		zs_decompress(attstream->t_payload, buf->data,
					  attstream->t_size - SizeOfZSAttStreamHeader,
					  attstream->t_decompressed_bufsize);
		buf->len = attstream->t_decompressed_size;
	}
	else
	{
		memcpy(buf->data, attstream->t_payload, attstream->t_size - SizeOfZSAttStreamHeader);
		buf->len = attstream->t_size - SizeOfZSAttStreamHeader;
	}

	buf->firsttid = get_chunk_first_tid(buf->attlen, buf->data + buf->cursor);
	buf->lasttid = attstream->t_lasttid;
}

void
init_attstream_buffer(attstream_buffer *buf, bool attbyval, int16 attlen)
{
#define ATTBUF_INIT_SIZE 1024
	buf->data = palloc(ATTBUF_INIT_SIZE);
	buf->len = 0;
	buf->maxlen = ATTBUF_INIT_SIZE;
	buf->cursor = 0;

	buf->firsttid = 0;
	buf->lasttid = 0;

	buf->attlen = attlen;
	buf->attbyval = attbyval;
}

void
vacuum_attstream(Relation rel, AttrNumber attno, attstream_buffer *dst,
				 ZSAttStream *attstream,
				 zstid *tids_to_remove, int num_tids_to_remove)
{
	Form_pg_attribute attr = &rel->rd_att->attrs[attno - 1];
	attstream_decoder decoder;
	int			removeidx;
	zstid	   *tids;
	Datum	   *datums;
	bool	   *isnulls;
	int			num_buffered;
	int			buffer_size = 1000;

	/*
	 * Decode the input, leave out the items that are to be removed, and
	 * re-encode as we go.
	 */
	tids = palloc(buffer_size * sizeof(zstid));
	datums = palloc(buffer_size * sizeof(Datum));
	isnulls = palloc(buffer_size * sizeof(bool));

	init_attstream_buffer(dst, attr->attbyval, attr->attlen);

	init_attstream_decoder(&decoder, attr->attbyval, attr->attlen);
	decode_attstream_begin(&decoder, attstream);

	num_buffered = 0;
	removeidx = 0;
	while (decode_attstream_cont(&decoder))
	{
		for (int idx = 0; idx < decoder.num_elements; idx++)
		{
			zstid		tid = decoder.tids[idx];
			Datum		datum = decoder.datums[idx];
			bool		isnull = decoder.isnulls[idx];

			/* also "merge" in the list of tids to remove */
			while (removeidx < num_tids_to_remove && tid > tids_to_remove[removeidx])
				removeidx++;
			if (removeidx < num_tids_to_remove && tid == tids_to_remove[removeidx])
			{
				/*
				 * This datum needs to be removed. Leave it out from the result.
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
				tids[num_buffered] = tid;
				datums[num_buffered] = datum;
				isnulls[num_buffered] = isnull;
				num_buffered++;

				if (num_buffered == buffer_size)
				{
					/* encode datums that we've buffered so far */
					int			num_encoded;
					int			num_remain;

					num_encoded = append_attstream(dst, false, num_buffered, tids, datums, isnulls);

					/* move the remaining ones to beginning of buffer, and continue */
					num_remain = num_buffered - num_encoded;
					memmove(tids, &tids[num_encoded], num_remain * sizeof(zstid));
					memmove(datums, &datums[num_encoded], num_remain * sizeof(Datum));
					memmove(isnulls, &isnulls[num_encoded], num_remain * sizeof(bool));
					num_buffered = num_remain;
				}
			}
		}
	}

	/* flush the rest of buffered datums to the attstream */
	if (num_buffered > 0)
		append_attstream(dst, true, num_buffered, tids, datums, isnulls);

#ifdef USE_ASSERT_CHECKING
	verify_attstream(dst);
#endif

	pfree(datums);
	pfree(isnulls);
	pfree(tids);
	destroy_attstream_decoder(&decoder);
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
void
merge_attstream(Form_pg_attribute attr, attstream_buffer *buf, ZSAttStream *attstream2)
{
	if (attstream2 == NULL)
		return;

	/*
	 * If the input is compressed, decompress it.
	 */
	if ((attstream2->t_flags & ATTSTREAM_COMPRESSED) != 0)
	{
		char	   *decompress_buf;

		decompress_buf = palloc(attstream2->t_decompressed_bufsize);
		zs_decompress(attstream2->t_payload, decompress_buf,
					  attstream2->t_size - SizeOfZSAttStreamHeader,
					  attstream2->t_decompressed_bufsize);

		merge_attstream_guts(attr, buf,
							 decompress_buf, attstream2->t_decompressed_size,
							 attstream2->t_lasttid);

		pfree(decompress_buf);
	}
	else
	{
		merge_attstream_guts(attr, buf,
							 attstream2->t_payload, attstream2->t_size - SizeOfZSAttStreamHeader,
							 attstream2->t_lasttid);
	}
}

void
merge_attstream_buffer(Form_pg_attribute attr, attstream_buffer *buf, attstream_buffer *buf2)
{
	merge_attstream_guts(attr, buf,
						 buf2->data + buf2->cursor, buf2->len - buf2->cursor, buf2->lasttid);
}
static void
merge_attstream_guts(Form_pg_attribute attr, attstream_buffer *buf, char *chunks2, int chunks2len, zstid lasttid2)
{
	attstream_decoder decoder1;
	bool		decoder1_continues;
	int			decoder1_idx;
	attstream_decoder decoder2;
	bool		decoder2_continues;
	int			decoder2_idx;
	Datum	   *result_datums;
	bool	   *result_isnulls;
	zstid	   *result_tids;
	int			num_buffered;
	zstid		lasttid1;
	zstid		firsttid2;
	int			buffer_size = 1000;
	char	   *olddata;
	int			newmaxlen;

	lasttid1 = buf->lasttid;
	firsttid2 = get_chunk_first_tid(buf->attlen, chunks2);

	/*
	 * Fast path:
	 *
	 * If the two streams don't overlap, then
	 * we can avoid re-encoding and just append one stream after the other.
	 * We only do this if the stream that comes first was compressed:
	 * otherwise it may not be optimally packed, and we want to re-encode it
	 * to make sure it's using densest possible codewords.
	 *
	 * XXX: we don't take this fastpath, if the new stream is strictly
	 * below the old stream. We could swap the inputs and do it in that
	 * case too...
	 *
	 * FIXME: we don't actually pay attention to the compression anymore.
	 * We never repack.
	 */
	if (firsttid2 > lasttid1)
	{
		char	   *pos_new;
		uint64		delta;

		enlarge_attstream_buffer(buf, chunks2len);
		pos_new = buf->data + buf->len;

		memcpy(pos_new, chunks2, chunks2len);

		delta = firsttid2 - lasttid1;
		replace_first_tid(buf->attlen, delta, pos_new);

		buf->len += chunks2len;
		buf->lasttid = lasttid2;

		return;
	}

	/*
	 * naive implementation: decode everything, merge arrays, and re-encode.
	 */
	init_attstream_decoder(&decoder1, attr->attbyval, attr->attlen);
	decode_chunks_begin(&decoder1, buf->data + buf->cursor, buf->len - buf->cursor, buf->lasttid);
	decoder1_continues = decode_attstream_cont(&decoder1);
	decoder1_idx = 0;

	init_attstream_decoder(&decoder2, attr->attbyval, attr->attlen);
	decode_chunks_begin(&decoder2, chunks2, chunks2len, lasttid2);
	decoder2_continues = decode_attstream_cont(&decoder2);
	decoder2_idx = 0;

	buffer_size = 1000;		/* arbitrary initial size */
	result_tids = palloc(buffer_size * sizeof(zstid));
	result_datums = palloc(buffer_size * sizeof(Datum));
	result_isnulls = palloc(buffer_size * sizeof(bool));

	/*
	 * Reallocate a new buffer, in the same memory context as the old one, to
	 * write the result to. (We can't write diretly to the old buffer, because
	 * we'll read it simultaneously.
	 */
	newmaxlen = (buf->len - buf->cursor) + chunks2len + 100; /* 100 is gives some headroom, to avoid
															  * repallocs */
	olddata = buf->data;
	buf->data = MemoryContextAlloc(GetMemoryChunkContext(olddata), newmaxlen);
	buf->maxlen = newmaxlen;
	buf->len = 0;
	buf->cursor = 0;
	buf->firsttid = 0;
	buf->lasttid = 0;

	num_buffered = 0;
	for (;;)
	{
		attstream_decoder *decodernext;
		int		   *decodernext_idx;
		bool	   *decodernext_continues;
		zstid		tid;
		Datum		datum;
		bool		isnull;

		if (decoder1_continues && decoder2_continues)
		{
			if (decoder1.tids[decoder1_idx] < decoder2.tids[decoder2_idx])
			{
				decodernext = &decoder1;
				decodernext_idx = &decoder1_idx;
				decodernext_continues = &decoder1_continues;
			}
			else if (decoder1.tids[decoder1_idx] > decoder2.tids[decoder2_idx])
			{
				decodernext = &decoder2;
				decodernext_idx = &decoder2_idx;
				decodernext_continues = &decoder2_continues;
			}
			else
				elog(ERROR, "cannot merge attstreams with duplicate TIDs");
		}
		else if (decoder1_continues)
		{
			decodernext = &decoder1;
			decodernext_idx = &decoder1_idx;
			decodernext_continues = &decoder1_continues;
		}
		else if (decoder2_continues)
		{
			decodernext = &decoder2;
			decodernext_idx = &decoder2_idx;
			decodernext_continues = &decoder2_continues;
		}
		else
		{
			break;	/* all done */
		}

		tid = decodernext->tids[*decodernext_idx];
		datum = decodernext->datums[*decodernext_idx];
		isnull = decodernext->isnulls[*decodernext_idx];
		(*decodernext_idx)++;

		result_tids[num_buffered] = tid;
		result_datums[num_buffered] = datum;
		result_isnulls[num_buffered] = isnull;
		num_buffered++;

		if (num_buffered == buffer_size)
		{
			/* encode datums that we've buffered so far */
			int			num_encoded;
			int			num_remain;

			num_encoded = append_attstream(buf, false, num_buffered,
										   result_tids, result_datums, result_isnulls);

			/* move the remaining ones to beginning of buffer, and continue */
			num_remain = num_buffered - num_encoded;
			memmove(result_tids, &result_tids[num_encoded], num_remain * sizeof(zstid));
			memmove(result_datums, &result_datums[num_encoded], num_remain * sizeof(Datum));
			memmove(result_isnulls, &result_isnulls[num_encoded], num_remain * sizeof(bool));
			num_buffered = num_remain;
		}

		if (*decodernext_idx == decodernext->num_elements)
		{
			*decodernext_continues = decode_attstream_cont(decodernext);
			*decodernext_idx = 0;
		}
	}

	/* flush the rest of buffered datums to the attstream */
	if (num_buffered > 0)
		append_attstream(buf, true, num_buffered, result_tids, result_datums, result_isnulls);

	pfree(olddata);
	pfree(result_datums);
	pfree(result_isnulls);
	pfree(result_tids);
	destroy_attstream_decoder(&decoder1);
	destroy_attstream_decoder(&decoder2);
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
append_attstream_inplace(Form_pg_attribute att, ZSAttStream *oldstream, int freespace,
						 attstream_buffer *newbuf)
{
	zstid		firstnewtid;
	char		*pos_new;
	zstid		delta;

	/*
	 * fast path requirements:
	 *
	 * - the new stream goes after the old one
	 * - there is enough space to append 'newbuf'
	 * - neither stream is compressed
	 */
	if (oldstream->t_flags & ATTSTREAM_COMPRESSED)
		return false;

	if (freespace < newbuf->len - newbuf->cursor)
		return false;	/* no space */

	firstnewtid = get_chunk_first_tid(att->attlen, newbuf->data + newbuf->cursor);
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
		   newbuf->data + newbuf->cursor,
		   newbuf->len - newbuf->cursor);

	delta = firstnewtid - oldstream->t_lasttid;
	replace_first_tid(att->attlen, delta, pos_new);
	oldstream->t_size += newbuf->len - newbuf->cursor;
	oldstream->t_lasttid = newbuf->lasttid;

	newbuf->cursor = newbuf->len;

	return true;
}


#ifdef USE_ASSERT_CHECKING
static void
verify_attstream(attstream_buffer *attbuf)
{
	char	   *p = attbuf->data + attbuf->cursor;
	char	   *pend = attbuf->data + attbuf->len;
	zstid		tid;

	tid = 0;

	while (p < pend)
	{
		p += skip_chunk(attbuf->attlen, p, &tid);
	}
	Assert(tid == attbuf->lasttid);
	Assert(p == pend);
}
#endif

void
print_attstream(int attlen, char *chunk, int len)
{
	char	   *p = chunk;
	char	   *pend = chunk + len;
	zstid		tid;

	tid = 0;
	while (p < pend)
	{
		uint64		codeword;
		int			len;

		memcpy(&codeword, p, sizeof(uint64));

		len = skip_chunk(attlen, p, &tid);
		elog(NOTICE, "%016lX: TID %lu, %d bytes", codeword, tid, len);

		p += len;
	}
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
encode_chunk_fixed(attstream_buffer *dst, zstid prevtid, int ntids,
				   zstid *tids, Datum *datums, bool *isnulls)
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
	bool		attbyval = dst->attbyval;
	int16		attlen = dst->attlen;
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
	enlarge_attstream_buffer(dst, size);
	p = &dst->data[dst->len];
	memcpy(p, (char *) &codeword, sizeof(uint64));
	p += sizeof(uint64);

	/*
	 * Now, the data
	 */

	/* FIXME: the loops below ignore alignment. 'p' might not be aligned */
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
	Assert(dst->len <= dst->maxlen);

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
 * chunk in mode 4 to 10*14 = 140 bytes. Below table shows the modes with the
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
 * 14 toast
 * 15 unused
 *
 * Modes 13 and 15 are currently unused. (The idea is that 15 could be
 * used for various extended modes with special handling, using more
 * bits to indicate which extended mode it is. And it seems logical to
 * have special modes, like the toast mode, at the end. We could use
 * 13 for another "regular" mode.. )
 *
 * Mode 14 is special: It is used to encode a toasted datum. The toast
 * datum is compressed with toast_compress_datum(). Unlike the other
 * modes, the toast mode lenbits field is overloaded and is used to
 * identify where the toast datum is stored. The toast datum could be
 * stored either inline or in separate toast pages. The TID of the
 * datum is stored in the codeword as is. If the toast datum is stored
 * inline, then after the codeword comes the zs_toast_header_inline
 * followed by the datum. If the toast datum is stored in separate
 * toast pages, then after the codeword comes the block number of the
 * first toast block, as a 32-bit integer.
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
	{ 48, 12, 1 },	/* mode 14 (toast) */
	{ 0, 0, 0 },	/* mode 15 */

	{ 0, 0, 0 }		/* sentinel */
};

#define ZS_VARLENA_INLINE_TOAST 1
#define ZS_VARLENA_TOAST_PAGE 0

typedef struct zs_toast_header_external {
	BlockNumber toast_blkno;
} zs_toast_header_external;

typedef struct zs_toast_header_inline
{
	uint32 compressed_size;
	uint32 rawsize;
} zs_toast_header_inline;

static int
get_toast_chunk_length(char *chunk, uint64 toast_mode_selector)
{
	int chunk_len;

	if (toast_mode_selector == ZS_VARLENA_INLINE_TOAST)
	{
		zs_toast_header_inline *hdr;
		chunk_len = sizeof(uint64) + sizeof(zs_toast_header_inline);

		hdr = (zs_toast_header_inline *) (chunk + sizeof(uint64));
		chunk_len += hdr->compressed_size;
	}
	else if (toast_mode_selector == ZS_VARLENA_TOAST_PAGE)
	{
		chunk_len = sizeof(uint64) + sizeof(zs_toast_header_external);
	}
	else
	{
		elog(ERROR, "Invalid toast chunk type");
	}

	return chunk_len;
}

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

		/* skip over the TIDs */
		codeword >>= tidbits * nints;

		if (selector == 14)
		{
			/* in-line toast datum or toast pointer */
			return get_toast_chunk_length(chunk, codeword & lenmask);
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
			codeword >>= tidbits * nints;
			return get_toast_chunk_length(chunk, codeword & lenmask);
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
			/* in-line toast datum or toast pointer */
			tid += (codeword & tidmask);

			if ((codeword >> (tidbits * nints)) & lenmask & ZS_VARLENA_INLINE_TOAST)
			{
				zs_toast_header_inline hdr;
				uint32 len;

				memcpy(&hdr, p, sizeof(zs_toast_header_inline));
				p += sizeof(zs_toast_header_inline);

				len = hdr.compressed_size;
				datump = palloc0(len + TOAST_COMPRESS_HDRSZ);
				SET_VARSIZE_COMPRESSED(datump, len + TOAST_COMPRESS_HDRSZ);
				TOAST_COMPRESS_SET_RAWSIZE(datump, hdr.rawsize);
				memcpy(datump + TOAST_COMPRESS_HDRSZ, p, len);
				p += len;

				datums[0] = PointerGetDatum(datump);
			}
			else
			{
				zs_toast_header_external hdr;
				BlockNumber	toastblkno;
				varatt_zs_toastptr *toastptr;

				memcpy(&hdr, p, sizeof(zs_toast_header_external));
				p += sizeof(zs_toast_header_external);

				toastblkno = hdr.toast_blkno;

				toastptr = palloc0(sizeof(varatt_zs_toastptr));
				SET_VARTAG_1B_E(toastptr, VARTAG_ZEDSTORE);
				toastptr->zst_block = toastblkno;

				datums[0] = PointerGetDatum(toastptr);
			}

			tids[0] = tid;
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
encode_chunk_varlen_inline_toast(attstream_buffer *dst, zstid prevtid,
									zstid *tids, Datum *datums)
{
	uint32 len;
	uint64		codeword;
	char	   *p;

	zs_toast_header_inline hdr;
	hdr.compressed_size = TOAST_COMPRESS_SIZE(datums[0]);
	hdr.rawsize = TOAST_COMPRESS_RAWSIZE(datums[0]);
	len = hdr.compressed_size;

	codeword = UINT64CONST(14) << 12;
	codeword = (codeword | ((uint64)ZS_VARLENA_INLINE_TOAST)) << 48;
	codeword = codeword | (tids[0] - prevtid);

	enlarge_attstream_buffer(dst, sizeof(uint64) +
		sizeof(zs_toast_header_inline) + len);
	p = dst->data + dst->len;
	memcpy(p, (char *) &codeword, sizeof(uint64));
	p += sizeof(uint64);
	memcpy(p, (char *) &hdr, sizeof(zs_toast_header_inline));
	p += sizeof(zs_toast_header_inline);
	memcpy(p, (char *) TOAST_COMPRESS_RAWDATA(datums[0]), len);
	dst->len += sizeof(uint64) + sizeof(zs_toast_header_inline) + len;
	return 1;
}

static int
encode_chunk_varlen_toast_page(attstream_buffer *dst, zstid prevtid, zstid *tids, Datum * datums)
{
	int64 codeword;
	char *p;
	zs_toast_header_external hdr;
	varatt_zs_toastptr *toastptr = (varatt_zs_toastptr *) DatumGetPointer(datums[0]);
	hdr.toast_blkno = toastptr->zst_block;

	codeword = UINT64CONST(14) << 12;
	codeword = (codeword | ((uint64)ZS_VARLENA_TOAST_PAGE)) << 48;
	codeword = codeword | (tids[0] - prevtid);

	enlarge_attstream_buffer(dst, sizeof(uint64) + sizeof(zs_toast_header_external));
	p = dst->data + dst->len;
	memcpy(p, (char *) &codeword, sizeof(uint64));
	p += sizeof(uint64);
	memcpy(p, (char *) &hdr, sizeof(zs_toast_header_external));
	dst->len += sizeof(uint64) + sizeof(zs_toast_header_external);
	return 1;
}

static int
encode_chunk_varlen(attstream_buffer *dst, zstid prevtid, int ntids,
					zstid *tids, Datum *datums, bool *isnulls)
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
	char	   *p;

	/* special case for inline toast */
	if (!isnulls[0] && VARATT_IS_COMPRESSED(datums[0]))
		return encode_chunk_varlen_inline_toast(dst, prevtid, tids, datums);

	/* special case for toast pointers */
	else if (!isnulls[0] && VARATT_IS_EXTERNAL(datums[0]) && VARTAG_EXTERNAL(datums[0]) == VARTAG_ZEDSTORE)
		return encode_chunk_varlen_toast_page(dst, prevtid, tids, datums);

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
					if ((VARATT_IS_EXTERNAL(datums[i]) && VARTAG_EXTERNAL(datums[i]) == VARTAG_ZEDSTORE) ||
						VARATT_IS_COMPRESSED(datums[i]))
					{
						/* toast pointer or toast datum, bail out */
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

	enlarge_attstream_buffer(dst, sizeof(uint64) + (1 << this_lenbits) * this_nints);
	p = &dst->data[dst->len];

	memcpy(p, (char *) &codeword, sizeof(uint64));
	p += sizeof(uint64);

	/*
	 * Now, the data
	 */
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
encode_chunk(attstream_buffer *buf, zstid prevtid, int ntids,
			 zstid *tids, Datum *datums, bool *isnulls)
{
	if (buf->attlen > 0)
		return encode_chunk_fixed(buf, prevtid, ntids,
								  tids, datums, isnulls);
	else
		return encode_chunk_varlen(buf, prevtid, ntids,
								   tids, datums, isnulls);
}
