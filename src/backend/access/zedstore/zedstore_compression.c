/*
 * zedstore_compression.c
 *		Routines for compression
 *
 * There are two implementations at the moment: LZ4, and the Postgres
 * pg_lzcompress(). LZ4 support requires that the server was compiled
 * with --with-lz4.
 *
 * The compressor works on ZSUncompressedBtreeItems.
 *
 * Compression interface
 * ---------------------
 *
 * Call zs_compress_init() to initialize.
 *
 * Call zs_compress_begin(), to begin compressing a group of items. Pass the
 * maximum amount of space it's allowed to use after compression, as argument.
 *
 * Feed them to the compressor one by one with zs_compress_add(), until it
 * returns false.
 *
 * Finally, call zs_compress_finish(). It returns a ZSCompressedBtreeItem,
 * which contains all the plain items that were added (except for the last one
 * for which zs_compress_add() returned false)
 *
 * Decompression interface
 * -----------------------
 *
 * zs_decompress_chunk() takes a ZSCompressedBtreeItem as argument. It
 * initializes a "context" with the given chunk.
 *
 * Call zs_decompress_read_item() to return the uncompressed items one by one.
 *
 *
 * NOTES:
 *
 * Currently, the compressor accepts input, until the *uncompressed* size exceeds
 * the *compressed* size available. I.e it assumes that the compressed size is never
 * larger than uncompressed size.
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/zedstore/zedstore_compression.c
 */
#include "postgres.h"

#ifdef USE_LZ4
#include <lz4.h>
#endif

#include "access/zedstore_compression.h"
#include "access/zedstore_internal.h"
#include "common/pg_lzcompress.h"
#include "utils/datum.h"


/*
 * There are two implementations at the moment: LZ4, and the Postgres
 * pg_lzcompress(). LZ4 support requires that the server was compiled
 * with --with-lz4.
 */
#ifdef USE_LZ4

/*
 * Begin compression, with given max compressed size.
 */
void
zs_compress_init(ZSCompressContext *context)
{
	context->uncompressedbuffer = palloc(BLCKSZ * 10); // FIXME: arbitrary size
	context->buffer = palloc(BLCKSZ);
	context->maxCompressedSize = 0;
	context->maxUncompressedSize = 0;
	context->nitems = 0;
	context->rawsize = 0;
}

void
zs_compress_begin(ZSCompressContext *context, int maxCompressedSize)
{
	context->buffer = repalloc(context->buffer, maxCompressedSize);

	maxCompressedSize -= offsetof(ZSCompressedBtreeItem, t_payload);
	if (maxCompressedSize < 0)
		maxCompressedSize = 0;

	context->maxCompressedSize = maxCompressedSize;
	context->nitems = 0;
	context->rawsize = 0;
}

/*
 * Try to add some data to the compressed block.
 *
 * If it wouldn't fit, return false.
 */
bool
zs_compress_add(ZSCompressContext *context, ZSBtreeItem *item)
{
	ZSCompressedBtreeItem *chunk = (ZSCompressedBtreeItem *) context->buffer;

	Assert((item->t_flags & ZSBT_COMPRESSED) == 0);
	Assert(item->t_tid != InvalidZSTid);

	if (LZ4_COMPRESSBOUND(context->rawsize + MAXALIGN(item->t_size)) > context->maxCompressedSize)
		return false;

	memcpy(context->uncompressedbuffer + context->rawsize, item, item->t_size);
	/* TODO: clear alignment padding */
	if (context->nitems == 0)
		chunk->t_tid = item->t_tid;
	chunk->t_lasttid = zsbt_item_lasttid(item);
	context->nitems++;
	context->rawsize += MAXALIGN(item->t_size);

	return true;
}

ZSCompressedBtreeItem *
zs_compress_finish(ZSCompressContext *context)
{
	ZSCompressedBtreeItem *chunk = (ZSCompressedBtreeItem *) context->buffer;
	int32		compressed_size;

	compressed_size = LZ4_compress_default(context->uncompressedbuffer,
										   chunk->t_payload,
										   context->rawsize,
										   context->maxCompressedSize);
	if (compressed_size < 0)
		elog(ERROR, "compression failed. what now?");

	chunk->t_size = offsetof(ZSCompressedBtreeItem, t_payload) + compressed_size;
	chunk->t_flags = ZSBT_COMPRESSED;
	chunk->t_uncompressedsize = context->rawsize;

	return chunk;
}

void
zs_compress_free(ZSCompressContext *context)
{
	pfree(context->uncompressedbuffer);
	pfree(context->buffer);
}

void
zs_decompress_init(ZSDecompressContext *context)
{
	context->buffer = NULL;
	context->bufsize = 0;
	context->uncompressedsize = 0;
}

void
zs_decompress_chunk(ZSDecompressContext *context, ZSCompressedBtreeItem *chunk)
{
	Assert((chunk->t_flags & ZSBT_COMPRESSED) != 0);
	Assert(chunk->t_uncompressedsize > 0);
	if (context->bufsize < chunk->t_uncompressedsize)
	{
		if (context->buffer)
			pfree(context->buffer);
		context->buffer = palloc(chunk->t_uncompressedsize);
		context->bufsize = chunk->t_uncompressedsize;
	}
	context->uncompressedsize = chunk->t_uncompressedsize;

	if (LZ4_decompress_safe(chunk->t_payload,
							context->buffer,
							chunk->t_size - offsetof(ZSCompressedBtreeItem, t_payload),
							context->uncompressedsize) != context->uncompressedsize)
		elog(ERROR, "could not decompress chunk");

	context->bytesread = 0;
}

ZSBtreeItem *
zs_decompress_read_item(ZSDecompressContext *context)
{
	ZSBtreeItem *next;

	if (context->bytesread == context->uncompressedsize)
		return NULL;
	next = (ZSBtreeItem *) (context->buffer + context->bytesread);
	if (context->bytesread + MAXALIGN(next->t_size) > context->uncompressedsize)
		elog(ERROR, "invalid compressed item");
	context->bytesread += MAXALIGN(next->t_size);

	Assert(next->t_size >= sizeof(ZSBtreeItem));
	Assert(next->t_tid != InvalidZSTid);

	return next;
}

void
zs_decompress_free(ZSDecompressContext *context)
{
	if (context->buffer)
		pfree(context->buffer);
	context->buffer = NULL;
	context->bufsize = 0;
	context->uncompressedsize = 0;
}


#else
/* PGLZ imlementation */

/*
 * In the worst case, pg_lz outputs everything as "literals", and emits one
 * "control byte" ever 8 bytes. Also, it requires 4 bytes extra at the end
 * of the buffer. And add 10 bytes of slop, for good measure.
 */
#define MAX_COMPRESS_EXPANSION_OVERHEAD	(8)
#define MAX_COMPRESS_EXPANSION_BYTES	(4 + 10)

/*
 * Begin compression, with given max compressed size.
 */
void
zs_compress_init(ZSCompressContext *context)
{
	context->uncompressedbuffer = palloc(BLCKSZ * 10); // FIXME: arbitrary size
	context->buffer = palloc(BLCKSZ);
	context->maxCompressedSize = 0;
	context->maxUncompressedSize = 0;
	context->nitems = 0;
	context->rawsize = 0;
}

void
zs_compress_begin(ZSCompressContext *context, int maxCompressedSize)
{
	int			maxUncompressedSize;

	context->buffer = repalloc(context->buffer, maxCompressedSize + 4 /* LZ slop */);

	context->maxCompressedSize = maxCompressedSize;

	/* determine the max uncompressed size */
	maxUncompressedSize = maxCompressedSize;
	maxUncompressedSize -= offsetof(ZSCompressedBtreeItem, t_payload);
	maxUncompressedSize -= maxUncompressedSize / MAX_COMPRESS_EXPANSION_OVERHEAD;
	maxUncompressedSize -= MAX_COMPRESS_EXPANSION_BYTES;
	if (maxUncompressedSize < 0)
		maxUncompressedSize = 0;
	context->maxUncompressedSize = maxUncompressedSize;
	context->nitems = 0;
	context->rawsize = 0;
}

/*
 * Try to add some data to the compressed block.
 *
 * If it wouldn't fit, return false.
 */
bool
zs_compress_add(ZSCompressContext *context, ZSBtreeItem *item)
{
	ZSCompressedBtreeItem *chunk = (ZSCompressedBtreeItem *) context->buffer;

	Assert ((item->t_flags & ZSBT_COMPRESSED) == 0);

	if (context->rawsize + item->t_size > context->maxUncompressedSize)
		return false;

	memcpy(context->uncompressedbuffer + context->rawsize, item, item->t_size);
	if (context->nitems == 0)
		chunk->t_tid = item->t_tid;
	chunk->t_lasttid = zsbt_item_lasttid(item);
	context->nitems++;
	context->rawsize += MAXALIGN(item->t_size);

	return true;
}

ZSCompressedBtreeItem *
zs_compress_finish(ZSCompressContext *context)
{
	ZSCompressedBtreeItem *chunk = (ZSCompressedBtreeItem *) context->buffer;
	int32		compressed_size;

	compressed_size = pglz_compress(context->uncompressedbuffer, context->rawsize,
									chunk->t_payload,
									PGLZ_strategy_always);
	if (compressed_size < 0)
		elog(ERROR, "compression failed. what now?");

	chunk->t_size = offsetof(ZSCompressedBtreeItem, t_payload) + compressed_size;
	chunk->t_flags = ZSBT_COMPRESSED;
	chunk->t_uncompressedsize = context->rawsize;

	return chunk;
}

void
zs_compress_free(ZSCompressContext *context)
{
	pfree(context->uncompressedbuffer);
	pfree(context->buffer);
}

void
zs_decompress_init(ZSDecompressContext *context)
{
	context->buffer = NULL;
	context->bufsize = 0;
	context->uncompressedsize = 0;
}

void
zs_decompress_chunk(ZSDecompressContext *context, ZSCompressedBtreeItem *chunk)
{
	Assert((chunk->t_flags & ZSBT_COMPRESSED) != 0);
	Assert(chunk->t_uncompressedsize > 0);
	if (context->bufsize < chunk->t_uncompressedsize)
	{
		if (context->buffer)
			pfree(context->buffer);
		context->buffer = palloc(chunk->t_uncompressedsize);
		context->bufsize = chunk->t_uncompressedsize;
	}
	context->uncompressedsize = chunk->t_uncompressedsize;

	if (pglz_decompress(chunk->t_payload,
						chunk->t_size - offsetof(ZSCompressedBtreeItem, t_payload),
						context->buffer,
						context->uncompressedsize, true) != context->uncompressedsize)
		elog(ERROR, "could not decompress chunk");

	context->bytesread = 0;
}

ZSBtreeItem *
zs_decompress_read_item(ZSDecompressContext *context)
{
	ZSBtreeItem *next;

	if (context->bytesread == context->uncompressedsize)
		return NULL;
	next = (ZSBtreeItem *) (context->buffer + context->bytesread);
	if (context->bytesread + MAXALIGN(next->t_size) > context->uncompressedsize)
		elog(ERROR, "invalid compressed item");
	context->bytesread += MAXALIGN(next->t_size);

	Assert(next->t_size >= sizeof(ZSBtreeItem));
	Assert(next->t_tid != InvalidZSTid);

	return next;
}

void
zs_decompress_free(ZSDecompressContext *context)
{
	if (context->buffer)
		pfree(context->buffer);
	context->buffer = NULL;
	context->bufsize = 0;
	context->uncompressedsize = 0;
}

#endif		/* !USE_LZ4 */
