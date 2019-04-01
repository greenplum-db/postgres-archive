/*
 * zedstore_compression.c
 *		Routines for compression
 *
 * The current implementation uses Postgre's pglz_compress's. It's not ideal, but gets
 * us started..
 *
 * The compressor works on ZSBtreeItems.
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
 * Finally, call zs_compress_finish(). It returns a compressed ZSBtreeItem,
 * which contains all the plain items that were added (except for the last one
 * for which zs_compress_add() returned false)
 *
 * Decompression interface
 * -----------------------
 *
 * zs_decompress_chunk() takes a compressed ZSBtreeItem as argument. It
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
 * Copyright (c) 2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/access/zedstore/zedstore_compression.c
 */
#include "postgres.h"

#include "access/zedstore_compression.h"
#include "access/zedstore_internal.h"
#include "common/pg_lzcompress.h"
#include "utils/datum.h"

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
	maxUncompressedSize -= offsetof(ZSBtreeItem, t_payload);
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
	ZSBtreeItem *chunk = (ZSBtreeItem *) context->buffer;

	Assert ((item->t_flags & ZSBT_COMPRESSED) == 0);

	if (context->rawsize + item->t_size > context->maxUncompressedSize)
		return false;

	memcpy(context->uncompressedbuffer + context->rawsize, item, item->t_size);
	if (context->nitems == 0)
		chunk->t_tid = item->t_tid;
	chunk->t_lasttid = item->t_tid;
	context->nitems++;
	context->rawsize += item->t_size;

	return true;
}

ZSBtreeItem *
zs_compress_finish(ZSCompressContext *context)
{
	ZSBtreeItem *chunk = (ZSBtreeItem *) context->buffer;
	int32		compressed_size;
	
	compressed_size = pglz_compress(context->uncompressedbuffer, context->rawsize,
									chunk->t_payload,
									PGLZ_strategy_always);
	if (compressed_size < 0)
		elog(ERROR, "compression failed. what now?");
	
	chunk->t_size = offsetof(ZSBtreeItem, t_payload) + compressed_size;
	chunk->t_flags = ZSBT_COMPRESSED;
	chunk->t_uncompressedsize = context->rawsize;

	return chunk;
}

void
zs_compress_free(ZSCompressContext *context)
{
	pfree(context->buffer);
}

void
zs_decompress_chunk(ZSDecompressContext *context, ZSBtreeItem *chunk)
{
	Assert((chunk->t_flags & ZSBT_COMPRESSED) != 0);
	context->buffer = palloc(chunk->t_uncompressedsize);
	context->bufsize = chunk->t_uncompressedsize;

	if (pglz_decompress(chunk->t_payload,
						chunk->t_size - offsetof(ZSBtreeItem, t_payload),
						context->buffer,
						context->bufsize) != context->bufsize)
		elog(ERROR, "could not decompress chunk");

	context->bytesread = 0;
}

ZSBtreeItem *
zs_decompress_read_item(ZSDecompressContext *context)
{
	ZSBtreeItem *next;

	if (context->bytesread == context->bufsize)
		return NULL;
	next = (ZSBtreeItem *) (context->buffer + context->bytesread);
	if (context->bytesread + next->t_size > context->bufsize)
		elog(ERROR, "invalid compressed item");
	context->bytesread += next->t_size;

	Assert(next->t_size >= sizeof(ZSBtreeItem));
	Assert(ItemPointerIsValid(&next->t_tid));

	return next;
}