/*
 * zedstore_compression.c
 *		Routines for compression
 *
 * There are two implementations at the moment: LZ4, and the Postgres
 * pg_lzcompress(). LZ4 support requires that the server was compiled
 * with --with-lz4.
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
#include "common/pg_lzcompress.h"
#include "utils/datum.h"

#ifdef USE_LZ4

int
zs_compress_destSize(const char *src, char *dst, int *srcSizePtr, int targetDstSize)
{
	return LZ4_compress_destSize(src, dst, srcSizePtr, targetDstSize);
}

void
zs_decompress(const char *src, char *dst, int compressedSize, int uncompressedSize)
{
	int			decompressed_size;

	decompressed_size = LZ4_decompress_safe(src, dst, compressedSize, uncompressedSize);
	if (decompressed_size < 0)
		elog(ERROR, "could not decompress chunk (%d bytes compressed, %d bytes uncompressed)",
			 compressedSize, uncompressedSize);
	if (decompressed_size != uncompressedSize)
		elog(ERROR, "unexpected decompressed size");
}

#else
/* PGLZ implementation */

int
zs_compress_destSize(const char *src, char *dst, int *srcSizePtr, int targetDstSize)
{
	int			maxInputSize;
	int			compressed_size;

	/*
	 * FIXME: pglz doesn't have an interface like LZ4 does, to compress up to a certain
	 * target compressed output size. We take a conservative approach and compress
	 * 'targetDstSize' bytes, and return that. Alternatively, we could guess the
	 * compression ratio, and try compressing a larget chunk hoping that it will fit
	 * in the target size, and try again if it didn't fit. Or we could enhance pglz
	 * code to do this cleverly. But it doesn't seem worth the effort, LZ4 (or something
	 * else, but not pglz) is the future.
	 */

	/* reverse the computation of PGLZ_MAX_OUTPUT */
	if (targetDstSize < 4)
		return 0;

	maxInputSize = targetDstSize - 4;
	Assert(PGLZ_MAX_OUTPUT(maxInputSize) <= targetDstSize);
	if (maxInputSize > *srcSizePtr)
		maxInputSize = *srcSizePtr;

	compressed_size = pglz_compress(src, maxInputSize, dst, PGLZ_strategy_always);
	*srcSizePtr = maxInputSize;

	return compressed_size;
}

void
zs_decompress(const char *src, char *dst, int compressedSize, int uncompressedSize)
{
	int			decompressed_size;

	decompressed_size = pglz_decompress(src, compressedSize, dst, uncompressedSize, true);
	if (decompressed_size < 0)
		elog(ERROR, "could not decompress chunk");
	if (decompressed_size != uncompressedSize)
		elog(ERROR, "unexpected decompressed size");
}

#endif		/* !USE_LZ4 */
