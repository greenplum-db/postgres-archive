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
zs_try_compress(const char *src, char *dst, int srcSize, int dstCapacity)
{
	int			compressed_size;

	compressed_size = LZ4_compress_default(src, dst, srcSize, dstCapacity);

	if (compressed_size > srcSize)
		return 0;
	else
		return compressed_size;
}

void
zs_decompress(const char *src, char *dst, int compressedSize, int uncompressedSize)
{
	int			decompressed_size;

	decompressed_size = LZ4_decompress_safe(src, dst, compressedSize, uncompressedSize);
	if (decompressed_size < 0)
		elog(ERROR, "could not decompress chunk");
	if (decompressed_size != uncompressedSize)
		elog(ERROR, "unexpected decompressed size");
}

#else
/* PGLZ implementation */

int
zs_try_compress(const char *src, char *dst, int srcSize, int dstCapacity)
{
	int			compressed_size;

	if (dstCapacity < PGLZ_MAX_OUTPUT(srcSize))
		return -1;

	compressed_size = pglz_compress(src, srcSize, dst, PGLZ_strategy_always);

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
