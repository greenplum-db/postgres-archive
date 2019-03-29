/*
 * zedstore_compression.h
 *		internal declarations for ZedStore compression
 *
 * Copyright (c) 2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/include/access/zedstore_compression.h
 */
#ifndef ZEDSTORE_COMPRESSION_H
#define ZEDSTORE_COMPRESSION_H

#include "storage/itemptr.h"

typedef struct ZSDecompressContext
{
	char	   *buffer;
	int			bufsize;
	int			bytesread;
} ZSDecompressContext;

typedef struct ZSCompressContext
{
	char	   *uncompressedbuffer;

	int			maxCompressedSize;
	int			maxUncompressedSize;
	char	   *buffer;
	int			nitems;
	int			rawsize;
} ZSCompressContext;

typedef struct ZSBtreeItem ZSBtreeItem;

/* compression functions */
extern void zs_compress_init(ZSCompressContext *context);
extern void zs_compress_begin(ZSCompressContext *context, int maxCompressedSize);
extern bool zs_compress_add(ZSCompressContext *context, ZSBtreeItem *item);
extern ZSBtreeItem *zs_compress_finish(ZSCompressContext *context);
extern void zs_compress_free(ZSCompressContext *context);

/* decompression functions */
extern void zs_decompress_chunk(ZSDecompressContext *context, ZSBtreeItem *chunk);
extern ZSBtreeItem *zs_decompress_read_item(ZSDecompressContext *context);

/* internal functions for streaming compression */
#if 0
extern ZS_PGLZ_Compressor *zs_pglz_compress_init(char *dest, int32 destlen);
extern int32 zs_pglz_compress(ZS_PGLZ_Compressor *cxt, const char *source, int32 slen);
extern int32 zs_pglz_finish(ZS_PGLZ_Compressor *cxt);
extern int32 zs_pglz_decompress(const char *source, int32 slen, char *dest,
								int32 rawsize);
#endif
#endif							/* ZEDSTORE_COMPRESSION_H */
