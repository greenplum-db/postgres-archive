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
	int			bufsize;		/* allocated size of 'buffer' */
	int			uncompressedsize;
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
extern void zs_decompress_init(ZSDecompressContext *context);
extern void zs_decompress_chunk(ZSDecompressContext *context, ZSBtreeItem *chunk);
extern ZSBtreeItem *zs_decompress_read_item(ZSDecompressContext *context);
extern void zs_decompress_free(ZSDecompressContext *context);

#endif							/* ZEDSTORE_COMPRESSION_H */
