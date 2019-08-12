/*
 * zedstore_attitem.c
 *		Routines for packing datums into "items", in the attribute trees.
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/zedstore/zedstore_attitem.c
 */
#include "postgres.h"

#include "access/zedstore_compression.h"
#include "access/zedstore_internal.h"
#include "access/zedstore_simple8b.h"
#include "miscadmin.h"
#include "utils/datum.h"

/*
 * We avoid creating items that are "too large". An item can legitimately use
 * up a whole page, but we try not to create items that large, because they
 * could lead to fragmentation. For example, if we routinely created items
 * that are 3/4 of page size, we could only fit one item per page, and waste
 * 1/4 of the disk space.
 *
 * MAX_ATTR_ITEM_SIZE is a soft limit on how large we make items. If there's
 * a very large datum on a row, we store it on a single item of its own
 * that can be larger, because we don't have much choice. But we don't pack
 * multiple datums into a single item so that it would exceed the limit.
 * NOTE: This soft limit is on the *uncompressed* item size. So in practice,
 * when compression is effective, the items we actually store are smaller
 * than this.
 *
 * MAX_TIDS_PER_ATTR_ITEM is the max number of TIDs that can be represented
 * by a single array item. Unlike MAX_ATTR_ITEM_SIZE, it is a hard limit.
 */
#define		MAX_ATTR_ITEM_SIZE		(MaxZedStoreDatumSize / 4)
#define		MAX_TIDS_PER_ATTR_ITEM	((BLCKSZ / 2) / sizeof(zstid))

static void fetch_att_array(char *src, int srcSize, bool hasnulls,
							int numelements, ZSAttrTreeScan *scan);

static ZSAttributeArrayItem *zsbt_attr_create_item(Form_pg_attribute att,
											Datum *datums, bool *isnulls, zstid *tids, int nitems,
											bool has_nulls, int datasz);
static ZSExplodedItem *zsbt_attr_explode_item(ZSAttributeArrayItem *item);

/*
 * Create an attribute item, or items, from an array of tids and datums.
 */
List *
zsbt_attr_create_items(Form_pg_attribute att,
					   Datum *datums, bool *isnulls, zstid *tids, int nitems)
{
	List	   *newitems;
	int			i;
	int			max_items_with_nulls = -1;
	int			max_items_without_nulls = -1;

	if (att->attlen > 0)
	{
		max_items_without_nulls = MAX_ATTR_ITEM_SIZE / att->attlen;
		Assert(max_items_without_nulls > 0);

		max_items_with_nulls = (MAX_ATTR_ITEM_SIZE * 8) / (att->attlen * 8 + 1);

		/* clamp at maximum number of tids */
		if (max_items_without_nulls > MAX_TIDS_PER_ATTR_ITEM)
			max_items_without_nulls = MAX_TIDS_PER_ATTR_ITEM;
		if (max_items_with_nulls > MAX_TIDS_PER_ATTR_ITEM)
			max_items_with_nulls = MAX_TIDS_PER_ATTR_ITEM;
	}

	/*
	 * Loop until we have packed each input datum.
	 */
	newitems = NIL;
	i = 0;
	while (i < nitems)
	{
		size_t		datasz;
		ZSAttributeArrayItem *item;
		int			num_elements;
		bool		has_nulls = false;

		/*
		 * Compute how many input datums we can pack into the next item,
		 * without exceeding MAX_ATTR_ITEM_SIZE or MAX_TIDS_PER_ATTR_ITEM.
		 *
		 * To do that, we have to loop through the datums and compute how
		 * much space they will take when packed.
		 */
		if (att->attlen > 0)
		{
			int			j;
			int			num_nonnull_items;

			for (j = i; j < nitems && j - i < max_items_without_nulls; j++)
			{
				if (isnulls[j])
				{
					has_nulls = true;
					break;
				}
			}
			num_nonnull_items = (j - i);
			datasz = num_nonnull_items * att->attlen;

			if (has_nulls)
			{
				for (;j < nitems && num_nonnull_items < max_items_with_nulls &&
						 j - i < MAX_TIDS_PER_ATTR_ITEM; j++)
				{
					if (!isnulls[j])
					{
						datasz += att->attlen;
						num_nonnull_items++;
					}
				}
			}
			num_elements = (j - i);
		}
		else
		{
			int			j;

			datasz = 0;
			for (j = i; j < nitems && j - i < MAX_TIDS_PER_ATTR_ITEM; j++)
			{
				size_t		this_sz;

				if (isnulls[j])
				{
					has_nulls = true;
					this_sz = 0;
				}
				else
				{
					if (att->attlen == -1)
					{
						if (VARATT_IS_EXTERNAL(datums[j]))
						{
							/*
							 * Any toasted datums should've been taken care of
							 * before we get here. We might see "zedstore-toasted"
							 * datums, but nothing else.
							 */
							if (VARTAG_EXTERNAL(datums[j]) != VARTAG_ZEDSTORE)
								elog(ERROR, "unrecognized toast tag");
							this_sz = 2 + sizeof(BlockNumber);

						}
						else if (VARATT_IS_COMPRESSED(datums[j]))
						{
							/*
							 * "inline" compressed datum. We will include it in an
							 * attribute item, which we will try to compress as whole,
							 * so compressing individual items is a bit silly.
							 * We could uncompress it here, but that also seems
							 * silly, because then it was a waste of time to compress
							 * it earlier. Furthermore, it's theoretically possible that
							 * it would not compress as well using LZ4, so that it would
							 * be too large to store on a page.
							 *
							 * TODO: what to do?
							 */
							elog(ERROR, "inline compressed datums not implemented");
						}
						else
						{
							this_sz = VARSIZE_ANY_EXHDR(DatumGetPointer(datums[j]));

							if ((this_sz + 1) > 0x7F)
								this_sz += 2;
							else
								this_sz += 1;
						}
					}
					else
					{
						Assert(att->attlen == -2);
						this_sz = strlen((char *) DatumGetPointer(datums[j]));

						if ((this_sz + 1) > 0x7F)
							this_sz += 2;
						else
							this_sz += 1;
					}
				}

				if (j != i && datasz + this_sz > MAX_ATTR_ITEM_SIZE)
					break;

				datasz += this_sz;
			}
			num_elements = j - i;
		}

		/* FIXME: account for TID codewords in size calculation. */

		item = zsbt_attr_create_item(att,
									 &datums[i], &isnulls[i], &tids[i], num_elements,
									 has_nulls, datasz);

		newitems = lappend(newitems, item);
		i += num_elements;
	}

	return newitems;
}

/* helper function to pack an array of bools into a NULL bitmap */
static bits8 *
write_null_bitmap(bool *isnulls, int num_elements, bits8 *dst)
{
	bits8		bits = 0;
	int			x = 0;

	for (int j = 0; j < num_elements; j++)
	{
		if (x == 8)
		{
			*dst = bits;
			dst++;
			bits = 0;
			x = 0;
		}

		if (isnulls[j])
			bits |= 1 << x;
		x++;
	}
	if (x > 0)
	{
		*dst = bits;
		dst++;
	}
	return dst;
}

/*
 * Create an array item from given datums and tids.
 *
 * The caller has already computed the size the datums will require.
 */
static ZSAttributeArrayItem *
zsbt_attr_create_item(Form_pg_attribute att,
					  Datum *datums, bool *isnulls, zstid *tids, int num_elements,
					  bool has_nulls, int datasz)
{
	uint64		deltas[MAX_TIDS_PER_ATTR_ITEM];
	uint64		codewords[MAX_TIDS_PER_ATTR_ITEM];
	int			num_codewords;
	int			total_encoded;
	char	   *p;
	char	   *pend;
	size_t		itemsz;
	ZSAttributeArrayItem *item;

	Assert(num_elements > 0);
	Assert(num_elements <= MAX_TIDS_PER_ATTR_ITEM);

	/* Compute TID distances */
	for (int i = 1; i < num_elements; i++)
		deltas[i] = tids[i] - tids[i - 1];

	deltas[0] = 0;
	num_codewords = 0;
	total_encoded = 0;
	while (total_encoded < num_elements)
	{
		int			num_encoded;

		codewords[num_codewords] =
			simple8b_encode(&deltas[total_encoded], num_elements - total_encoded, &num_encoded);

		total_encoded += num_encoded;
		num_codewords++;
	}

	itemsz = offsetof(ZSAttributeArrayItem, t_tid_codewords);
	itemsz += num_codewords * sizeof(uint64);
	if (has_nulls)
		itemsz += ZSBT_ATTR_BITMAPLEN(num_elements);
	itemsz += datasz;

	item = palloc(itemsz);
	item->t_size = itemsz;
	item->t_flags = 0;
	if (has_nulls)
		item->t_flags |= ZSBT_HAS_NULLS;
	item->t_num_elements = num_elements;
	item->t_num_codewords = num_codewords;
	item->t_firsttid = tids[0];
	item->t_endtid = tids[num_elements - 1] + 1;

	for (int j = 0; j < num_codewords; j++)
		item->t_tid_codewords[j] = codewords[j];

	p = (char *) &item->t_tid_codewords[num_codewords];
	pend = ((char *) item) + itemsz;

	if (has_nulls)
		p = (char *) write_null_bitmap(isnulls, num_elements, (bits8 *) p);

	if (att->attlen > 0)
	{
		if (att->attbyval)
		{
			for (int j = 0; j < num_elements; j++)
			{
				if (!isnulls[j])
				{
					store_att_byval(p, datums[j], att->attlen);
					p += att->attlen;
				}
			}
		}
		else
		{
			for (int j = 0; j < num_elements; j++)
			{
				if (!isnulls[j])
				{
					memcpy(p, DatumGetPointer(datums[j]), att->attlen);
					p += att->attlen;
				}
			}
		}
	}
	else
	{
		for (int j = 0; j < num_elements; j++)
		{
			if (!isnulls[j])
			{
				if (att->attlen == -1 && VARATT_IS_EXTERNAL(datums[j]))
				{
					varatt_zs_toastptr *zstoast;

					/*
					 * Any toasted datums should've been taken care of
					 * before we get here. We might see "zedstore-toasted"
					 * datums, but nothing else.
					 */
					if (VARTAG_EXTERNAL(datums[j]) != VARTAG_ZEDSTORE)
						elog(ERROR, "unrecognized toast tag");

					zstoast = (varatt_zs_toastptr *) DatumGetPointer(datums[j]);

					/*
					 * 0xFFFF identifies a toast pointer. Followed by the block
					 * number of the first toast page.
					 */
					*(p++) = 0xFF;
					*(p++) = 0xFF;
					memcpy(p, &zstoast->zst_block, sizeof(BlockNumber));
					p += sizeof(BlockNumber);
				}
				else
				{
					size_t		this_sz;
					char	   *src;

					if (att->attlen == -1)
					{
						this_sz = VARSIZE_ANY_EXHDR(DatumGetPointer(datums[j]));
						src = VARDATA_ANY(DatumGetPointer(datums[j]));
					}
					else
					{
						Assert(att->attlen == -2);
						this_sz = strlen((char *) DatumGetPointer(datums[j]));
						src = (char *) DatumGetPointer(datums[j]);
					}

					if ((this_sz + 1) > 0x7F)
					{
						*(p++) = 0x80 | ((this_sz + 1) >> 8);
						*(p++) = (this_sz + 1) & 0xFF;
					}
					else
					{
						*(p++) = (this_sz + 1);
					}
					memcpy(p, src, this_sz);
					p += this_sz;
				}
				Assert(p <= pend);
			}
		}
	}
	if (p != pend)
		elog(ERROR, "mismatch in item size calculation");

	return item;
}

static inline int
zsbt_attr_datasize(int attlen, char *src)
{
	unsigned char *p = (unsigned char *) src;

	if (attlen > 0)
		return attlen;
	else if ((p[0] & 0x80) == 0)
	{
		/* single-byte header */
		return p[0];
	}
	else if (p[0] == 0xFF && p[1] == 0xFF)
	{
		/* zedstore-toast pointer. */
		return 6;
	}
	else
	{
		/* two-byte header */
		return ((p[0] & 0x7F) << 8 | p[1]) + 1;
	}
}

/*
 * Remove elements with given TIDs from an array item.
 *
 * Returns NULL, if all elements were removed.
 */
ZSExplodedItem *
zsbt_attr_remove_from_item(Form_pg_attribute attr,
						   ZSAttributeArrayItem *olditem,
						   zstid *removetids)
{
	ZSExplodedItem *origitem;
	ZSExplodedItem *newitem;
	int			i;
	int			j;
	char	   *src;
	char	   *dst;

	origitem = zsbt_attr_explode_item(olditem);

	newitem = palloc(sizeof(ZSExplodedItem));
	newitem->tids = palloc(origitem->t_num_elements * sizeof(zstid));
	newitem->nullbitmap = palloc0(ZSBT_ATTR_BITMAPLEN(origitem->t_num_elements));
	newitem->datumdata = palloc(origitem->datumdatasz);

	/* walk through every element */
	j = 0;
	src = origitem->datumdata;
	dst = newitem->datumdata;
	for (i = 0; i < origitem->t_num_elements; i++)
	{
		int			this_datasz;
		bool		this_isnull;

		while (origitem->tids[i] > *removetids)
			removetids++;

		this_isnull = zsbt_attr_item_isnull(origitem->nullbitmap, i);
		if (!this_isnull)
			this_datasz = zsbt_attr_datasize(attr->attlen, src);
		else
			this_datasz = 0;

		if (origitem->tids[i] == *removetids)
		{
			/* leave this one out */
			removetids++;
		}
		else
		{
			newitem->tids[j] = origitem->tids[i];
			if (this_isnull)
			{
				zsbt_attr_item_setnull(newitem->nullbitmap, j);
			}
			else
			{
				memcpy(dst, src, this_datasz);
				dst += this_datasz;
			}
			j++;
		}
		src += this_datasz;
	}

	if (j == 0)
	{
		pfree(newitem);
		return NULL;
	}

	newitem->t_size = 0;
	newitem->t_flags = 0;
	newitem->t_num_elements = j;
	newitem->datumdatasz = dst - newitem->datumdata;

	Assert(newitem->datumdatasz <= origitem->datumdatasz);

	return newitem;
}

/*
 *
 * Extract TID and Datum/isnull arrays the given array item.
 *
 * The arrays are stored directly into the scan->array_* fields.
 *
 * TODO: avoid extracting elements we're not interested in, by passing starttid/endtid.
 */
void
zsbt_attr_item_extract(ZSAttrTreeScan *scan, ZSAttributeArrayItem *item)
{
	int			nelements = item->t_num_elements;
	char	   *p;
	char	   *pend;
	zstid		currtid;
	zstid	   *tids;
	uint64	   *codewords;

	if (nelements > scan->array_datums_allocated_size)
	{
		int			newsize = nelements * 2;

		if (scan->array_datums)
			pfree(scan->array_datums);
		if (scan->array_isnulls)
			pfree(scan->array_isnulls);
		if (scan->array_tids)
			pfree(scan->array_tids);
		scan->array_datums = MemoryContextAlloc(scan->context, newsize * sizeof(Datum));
		scan->array_isnulls = MemoryContextAlloc(scan->context, newsize * sizeof(bool) + 7);
		scan->array_tids = MemoryContextAlloc(scan->context, newsize * sizeof(zstid));
		scan->array_datums_allocated_size = newsize;
	}

	/* decompress if needed */
	if ((item->t_flags & ZSBT_ATTR_COMPRESSED) != 0)
	{
		ZSAttributeCompressedItem *citem = (ZSAttributeCompressedItem *) item;

		if (scan->decompress_buf_size < citem->t_uncompressed_size)
		{
			size_t newsize = citem->t_uncompressed_size * 2;

			if (scan->decompress_buf != NULL)
				pfree(scan->decompress_buf);
			scan->decompress_buf = MemoryContextAlloc(scan->context, newsize);
			scan->decompress_buf_size = newsize;
		}

		p = (char *) citem->t_payload;
		zs_decompress(p, scan->decompress_buf,
					  citem->t_size - offsetof(ZSAttributeCompressedItem, t_payload),
					  citem->t_uncompressed_size);
		p = scan->decompress_buf;
		pend = p + citem->t_uncompressed_size;
	}
	else
	{
		p = (char *) item->t_tid_codewords;
		pend = ((char *) item) + item->t_size;
	}

	/* Decode TIDs from codewords */
	tids = scan->array_tids;
	codewords = (uint64 *) p;
	p += item->t_num_codewords * sizeof(uint64);

	simple8b_decode_words(codewords, item->t_num_codewords, tids, nelements);

	currtid = item->t_firsttid;
	for (int i = 0; i < nelements; i++)
	{
		currtid += tids[i];
		tids[i] = currtid;
	}

	/*
	 * Expand the packed array data into an array of Datums.
	 *
	 * It would perhaps be more natural to loop through the elements with
	 * datumGetSize() and fetch_att(), but this is a pretty hot loop, so it's
	 * better to avoid checking attlen/attbyval in the loop.
	 *
	 * TODO: a different on-disk representation might make this better still,
	 * for varlenas (this is pretty optimal for fixed-lengths already).
	 * For example, storing an array of sizes or an array of offsets, followed
	 * by the data itself, might incur fewer pipeline stalls in the CPU.
	 */
	fetch_att_array(p, pend - p,
					((item->t_flags & ZSBT_HAS_NULLS) != 0),
					nelements,
					scan);
	scan->array_num_elements = nelements;
}


/*
 * Subroutine of zsbt_attr_item_extract(). Unpack an array item into an array of
 * TIDs, and an array of Datums and nulls.
 */
static void
fetch_att_array(char *src, int srcSize, bool hasnulls,
				int numelements, ZSAttrTreeScan *scan)
{
	Form_pg_attribute attr = scan->attdesc;
	int			attlen = attr->attlen;
	bool		attbyval = attr->attbyval;
	char		attalign = attr->attalign;
	bool	   *nulls = scan->array_isnulls;
	Datum	   *datums = scan->array_datums;
	unsigned char *p = (unsigned char *) src;

	if (hasnulls)
	{
		/* expand null bitmap */
		for (int i = 0; i < numelements; i += 8)
		{
			bits8 nullbits = *(bits8 *) (p++);

			/* NOTE: we always overallocate the nulls array, so that we don't
			 * need to check for out of bounds here! */
			nulls[i] = nullbits & 1;
			nulls[i + 1] = (nullbits & (1 << 1)) >> 1;
			nulls[i + 2] = (nullbits & (1 << 2)) >> 2;
			nulls[i + 3] = (nullbits & (1 << 3)) >> 3;
			nulls[i + 4] = (nullbits & (1 << 4)) >> 4;
			nulls[i + 5] = (nullbits & (1 << 5)) >> 5;
			nulls[i + 6] = (nullbits & (1 << 6)) >> 6;
			nulls[i + 7] = (nullbits & (1 << 7)) >> 7;
		}
	}
	else
		memset(nulls, 0, numelements);

	if (attlen > 0 && !hasnulls && attbyval)
	{
		memset(nulls, 0, numelements * sizeof(bool));

		/* this looks a lot like fetch_att... */
		if (attlen == sizeof(Datum))
		{
			memcpy(datums, p, sizeof(Datum) * numelements);
			p += sizeof(Datum) * numelements;
		}
		else if (attlen == sizeof(int32))
		{
			for (int i = 0; i < numelements; i++)
			{
				uint32		x;

				memcpy(&x, p, sizeof(int32));
				p += sizeof(int32);
				datums[i] = Int32GetDatum(x);
			}
		}
		else if (attlen == sizeof(int16))
		{
			for (int i = 0; i < numelements; i++)
			{
				uint16		x;

				memcpy(&x, p, sizeof(int16));
				p += sizeof(int16);
				datums[i] = Int16GetDatum(x);
			}
		}
		else
		{
			Assert(attlen == 1);

			for (int i = 0; i < numelements; i++)
			{
				datums[i] = CharGetDatum(*p);
				p++;
			}
		}
	}
	else if (attlen > 0 && attbyval)
	{
		/* this looks a lot like fetch_att... but the source might not be aligned */
		if (attlen == sizeof(int64))
		{
			for (int i = 0; i < numelements; i++)
			{
				if (nulls[i])
					datums[i] = (Datum) 0;
				else
				{
					uint64		x;

					memcpy(&x, p, sizeof(int64));
					p += sizeof(int64);
					datums[i] = Int64GetDatum(x);
				}
			}
		}
		else if (attlen == sizeof(int32))
		{
			for (int i = 0; i < numelements; i++)
			{
				if (nulls[i])
					datums[i] = (Datum) 0;
				else
				{
					uint32		x;

					memcpy(&x, p, sizeof(int32));
					p += sizeof(int32);
					datums[i] = Int32GetDatum(x);
				}
			}
		}
		else if (attlen == sizeof(int16))
		{
			for (int i = 0; i < numelements; i++)
			{
				if (nulls[i])
					datums[i] = (Datum) 0;
				else
				{
					uint16		x;

					memcpy(&x, p, sizeof(int16));
					p += sizeof(int16);
					datums[i] = Int16GetDatum(x);
				}
			}
		}
		else
		{
			Assert(attlen == 1);

			for (int i = 0; i < numelements; i++)
			{
				if (nulls[i])
					datums[i] = (Datum) 0;
				else
				{
					datums[i] = CharGetDatum(*p);
					p++;
				}
			}
		}
	}
	else if (attlen > 0 && !attbyval)
	{
		/*
		 * pass-by-ref fixed size.
		 *
		 * Because the on-disk format doesn't guarantee any alignment, we need to
		 * take care of that here. XXX: we could skip the copying if attalign='c'
		 */
		int			buf_needed;
		int			alignlen;
		char	   *bufp;

		switch (attalign)
		{
			case 'd':
				alignlen = ALIGNOF_DOUBLE;
				break;
			case 'i':
				alignlen = ALIGNOF_INT;
				break;
			case 's':
				alignlen = ALIGNOF_SHORT;
				break;
			case 'c':
				alignlen = 1;
				break;
			default:
				elog(ERROR, "invalid alignment '%c'", attalign);
		}

		buf_needed = srcSize + (alignlen - 1) * numelements;

		if (scan->attr_buf_size < buf_needed)
		{
			if (scan->attr_buf)
				pfree(scan->attr_buf);
			scan->attr_buf = MemoryContextAlloc(scan->context, buf_needed);
			scan->attr_buf_size = buf_needed;
		}

		bufp = scan->attr_buf;

		for (int i = 0; i < numelements; i++)
		{
			if (nulls[i])
				datums[i] = (Datum) 0;
			else
			{
				bufp = (char *) att_align_nominal(bufp, attalign);

				Assert(bufp + attlen - scan->attr_buf <= buf_needed);

				memcpy(bufp, p, attlen);
				datums[i] = PointerGetDatum(bufp);
				p += attlen;
				bufp += attlen;
			}
		}
	}
	else if (attlen == -1)
	{
		/*
		 * Decode varlenas.
		 * Because we store varlenas unaligned, we might need a buffer for them, too,
		 * like for pass-by-ref fixed-widths above.
		 */
		/*
		 * pass-by-ref fixed size.
		 *
		 * Because the on-disk format doesn't guarantee any alignment, we need to
		 * take care of that here. XXX: we could skip the copying if attalign='c'
		 */
		int			buf_needed;
		char	   *bufp;

		buf_needed = srcSize + (VARHDRSZ) * numelements;

		if (scan->attr_buf_size < buf_needed)
		{
			if (scan->attr_buf)
				pfree(scan->attr_buf);
			scan->attr_buf = MemoryContextAlloc(scan->context, buf_needed);
			scan->attr_buf_size = buf_needed;
		}

		bufp = scan->attr_buf;

		for (int i = 0; i < numelements; i++)
		{
			if (nulls[i])
				datums[i] = (Datum) 0;
			else
			{
				if (*p == 0)
					elog(ERROR, "invalid zs varlen header");

				if ((*p & 0x80) == 0)
				{
					/*
					 * XXX: it would be nice if these were identical to the
					 * short varlen format used elsewhere in PostgreSQL, so
					 * we wouldn't need to copy these.
					 */
					int			this_sz = *p - 1;

					datums[i] = PointerGetDatum(bufp);

					/* XXX: I'm not sure if it makes sense to use
					 * the short varlen format, since this is just an in-memory
					 * copy. I think it's a good way to shake out bugs, though,
					 * so do it for now.
					 */
					if (attr->attstorage != 'p')
					{
						SET_VARSIZE_1B(bufp, 1 + this_sz);
						memcpy(bufp + 1, p + 1, this_sz);
						p += 1 + this_sz;
						bufp += 1 + this_sz;
					}
					else
					{
						SET_VARSIZE(bufp, VARHDRSZ + this_sz);
						memcpy(VARDATA(bufp), p + 1, this_sz);
						p += 1 + this_sz;
						bufp += VARHDRSZ + this_sz;
					}
				}
				else if (p[0] == 0xFF && p[1] == 0xFF)
				{
					/*
					 * zedstore toast pointer.
					 *
					 * Note that the zedstore toast pointer is stored
					 * unaligned. That's OK. Per postgres.h, varatts
					 * with 1-byte header don't need to aligned, and that
					 * applies to toast pointers, too.
					 */
					varatt_zs_toastptr toastptr;

					datums[i] = PointerGetDatum(bufp);

					SET_VARTAG_1B_E(&toastptr, VARTAG_ZEDSTORE);
					memcpy(&toastptr.zst_block, p + 2, sizeof(BlockNumber));
					memcpy(bufp, &toastptr, sizeof(varatt_zs_toastptr));
					p += 2 + sizeof(BlockNumber);
					bufp += sizeof(varatt_zs_toastptr);
				}
				else
				{
					int			this_sz = (((p[0] & 0x7f) << 8) | p[1]) - 1;

					bufp = (char *) att_align_nominal(bufp, 'i');
					datums[i] = PointerGetDatum(bufp);

					Assert(bufp + VARHDRSZ + this_sz - scan->attr_buf <= buf_needed);

					SET_VARSIZE(bufp, VARHDRSZ + this_sz);
					memcpy(VARDATA(bufp), p + 2, this_sz);

					p += 2 + this_sz;
					bufp += VARHDRSZ + this_sz;
				}
			}
		}
	}
	else
		elog(ERROR, "not implemented");

	if (p - (unsigned char *) src != srcSize)
		elog(ERROR, "corrupt item array");
}



/*
 * Routines to split, merge, and recompress items.
 */

static ZSExplodedItem *
zsbt_attr_explode_item(ZSAttributeArrayItem *item)
{
	ZSExplodedItem *eitem;
	int			tidno;
	zstid		currtid;
	zstid	   *tids;
	char	   *databuf;
	char	   *p;
	char	   *pend;
	uint64	   *codewords;

	eitem = palloc(sizeof(ZSExplodedItem));
	eitem->t_size = 0;
	eitem->t_flags = 0;
	eitem->t_num_elements = item->t_num_elements;

	if ((item->t_flags & ZSBT_ATTR_COMPRESSED) != 0)
	{
		ZSAttributeCompressedItem *citem = (ZSAttributeCompressedItem *) item;
		int			payloadsz;

		payloadsz = citem->t_uncompressed_size;
		Assert(payloadsz > 0);

		databuf = palloc(payloadsz);

		zs_decompress(citem->t_payload, databuf,
					  citem->t_size - offsetof(ZSAttributeCompressedItem, t_payload),
					  payloadsz);

		p = databuf;
		pend = databuf + payloadsz;
	}
	else
	{
		p = (char *) item->t_tid_codewords;
		pend = ((char *) item) + item->t_size;
	}

	/* Decode TIDs from codewords */
	tids = eitem->tids = palloc(item->t_num_elements * sizeof(zstid));
	tidno = 0;
	currtid = item->t_firsttid;
	codewords = (uint64 *) p;
	for (int i = 0; i < item->t_num_codewords; i++)
	{
		int			ntids;

		ntids = simple8b_decode(codewords[i], &tids[tidno]);

		for (int j = 0; j < ntids; j++)
		{
			currtid += tids[tidno];
			tids[tidno] = currtid;
			tidno++;
		}
	}
	p += item->t_num_codewords * sizeof(uint64);

	/* nulls */
	if ((item->t_flags & ZSBT_HAS_NULLS) != 0)
	{
		eitem->nullbitmap = (bits8 *) p;

		p += ZSBT_ATTR_BITMAPLEN(item->t_num_elements);
	}
	else
	{
		eitem->nullbitmap = palloc0(ZSBT_ATTR_BITMAPLEN(item->t_num_elements));
	}

	/* datum data */
	eitem->datumdata = p;
	eitem->datumdatasz = pend - p;

	return eitem;
}

/*
 * Estimate how much space an array item takes, when it's uncompressed.
 */
static int
zsbt_item_uncompressed_size(ZSAttributeArrayItem *item)
{
	if (item->t_size == 0)
	{
		ZSExplodedItem *eitem = (ZSExplodedItem *) item;
		size_t		sz = 0;

		// FIXME: account for tids and null bitmap accurately.

		sz += eitem->t_num_elements * 2; // Conservatively estimate 2 bytes per TID.
		sz += eitem->datumdatasz;

		return sz;
	}
	else if (item->t_flags & ZSBT_ATTR_COMPRESSED)
	{
		ZSAttributeCompressedItem *citem = (ZSAttributeCompressedItem *) item;

		return offsetof(ZSAttributeCompressedItem, t_payload) + citem->t_uncompressed_size;
	}
	else
		return item->t_size;
}

void
zsbt_split_item(Form_pg_attribute attr, ZSExplodedItem *origitem, zstid first_right_tid,
				ZSExplodedItem **leftitem_p, ZSExplodedItem **rightitem_p)
{
	int			i;
	int			left_num_elements;
	int			left_datasz;
	int			right_num_elements;
	int			right_datasz;
	char	   *p;
	ZSExplodedItem *leftitem;
	ZSExplodedItem *rightitem;

	if (origitem->t_size != 0)
		origitem = zsbt_attr_explode_item((ZSAttributeArrayItem *) origitem);

	p = origitem->datumdata;
	for (i = 0; i < origitem->t_num_elements; i++)
	{
		if (origitem->tids[i] >= first_right_tid)
			break;

		p += zsbt_attr_datasize(attr->attlen, p);
	}
	left_num_elements = i;
	left_datasz = p - origitem->datumdata;

	right_num_elements = origitem->t_num_elements - left_num_elements;
	right_datasz = origitem->datumdatasz - left_datasz;

	if (left_num_elements == origitem->t_num_elements)
		elog(ERROR, "item split failed");

	leftitem = palloc(sizeof(ZSExplodedItem));
	leftitem->t_size = 0;
	leftitem->t_flags = 0;
	leftitem->t_num_elements = left_num_elements;
	leftitem->tids = palloc(left_num_elements * sizeof(zstid));
	leftitem->nullbitmap = palloc0(left_num_elements * sizeof(bool));
	leftitem->datumdata = palloc(left_datasz);
	leftitem->datumdatasz = left_datasz;

	memcpy(leftitem->tids, &origitem->tids[0], left_num_elements * sizeof(zstid));
	/* XXX: should copy the null bitmap in a smarter way */
	for (i = 0; i < left_num_elements; i++)
	{
		if (zsbt_attr_item_isnull(origitem->nullbitmap, i))
			zsbt_attr_item_setnull(leftitem->nullbitmap, i);
	}
	memcpy(leftitem->datumdata, &origitem->datumdata[0], left_datasz);

	rightitem = palloc(sizeof(ZSExplodedItem));
	rightitem->t_size = 0;
	rightitem->t_flags = 0;
	rightitem->t_num_elements = right_num_elements;
	rightitem->tids = palloc(right_num_elements * sizeof(zstid));
	rightitem->nullbitmap = palloc(right_num_elements * sizeof(bool));
	rightitem->datumdata = palloc(right_datasz);
	rightitem->datumdatasz = right_datasz;

	memcpy(rightitem->tids, &origitem->tids[left_num_elements], right_num_elements * sizeof(zstid));
	/* XXX: should copy the null bitmap in a smarter way */
	for (i = 0; i < right_num_elements; i++)
	{
		if (zsbt_attr_item_isnull(origitem->nullbitmap, left_num_elements + i))
			zsbt_attr_item_setnull(leftitem->nullbitmap, i);
	}
	memcpy(rightitem->datumdata, &origitem->datumdata[left_datasz], right_datasz);

	*leftitem_p = leftitem;
	*rightitem_p = rightitem;
}

static ZSExplodedItem *
zsbt_combine_items(List *items, int start, int end)
{
	ZSExplodedItem *newitem;
	int			total_elements;
	int			total_datumdatasz;
	List	   *exploded_items = NIL;

	total_elements = 0;
	total_datumdatasz = 0;
	for (int i = start; i < end; i++)
	{
		ListCell	   *lc = list_nth_cell(items, i);
		ZSAttributeArrayItem *item = lfirst(lc);
		ZSExplodedItem *eitem;

		if (item->t_size != 0)
		{
			eitem = zsbt_attr_explode_item(item);
			lfirst(lc) = eitem;
		}
		else
			eitem = (ZSExplodedItem *) item;

		exploded_items = lappend(exploded_items, eitem);

		total_elements += eitem->t_num_elements;
		total_datumdatasz += eitem->datumdatasz;
	}
	Assert(total_elements <= MAX_TIDS_PER_ATTR_ITEM);

	newitem = palloc(sizeof(ZSExplodedItem));
	newitem->t_size = 0; // to indicate explodeditem
	newitem->t_flags = 0;
	newitem->t_num_elements = total_elements;

	newitem->tids = palloc(total_elements * sizeof(zstid));
	newitem->nullbitmap = palloc0(ZSBT_ATTR_BITMAPLEN(total_elements));
	newitem->datumdata = palloc(total_datumdatasz);
	newitem->datumdatasz = total_datumdatasz;

	{
		char *p = newitem->datumdata;
		int elemno = 0;
		for (int i = start; i < end; i++)
		{
			ZSExplodedItem *eitem = list_nth(items, i);

			memcpy(&newitem->tids[elemno], eitem->tids, eitem->t_num_elements * sizeof(zstid));

			/* XXX: should copy the null bitmap in a smarter way */
			for (int j = 0; j < eitem->t_num_elements; j++)
			{
				if (zsbt_attr_item_isnull(eitem->nullbitmap, j))
					zsbt_attr_item_setnull(newitem->nullbitmap, elemno + j);
			}

			memcpy(p, eitem->datumdata, eitem->datumdatasz);
			p += eitem->datumdatasz;
			elemno += eitem->t_num_elements;
		}
	}

	return newitem;
}

static ZSAttributeArrayItem *
zsbt_pack_item(Form_pg_attribute att, ZSExplodedItem *eitem)
{
	ZSAttributeArrayItem *newitem;
	int			num_elements = eitem->t_num_elements;
	zstid		firsttid;
	zstid		prevtid;
	uint64		deltas[MAX_TIDS_PER_ATTR_ITEM];
	uint64		codewords[MAX_TIDS_PER_ATTR_ITEM];
	int			num_codewords;
	int			total_encoded;
	size_t		itemsz;
	char	   *p;
	bool		has_nulls;
	int			nullbitmapsz;

	Assert(num_elements > 0);
	Assert(num_elements <= MAX_TIDS_PER_ATTR_ITEM);

	/* compute deltas */
	firsttid = eitem->tids[0];
	prevtid = firsttid;
	deltas[0] = 0;
	for (int i = 1; i < num_elements; i++)
	{
		zstid		this_tid = eitem->tids[i];

		deltas[i] = this_tid - prevtid;
		prevtid = this_tid;
	}

	/* pack into codewords */
	num_codewords = 0;
	total_encoded = 0;
	while (total_encoded < num_elements)
	{
		int			num_encoded;

		codewords[num_codewords] =
			simple8b_encode(&deltas[total_encoded], num_elements - total_encoded, &num_encoded);

		total_encoded += num_encoded;
		num_codewords++;
	}

	nullbitmapsz = ZSBT_ATTR_BITMAPLEN(num_elements);
	has_nulls = false;
	for (int i = 0; i < nullbitmapsz; i++)
	{
		if (eitem->nullbitmap[i] != 0)
		{
			has_nulls = true;
			break;
		}
	}

	itemsz = offsetof(ZSAttributeArrayItem, t_tid_codewords);
	itemsz += num_codewords * sizeof(uint64);
	if (has_nulls)
	{
		/* reserve space for NULL bitmap */
		itemsz += nullbitmapsz;
	}
	itemsz += eitem->datumdatasz;

	Assert(has_nulls || eitem->datumdatasz > 0);

	newitem = palloc(itemsz);
	newitem->t_size = itemsz;
	newitem->t_flags = 0;
	if (has_nulls)
		newitem->t_flags |= ZSBT_HAS_NULLS;
	newitem->t_num_elements = num_elements;
	newitem->t_num_codewords = num_codewords;
	newitem->t_firsttid = eitem->tids[0];
	newitem->t_endtid = eitem->tids[num_elements - 1] + 1;

	memcpy(newitem->t_tid_codewords, codewords, num_codewords * sizeof(uint64));

	p = (char *) &newitem->t_tid_codewords[num_codewords];

	if (has_nulls)
	{
		memcpy(p, eitem->nullbitmap, nullbitmapsz);
		p += nullbitmapsz;
	}

	memcpy(p, eitem->datumdata, eitem->datumdatasz);
	p += eitem->datumdatasz;

	Assert(p - ((char *) newitem) == itemsz);

	return newitem;
}

static ZSAttributeArrayItem *
zsbt_compress_item(ZSAttributeArrayItem *item)
{
	ZSAttributeCompressedItem *citem;
	char	   *uncompressed_payload;
	int			uncompressed_size;
	int			compressed_size;
	int			item_allocsize;

	Assert(item->t_size > 0);

	uncompressed_payload = (char *) &item->t_tid_codewords;
	uncompressed_size = ((char *) item) + item->t_size - uncompressed_payload;

	item_allocsize = item->t_size;
	/*
	 * XXX: because pglz requires a slightly larger buffer to even try compressing,
	 * make a slightly larger allocation. If the compression succeeds but with a
	 * poor ratio, so that we actually use the extra space, then we will store it
	 * uncompressed, but pglz refuses to even try if the destination buffer is not
	 * large enough.
	 */
	item_allocsize += 10;

	citem = palloc(item_allocsize);
	citem->t_flags = ZSBT_ATTR_COMPRESSED;
	if ((item->t_flags & ZSBT_HAS_NULLS) != 0)
		citem->t_flags |= ZSBT_HAS_NULLS;
	citem->t_num_elements = item->t_num_elements;
	citem->t_num_codewords = item->t_num_codewords;
	citem->t_uncompressed_size = uncompressed_size;
	citem->t_firsttid = item->t_firsttid;
	citem->t_endtid = item->t_endtid;

	/* try compressing */
	compressed_size = zs_try_compress(uncompressed_payload,
									  citem->t_payload,
									  uncompressed_size,
									  item_allocsize - offsetof(ZSAttributeCompressedItem, t_payload));
	/*
	 * Skip compression if it wouldn't save at least 8 bytes. There are some
	 * extra header bytes on compressed items, so if we didn't check for this,
	 * the compressed item might actually be larger than the original item,
	 * even if the size of the compressed portion was the same as uncompressed
	 * size, (or 1-2 bytes less). The 8 byte marginal fixes that problem.
	 * Besides, it's hardly worth the CPU overhead of having to decompress
	 * on reading, for a saving of a few bytes.
	 */
	if (compressed_size > 0 && compressed_size + 8 < uncompressed_size)
	{
		citem->t_size = offsetof(ZSAttributeCompressedItem, t_payload) + compressed_size;
		Assert(citem->t_size < item->t_size);
		return (ZSAttributeArrayItem *) citem;
	}
	else
		return item;
}


/*
 * Re-pack and compress a list of items.
 *
 * If there are small items in the input list, such that they can be merged
 * together into larger items, we'll do that. And if there are uncompressed
 * items, we'll try to compress them. If the input list contains "exploded"
 * in-memory items, they will be packed into proper items suitable for
 * storing on-disk.
 */
List *
zsbt_attr_recompress_items(Form_pg_attribute attr, List *items)
{
	List	   *newitems = NIL;
	int			i;

	/*
	 * Heuristics needed on when to try recompressing or merging existing
	 * items. Some musings on that:
	 *
	 * - If an item is already compressed, and close to maximum size, then
	 *   it probably doesn't make sense to recompress.
	 * - If there are two adjacent items that are short, then it is probably
	 *   worth trying to merge them.
	 */

	/* loop through items, and greedily pack them */

	i = 0;
	while (i < list_length(items))
	{
		int			total_num_elements = 0;
		size_t		total_size = 0;
		int			j;
		ZSAttributeArrayItem *newitem;

		for (j = i; j < list_length(items); j++)
		{
			ZSAttributeArrayItem *this_item = (ZSAttributeArrayItem *) list_nth(items, j);
			size_t		this_size;
			int			this_num_elements;

			this_size = zsbt_item_uncompressed_size(this_item);
			this_num_elements = this_item->t_num_elements;

			/* don't create an item that's too large, in terms of size, or in # of tids */
			if (total_num_elements + this_num_elements > MAX_TIDS_PER_ATTR_ITEM)
				break;
			if (total_size + this_size > MAX_ATTR_ITEM_SIZE)
				break;
			total_size += this_size;
			total_num_elements += this_num_elements;
		}
		if (j == i)
			j++;		/* tolerate existing oversized items */

		/* i - j are the items to pack */
		if (j - i > 1)
		{
			ZSAttributeArrayItem *packeditem;
			ZSExplodedItem *combineditem;

			combineditem = zsbt_combine_items(items, i, j);
			packeditem = zsbt_pack_item(attr, combineditem);
			newitem = zsbt_compress_item(packeditem);
		}
		else
		{
			ZSAttributeArrayItem *olditem = list_nth(items, i);

			if (olditem->t_size == 0)
			{
				newitem = zsbt_pack_item(attr, (ZSExplodedItem *) olditem);
				newitem = zsbt_compress_item(newitem);
			}
			else if (olditem->t_flags & ZSBT_ATTR_COMPRESSED)
				newitem = olditem;
			else
				newitem = zsbt_compress_item(olditem);
		}

		newitems = lappend(newitems, newitem);

		i = j;
	}

	/* Check that the resulting items are in correct order, and don't overlap. */
#ifdef USE_ASSERT_CHECKING
	{
		zstid endtid = 0;
		ListCell *lc;

		foreach (lc, newitems)
		{
			ZSAttributeArrayItem *i = (ZSAttributeArrayItem *) lfirst(lc);

			Assert(i->t_firsttid >= endtid);
			Assert(i->t_endtid > i->t_firsttid);
			endtid = i->t_endtid;

			/* there should be no exploded items left */
			Assert(i->t_size != 0);
		}
	}
#endif

	return newitems;
}
