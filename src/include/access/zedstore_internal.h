/*
 *
 *
 */
#ifndef ZEDSTORE_INTERNAL_H
#define ZEDSTORE_INTERNAL_H

#include "storage/bufmgr.h"

/*
 * Different page types:
 *
 * - metapage (block 0)
 * - Btree pages
 *   - root, internal, leaf
 * - FSM pages
 * - "toast" pages.
 *
 */

//#define ZS_

#define ZS_BTREE_LEAF		0x0001
#define ZS_FOLLOW_RIGHT		0x0002

#define	ZS_META_PAGE_ID		0xF083
#define	ZS_BTREE_PAGE_ID	0xF084

typedef struct ZSBtreePageOpaque
{
	BlockNumber zs_next;
	ItemPointerData zs_lokey;
	ItemPointerData zs_hikey;
	uint16		zs_level;
	uint16		zs_flags;
	uint16		zs_page_id;
} ZSBtreePageOpaque;

typedef struct ZSBtreeInternalPageItem
{
	ItemPointerData tid;
	BlockIdData childblk;
} ZSBtreeInternalPageItem;


typedef struct ZSBtreeScan
{
	TupleDesc	desc;
	AttrNumber	attno;

	bool		active;
	Buffer		lastbuf;
	OffsetNumber lastoff;
	ItemPointerData lasttid;
} ZSBtreeScan;

typedef struct ZSMetaPage
{
	int			nattributes;
	BlockNumber	roots[FLEXIBLE_ARRAY_MEMBER];	/* one for each attribute */
} ZSMetaPage;

typedef struct ZSMetaPageOpaque
{
	uint16		zs_flags;
	uint16		zs_page_id;
} ZSMetaPageOpaque;

#define ZS_META_BLK		0

#define ZSBtreePageGetOpaque(page) (ZSBtreePageOpaque *) PageGetSpecialPointer(page)
#define ZSBtreePageGetData(page) (ZSBtreeInternalPageItem *) PageGetContents(page)

/* leafs follow the normal page layout ?? */

/* prototypes for functions in zstore_btree.c */
extern ItemPointerData zsbt_insert(Relation indrel, AttrNumber attno, Datum datum);

extern void zsbt_begin_scan(Relation indrel, AttrNumber attno, ItemPointer starttid, ZSBtreeScan *scan);
extern bool zsbt_scan_next(ZSBtreeScan *scan, Datum *datum, ItemPointerData *tid);
extern void zsbt_end_scan(ZSBtreeScan *scan);

/* prototypes for functions in zstore_meta.c */
extern void zs_initmetapage(Relation indrel, int nattributes);
extern Buffer zs_getnewbuf(Relation indrel);
extern BlockNumber zsmeta_get_root_for_attribute(Relation indrel, AttrNumber attno, bool for_update);

#endif							/* ZEDSTORE_INTERNAL_H */
