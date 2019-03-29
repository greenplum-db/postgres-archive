/*-------------------------------------------------------------------------
 *
 * zedstoream.c
 *	  zedstore access method code
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/heap/heapam.c
 *
 * NOTES
 *	  This file contains the zedstore_ routines which implement
 *	  the POSTGRES zedstore access method.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/htup.h"
#include "access/htup_details.h"
#include "access/parallel.h"
#include "storage/itemptr.h"
#include "storage/predicate.h"
#include "access/xact.h"
#include "access/zedstore_internal.h"
#include "miscadmin.h"
/*
 * This routine is similar to heap_prepare_insert(). This sets the
 * tuple header fields, assigns an OID, and toasts the tuple if necessary.
 * Returns a toasted version of the tuple if it was toasted, or the original
 * tuple if not. Note that in any case, the header fields are also set in
 * the original tuple.
 */
void
zs_prepare_insert(Relation relation, HeapTupleHeader hdr, TransactionId xid,
					CommandId cid, int options)
{
	/*
	 * Parallel operations are required to be strictly read-only in a parallel
	 * worker.  Parallel inserts are not safe even in the leader in the
	 * general case, because group locking means that heavyweight locks for
	 * relation extension or GIN page locks will not conflict between members
	 * of a lock group, but we don't prohibit that case here because there are
	 * useful special cases that we can safely allow, such as CREATE TABLE AS.
	 */
	if (IsParallelWorker())
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TRANSACTION_STATE),
				 errmsg("cannot insert tuples in a parallel worker")));

	hdr->t_infomask &= ~(HEAP_XACT_MASK);
	hdr->t_infomask2 &= ~(HEAP2_XACT_MASK);
	hdr->t_infomask |= HEAP_XMAX_INVALID;
	HeapTupleHeaderSetXmin(hdr, xid);
	if (options & HEAP_INSERT_FROZEN)
		HeapTupleHeaderSetXminFrozen(hdr);

	HeapTupleHeaderSetCmin(hdr, cid);
	HeapTupleHeaderSetXmax(hdr, 0); /* for cleanliness */

	/* TODO: handle toasting. */
#if 0
	/*
	 * If the new tuple is too big for storage or contains already toasted
	 * out-of-line attributes from some other relation, invoke the toaster.
	 */
	if (relation->rd_rel->relkind != RELKIND_RELATION &&
		relation->rd_rel->relkind != RELKIND_MATVIEW)
	{
		/* toast table entries should never be recursively toasted */
		Assert(!HeapTupleHasExternal(tup));
		return tup;
	}
	else if (HeapTupleHasExternal(tup) || tup->t_len > TOAST_TUPLE_THRESHOLD)
		return toast_insert_or_update(relation, tup, NULL, options);
	else
		return tup;
#endif
}

bool
zs_tuple_satisfies_visibility(HeapTupleHeader hdr, ItemPointer tid, Snapshot snapshot, Buffer buffer)
{
	HeapTupleData tuple;
	bool valid;

	tuple.t_data = hdr;
	tuple.t_len = SizeofHeapTupleHeader;
	ItemPointerCopy(tid, &(tuple.t_self));

	/*
	 * if current tuple qualifies, return it.
	 */
	valid = HeapTupleSatisfiesVisibility(&tuple, snapshot, buffer);
	return valid;
}
