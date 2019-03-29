/*-------------------------------------------------------------------------
 *
 * zedstoream_handler.c
 *	  heap table access method code
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/zedstore/zedstoream_handler.c
 *
 *
 * NOTES
 *	  This file contains the zedstore_ routines which implement
 *	  the POSTGRES zedstore table access method used for all POSTGRES
 *	  relations.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "miscadmin.h"

#include "access/heapam.h"
#include "access/relscan.h"
#include "access/tableam.h"
#include "access/zedstore_internal.h"
#include "catalog/catalog.h"
#include "catalog/index.h"
#include "catalog/pg_am_d.h"
#include "catalog/storage.h"
#include "catalog/storage_xlog.h"
#include "executor/executor.h"
#include "optimizer/plancat.h"
#include "storage/procarray.h"
#include "utils/builtins.h"
#include "utils/rel.h"

typedef enum
{
	ZSSCAN_STATE_UNSTARTED,
	ZSSCAN_STATE_SCANNING,
	ZSSCAN_STATE_FINISHED_RANGE,
	ZSSCAN_STATE_FINISHED
} zs_scan_state;

typedef struct ZedStoreDescData
{
	/* scan parameters */
	TableScanDescData rs_scan;  /* */
	int		   *proj_atts;
	ZSBtreeScan *btree_scans;
	int			num_proj_atts;

	zs_scan_state state;
	ItemPointerData cur_range_start;
	ItemPointerData cur_range_end;
	bool		finished;
} ZedStoreDescData;

typedef struct ParallelZSScanDescData *ParallelZSScanDesc;

static Size zs_parallelscan_estimate(Relation rel);
static Size zs_parallelscan_initialize(Relation rel, ParallelTableScanDesc pscan);
static void zs_parallelscan_reinitialize(Relation rel, ParallelTableScanDesc pscan);
static bool zs_parallelscan_nextrange(Relation rel, ParallelZSScanDesc pzscan,
						  ItemPointer start, ItemPointer end);



typedef struct ZedStoreDescData *ZedStoreDesc;
/* ----------------------------------------------------------------
 *				storage AM support routines for zedstoream
 * ----------------------------------------------------------------
 */

static bool
zedstoream_fetch_row_version(Relation relation,
							 ItemPointer tid,
							 Snapshot snapshot,
							 TupleTableSlot *slot)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("function not implemented yet")));
	return false;
}

/*
 * Insert a heap tuple from a slot, which may contain an OID and speculative
 * insertion token.
 */
static void
zedstoream_insert(Relation relation, TupleTableSlot *slot, CommandId cid,
				   int options, BulkInsertState bistate)
{
	int i;
	Datum *d;
	bool *isnull;
	slot_getallattrs(slot);

	d = slot->tts_values;
	isnull = slot->tts_isnull;

	for(i=0; i < relation->rd_att->natts; i++)
	{
		Form_pg_attribute attr = &relation->rd_att->attrs[i];

		if (attr->attlen < 0)
			elog(LOG, "over ambitious. zedstore is only few weeks old, yet to learn handling variable lengths");

		if (isnull[i])
			elog(ERROR, "you are going too fast. zedstore can't handle NULLs currently.");

		zsbt_insert(relation, i + 1, d[i]);
	}
}

static void
zedstoream_insert_speculative(Relation relation, TupleTableSlot *slot, CommandId cid,
							   int options, BulkInsertState bistate, uint32 specToken)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("function not implemented yet")));
}

static void
zedstoream_complete_speculative(Relation relation, TupleTableSlot *slot, uint32 spekToken,
								 bool succeeded)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("function not implemented yet")));
}


static TM_Result
zedstoream_delete(Relation relation, ItemPointer tid, CommandId cid,
				   Snapshot snapshot, Snapshot crosscheck, bool wait,
				   TM_FailureData *hufd, bool changingPart)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("function not implemented yet")));
}


static TM_Result
zedstoream_lock_tuple(Relation relation, ItemPointer tid, Snapshot snapshot,
				TupleTableSlot *slot, CommandId cid, LockTupleMode mode,
				LockWaitPolicy wait_policy, uint8 flags,
				TM_FailureData *hufd)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("function not implemented yet")));
}


static TM_Result
zedstoream_update(Relation relation, ItemPointer otid, TupleTableSlot *slot,
				   CommandId cid, Snapshot snapshot, Snapshot crosscheck,
				   bool wait, TM_FailureData *hufd,
				   LockTupleMode *lockmode, bool *update_indexes)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("function not implemented yet")));
}

static const TupleTableSlotOps *
zedstoream_slot_callbacks(Relation relation)
{
	return &TTSOpsVirtual;
}

static TableScanDesc
zedstoream_beginscan_with_column_projection(Relation relation, Snapshot snapshot,
											int nkeys, ScanKey key,
											ParallelTableScanDesc parallel_scan,
											bool *project_columns,
											bool allow_strat,
											bool allow_sync,
											bool allow_pagemode,
											bool is_bitmapscan,
											bool is_samplescan,
											bool temp_snap)
{
	int i;
	ZedStoreDesc scan;

	/*
	 * allocate and initialize scan descriptor
	 */
	scan = (ZedStoreDesc) palloc(sizeof(ZedStoreDescData));

	scan->rs_scan.rs_rd = relation;
	scan->rs_scan.rs_snapshot = snapshot;
	scan->rs_scan.rs_nkeys = nkeys;
	scan->rs_scan.rs_bitmapscan = is_bitmapscan;
	scan->rs_scan.rs_samplescan = is_samplescan;
	scan->rs_scan.rs_allow_strat = allow_strat;
	scan->rs_scan.rs_allow_sync = allow_sync;
	scan->rs_scan.rs_temp_snap = temp_snap;
	scan->rs_scan.rs_parallel = parallel_scan;

	/*
	 * we can use page-at-a-time mode if it's an MVCC-safe snapshot
	 */
	scan->rs_scan.rs_pageatatime = allow_pagemode && snapshot && IsMVCCSnapshot(snapshot);

	scan->state = ZSSCAN_STATE_UNSTARTED;

	/*
	 * we do this here instead of in initscan() because heap_rescan also calls
	 * initscan() and we don't want to allocate memory again
	 */
	if (nkeys > 0)
		scan->rs_scan.rs_key = (ScanKey) palloc(sizeof(ScanKeyData) * nkeys);
	else
		scan->rs_scan.rs_key = NULL;

	scan->proj_atts = palloc(relation->rd_att->natts * sizeof(int));

	scan->btree_scans = palloc0(relation->rd_att->natts * sizeof(ZSBtreeScan));
	scan->num_proj_atts = 0;

	/*
	 * convert booleans array into an array of the attribute numbers of the
	 * required columns.
	 */
	for (i = 0; i < relation->rd_att->natts; i++)
	{
		/* if project_columns is empty means need all the columns */
		if (project_columns == NULL || project_columns[i])
		{
			scan->proj_atts[scan->num_proj_atts++] = i;
		}
	}

	return (TableScanDesc) scan;
}

static TableScanDesc
zedstoream_beginscan(Relation relation, Snapshot snapshot,
					 int nkeys, ScanKey key,
					 ParallelTableScanDesc parallel_scan,
					 bool allow_strat,
					 bool allow_sync,
					 bool allow_pagemode,
					 bool is_bitmapscan,
					 bool is_samplescan,
					 bool temp_snap)
{
	return zedstoream_beginscan_with_column_projection(relation, snapshot, nkeys, key, parallel_scan,
													   NULL, allow_strat, allow_sync, allow_pagemode,
													   is_bitmapscan, is_samplescan, temp_snap);
}

static void
zedstoream_endscan(TableScanDesc sscan)
{
	int i;
	ZedStoreDesc scan = (ZedStoreDesc) sscan;
	if (scan->proj_atts)
		pfree(scan->proj_atts);

	for (i = 0; i < sscan->rs_rd->rd_att->natts; i++)
	{
		zsbt_end_scan(&scan->btree_scans[i]);
	}

	if (scan->rs_scan.rs_temp_snap)
		UnregisterSnapshot(scan->rs_scan.rs_snapshot);

	pfree(scan->btree_scans);
	pfree(scan);
}

static bool
zedstoream_getnextslot(TableScanDesc sscan, ScanDirection direction, TupleTableSlot *slot)
{
	ZedStoreDesc scan = (ZedStoreDesc) sscan;

	Assert(scan->num_proj_atts <= slot->tts_tupleDescriptor->natts);

	slot->tts_nvalid = 0;
	slot->tts_flags |= TTS_FLAG_EMPTY;

	while (scan->state != ZSSCAN_STATE_FINISHED)
	{
		if (scan->state == ZSSCAN_STATE_UNSTARTED ||
			scan->state == ZSSCAN_STATE_FINISHED_RANGE)
		{
			if (scan->rs_scan.rs_parallel)
			{
				/* Allocate next range of TIDs to scan */
				if (!zs_parallelscan_nextrange(scan->rs_scan.rs_rd,
											   (ParallelZSScanDesc) scan->rs_scan.rs_parallel,
											   &scan->cur_range_start, &scan->cur_range_end))
				{
					scan->state = ZSSCAN_STATE_FINISHED;
					break;
				}
			}
			else
			{
				if (scan->state == ZSSCAN_STATE_FINISHED_RANGE)
				{
					scan->state = ZSSCAN_STATE_FINISHED;
					break;
				}
				ItemPointerSet(&scan->cur_range_start, 0, 1);
				ItemPointerSet(&scan->cur_range_end, MaxBlockNumber, 0xffff);
			}

			for (int i = 0; i < scan->num_proj_atts; i++)
			{
				zsbt_begin_scan(scan->rs_scan.rs_rd, i + 1,
								scan->cur_range_start,
								&scan->btree_scans[i]);
			}
			scan->state = ZSSCAN_STATE_SCANNING;
		}

		/* We now have a range to scan */
		Assert(scan->state == ZSSCAN_STATE_SCANNING);
		for (int i = 0; i < scan->num_proj_atts; i++)
		{
			int			natt = scan->proj_atts[i];
			Datum		datum;
			ItemPointerData tid;

			if (!zsbt_scan_next(&scan->btree_scans[natt], &datum, &tid))
			{
				scan->state = ZSSCAN_STATE_FINISHED_RANGE;
				break;
			}
			if (ItemPointerCompare(&tid, &scan->cur_range_end) >= 0)
			{
				scan->state = ZSSCAN_STATE_FINISHED_RANGE;
				break;
			}

			slot->tts_values[natt] = datum;
			slot->tts_isnull[natt] = false;
		}
		if (scan->state == ZSSCAN_STATE_FINISHED_RANGE)
		{
			for (int i = 0; i < scan->num_proj_atts; i++)
			{
				int			natt = scan->proj_atts[i];

				zsbt_end_scan(&scan->btree_scans[natt]);
			}
		}
		else
		{
			Assert(scan->state == ZSSCAN_STATE_SCANNING);
			slot->tts_nvalid = slot->tts_tupleDescriptor->natts;
			slot->tts_flags &= ~TTS_FLAG_EMPTY;
			return true;
		}
	}

	ExecClearTuple(slot);
	return false;
}

static bool
zedstoream_tuple_satisfies_snapshot(Relation rel, TupleTableSlot *slot, Snapshot snapshot)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("function not implemented yet")));
}

static IndexFetchTableData*
zedstoream_begin_index_fetch(Relation rel)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("function not implemented yet")));
}


static void
zedstoream_reset_index_fetch(IndexFetchTableData* scan)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("function not implemented yet")));
}

static void
zedstoream_end_index_fetch(IndexFetchTableData* scan)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("function not implemented yet")));
}

static bool
zedstoream_index_fetch_tuple(struct IndexFetchTableData *scan,
						 ItemPointer tid,
						 Snapshot snapshot,
						 TupleTableSlot *slot,
						 bool *call_again, bool *all_dead)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("function not implemented yet")));
}

static void
zedstoream_index_validate_scan(Relation heapRelation,
						   Relation indexRelation,
						   IndexInfo *indexInfo,
						   Snapshot snapshot,
						   ValidateIndexState * state)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("function not implemented yet")));
}

static double
zedstoream_index_build_range_scan(Relation heapRelation,
							  Relation indexRelation,
							  IndexInfo *indexInfo,
							  bool allow_sync,
							  bool anyvisible,
							  BlockNumber start_blockno,
							  BlockNumber numblocks,
							  IndexBuildCallback callback,
							  void *callback_state,
							  TableScanDesc scan)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("function not implemented yet")));
	
}

static const TableAmRoutine zedstoream_methods = {
	.type = T_TableAmRoutine,
	.scans_leverage_column_projection = true,

	.slot_callbacks = zedstoream_slot_callbacks,

	.scan_begin = zedstoream_beginscan,
	.scan_begin_with_column_projection = zedstoream_beginscan_with_column_projection,
	.scan_end = zedstoream_endscan,
	.scan_rescan = heap_rescan,
	.scan_getnextslot = zedstoream_getnextslot,

	.parallelscan_estimate = zs_parallelscan_estimate,
	.parallelscan_initialize = zs_parallelscan_initialize,
	.parallelscan_reinitialize = zs_parallelscan_reinitialize,

	.index_fetch_begin = zedstoream_begin_index_fetch,
	.index_fetch_reset = zedstoream_reset_index_fetch,
	.index_fetch_end = zedstoream_end_index_fetch,
	.index_fetch_tuple = zedstoream_index_fetch_tuple,

//	.scansetlimits = zedstoream_setscanlimits,
//	.scan_update_snapshot = heap_update_snapshot,

//	.scan_bitmap_pagescan = zedstoream_scan_bitmap_pagescan,
//	.scan_bitmap_pagescan_next = zedstoream_scan_bitmap_pagescan_next,

//	.scan_sample_next_block = zedstoream_scan_sample_next_block,
//	.scan_sample_next_tuple = zedstoream_scan_sample_next_tuple,

//	.tuple_fetch_follow = zedstoream_fetch_follow,
	.tuple_insert = zedstoream_insert,
	.tuple_insert_speculative = zedstoream_insert_speculative,
	.tuple_complete_speculative = zedstoream_complete_speculative,
	.tuple_delete = zedstoream_delete,
	.tuple_update = zedstoream_update,
	.tuple_lock = zedstoream_lock_tuple,

	.tuple_fetch_row_version = zedstoream_fetch_row_version,
	.tuple_get_latest_tid = heap_get_latest_tid,
	.tuple_satisfies_snapshot = zedstoream_tuple_satisfies_snapshot,
	.compute_xid_horizon_for_tuples = heap_compute_xid_horizon_for_tuples,

	.index_build_range_scan = zedstoream_index_build_range_scan,
	.index_validate_scan = zedstoream_index_validate_scan,

//	.multi_insert = heap_multi_insert,
//	.finish_bulk_insert = zedstoream_finish_bulk_insert,
//	.relation_vacuum = heap_vacuum_rel,
//	.scan_analyze_next_block = zedstoream_scan_analyze_next_block,
//	.scan_analyze_next_tuple = zedstoream_scan_analyze_next_tuple,
//	.relation_nontransactional_truncate = zedstoream_relation_nontransactional_truncate,
//	.relation_copy_for_cluster = heap_copy_for_cluster,
//	.relation_set_new_filenode = zedstoream_set_new_filenode,
//	.relation_copy_data = zedstoream_relation_copy_data,
//	.relation_sync = heap_sync,
//	.relation_estimate_size = zedstoream_estimate_rel_size,
};

Datum
zedstore_tableam_handler(PG_FUNCTION_ARGS)
{
	PG_RETURN_POINTER(&zedstoream_methods);
}




/*
 * Routines for dividing up the TID range for parallel seq scans
 */

typedef struct ParallelZSScanDescData
{
	ParallelTableScanDescData base;

	ItemPointerData pzs_endtid;		/* last tid + 1 in relation at start of scan */
	pg_atomic_uint64 pzs_allocatedtid_blk;	/* TID space allocated to workers so far. (in  65536 increments) */
} ParallelZSScanDescData;
typedef struct ParallelZSScanDescData *ParallelZSScanDesc;

static Size
zs_parallelscan_estimate(Relation rel)
{
	return sizeof(ParallelZSScanDescData);
}

static Size
zs_parallelscan_initialize(Relation rel, ParallelTableScanDesc pscan)
{
	ParallelZSScanDesc zpscan = (ParallelZSScanDesc) pscan;

	zpscan->base.phs_relid = RelationGetRelid(rel);
	/* FIXME: if attribue 1 is dropped, should use another attribute */
	zpscan->pzs_endtid = zsbt_get_last_tid(rel, 1);
	pg_atomic_init_u64(&zpscan->pzs_allocatedtid_blk, 0);

	return sizeof(ParallelZSScanDescData);
}

static void
zs_parallelscan_reinitialize(Relation rel, ParallelTableScanDesc pscan)
{
	ParallelZSScanDesc bpscan = (ParallelZSScanDesc) pscan;

	pg_atomic_write_u64(&bpscan->pzs_allocatedtid_blk, 0);
}

/*
 * get the next TID range to scan
 *
 * Returns true if there is more to scan, false otherwise.
 *
 * Get the next TID range to scan.  Even if there are no TIDs left to scan,
 * another backend could have grabbed a range to scan and not yet finished
 * looking at it, so it doesn't follow that the scan is done when the first
 * backend gets 'false' return.
 */
static bool
zs_parallelscan_nextrange(Relation rel, ParallelZSScanDesc pzscan,
						  ItemPointer start, ItemPointer end)
{
	uint64		allocatedtid_blk;

	/*
	 * zhs_allocatedtid tracks how much has been allocated to workers
	 * already.  When phs_allocatedtid >= rs_lasttid, all TIDs have been
	 * allocated.
	 *
	 * Because we use an atomic fetch-and-add to fetch the current value, the
	 * phs_allocatedtid counter will exceed rs_lasttid, because workers will
	 * still increment the value, when they try to allocate the next block but
	 * all blocks have been allocated already. The counter must be 64 bits
	 * wide because of that, to avoid wrapping around when rs_lasttid is close
	 * to 2^32.  That's also one reason we do this at granularity of 2^16 TIDs,
	 * even though zedstore isn't block-oriented.
	 *
	 * TODO: we divide the TID space into chunks of 2^16 TIDs each. That's
	 * pretty inefficient, there's a fair amount of overhead in re-starting
	 * the B-tree scans between each range. We probably should use much larger
	 * ranges. But this is good for testing.
	 */
	allocatedtid_blk = pg_atomic_fetch_add_u64(&pzscan->pzs_allocatedtid_blk, 1);
	ItemPointerSet(start, allocatedtid_blk, 1);
	ItemPointerSet(end, allocatedtid_blk + 1, 1);

	return ItemPointerCompare(start, &pzscan->pzs_endtid) < 0;
}
