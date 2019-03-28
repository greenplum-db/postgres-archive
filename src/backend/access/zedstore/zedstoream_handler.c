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
#include "catalog/catalog.h"
#include "catalog/index.h"
#include "catalog/pg_am_d.h"
#include "catalog/storage.h"
#include "catalog/storage_xlog.h"
#include "optimizer/plancat.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "storage/bufmgr.h"

typedef struct ZedStoreDescData
{
	/* scan parameters */
	TableScanDescData rs_scan;  /* */
	int *proj_atts;
	FILE **fds;
	int num_proj_atts;
} ZedStoreDescData;

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

static void
write_datum_to_file(Relation relation, Datum d, int att_num, Form_pg_attribute attr)
{
	char	   *path;
	char *path_col;
	FILE *fd;

	path = relpathperm(relation->rd_node, MAIN_FORKNUM);
	path_col = psprintf("%s.%d", path, att_num+1);
	fd = fopen(path_col, "a");

	if (fd < 0)
	{
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m", path)));
	}

	if (!attr->attbyval)
		fwrite(DatumGetPointer(d), 1, attr->attlen, fd);
	else
		fwrite(&d, 1, attr->attlen, fd);
	fflush(fd);
	fclose(fd);
	pfree(path);
	pfree(path_col);
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

		write_datum_to_file(relation, d[i], i, attr);
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
	char       *path;
	char *path_col;

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

	/*
	 * we do this here instead of in initscan() because heap_rescan also calls
	 * initscan() and we don't want to allocate memory again
	 */
	if (nkeys > 0)
		scan->rs_scan.rs_key = (ScanKey) palloc(sizeof(ScanKeyData) * nkeys);
	else
		scan->rs_scan.rs_key = NULL;

	scan->proj_atts = palloc(relation->rd_att->natts * sizeof(int));
	scan->fds = palloc(relation->rd_att->natts * sizeof(FILE*));
	scan->num_proj_atts = 0;

	path = relpathperm(relation->rd_node, MAIN_FORKNUM);
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
			path_col = psprintf("%s.%d", path, i+1);
			scan->fds[i] = fopen(path_col, "r");
			if (scan->fds[i] < 0)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not open file \"%s\": %m", path)));
			pfree(path_col);
		}
	}

	pfree(path);
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
		if (scan->fds[i])
			fclose(scan->fds[i]);
	}

	pfree(scan->fds);
	pfree(scan);
}

static bool
zedstoream_getnextslot(TableScanDesc sscan, ScanDirection direction, TupleTableSlot *slot)
{
	ZedStoreDesc scan = (ZedStoreDesc) sscan;

	Assert(scan->num_proj_atts <= slot->tts_tupleDescriptor->natts);

	slot->tts_nvalid = 0;
	slot->tts_flags |= TTS_FLAG_EMPTY;

	for (int i = 0; i < scan->num_proj_atts; i++)
	{
		int natt = scan->proj_atts[i];

		fread(&slot->tts_values[natt], 1,
			  slot->tts_tupleDescriptor->attrs[i].attlen, scan->fds[natt]);

		if (ferror(scan->fds[natt]))
			elog(ERROR, "file read failed.");
		if (feof(scan->fds[natt]))
		{
			ExecClearTuple(slot);
			return false;
		}
		slot->tts_isnull[natt] = false;
	}

	slot->tts_nvalid = slot->tts_tupleDescriptor->natts;
	slot->tts_flags &= ~TTS_FLAG_EMPTY;
	return true;
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

	.parallelscan_estimate = table_block_parallelscan_estimate,
	.parallelscan_initialize = table_block_parallelscan_initialize,
	.parallelscan_reinitialize = table_block_parallelscan_reinitialize,

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
