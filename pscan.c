/*-------------------------------------------------------------------------
 *
 * pscan extension
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/heapam.h"
#include "access/parallel.h"
#include "access/relscan.h"
#include "access/xact.h"
#include "catalog/pg_type.h"
#include "catalog/storage_xlog.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/procarray.h"
#include "storage/smgr.h"
#include "storage/spin.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/rel.h"

/* Common */
#define KEY_PSCAN_STATS				500

/* Keys for tuple scan */
#define KEY_TUPLESCAN_PARALLEL_SCAN 501
#define KEY_TUPLESCAN_TASK			502

/* Keys for brange scan */
#define KEY_BRANGESCAN_TASK			503

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(p_tuplescan);
PG_FUNCTION_INFO_V1(p_brangescan);

/* GUC variablies */
int pscan_workers;
int pscan_blocks;

typedef struct PScanStats
{
	BlockNumber n_read_tup;

	/* Belows for brange scan */
	BlockNumber begin_blkno;
	BlockNumber end_blkno;
	BlockNumber nblocks;
} PScanStats;

typedef struct TupleScanTask
{
	slock_t	ts_mutex;
} TupleScanTask;

typedef struct BRangeScanTask
{
	Oid relid;
	int	nworkers;
	int	nblocks;
} BRangeScanTask;

/* Common */
void _PG_init(void);
static BlockNumber report_stats(PScanStats *stat);

/* For tuple scan mode */
static void tuplescan_estimate_dsm(ParallelContext *pcxt, Snapshot snapshot);
static PScanStats *tuplescan_initialize_dsm(ParallelContext *pcxt, Relation onerel,
									 Snapshot snapshot);
static void p_tuplescan_worker(dsm_segment *seg, shm_toc *toc);
static void tuplescan_scan_worker(ParallelHeapScanDesc pscan, Relation onerel,
								  TupleScanTask *task, PScanStats *stats);
static void tuplescan_initialize_worker(shm_toc *toc, ParallelHeapScanDesc *pscan,
										TupleScanTask **task, PScanStats **stats);

/* For brange scan mode */
static void brangescan_estimate_dsm(ParallelContext *pcxt);
static PScanStats *brangescan_initialize_dsm(ParallelContext *pcxt, Relation onerel);
static void p_brangescan_worker(dsm_segment *seg, shm_toc *toc);
static void brangescan_scan_worker(Relation onerel, BlockNumber begin,
								   BlockNumber nblocks, PScanStats *stats);
static void brangescan_initialize_worker(shm_toc *toc, BRangeScanTask **task,
										 PScanStats **stats);

/*
 * ----------------------------------------------------------------
 * Common Functions
 * ----------------------------------------------------------------
 */

/*
 * Report scan statistics and return total block numer.
 */
static BlockNumber
report_stats(PScanStats *all_stats)
{
	StringInfoData info;
	int i;
	BlockNumber ntuples = 0;

	initStringInfo(&info);

	for (i = 0; i < pscan_workers; i++)
	{
		PScanStats *stats = all_stats + (sizeof(PScanStats) * i);

		ntuples += stats->n_read_tup;
		elog(NOTICE, "[%d] n_read_tup = %u", i, stats->n_read_tup);
	}

	elog(NOTICE, "--- Total ---");
	elog(NOTICE, "n_read_tup = %u", ntuples);

	return ntuples;
}
void
_PG_init(void)
{
	DefineCustomIntVariable("pscan.n_workers",
							"The number of parallel workers",
							NULL,
							&pscan_workers,
							1,
							1,
							INT_MAX,
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("pscan.n_blocks",
							"The number of blocks each worker read in brange scan mode",
							NULL,
							&pscan_blocks,
							100,
							1,
							INT_MAX,
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);

}


/*
 * ----------------------------------------------------------------
 * Function for Tuple Scan Mode
 * ----------------------------------------------------------------
 */

/* Estimate DSM size for tuple scan mode */
static void
tuplescan_estimate_dsm(ParallelContext *pcxt, Snapshot snapshot)
{
	int size = 0;
	int keys = 0;

	size += heap_parallelscan_estimate(snapshot);
	keys++;

	size += BUFFERALIGN(sizeof(TupleScanTask));
	keys++;

	size += BUFFERALIGN(sizeof(PScanStats) * pscan_workers);
	keys++;

	shm_toc_estimate_chunk(&pcxt->estimator, size);
	shm_toc_estimate_keys(&pcxt->estimator, keys);
}

/*
 * Initialize DSM area for tuple scan mode. Return pointer to
 * head of array of PScanStats.
 */
static PScanStats *
tuplescan_initialize_dsm(ParallelContext *pcxt, Relation onerel, Snapshot snapshot)
{
	ParallelHeapScanDesc pscan;
	TupleScanTask	*task;
	PScanStats		*stats;

	/* Prepare for parallel heap scan */
	pscan = (ParallelHeapScanDesc) shm_toc_allocate(pcxt->toc,
													heap_parallelscan_estimate(snapshot));
	shm_toc_insert(pcxt->toc, KEY_TUPLESCAN_PARALLEL_SCAN, pscan);
	heap_parallelscan_initialize(pscan, onerel, snapshot);

	/* Prepare for tuple scan task */
	task = (TupleScanTask *) shm_toc_allocate(pcxt->toc,
											  sizeof(TupleScanTask));
	shm_toc_insert(pcxt->toc, KEY_TUPLESCAN_TASK, task);
	SpinLockInit(&(task->ts_mutex));

	/* Prepare for stats */
	stats = (PScanStats *) shm_toc_allocate(pcxt->toc,
											sizeof(PScanStats) * pscan_workers);
	shm_toc_insert(pcxt->toc, KEY_PSCAN_STATS, stats);

	return stats;
}

/* Entry point for parallel worker in tuple scan mode */
static void
p_tuplescan_worker(dsm_segment *seg, shm_toc *toc)
{
	ParallelHeapScanDesc pscan;
	TupleScanTask *task;
	PScanStats	*stats;
	Relation rel;

	/* Initialize worker information */
	tuplescan_initialize_worker(toc, &pscan, &task, &stats);
	
	rel = relation_open(pscan->phs_relid, NoLock);

	tuplescan_scan_worker(pscan, rel, task, stats);

	heap_close(rel, NoLock);
}

/* Parallel scan 'onerel' in tuple scan mode */
static void
tuplescan_scan_worker(ParallelHeapScanDesc pscan, Relation onerel,
					  TupleScanTask *task, PScanStats *stats)
{
	HeapScanDesc scan;
	HeapTuple tuple;
	BlockNumber	n_read_tup = 0;

	/* Begin scan */
	scan = heap_beginscan_parallel(onerel, pscan);

	/* Do scanning */
	while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		n_read_tup++;
	}

	/* End scan */
	heap_endscan(scan);

	stats->n_read_tup = n_read_tup;
}

/*
 * Look up for parallel scan description and tuple scan task and
 * set them to arguments.
 */
static void
tuplescan_initialize_worker(shm_toc *toc, ParallelHeapScanDesc *pscan, 
							TupleScanTask **task, PScanStats **stats)
{
	PScanStats *all_stats;

	/* Look up for parallel heap scan description */
	*pscan = (ParallelHeapScanDesc) shm_toc_lookup(toc,
												  KEY_TUPLESCAN_PARALLEL_SCAN);

	/* Look up for tuple scan task */
	*task = (TupleScanTask *) shm_toc_lookup(toc,
											 KEY_TUPLESCAN_TASK);

	/* Look up for scan statistics */
	all_stats = (PScanStats *) shm_toc_lookup(toc,
											  KEY_PSCAN_STATS);
	*stats = all_stats + (sizeof(PScanStats) * ParallelWorkerNumber);
}

Datum
p_tuplescan(PG_FUNCTION_ARGS)
{
	Oid	relid = PG_GETARG_OID(0);
	ParallelContext *pcxt;
	Relation onerel;
	Snapshot snapshot;
	PScanStats *all_stats;
	BlockNumber tuples;

	onerel = try_relation_open(relid, AccessShareLock);
	
	snapshot = GetActiveSnapshot();

	/* Begin parallel mode */
	EnterParallelMode();

	pcxt = CreateParallelContext(p_tuplescan_worker, pscan_workers);
	tuplescan_estimate_dsm(pcxt, snapshot);
	InitializeParallelDSM(pcxt);

	/* Set up DSM are */
	all_stats = tuplescan_initialize_dsm(pcxt, onerel, snapshot);

	/* Do parallel heap scan */
	LaunchParallelWorkers(pcxt);

	/* Wait for parallel worker finish */
	WaitForParallelWorkersToFinish(pcxt);
	
	/* Report all statistics */
	tuples = report_stats(all_stats);

	/* Finalize parallel scanning */
	DestroyParallelContext(pcxt);
	ExitParallelMode();
	
	relation_close(onerel, AccessShareLock);

	PG_RETURN_UINT32(tuples);
}

/*
 * ----------------------------------------------------------------
 * Functions For Block Range Scan Mode
 * ----------------------------------------------------------------
 */

/* Estimate DSM size for brange scan mode */
static void
brangescan_estimate_dsm(ParallelContext *pcxt)
{
	int size = 0;
	int keys = 0;

	size += BUFFERALIGN(sizeof(BRangeScanTask));
	keys++;

	size += BUFFERALIGN(sizeof(PScanStats) * pscan_workers);
	keys++;

	shm_toc_estimate_chunk(&pcxt->estimator, size);
	shm_toc_estimate_keys(&pcxt->estimator, keys);
}

static PScanStats*
brangescan_initialize_dsm(ParallelContext *pcxt, Relation onerel)
{
	BRangeScanTask *task;
	PScanStats	*stats;

	/* Prepare for brange scan task */
	task = (BRangeScanTask *) shm_toc_allocate(pcxt->toc,
											  sizeof(BRangeScanTask));
	shm_toc_insert(pcxt->toc, KEY_BRANGESCAN_TASK, task);
	task->relid = onerel->rd_id;
	task->nworkers = pscan_workers;
	task->nblocks = RelationGetNumberOfBlocks(onerel);

	/* Prepare for stats */
	stats = (PScanStats *) shm_toc_allocate(pcxt->toc,
										   sizeof(PScanStats) * pscan_workers);
	shm_toc_insert(pcxt->toc, KEY_PSCAN_STATS, stats);

	return stats;
}

/* Entry point for parallel worker in brange scan mode */
static void
p_brangescan_worker(dsm_segment *seg, shm_toc *toc)
{
	BRangeScanTask *task;
	PScanStats	*stats;
	Relation rel;
	BlockNumber begin;
	BlockNumber nblocks_worker;
	BlockNumber nblocks_per_worker;

	/* Initialize worker information */
	brangescan_initialize_worker(toc, &task, &stats);

	rel = relation_open(task->relid, NoLock);

	/* Calculate begin block nubmer and the number of blocks have to read */
	nblocks_per_worker = task->nblocks / task->nworkers;
	begin = nblocks_per_worker * ParallelWorkerNumber;
	if (begin + nblocks_per_worker > task->nblocks)
		nblocks_worker = task->nblocks - begin;
	else
		nblocks_worker = nblocks_per_worker;

	brangescan_scan_worker(rel, begin, nblocks_worker, stats);

	/* Save to statistics */
	stats->begin_blkno = begin;
	stats->end_blkno = begin + nblocks_worker;
	stats->nblocks = nblocks_worker;

	heap_close(rel, NoLock);
}

static void
brangescan_scan_worker(Relation onerel, BlockNumber begin, BlockNumber nblocks,
					   PScanStats *stats)
{
	BlockNumber n_read_tup = 0;
	BlockNumber blkno;
	BlockNumber end = begin + nblocks;
	BufferAccessStrategy bstrategy;

	bstrategy = GetAccessStrategy(BAS_NORMAL);

	for (blkno = begin; blkno < end; blkno++)
	{
		OffsetNumber offnum;
		OffsetNumber maxoff;
		Buffer buf;
		Page page;

		buf = ReadBufferExtended(onerel, MAIN_FORKNUM, blkno,
						   RBM_NORMAL, bstrategy);
		page = BufferGetPage(buf);
		maxoff = PageGetMaxOffsetNumber(page);

		for (offnum = FirstOffsetNumber;
			 offnum <= maxoff;
			 offnum = OffsetNumberNext(offnum))
		{
			HeapTupleData tuple;
			ItemId	itemid;

			itemid = PageGetItemId(page, offnum);
			tuple.t_data = (HeapTupleHeader) PageGetItem(page, itemid);
			tuple.t_len = ItemIdGetLength(itemid);

			n_read_tup++;
		}

		ReleaseBuffer(buf);
	}

	stats->n_read_tup = n_read_tup;
}

/*
 * Look up for parallel scan description and brange scan task and
 * set them to arguments.
 */
static void
brangescan_initialize_worker(shm_toc *toc, BRangeScanTask **task,
							 PScanStats **stats)
{
	PScanStats *all_stats;

	/* Look up for brange scan task */
	*task = (BRangeScanTask *) shm_toc_lookup(toc,
											   KEY_BRANGESCAN_TASK);
	/* Look up for scan statistics */
	all_stats = (PScanStats *) shm_toc_lookup(toc,
											  KEY_PSCAN_STATS);
	*stats = all_stats + (sizeof(PScanStats) * ParallelWorkerNumber);
}

Datum
p_brangescan(PG_FUNCTION_ARGS)
{
	Oid	relid = PG_GETARG_OID(0);
	ParallelContext *pcxt;
	Relation onerel;
	PScanStats *all_stats;
	BlockNumber tuples;

	onerel = try_relation_open(relid, AccessShareLock);

	/* Begin parallel mode */
	EnterParallelMode();

	pcxt = CreateParallelContext(p_brangescan_worker, pscan_workers);
	brangescan_estimate_dsm(pcxt);
	InitializeParallelDSM(pcxt);

	/* Set up DSM are */
	all_stats = brangescan_initialize_dsm(pcxt, onerel);

	/* Do parallel heap scan */
	LaunchParallelWorkers(pcxt);

	/* Wait for parallel worker finish */
	WaitForParallelWorkersToFinish(pcxt);

	/* Report all statistics */
	tuples = report_stats(all_stats);

	/* Finalize parallel scanning */
	DestroyParallelContext(pcxt);
	ExitParallelMode();

	relation_close(onerel, AccessShareLock);

	PG_RETURN_UINT32(tuples);
}

