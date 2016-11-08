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

/* For tuple scan */
#define KEY_TUPLESCAN_PARALLEL_SCAN 501
#define KEY_TUPLESCAN_TASK			502

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(p_tuplescan);
PG_FUNCTION_INFO_V1(p_brangescan);

/* GUC variablies */
int pscan_workers;
int pscan_blocks;

typedef struct PScanStats
{
	BlockNumber n_read_tup;
} PScanStats;

typedef struct TupleScanTask
{
	slock_t	ts_mutex;
} TupleScanTask;

typedef struct BRangeScanTask
{
	Oid relid;
	int	workers;
	int	blocks;
} BRnageScanTask;

/* Common */
void _PG_init(void);
static void report_stats(PScanStats *stat);

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

static void
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
	report_stats(all_stats);

	/* Finalize parallel scanning */
	DestroyParallelContext(pcxt);
	ExitParallelMode();
	
	relation_close(onerel, AccessShareLock);

	PG_RETURN_NULL();
}

Datum
p_brangescan(PG_FUNCTION_ARGS)
{
	PG_RETURN_NULL();
}

