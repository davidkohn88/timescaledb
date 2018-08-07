/* needed for initializing shared memory and using various locks */
#include <postgres.h>

#include <miscadmin.h>
#include <storage/lwlock.h>
#include <utils/hsearch.h>
#include <storage/spin.h>
#include <storage/shmem.h>
#include <storage/ipc.h>
#include <storage/latch.h>
#include <utils/guc.h>

#include "tsbgw_counter.h"

#define TSBGW_COUNTER_STATE_NAME "tsbgw_counter_state"

int			guc_max_bgw_processes = 8;

/*
 * We need a bit of shared state here to deal with keeping track of
 * the total number of background workers we've launched across the instance
 * since we don't want to exceed some configured value.
 * We were excited, briefly, about the possibility of using pg_sema for this,
 * unfortunately it does not appear to be accessible to code outside of postgres
 * core in any meaningful way. So we'r not using that.
 */


typedef struct TsbgwCounterState
{
	slock_t		mutex;			/* controls modification of total_workers */
	int			total_workers;
}			TsbgwCounterState;


static TsbgwCounterState * tsbgw_ct = NULL;
static void
tsbgw_counter_state_init(bool reinit)
{
	bool		found;

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	tsbgw_ct = ShmemInitStruct(TSBGW_COUNTER_STATE_NAME, sizeof(TsbgwCounterState), &found);
	if (!found || reinit)		/* initialize the shared memory structure */
	{
		memset(tsbgw_ct, 0, sizeof(TsbgwCounterState));
		SpinLockInit(&tsbgw_ct->mutex);
		tsbgw_ct->total_workers = 0;
	}
	LWLockRelease(AddinShmemInitLock);
}

extern void
tsbgw_counter_setup_gucs(void)
{

	DefineCustomIntVariable("timescaledb.max_bgw_processes",
							"Maximum background worker processes allocated to TimescaleDB",
							"Max background worker processes allocated to TimescaleDB - set to at least 1 + number of databases in Postgres instance to use background workers ",
							&guc_max_bgw_processes,
							guc_max_bgw_processes,
							0,
							max_worker_processes,
							PGC_SUSET,
							0,
							NULL,
							NULL,
							NULL);
}

/* this gets called by the loader (and therefore the postmaster) at shared_preload_libraries time*/
extern void
tsbgw_counter_shmem_alloc(void)
{
	RequestAddinShmemSpace(sizeof(TsbgwCounterState));
}
extern void
tsbgw_counter_shmem_startup(void)
{
	tsbgw_counter_state_init(false);
}

extern void
tsbgw_counter_shmem_cleanup(void)
{
	tsbgw_counter_state_init(true);
}
extern bool
tsbgw_total_workers_increment()
{
	bool		incremented = false;
	int			max_workers = guc_max_bgw_processes;

	SpinLockAcquire(&tsbgw_ct->mutex);
	if (tsbgw_ct->total_workers < max_workers)	/* result can be <=
												 * max_workers, so we test for
												 * less than */
	{
		tsbgw_ct->total_workers++;
		incremented = true;
	}
	SpinLockRelease(&tsbgw_ct->mutex);
	return incremented;
}

extern void
tsbgw_total_workers_decrement()
{
	SpinLockAcquire(&tsbgw_ct->mutex);
	tsbgw_ct->total_workers--;
	SpinLockRelease(&tsbgw_ct->mutex);
}

extern int
tsbgw_total_workers_get()
{
	int			nworkers;

	SpinLockAcquire(&tsbgw_ct->mutex);
	nworkers = tsbgw_ct->total_workers;
	SpinLockRelease(&tsbgw_ct->mutex);
	return nworkers;
}
