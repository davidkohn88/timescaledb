/* needed for initializing shared memory and using various locks */
#include <postgres.h>

#include <miscadmin.h>
#include <storage/lwlock.h>
#include <utils/hsearch.h>
#include <storage/spin.h>
#include <storage/shmem.h>
#include <storage/ipc.h>
#include <storage/latch.h>

#include "tsbgw_counter.h"

#define TSBGW_COUNTER_STATE_NAME "tsbgw_counter_state"
/*
 * We need a bit of shared state here to deal with keeping track of
 * the total number of background workers we've launched across the instance
 * since we don't want to exceed some configured value.
 * We were excited, briefly, about the possibility of using pg_sema for this,
 * unfortunately it does not appear to be accessible to code outside of postgres
 * core in any meaningful way. So we're not using that.
 */


typedef struct tsbgwCounterState
{
	slock_t		mutex;			/* controls modification of total_workers */
	int			total_workers;
}			tsbgwCounterState;


static tsbgwCounterState * tsbgw_counter_state_get(bool possible_restart){
	static tsbgwCounterState * tsbgw_ct = NULL;
	bool		found;

	/* reset in case this is a restart within the postmaster */
	if (possible_restart)
		tsbgw_ct = NULL;
	if (tsbgw_ct == NULL)
	{
		LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
		tsbgw_ct = ShmemInitStruct(TSBGW_COUNTER_STATE_NAME, sizeof(tsbgwCounterState), &found);
		if (!found)				/* initialize the shared memory structure */
		{
			SpinLockInit(&tsbgw_ct->mutex);
			tsbgw_ct->total_workers = 0;
		}

		LWLockRelease(AddinShmemInitLock);
	}
	return tsbgw_ct;
}

/* this gets called by the loader (and therefore the postmaster) at shared_preload_libraries time*/
extern void
tsbgw_counter_shmem_alloc(void)
{
	RequestAddinShmemSpace(sizeof(tsbgwCounterState));
}
extern void tsbgw_counter_shmem_startup(void)
{
	/* possible_restart = true in case this is a restart within the postmaster */
	tsbgw_counter_state_get(true);
}


extern bool tsbgw_total_workers_increment(int max_workers)
{
	bool		incremented = false;
	tsbgwCounterState *ss = tsbgw_counter_state_get(false);

	SpinLockAcquire(&ss->mutex);
	if (ss->total_workers < max_workers)	/* result can be <= max_workers,
											 * so we test for less than */
	{
		ss->total_workers++;
		incremented = true;
	}
	SpinLockRelease(&ss->mutex);
	return incremented;
}

extern void tsbgw_total_workers_decrement()
{
	tsbgwCounterState *ss = tsbgw_counter_state_get(false);

	SpinLockAcquire(&ss->mutex);
	ss->total_workers--;
	SpinLockRelease(&ss->mutex);
}

extern int tsbgw_total_workers_get()
{
	tsbgwCounterState *ss = tsbgw_counter_state_get(false);
	int			nworkers;

	SpinLockAcquire(&ss->mutex);
	nworkers = ss->total_workers;
	SpinLockRelease(&ss->mutex);
	return nworkers;
}
