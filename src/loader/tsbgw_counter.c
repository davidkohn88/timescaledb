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


typedef struct TsbgwCounterState
{
	slock_t		mutex;			/* controls modification of total_workers */
	int			total_workers;
}			TsbgwCounterState;

/*
 * Possible restart should be set to true only when this is called in the shmem_startup_hook.
 * The ShmemInitStruct is called either when initializing the struct or to find/map the struct in the backend.
 * The only indication of whether it has been found is the found boolean and so we have to take the lock and be ready
 * to initialize it whenever we call this. The only reason we need the possible restart to reset the state to NULL
 * is that the Postmaster maps shared memory and so, when we have to do a hard reset of shared memory (say on death of another backend),
 * it calls the shmem init hook in itself and needs to re-map shared mem because something might have been corrupted
 */
static TsbgwCounterState * tsbgw_counter_state_get(bool possible_restart)
{
	static TsbgwCounterState * tsbgw_ct = NULL;
	bool		found;

	/* reset in case this is a restart within the postmaster */
	if (possible_restart)
		tsbgw_ct = NULL;

	if (tsbgw_ct == NULL)
	{
		LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
		tsbgw_ct = ShmemInitStruct(TSBGW_COUNTER_STATE_NAME, sizeof(TsbgwCounterState), &found);
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
	RequestAddinShmemSpace(sizeof(TsbgwCounterState));
}
extern void
tsbgw_counter_shmem_startup(void)
{
	/* possible_restart = true in case this is a restart within the postmaster */
	tsbgw_counter_state_get(true);
}


extern bool
tsbgw_total_workers_increment()
{
	bool		incremented = false;
	TsbgwCounterState *cs = tsbgw_counter_state_get(false);
	int			max_workers = TSBGW_MAX_WORKERS_GUC_STANDIN;

	SpinLockAcquire(&cs->mutex);
	if (cs->total_workers < max_workers)	/* result can be <= max_workers,
											 * so we test for less than */
	{
		cs->total_workers++;
		incremented = true;
	}
	SpinLockRelease(&cs->mutex);
	return incremented;
}

extern void
tsbgw_total_workers_decrement()
{
	TsbgwCounterState *cs = tsbgw_counter_state_get(false);

	SpinLockAcquire(&cs->mutex);
	cs->total_workers--;
	SpinLockRelease(&cs->mutex);
}

extern int
tsbgw_total_workers_get()
{
	TsbgwCounterState *cs = tsbgw_counter_state_get(false);
	int			nworkers;

	SpinLockAcquire(&cs->mutex);
	nworkers = cs->total_workers;
	SpinLockRelease(&cs->mutex);
	return nworkers;
}
