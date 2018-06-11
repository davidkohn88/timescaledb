/* 
 * Much like extension_utils.c, this file will be used by the versioned timescaledb extension and the loader
 * Because we want the loader not to export symbols all functions here should be static
 * and be included via #include "timescale_bgw_utils.c" instead of the regular linking process.
 * We include extension_utils.c, but add in any functions necessary for those files needing to 
 * interact with background workers here, which both the loader and versioned extension need to. 
 * These functions should maintain backwards compatibility. 
 */

#include <postgres.h>

/* BGW includes below */
/* These are always necessary for a bgworker */
#include <miscadmin.h>
#include <postmaster/bgworker.h>
#include <storage/ipc.h>
#include <storage/latch.h>
#include <storage/lwlock.h>
#include <storage/proc.h>
#include <storage/shmem.h>             


/* needed for initializing shared memory and using various locks */
#include <storage/lwlock.h>
#include <utils/hsearch.h>
#include <storage/spin.h>

/* for setting our wait event during waitlatch*/
#include <pgstat.h>
#include "extension.h"

#define TSBGW_INIT_DBS 8
#define TSBGW_MAX_DBS 64
#define TSBGW_LW_TRANCHE_NAME "timescale_bgw_hash_lock"
#define TSBGW_SS_NAME "timescale_bgw_shared_state"
#define TSBGW_HASH_NAME "timescale_bgw_shared_hash_table"
#define TSBGW_DB_SCHEDULER_FUNCNAME "timescale_bgw_db_scheduler_main"
#define TSBGW_DB_SCHEDULER_RESTART_TIME 5
/* Background Worker structs, included here as they are used by both loader and versioned extension*/

typedef struct tsbgw_shared_state {
    LWLock      *lock; /*pointer to shared hashtable lock, to protect modification */
    HTAB        *hashtable;
    slock_t     mutex; /*controls modification of total_workers*/
    int         total_workers;
} tsbgw_shared_state;


/* 
 * Not horribly happy about this, but we should do it for now. We're copying the struct from bgworker.c over here so that we can correctly allocate its size and 
 * pass it between procs in shared memory. Otherwise we can only access it with a pointer and that doesn't work between procs. It is meant to be opaque to the 
 * reader because it could change implementation, so we may at some point have to do some #define magic with pg versions in order to make it work.
 */
struct BackgroundWorkerHandle
{
	int			slot;
	uint64		generation;
};

typedef struct tsbgw_hash_entry {
    Oid                         db_oid; /* key for the hash table, must be first */
    bool                        ts_installed;
    char                        ts_version[MAX_VERSION_LEN];
    bool                        valid_db_scheduler_handle; /*because zero is a valid value for db_scheduler_handles things, we should have a bool saying whether we've actually populated this field*/
    BackgroundWorkerHandle      db_scheduler_handle; /* needed to shut down properly */
    int                         num_active_jobs; /* this is for the number of workers started for active jobs, not scheduler workers */
} tsbgw_hash_entry;


static tsbgw_shared_state* get_tsbgw_shared_state(bool possible_restart){
	static tsbgw_shared_state *tsbgw_ss = NULL;
	bool found;
    /* reset in case this is a restart within the postmaster */
	if (possible_restart)
   		tsbgw_ss = NULL;
	if (tsbgw_ss == NULL)
    {
		LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

		tsbgw_ss = ShmemInitStruct(TSBGW_SS_NAME, sizeof(tsbgw_shared_state), &found);

		if(!found) /* initialize the shared memory structure*/
		{
 			HASHCTL     info;
			memset(&info, 0, sizeof(info));
			info.keysize = sizeof(Oid);
			info.entrysize = sizeof(tsbgw_hash_entry);
			tsbgw_ss->lock = &(GetNamedLWLockTranche(TSBGW_LW_TRANCHE_NAME))->lock;
			tsbgw_ss->hashtable = ShmemInitHash(TSBGW_HASH_NAME, TSBGW_INIT_DBS, TSBGW_MAX_DBS, &info, HASH_ELEM);
			SpinLockInit(&tsbgw_ss->mutex);
			tsbgw_ss->total_workers = 0; 
		}

		LWLockRelease(AddinShmemInitLock);
	}
	return tsbgw_ss;
}

/*
 * Wait for a background worker to stop. With a timeout. Modified version of WaitForBackgroundWorkerShutdown in bgworker.c.
 *
 * If the worker hasn't yet started, or is running, we wait for it to stop
 * and then return BGWH_STOPPED.  However, if the postmaster has died, we give
 * up and return BGWH_POSTMASTER_DIED, because it's the postmaster that
 * notifies us when a worker's state changes. We check on a timeout in case we haven't been notified, because unlike
 * the normal version we haven't necessarily registered to be notified of BGWorkerShutdown.
 * timeout is in seconds
 */

#define TSBGW_CHECK_TIMEOUT 1
static BgwHandleStatus WaitForBackgroundWorkerShutdownWithTimeout(BackgroundWorkerHandle *handle, int timeout)
{
	BgwHandleStatus status;
	int			rc;
    pid_t		pid;
    int         timing;

	for (timing = 0; timing < timeout; timing += TSBGW_CHECK_TIMEOUT)
	{
		CHECK_FOR_INTERRUPTS();

		status = GetBackgroundWorkerPid(handle, &pid);
		if (status == BGWH_STOPPED)
			break;

		rc = WaitLatch(MyLatch, WL_LATCH_SET | WL_POSTMASTER_DEATH | WL_TIMEOUT, TSBGW_CHECK_TIMEOUT * 1000L, WAIT_EVENT_BGWORKER_SHUTDOWN);

		if (rc & WL_POSTMASTER_DEATH)
		{
			status = BGWH_POSTMASTER_DIED;
			break;
		}
        
		ResetLatch(MyLatch);
	}

	return status;
}
static void increment_total_workers(void)  {
    volatile tsbgw_shared_state *ss = (volatile tsbgw_shared_state *) get_tsbgw_shared_state(false);
    SpinLockAcquire(&ss->mutex);
    ss->total_workers++;
    SpinLockRelease(&ss->mutex);
}
static void decrement_total_workers(void)  {
    volatile tsbgw_shared_state *ss = (volatile tsbgw_shared_state *) get_tsbgw_shared_state(false);
    SpinLockAcquire(&ss->mutex);
    ss->total_workers--;
    SpinLockRelease(&ss->mutex);
}
static int get_total_workers(void){
    volatile tsbgw_shared_state *ss = (volatile tsbgw_shared_state *) get_tsbgw_shared_state(false);
    int nworkers;
    SpinLockAcquire(&ss->mutex);
    nworkers = ss->total_workers;
    SpinLockRelease(&ss->mutex);
    return nworkers;
} 

