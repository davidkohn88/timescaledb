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

typedef struct tsbgw_hash_entry {
    Oid                         db_oid; /* key for the hash table, must be first */
    bool                        ts_installed;
    char                        ts_version[MAX_VERSION_LEN];
    BackgroundWorkerHandle      *db_scheduler_handle; /* needed to shut down properly */
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

