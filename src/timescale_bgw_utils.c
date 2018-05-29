/* 
 * Much like extension_utils.c, this file will be used by the versioned timescaledb extension and the loader
 * Because we want the loader not to export symbols all functions here should be static
 * and be included via #include "timescale_bgw_utils.c" instead of the regular linking process.
 * We include extension_utils.c, but add in any functions necessary for those files needing to 
 * interact with background workers here, which both the loader and versioned extension need to. 
 * These functions should maintain backwards compatibility. 
 */

/* BGW includes below */
/* These are always necessary for a bgworker */
#include <miscadmin.h>
#include <postmaster/bgworker.h>
#include <storage/ipc.h>
#include <storage/latch.h>
#include <storage/lwlock.h>
#include <storage/proc.h>
#include <storage/shmem.h>             


/* needed for getting database list*/
#include <catalog/pg_database.h>
#include <access/heapam.h>
#include <access/htup_details.h>
#include <utils/snapmgr.h>

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

/* 
 * Model this on autovacuum.c -> get_database_list
 * Note that we are not doing all the things around memory context that they do, because 
 * a) we're using shared memory to store the list of dbs and b) we're in a function and 
 * shorter lived context here. 
 * This can get called at two different times 1) when the cluster launcher starts and is looking for dbs
 * and 2) if the cluster is reinitialized and a db_scheduler is restarted, but shmem has been cleared and therefore we need to redo population of the htab. 
 */
static void populate_database_htab(void){
    Relation        rel;
    HeapScanDesc    scan;
    HeapTuple       tup;
    tsbgw_shared_state  *tsbgw_ss=get_tsbgw_shared_state(false);
    
    
    /* by this time we should already be connected to the db, and only have access to shared catalogs*/
    /*start a txn, see note in autovacuum.c for why*/
    StartTransactionCommand();
    (void) GetTransactionSnapshot();

    rel = heap_open(DatabaseRelationId, AccessShareLock);
    scan = heap_beginscan_catalog(rel, 0, NULL);
    
    while (HeapTupleIsValid(tup = heap_getnext(scan, ForwardScanDirection)))
    {
        Form_pg_database    pgdb = (Form_pg_database) GETSTRUCT(tup);
        tsbgw_hash_entry    *tsbgw_he;
        Oid                 db_oid;
        bool                hash_found;
        
        if (!pgdb->datallowconn) 
            continue; /* don't bother with dbs that don't allow connections, we'll fail when starting a worker anyway*/
        
        db_oid = HeapTupleGetOid(tup);
        if (hash_get_num_entries(tsbgw_ss->hashtable) >= TSBGW_MAX_DBS)
            ereport(FATAL, (errmsg("More databases in cluster than allocated in shared memory, stopping cluster launcher")));
            
        /*acquire lock so we can access hash table*/
        tsbgw_he = (tsbgw_hash_entry *) hash_search(tsbgw_ss->hashtable, &db_oid, HASH_ENTER, &hash_found);
        if (!hash_found)
            tsbgw_he->ts_installed = FALSE;
            snprintf(tsbgw_he->ts_version, MAX_VERSION_LEN, "");
            tsbgw_he->db_scheduler_handle = NULL;
            tsbgw_he->num_active_jobs = 0; 

       
    }
    heap_endscan(scan);
    heap_close(rel, AccessShareLock);
    CommitTransactionCommand();
}

/*
 * Register a background worker that calls the main timescaledb library (ie loader) and uses the scheduler entrypoint function
 * the scheduler entrypoint will deal with starting a new worker, and waiting on any txns that it needs to, if we pass along a vxid in the bgw_extra field of the BgWorker
 * 
 */
static bool register_tsbgw_entrypoint_for_db(Oid db_id, VirtualTransactionId vxid, BackgroundWorkerHandle **handle) {

    BackgroundWorker        worker;
    
    
    memset(&worker, 0, sizeof(worker));
    snprintf(worker.bgw_name, BGW_MAXLEN, "Timescale BGW Entrypoint DB %d", db_id);
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_restart_time = BGW_NEVER_RESTART;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    sprintf(worker.bgw_library_name, "timescaledb");
    sprintf(worker.bgw_function_name , "timescale_bgw_db_scheduler_entrypoint");
    worker.bgw_notify_pid = MyProcPid;
    worker.bgw_main_arg = db_id;
    memcpy(worker.bgw_extra, &vxid, sizeof(VirtualTransactionId));
    
    return RegisterDynamicBackgroundWorker(&worker, handle);
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

