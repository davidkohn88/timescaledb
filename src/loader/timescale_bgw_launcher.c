/*
 * Main bgw launcher for the cluster. Run through the timescale loader, so needs to have a 
 * small footprint as any interactions it has will need to remain backwards compatible for 
 * the foreseeable future. 
 * 
 * Notes: multiple databases in a PG Cluster can have Timescale installed. They are not necessarily 
 * the same version of Timescale (though they could be)
 * Shared memory is allocated and background workers are registered at shared_preload_libraries time
 * We do not know what databases exist, nor which databases Timescale is installed in (if any) at
 * shared_preload_libraries time. 
 * 
 * It contains code that will be called by the loader at two points
 *  1) Initialize a shared memory hash table as well as a lock for that hash table. 
 *  2) Start a cluster launcher that gets the dbs in the cluster, and starts a worker for each
 *   of them. 
 * 
 *
 * 
 * 
 * 
*/

#include <postgres.h>

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
#include <access/xact.h>
#include <access/heapam.h>
#include <access/htup_details.h>
#include <utils/snapmgr.h>

/* needed for initializing shared memory and using various locks */
#include <utils/hsearch.h>
#include <storage/spin.h>

#include "timescale_bgw_launcher.h"

#define TSBGW_INIT_DBS 8
#define TSBGW_MAX_DBS 64
#define TSBGW_LW_TRANCHE_NAME "timescale_bgw_hash_lock"
#define TSBGW_SS_NAME "timescale_bgw_shared_state"
#define TSBGW_HASH_NAME "timescale_bgw_shared_hash_table"

/* flags set by signal handlers */
static volatile sig_atomic_t got_sighup = false;
static volatile sig_atomic_t got_sigterm = false;

static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

static tsbgw_shared_state *tsbgw_ss = NULL;

/*
 * Signal handler for SIGTERM
 *		Set a flag to let the main loop to terminate, and set our latch to wake
 *		it up.
 */
static void
timescale_bgw_sigterm(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_sigterm = true;
	SetLatch(MyLatch);

	errno = save_errno;
}

/*
 * Signal handler for SIGHUP
 *		Set a flag to tell the main loop to reread the config file, and set
 *		our latch to wake it up.
 */
static void
timescale_bgw_sighup(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_sighup = true;
	SetLatch(MyLatch);

	errno = save_errno;
}


extern void register_timescale_bgw_launcher(void) {
    BackgroundWorker worker;

    ereport(LOG, (errmsg("Registering Timescale BGW Launcher")));

    /*set up worker settings for our main worker */
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_restart_time = BGW_NEVER_RESTART;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    worker.bgw_notify_pid = 0;
    /* TODO: maybe pass in the library name, we know it, one assumes, as we are in 
    shared-preload-libraries when we call this. The problem could be that otherwise
    a specific versioned library of timescaledb in shared preload libraries will 
    break this otherwise?*/
    sprintf(worker.bgw_library_name, "timescaledb");
    sprintf(worker.bgw_function_name , "timescale_bgw_launcher_main");
    snprintf(worker.bgw_name, BGW_MAXLEN, "timescale_bgw_launcher");

    RegisterBackgroundWorker(&worker);
    
}

static Size tsbgw_memsize(void){
    Size    size;

    size = MAXALIGN(sizeof(tsbgw_shared_state));
    size = add_size(size, hash_estimate_size(TSBGW_MAX_DBS, sizeof(tsbgw_hash_entry)));
    return size;
}



/* this gets called when shared memory is initialized in a backend (shmem_startup_hook)
 * based on pg_stat_statements.c*/
static void timescale_bgw_shmem_startup(void){
    bool        found;
    

    if (prev_shmem_startup_hook)
        prev_shmem_startup_hook();


    /* reset in case this is a restart within the postmaster */
    tsbgw_ss = NULL;
    
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

/* 
 * Model this on autovacuum.c -> get_database_list
 * Note that we are not doing all the things around memory context that they do, because 
 * a) we're using shared memory to store the list of dbs and b) we're in a function and 
 * shorter lived context here. 
 */
static void populate_database_htab(void){
    Relation        rel;
    HeapScanDesc    scan;
    HeapTuple       tup;
    
    
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
        LWLockAcquire(tsbgw_ss->lock, LW_EXCLUSIVE);

        tsbgw_he = (tsbgw_hash_entry *) hash_search(tsbgw_ss->hashtable, &db_oid, HASH_ENTER, &hash_found);
        if (!hash_found)
            tsbgw_he->ts_installed = FALSE;
            snprintf(tsbgw_he->ts_version, NAMEDATALEN, "");
            tsbgw_he->db_launcher_pid = 0;
            tsbgw_he->num_active_workers = 0; 

        LWLockRelease(tsbgw_ss->lock);
    }

    heap_endscan(scan);
    heap_close(rel, AccessShareLock);
    CommitTransactionCommand();


}
static void increment_total_workers(void)  {
    volatile tsbgw_shared_state *ss = (volatile tsbgw_shared_state *) tsbgw_ss;
    SpinLockAcquire(&ss->mutex);
    ss->total_workers++;
    SpinLockRelease(&ss->mutex);
}
static void decrement_total_workers(void)  {
    volatile tsbgw_shared_state *ss = (volatile tsbgw_shared_state *) tsbgw_ss;
    SpinLockAcquire(&ss->mutex);
    ss->total_workers--;
    SpinLockRelease(&ss->mutex);
}
static int get_total_workers(void){
    volatile tsbgw_shared_state *ss = (volatile tsbgw_shared_state *) tsbgw_ss;
    int nworkers;
    SpinLockAcquire(&ss->mutex);
    nworkers = ss->total_workers;
    SpinLockRelease(&ss->mutex);
    return nworkers;
} 

extern void timescale_bgw_launcher_main(void) {
    
    BackgroundWorker        worker;
    HASH_SEQ_STATUS         hash_seq;
    tsbgw_hash_entry        *current_entry;


    /* Establish signal handlers before unblocking signals. */
	pqsignal(SIGHUP, timescale_bgw_sighup);
	pqsignal(SIGTERM, timescale_bgw_sigterm);
    BackgroundWorkerUnblockSignals();
    increment_total_workers();  
    
    /* Connect to the db, no db name yet, so can only access shared catalogs*/
    BackgroundWorkerInitializeConnection(NULL, NULL);
    ereport(LOG, (errmsg("Timescale BGW Launcher Connected To DB")));
    populate_database_htab();
    ereport(LOG, (errmsg("Database HTAB Populated")));

    /*common parameters for all our scheduler workers*/
    memset(&worker, 0, sizeof(worker));
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_restart_time = BGW_NEVER_RESTART;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    sprintf(worker.bgw_library_name, "timescaledb");
    sprintf(worker.bgw_function_name , "timescale_bgw_db_scheduler_main");
    worker.bgw_notify_pid = MyProcPid;
    
    /*now scan our hash table of dbs and register a worker for each*/
    LWLockAcquire(tsbgw_ss->lock, AccessShareLock);
    hash_seq_init(&hash_seq, tsbgw_ss->hashtable);
    
    while ((current_entry = hash_seq_search(&hash_seq)) != NULL)
    {
        BackgroundWorkerHandle  *handle;
        BgwHandleStatus         status;
        pid_t                   worker_pid;

        ereport(LOG, (errmsg("On hash entry for db Oid = %d", current_entry->db_oid)));
        worker.bgw_main_arg = current_entry->db_oid;
        RegisterDynamicBackgroundWorker(&worker, &handle);
        status = WaitForBackgroundWorkerStartup(handle, &worker_pid);
        ereport(LOG, (errmsg("Worker started with PID %d", worker_pid)));
    }
    LWLockRelease(tsbgw_ss->lock);
    decrement_total_workers();
    ereport(LOG, (errmsg("Cluster Launcher shutting down")));

    
}

extern void timescale_bgw_db_scheduler_main(Oid db_id){
    tsbgw_hash_entry        *hash_entry;
    bool                    entry_found;

    pqsignal(SIGHUP, timescale_bgw_sighup);
	pqsignal(SIGTERM, timescale_bgw_sigterm);
    BackgroundWorkerUnblockSignals();
    ereport(LOG, (errmsg("Worker started for Database id = %d with pid %d", db_id, MyProcPid))); 
    increment_total_workers();
    BackgroundWorkerInitializeConnectionByOid(db_id, NULL);
    ereport(LOG, (errmsg("Connected to Database id = %d", db_id)));
    ereport(LOG, (errmsg("Total Workers = %d", get_total_workers())));
    /*db scheduler can access shared memory and update pid with only a shared lock as there's only one worker per db*/
    LWLockAcquire(tsbgw_ss->lock, AccessShareLock);
    hash_entry = hash_search(tsbgw_ss->hashtable, &db_id, HASH_ENTER, &entry_found);
    if (entry_found)
    {
        hash_entry->db_launcher_pid = MyProcPid;
    }
    LWLockRelease(tsbgw_ss->lock);
    decrement_total_workers();

}
/* this gets called by the loader (and therefore the postmaster) at shared_preload_libraries time*/
extern void timescale_bgw_shmem_init(void){
    RequestAddinShmemSpace(tsbgw_memsize());
    RequestNamedLWLockTranche(TSBGW_LW_TRANCHE_NAME, 1);
    prev_shmem_startup_hook = shmem_startup_hook;
    shmem_startup_hook = timescale_bgw_shmem_startup;
}