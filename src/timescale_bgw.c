/*
 * Contains code for the db_launcher background worker as well as the child workers 
 * spawned to run various tasks.
 * 
 *
*/
#include <postgres.h>

#include "timescale_bgw_utils.c"


/* for setting our wait event during waitlatch*/
#include <pgstat.h>
#include "timescale_bgw.h"



/* flags set by signal handlers */
static volatile sig_atomic_t got_sighup = false;
static volatile sig_atomic_t got_sigterm = false;


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


extern void timescale_bgw_db_scheduler_main(Oid db_id){
    tsbgw_hash_entry        *tsbgw_he;
    bool                    hash_found=false;
    tsbgw_shared_state      *tsbgw_ss = get_tsbgw_shared_state(false);
    int                     num_wakes = 0;

    
    BackgroundWorkerUnblockSignals(); /*unblock signals with default handlers for now. */
    ereport(LOG, (errmsg("Versioned Worker started for Database id = %d with pid %d", db_id, MyProcPid))); 
    increment_total_workers();
    BackgroundWorkerInitializeConnectionByOid(db_id, InvalidOid);
    ereport(LOG, (errmsg("Connected to Database id = %d", db_id)));
   /* 
     * now look up our hash_entry, make sure we take an exclusive lock on the table even though we're just getting our entry, 
     * because we might need to populate the hash table if it doesn't exist. When we're updating it later, we can use a shared 
     * lock as we're the only folks accessing. 
     */
    LWLockAcquire(tsbgw_ss->lock, LW_EXCLUSIVE);
    ereport(LOG, (errmsg("Got lock")));
    tsbgw_he = hash_search(tsbgw_ss->hashtable, &db_id, HASH_FIND, &hash_found);
    LWLockRelease(tsbgw_ss->lock);
    if (!hash_found)
    {
        /* 
        * We don't have an entry in the hash table for our db id, which is impossible if we've been started from the entrypoint, 
        * this means we've been restarted by the postmaster for some reason (ie improper exit of another proc)
        * So we should call the entry point function to re launch ourselves in the proper way and then shut down. 
        * Right now we're calling the function to launch another worker, which is a bit inefficient because it's going to start up, 
        * then launch another worker, we could probably just transmute ourselves into the other worker, but it gets a little complicated
        * because we need to be able to call that function from the loader context and because of vxid in bgw_extra issues.
        *  For now this seems like the safe, though less efficient choice. 
        */
        BackgroundWorkerHandle 				*worker_handle = NULL;
		bool								worker_registered = false;
		pid_t								worker_pid;
		VirtualTransactionId				vxid;
		
		SetInvalidVirtualTransactionId(vxid);

		worker_registered = register_tsbgw_entrypoint_for_db(db_id, vxid, &worker_handle);

		if (worker_registered){
			WaitForBackgroundWorkerStartup(worker_handle, &worker_pid);
			ereport(LOG, (errmsg("TimescaleDB entrypoint worker started with PID %d", worker_pid )));
		} 
        else
            ereport(ERROR, (errmsg("TimescaleDB entrypoint worker failed to start")));
        
        proc_exit(0);
    }
    BackgroundWorkerBlockSignals();
    pqsignal(SIGTERM, timescale_bgw_sigterm); /*now set up our own signal handler cause we know we've been started correctly.*/
    BackgroundWorkerUnblockSignals();
    
    while(!got_sigterm){
        int wl_rc;

        wl_rc = WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH, TSBGW_DB_SCHEDULER_RESTART_TIME * 1000L, PG_WAIT_EXTENSION );
        ResetLatch(MyLatch);
        num_wakes++;
        if (wl_rc & WL_POSTMASTER_DEATH)
            proc_exit(1);
        
        CHECK_FOR_INTERRUPTS();
        ereport(LOG, (errmsg("Database id = %d, Wake # %d ", db_id, num_wakes)));
        ereport(LOG, (errmsg("Total Workers = %d", get_total_workers())));
    }
    ereport(LOG, (errmsg("Exiting db %d", db_id)));
    proc_exit(1);
 
};