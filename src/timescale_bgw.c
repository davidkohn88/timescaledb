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
#include "extension.h"


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

    pqsignal(SIGTERM, timescale_bgw_sigterm); /*now set up our own signal handler cause we know we've been started correctly.*/
    BackgroundWorkerUnblockSignals(); /*unblock signals with default handlers for now. */
    ereport(LOG, (errmsg("Versioned Worker started for Database id = %d with pid %d", db_id, MyProcPid))); 
    increment_total_workers();
    BackgroundWorkerInitializeConnectionByOid(db_id, InvalidOid);
    ereport(LOG, (errmsg("Connected to Database id = %d", db_id)));


    LWLockAcquire(tsbgw_ss->lock, LW_SHARED);
    tsbgw_he = hash_search(tsbgw_ss->hashtable, &db_id, HASH_FIND, &hash_found);
    LWLockRelease(tsbgw_ss->lock);
    if (!hash_found)
        ereport(ERROR,(errmsg("No entry found in shared hashtable for this db_id = %d", db_id)));
    
    
    while(!got_sigterm)
    {
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
    /* TODO: Kill children when they exist*/
    decrement_total_workers();
    proc_exit(1);
 
};