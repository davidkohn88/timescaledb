/*
 * Contains code for the db_launcher background worker as well as the child workers 
 * spawnded to run various tasks.
 * 
 *
*/

#include "postgres.h"

/* These are always necessary for a bgworker */
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"

/* More specific to our code */

#include "timescale_bgw.h"
#include "timescale_bgw_utils.c"



/* signal handlers */


/* flags set by signal handlers */
static volatile sig_atomic_t got_sighup = false;
static volatile sig_atomic_t got_sigterm = false;
static volatile sig_atomic_t got_sigusr1 = false;


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
/*
 * Signal handler for SIGHUP
 *		Set a flag to tell the main loop to reread the config file, and set
 *		our latch to wake it up.
 */
static void
timescale_bgw_sigusr1(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_sigusr1 = true;
	SetLatch(MyLatch);

	errno = save_errno;
}
extern void timescale_bgw_db_scheduler_main(Oid db_id){

    pqsignal(SIGHUP, timescale_bgw_sighup);
	pqsignal(SIGTERM, timescale_bgw_sigterm);
    pqsignal(SIGUSR1, timescale_bgw_sigusr1);
    BackgroundWorkerUnblockSignals();
    ereport(LOG, (errmsg("Versioned Worker started for Database id = %d with pid %d", db_id, MyProcPid))); 
    increment_total_workers();
    BackgroundWorkerInitializeConnectionByOid(db_id, InvalidOid);
    ereport(LOG, (errmsg("Connected to Database id = %d", db_id)));
    ereport(LOG, (errmsg("Total Workers = %d", get_total_workers())));
    decrement_total_workers();
    proc_exit(0);
};