/*
 * Contains code for the db_launcher background worker as well as the child workers
 * spawned to run various tasks.
 *
 *
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


/* for setting our wait event during waitlatch*/
#include <pgstat.h>
#include "extension.h"

#include "bgw_scheduler.h"



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



extern Datum
tsbgw_db_scheduler_main(PG_FUNCTION_ARGS)
{
	int			num_wakes = 0;
	PGFunction	get_unreserved = load_external_function("timescaledb", "tsbgw_num_unreserved", false, NULL);

	BackgroundWorkerBlockSignals();
	pqsignal(SIGTERM, timescale_bgw_sigterm);	/* now set up our own signal
												 * handler cause we know we've
												 * been started correctly. */
	BackgroundWorkerUnblockSignals();	/* unblock signals with default
										 * handlers for now. */
	ereport(LOG, (errmsg("Versioned Worker started for Database id = %d with pid %d", MyDatabaseId, MyProcPid)));

	while (!got_sigterm)
	{
		int			wl_rc;

		wl_rc = WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH, 5 * 1000L, PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);
		num_wakes++;
		if (wl_rc & WL_POSTMASTER_DEATH)
			proc_exit(1);

		CHECK_FOR_INTERRUPTS();
		ereport(LOG, (errmsg("Database id = %d, Wake # %d ", MyDatabaseId, num_wakes)));
		ereport(LOG, (errmsg("Unrserved Workers = %d", DatumGetInt32(DirectFunctionCall1(get_unreserved, Int8GetDatum(0))))));
	}
	ereport(LOG, (errmsg("Exiting db %d", MyDatabaseId)));
	/* TODO: Kill children when they exist */
	proc_exit(0);

};
