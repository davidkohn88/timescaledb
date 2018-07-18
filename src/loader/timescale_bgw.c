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

/* needed for getting database list*/
#include <access/heapam.h>
#include <access/htup_details.h>
#include <catalog/pg_database.h>
#include <utils/snapmgr.h>
#include <access/xact.h>

/* for calling external function*/
#include <fmgr.h>

#include <storage/procarray.h>

#include "../extension.h"
#include "../extension_utils.c"
#include "timescale_bgw.h"
#include "timescale_bgw_internal.h"
#include "bgw_message_queue.h"


#define TSBGW_SS_NAME "timescale_bgw_shared_state"
#define TSBGW_DB_SCHEDULER_FUNCNAME "timescale_bgw_db_scheduler_main"
#define TSBGW_ENTRYPOINT_FUNCNAME "timescale_bgw_db_scheduler_entrypoint"
#define TSBGW_LAUNCHER_RESTART_TIME 10
/*
 * Main bgw launcher for the cluster. Run through the timescale loader, so needs to have a
 * small footprint as any interactions it has will need to remain backwards compatible for
 * the foreseeable future.
 *
 * Notes: multiple databases in an instance (PG cluster) can have Timescale installed. They are not necessarily
 * the same version of Timescale (though they could be)
 * Shared memory is allocated and background workers are registered at shared_preload_libraries time
 * We do not know what databases exist, nor which databases Timescale is installed in (if any) at
 * shared_preload_libraries time.
 */


/*
 * We still need a bit of shared state here to deal with keeping track of
 * the total number of background workers we've launched across the instance
 * since we don't want to exceed some configured value.
 * We were excited, briefly, about the possibility of using pg_sema for this,
 * unfortunately it does not appear to be accessible to code outside of postgres
 * core in any meaningful way. So we're not using that.
 */
typedef struct tsbgwSharedState
{
	slock_t		mutex;			/* controls modification of total_workers */
	int			total_workers;
}			tsbgwSharedState;

typedef struct tsbgwHashEntry
{
	Oid			db_oid;			/* key for the hash table, must be first */
	BackgroundWorkerHandle *db_scheduler_handle;	/* needed to shut down
													 * properly */
}			tsbgwHashEntry;

static tsbgwSharedState * get_tsbgw_shared_state(bool possible_restart)
{
	static tsbgwSharedState * tsbgw_ss = NULL;
	bool		found;

	/* reset in case this is a restart within the postmaster */
	if (possible_restart)
		tsbgw_ss = NULL;
	if (tsbgw_ss == NULL)
	{
		LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
		tsbgw_ss = ShmemInitStruct(TSBGW_SS_NAME, sizeof(tsbgwSharedState), &found);
		if (!found)				/* initialize the shared memory structure */
		{
			SpinLockInit(&tsbgw_ss->mutex);
			tsbgw_ss->total_workers = 0;
		}

		LWLockRelease(AddinShmemInitLock);
	}
	return tsbgw_ss;
}


extern void
timescale_bgw_shmem_startup(void)
{
	/* possible_restart = true in case this is a restart within the postmaster */
	get_tsbgw_shared_state(true);
}


extern Datum
timescale_bgw_increment_total_workers(PG_FUNCTION_ARGS)
{
	bool		incremented = false;
	int16		max_workers = PG_GETARG_INT32(0);
	tsbgwSharedState *ss = get_tsbgw_shared_state(false);

	SpinLockAcquire(&ss->mutex);
	if (ss->total_workers < max_workers)	/* result can be <= max_workers,
											 * so we test for less than */
	{
		ss->total_workers++;
		incremented = true;
	}
	SpinLockRelease(&ss->mutex);
	PG_RETURN_BOOL(incremented);
}

extern Datum
timescale_bgw_decrement_total_workers(PG_FUNCTION_ARGS)
{
	tsbgwSharedState *ss = get_tsbgw_shared_state(false);

	SpinLockAcquire(&ss->mutex);
	ss->total_workers--;
	SpinLockRelease(&ss->mutex);
	PG_RETURN_VOID();
}

extern Datum
timescale_bgw_get_total_workers(PG_FUNCTION_ARGS)
{
	tsbgwSharedState *ss = get_tsbgw_shared_state(false);
	int			nworkers;

	SpinLockAcquire(&ss->mutex);
	nworkers = ss->total_workers;
	SpinLockRelease(&ss->mutex);
	PG_RETURN_INT32(nworkers);
}


/*
 * Register a background worker that calls the main timescaledb library (ie loader) and uses the scheduler entrypoint function
 * the scheduler entrypoint will deal with starting a new worker, and waiting on any txns that it needs to, if we pass along a vxid in the bgw_extra field of the BgWorker
 *
 */
static bool
register_tsbgw_entrypoint_for_db(Oid db_id, VirtualTransactionId vxid, BackgroundWorkerHandle **handle)
{

	BackgroundWorker worker;


	memset(&worker, 0, sizeof(worker));
	snprintf(worker.bgw_name, BGW_MAXLEN, "Timescale BGW Scheduler Entrypoint");
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	sprintf(worker.bgw_library_name, EXTENSION_NAME);
	sprintf(worker.bgw_function_name, TSBGW_ENTRYPOINT_FUNCNAME);
	worker.bgw_notify_pid = MyProcPid;
	worker.bgw_main_arg = db_id;
	memcpy(worker.bgw_extra, &vxid, sizeof(VirtualTransactionId));

	return RegisterDynamicBackgroundWorker(&worker, handle);

}

static volatile sig_atomic_t got_sigterm = false;
static void
timescale_bgw_sigterm(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_sigterm = true;
	SetLatch(MyLatch);

	errno = save_errno;
}



/*
 * Model this on autovacuum.c -> get_database_list
 * Note that we are not doing all the things around memory context that they do, because
 * a) we're using shared memory to store the list of dbs and b) we're in a function and
 * shorter lived context here.
 * This can get called at two different times 1) when the cluster launcher starts and is looking for dbs
 * and 2) if it restarts due to a postmaster signal.
 */
static HTAB *
populate_database_htab(void)
{
	Relation	rel;
	HeapScanDesc scan;
	HeapTuple	tup;
	HTAB	   *db_htab = NULL;
	HASHCTL		info;

	/*
	 * first initialize the hashtable before we start a txn, we'll be writing
	 * info about dbs there
	 */
	info = (HASHCTL)
	{
		.keysize = sizeof(Oid),
			.entrysize = sizeof(tsbgwHashEntry)
	};
	db_htab = hash_create("tsbgw_launcher_db_htab", 8, &info, HASH_BLOBS | HASH_ELEM);


	/*
	 * by this time we should already be connected to the db, and only have
	 * access to shared catalogs
	 */
	/* start a txn, see note in autovacuum.c for why */
	StartTransactionCommand();
	(void) GetTransactionSnapshot();

	rel = heap_open(DatabaseRelationId, AccessShareLock);
	scan = heap_beginscan_catalog(rel, 0, NULL);


	while (HeapTupleIsValid(tup = heap_getnext(scan, ForwardScanDirection)))
	{
		Form_pg_database pgdb = (Form_pg_database) GETSTRUCT(tup);
		tsbgwHashEntry *tsbgw_he;
		Oid			db_oid;
		bool		hash_found;

		if (!pgdb->datallowconn)
			continue;			/* don't bother with dbs that don't allow
								 * connections, we'll fail when starting a
								 * worker anyway */

		db_oid = HeapTupleGetOid(tup);
		tsbgw_he = (tsbgwHashEntry *) hash_search(db_htab, &db_oid, HASH_ENTER, &hash_found);
		if (!hash_found)
			tsbgw_he->db_scheduler_handle = NULL;


	}
	heap_endscan(scan);
	heap_close(rel, AccessShareLock);
	CommitTransactionCommand();
	return db_htab;
}

static void
start_db_schedulers(HTAB *db_htab)
{
	HASH_SEQ_STATUS hash_seq;
	tsbgwHashEntry *current_entry;

	/* now scan our hash table of dbs and register a worker for each */
	hash_seq_init(&hash_seq, db_htab);


	while ((current_entry = hash_seq_search(&hash_seq)) != NULL)
	{
		bool		worker_registered = false;
		pid_t		worker_pid;
		VirtualTransactionId vxid;

		/* When called at server start, no need to wait on a vxid */
		SetInvalidVirtualTransactionId(vxid);

		worker_registered = register_tsbgw_entrypoint_for_db(current_entry->db_oid, vxid, &current_entry->db_scheduler_handle);
		if (worker_registered)
		{
			WaitForBackgroundWorkerStartup(current_entry->db_scheduler_handle, &worker_pid);
			ereport(LOG, (errmsg("Worker started with PID %d", worker_pid)));
		}
		else
			break;				/* should we complain? */


	}
}

static void
check_for_stopped_db_schedulers(HTAB *db_htab)
{
	HASH_SEQ_STATUS hash_seq;
	tsbgwHashEntry *current_entry;
	List	   *dead_oids = NIL;
	ListCell   *lc;
	bool		found;


	hash_seq_init(&hash_seq, db_htab);

	while ((current_entry = hash_seq_search(&hash_seq)) != NULL)
	{
		pid_t		worker_pid;

		if (GetBackgroundWorkerPid(current_entry->db_scheduler_handle, &worker_pid) == BGWH_STOPPED)
			dead_oids = lappend_oid(dead_oids, current_entry->db_oid);
	}

	foreach(lc, dead_oids)
	{
		Oid			dead_oid = lfirst_oid(lc);

		DirectFunctionCall1(timescale_bgw_decrement_total_workers, Int8GetDatum(0));
		hash_search(db_htab, &dead_oid, HASH_REMOVE, &found);
		Assert(found);
	}
	list_free(dead_oids);
}


static void
stop_all_db_schedulers(HTAB *db_htab)
{
	HASH_SEQ_STATUS hash_seq;
	tsbgwHashEntry *current_entry;

	hash_seq_init(&hash_seq, db_htab);



	while ((current_entry = hash_seq_search(&hash_seq)) != NULL)
	{
		BgwHandleStatus worker_status;

		TerminateBackgroundWorker(current_entry->db_scheduler_handle);
		worker_status = WaitForBackgroundWorkerShutdown(current_entry->db_scheduler_handle);
		if (worker_status == BGWH_STOPPED)
		{
			DirectFunctionCall1(timescale_bgw_decrement_total_workers, Int8GetDatum(0));
		}
		else if (worker_status == BGWH_POSTMASTER_DIED) /* bailout */
		{
			proc_exit(1);
		}

	}
}

/*
 * Drain queue, handle each message synchronously.
 */
static void
launcher_handle_messages(HTAB *db_htab)
{
	tsbgwMessage *message;


	for (message = tsbgw_message_receive(); message != NULL; message = tsbgw_message_receive())
	{
		tsbgwHashEntry *tsbgw_he;
		PGPROC	   *sender;
		bool		found = false;
		VirtualTransactionId vxid;
		bool		worker_registered;

		sender = BackendPidGetProc(message->sender_pid);
		if (sender != NULL)
			GET_VXID_FROM_PGPROC(vxid, *sender);
		else
		{
			ereport(LOG, (errmsg("timescalebgw cluster launcher received message from non-existent backend")));
			continue;
		}

		switch (message->message_type)
		{
			case START:
				ereport(LOG, (errmsg("in start condition")));

				/*
				 * we've just run check_for_stopped_schedulers, so we know if
				 * we have found this in our hashtable that it is still alive
				 * and we want to be idempotent, so don't need to do anything
				 */
				tsbgw_he = hash_search(db_htab, &message->db_oid, HASH_ENTER, &found);
				if (!found)
				{
					if (!DatumGetBool(DirectFunctionCall1(timescale_bgw_increment_total_workers, Int32GetDatum(TIMESCALE_MAX_WORKERS_GUC_STANDIN))))
						ereport(ERROR, (errmsg("timescale background worker could not be started")));
					worker_registered = register_tsbgw_entrypoint_for_db(tsbgw_he->db_oid, vxid, &tsbgw_he->db_scheduler_handle);
					if (worker_registered)
					{
						pid_t		worker_pid;

						WaitForBackgroundWorkerStartup(tsbgw_he->db_scheduler_handle, &worker_pid);
						ereport(LOG, (errmsg("Worker started with PID %d", worker_pid)));
					}
				}
				tsbgw_message_send_ack(message, true);
				break;
			case STOP:
				ereport(LOG, (errmsg("in stop condition")));

				tsbgw_he = hash_search(db_htab, &message->db_oid, HASH_REMOVE, &found);
				if (found)
				{
					TerminateBackgroundWorker(tsbgw_he->db_scheduler_handle);
					WaitForBackgroundWorkerShutdown(tsbgw_he->db_scheduler_handle);
					DirectFunctionCall1(timescale_bgw_decrement_total_workers, Int8GetDatum(0));
				}
				tsbgw_message_send_ack(message, true);
				break;

			case RESTART:
				ereport(LOG, (errmsg("in restart condition")));

				tsbgw_he = hash_search(db_htab, &message->db_oid, HASH_ENTER, &found);
				if (found)
				{
					TerminateBackgroundWorker(tsbgw_he->db_scheduler_handle);
					WaitForBackgroundWorkerShutdown(tsbgw_he->db_scheduler_handle);
				}
				else if (!DatumGetBool(DirectFunctionCall1(timescale_bgw_increment_total_workers, Int32GetDatum(TIMESCALE_MAX_WORKERS_GUC_STANDIN))))
					ereport(ERROR, (errmsg("timescale background worker could not be started")));

				tsbgw_message_send_ack(message, true);
				worker_registered = register_tsbgw_entrypoint_for_db(tsbgw_he->db_oid, vxid, &tsbgw_he->db_scheduler_handle);
				if (worker_registered)
				{
					pid_t		worker_pid;

					WaitForBackgroundWorkerStartup(tsbgw_he->db_scheduler_handle, &worker_pid);
					ereport(LOG, (errmsg("Worker started with PID %d", worker_pid)));
				}
				break;
			default:
				ereport(LOG, ((errmsg("timescalebgw cluster launcher unexpected message received."))));
		}
	}
}

extern void
timescale_bgw_cluster_launcher_main(void)
{

	tsbgwSharedState *tsbgw_ss = get_tsbgw_shared_state(false);
	HTAB	   *db_htab;


	pqsignal(SIGTERM, timescale_bgw_sigterm);
	BackgroundWorkerUnblockSignals();
	if (!DatumGetBool(DirectFunctionCall1(timescale_bgw_increment_total_workers, Int32GetDatum(TIMESCALE_MAX_WORKERS_GUC_STANDIN))))
	{
		/*
		 * should be the first thing happening so if we already exceeded our
		 * limits it means we have a limit of 0 and we should just exit
		 */
		ereport(LOG, (errmsg("Timescale Background Worker Limit Set To %d. Please set higher if you would like to use Background Workers.", TIMESCALE_MAX_WORKERS_GUC_STANDIN)));
		proc_exit(0);
	}

	/* Connect to the db, no db name yet, so can only access shared catalogs */
	BackgroundWorkerInitializeConnection(NULL, NULL);
	db_htab = populate_database_htab();

	/*
	 * don't need to take the spinlock o initialize this cause we're the only
	 * backend around right now
	 */
	tsbgw_ss->total_workers += hash_get_num_entries(db_htab);
	if (tsbgw_ss->total_workers > TIMESCALE_MAX_WORKERS_GUC_STANDIN)
	{
		ereport(LOG, (errmsg("Total databases = %d. Timescale Background Worker Limit Set To %d. Please set higher if you would like to use Background Workers.", tsbgw_ss->total_workers, TIMESCALE_MAX_WORKERS_GUC_STANDIN)));
		proc_exit(0);
	}
	start_db_schedulers(db_htab);


	while (!got_sigterm)
	{
		int			wl_rc;

		check_for_stopped_db_schedulers(db_htab);
		launcher_handle_messages(db_htab);



		wl_rc = WaitLatch(MyLatch, WL_LATCH_SET | WL_POSTMASTER_DEATH, 0, PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);
		if (wl_rc & WL_POSTMASTER_DEATH)
			proc_exit(1);		/* bailout */
		CHECK_FOR_INTERRUPTS();


	}
	stop_all_db_schedulers(db_htab);
	proc_exit(0);
}


/*
 * This can be run either from the cluster launcher at db_startup time, or in the case of an install/uninstall/update of the extension,
 * in the first case, we have no vxid that we're waiting on. In the second case, we do, because we have to wait to see whether the txn that did the alter extension succeeded. So we wait for it to finish, then we a)
 * check to see whether the version of Timescale shown as installed in the catalogs is different from the version we populated in
 * our shared hash table, then if it is b) tell the old version's db_scheduler to shut down, ideally gracefully and it will cascade any
 * shutdown events to any workers it has started then c) start a new db_scheduler worker using the updated .so  .
 *
 * TODO: Make sure no race conditions if this is called multiple times when, say, upgrading or through the sql interface.
 */


extern void
timescale_bgw_db_scheduler_entrypoint(Oid db_id)
{
	bool		ts_installed = false;
	char		version[MAX_VERSION_LEN];
	VirtualTransactionId vxid;


	/* unblock signals and use default signal handlers */
	BackgroundWorkerUnblockSignals();
	ereport(LOG, (errmsg("Worker started for Database id = %d with pid %d", db_id, MyProcPid)));
	BackgroundWorkerInitializeConnectionByOid(db_id, InvalidOid);
	ereport(LOG, (errmsg("Connected to db %d", db_id)));

	/*
	 * Wait until whatever vxid that potentially called us finishes before we
	 * get a transaction so we can see the correct state after its effects
	 */
	memcpy(&vxid, MyBgworkerEntry->bgw_extra, sizeof(VirtualTransactionId));
	if (VirtualTransactionIdIsValid(vxid))
		VirtualXactLock(vxid, true);



	/* now we can start our transaction and get the version currently
	 * installed */
	StartTransactionCommand();
	(void) GetTransactionSnapshot();

	ts_installed = extension_exists();
	if (ts_installed)
		StrNCpy(version, extension_version(), MAX_VERSION_LEN);

	/*
	 * @Mat for safety we should call extension_check() here, but unclear how
	 * to call that now
	 */
	CommitTransactionCommand();
	if (ts_installed)
	{
		char		soname[MAX_SO_NAME_LEN];
		PGFunction	versioned_scheduler_main_loop;


		snprintf(soname, MAX_SO_NAME_LEN, "%s-%s", EXTENSION_NAME, version);
		versioned_scheduler_main_loop = load_external_function(soname, TSBGW_DB_SCHEDULER_FUNCNAME, false, NULL);
		if (versioned_scheduler_main_loop == NULL)
			ereport(LOG, (errmsg("Version %s does not have a background worker, exiting.", soname)));
		else
		{
			/* essentially we morph into the versioned worker here */
			DirectFunctionCall1(versioned_scheduler_main_loop, InvalidOid);
		}

	}
	proc_exit(0);

}


/*
 * kills old background workers for updates, and starts the entrypoint worker, passing in our current virtual transaction id so that we wait to
 * start the new scheduler until after the txn that may have changed the extension has either committed or aborted.
 * Called from a SQL interface inside of a transaction. We're already connected to a db and are either installing or updating the extension.
 */
Datum
timescale_bgw_restart_db_workers(PG_FUNCTION_ARGS)
{
	tsbgwMessage *message;
	bool		success;

	message = tsbgw_message_create(RESTART, MyDatabaseId);
	success = tsbgw_message_send_and_wait(message);
	PG_RETURN_BOOL(success);
}
Datum
timescale_bgw_start_db_workers(PG_FUNCTION_ARGS)
{
	tsbgwMessage *message;
	bool		success;

	message = tsbgw_message_create(START, MyDatabaseId);
	success = tsbgw_message_send_and_wait(message);
	PG_RETURN_BOOL(success);
}
Datum
timescale_bgw_stop_db_workers(PG_FUNCTION_ARGS)
{
	tsbgwMessage *message;
	bool		success;

	message = tsbgw_message_create(STOP, MyDatabaseId);
	success = tsbgw_message_send_and_wait(message);
	PG_RETURN_BOOL(success);
}


/* called by the loader when we intercept a drop database statement, must pass in oid rather than use MyDatabaseId*/
extern void
timescale_bgw_on_db_drop(Oid dropped_oid)
{
	tsbgwMessage *message;
	bool		success;

	message = tsbgw_message_create(STOP, dropped_oid);
	success = tsbgw_message_send_and_wait(message);
}

extern void
timescale_bgw_register_cluster_launcher(void)
{
	BackgroundWorker worker;

	ereport(LOG, (errmsg("Registering Timescale BGW Launcher")));

	/* set up worker settings for our main worker */
	snprintf(worker.bgw_name, BGW_MAXLEN, "Timescale BGW Cluster Launcher");
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_restart_time = TSBGW_LAUNCHER_RESTART_TIME;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_notify_pid = 0;

	/*
	 * TODO: Fix length things to make sure we don't go over BGW_MAXLEN for so
	 * name
	 */
	sprintf(worker.bgw_library_name, EXTENSION_NAME);
	sprintf(worker.bgw_function_name, "timescale_bgw_cluster_launcher_main");


	RegisterBackgroundWorker(&worker);

}

/* this gets called by the loader (and therefore the postmaster) at shared_preload_libraries time*/
extern void
timescale_bgw_shmem_init(void)
{
	RequestAddinShmemSpace(sizeof(tsbgwSharedState));

}
