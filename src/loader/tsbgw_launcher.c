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

#include <storage/ipc.h>		/* for setting proc_exit callbacks */

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
#include "tsbgw_counter.h"
#include "tsbgw_message_queue.h"
#include "tsbgw_launcher.h"

#define TSBGW_DB_SCHEDULER_FUNCNAME "tsbgw_db_scheduler_main"
#define TSBGW_ENTRYPOINT_FUNCNAME "tsbgw_db_scheduler_entrypoint"

#ifdef DEBUG
#define TSBGW_LAUNCHER_RESTART_TIME 0
#else
#define TSBGW_LAUNCHER_RESTART_TIME 10
#endif



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


typedef struct TsbgwHashEntry
{
	Oid			db_oid;			/* key for the hash table, must be first */
	BackgroundWorkerHandle *db_scheduler_handle;	/* needed to shut down
													 * properly */
}			TsbgwHashEntry;


/*
 * aliasing a few things in bgWorker.h so that we exit correctly on postmaster death so we don't have to duplicate code
 * basically telling it we shouldn't call exit hooks cause we want to bail out quickly - similar to how
 * the quickdie function works when we receive a sigquit. This should work similarly because postmaster death is a similar
 * severity of issue.
 * Additionally, we're wrapping these calls to make sure we never have a NULL handle, if we have a null handle, we return  normal things.
 * I have left the capitalization in their format to make clear the relationship to the functions in bgworker.h and that
 * these are just passthroughs and should maintain the same behavior unless BGWH_POSTMASTER_DIED is returned.
 */

static inline void
tsbgw_on_postmaster_death(void)
{
	on_exit_reset();			/* don't call exit hooks cause we want to bail
								 * out quickly */
	ereport(FATAL,
			(errcode(ERRCODE_ADMIN_SHUTDOWN),
			 errmsg("postmaster exited while pg_sleep(0.1) was working")));
}
static inline BgwHandleStatus
ts_GetBackgroundWorkerPid(BackgroundWorkerHandle *handle, pid_t *pidp)
{
	BgwHandleStatus status;

	if (handle == NULL)
		status = BGWH_STOPPED;
	else
		status = GetBackgroundWorkerPid(handle, pidp);

	if (status == BGWH_POSTMASTER_DIED)
		tsbgw_on_postmaster_death();
	return status;

}
static inline BgwHandleStatus
ts_WaitForBackgroundWorkerStartup(BackgroundWorkerHandle *handle, pid_t *pidp)
{
	BgwHandleStatus status;

	if (handle == NULL)
		status = BGWH_STOPPED;
	else
		status = WaitForBackgroundWorkerStartup(handle, pidp);

	if (status == BGWH_POSTMASTER_DIED)
		tsbgw_on_postmaster_death();
	return status;
}
static inline BgwHandleStatus
ts_WaitForBackgroundWorkerShutdown(BackgroundWorkerHandle *handle)
{
	BgwHandleStatus status;

	if (handle == NULL)
		status = BGWH_STOPPED;
	else
		status = WaitForBackgroundWorkerShutdown(handle);

	if (status == BGWH_POSTMASTER_DIED)
		tsbgw_on_postmaster_death();
	return status;
}

static inline void
ts_TerminateBackgroundWorker(BackgroundWorkerHandle *handle)
{
	if (handle == NULL)
		return;
	else
		return TerminateBackgroundWorker(handle);
}
extern void
tsbgw_cluster_launcher_register(void)
{
	BackgroundWorker worker;


	/* set up worker settings for our main worker */
	snprintf(worker.bgw_name, BGW_MAXLEN, "Timescale BGW Cluster Launcher");
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_restart_time = TSBGW_LAUNCHER_RESTART_TIME;

	/*
	 * Starting at BgWorkerStart_RecoveryFinished means we won't ever get
	 * started on a hot_standby see
	 * https://www.postgresql.org/docs/10/static/bgworker.html as it's not
	 * documented in bgworker.c.
	 */
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_notify_pid = 0;
	snprintf(worker.bgw_library_name, BGW_MAXLEN, EXTENSION_NAME);
	snprintf(worker.bgw_function_name, BGW_MAXLEN, "tsbgw_cluster_launcher_main");

	RegisterBackgroundWorker(&worker);

}


/*
 * Register a background worker that calls the main timescaledb library (ie loader) and uses the scheduler entrypoint function
 * the scheduler entrypoint will deal with starting a new worker, and waiting on any txns that it needs to, if we pass along a vxid in the bgw_extra field of the BgWorker
 *
 */
static bool
register_entrypoint_for_db(Oid db_id, VirtualTransactionId vxid, BackgroundWorkerHandle **handle)
{

	BackgroundWorker worker;


	memset(&worker, 0, sizeof(worker));
	snprintf(worker.bgw_name, BGW_MAXLEN, "Timescale BGW Scheduler Entrypoint");
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	snprintf(worker.bgw_library_name, BGW_MAXLEN, EXTENSION_NAME);
	snprintf(worker.bgw_function_name, BGW_MAXLEN, TSBGW_ENTRYPOINT_FUNCNAME);
	worker.bgw_notify_pid = MyProcPid;
	worker.bgw_main_arg = db_id;
	memcpy(worker.bgw_extra, &vxid, sizeof(VirtualTransactionId));

	return RegisterDynamicBackgroundWorker(&worker, handle);

}


/*
 * Model this on autovacuum.c -> get_database_list
 * Note that we are not doing all the things around memory context that they do, because
 * the hashtable we're using to store db entries is automatically created in its own memory context (a child of TopMemoryContext)
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
			.entrysize = sizeof(TsbgwHashEntry)
	};
	db_htab = hash_create("tsbgw_launcher_db_htab", 8, &info, HASH_BLOBS | HASH_ELEM);


	/*
	 * by this time we should already be connected to the db, and only have
	 * access to shared catalogs
	 */
	StartTransactionCommand();
	(void) GetTransactionSnapshot();

	rel = heap_open(DatabaseRelationId, AccessShareLock);
	scan = heap_beginscan_catalog(rel, 0, NULL);

	while (HeapTupleIsValid(tup = heap_getnext(scan, ForwardScanDirection)))
	{
		Form_pg_database pgdb = (Form_pg_database) GETSTRUCT(tup);
		TsbgwHashEntry *tsbgw_he;
		Oid			db_oid;
		bool		hash_found;

		if (!pgdb->datallowconn)
			continue;			/* don't bother with dbs that don't allow
								 * connections, we'll fail when starting a
								 * worker anyway */

		db_oid = HeapTupleGetOid(tup);
		tsbgw_he = (TsbgwHashEntry *) hash_search(db_htab, &db_oid, HASH_ENTER, &hash_found);
		if (!hash_found)
			tsbgw_he->db_scheduler_handle = NULL;


	}
	heap_endscan(scan);
	heap_close(rel, AccessShareLock);
	CommitTransactionCommand();
	return db_htab;
}



/*
 * We want to avoid the race condition where we have enough workers allocated to start schedulers
 * for all databases, but before we could get all of them started, the (say) first scheduler has
 * started too many jobs and then we don't have enough schedulers for the dbs. So we need to first
 * get the number of dbs for which we may need schedulers, then reserve the correct number of workers,
 * then start the workers.
 */
static void
start_db_schedulers(HTAB *db_htab)
{
	HASH_SEQ_STATUS hash_seq;
	TsbgwHashEntry *current_entry;


	/* now scan our hash table of dbs and register a worker for each */
	hash_seq_init(&hash_seq, db_htab);


	while ((current_entry = hash_seq_search(&hash_seq)) != NULL)
	{
		bool		worker_registered = false;
		pid_t		worker_pid;
		VirtualTransactionId vxid;

		/* When called at server start, no need to wait on a vxid */
		SetInvalidVirtualTransactionId(vxid);

		worker_registered = register_entrypoint_for_db(current_entry->db_oid, vxid, &current_entry->db_scheduler_handle);
		if (worker_registered)
		{
			ts_WaitForBackgroundWorkerStartup(current_entry->db_scheduler_handle, &worker_pid);
		}
		else
			break;				/* should we complain? */


	}
}

/* this is called when we're going to shut down so we don't leave things messy
* we*/
static void
launcher_pre_shmem_cleanup(int code, Datum arg)
{
	HTAB	   *db_htab = (HTAB *) DatumGetPointer(arg);
	HASH_SEQ_STATUS hash_seq;
	TsbgwHashEntry *current_entry;

	hash_seq_init(&hash_seq, db_htab);
	/* stop everyone */
	while ((current_entry = hash_seq_search(&hash_seq)) != NULL)
		ts_TerminateBackgroundWorker(current_entry->db_scheduler_handle);

	hash_destroy(db_htab);
	/* must reinitialize shared memory structs if we want to restart */
	tsbgw_message_queue_shmem_cleanup();
	tsbgw_counter_shmem_cleanup();
}


/*Garbage collector cleaning up any stopped schedulers*/
static void
stopped_db_schedulers_cleanup(HTAB *db_htab)
{
	HASH_SEQ_STATUS hash_seq;
	TsbgwHashEntry *current_entry;
	bool		found;

	hash_seq_init(&hash_seq, db_htab);

	while ((current_entry = hash_seq_search(&hash_seq)) != NULL)
	{
		pid_t		worker_pid;

		if (ts_GetBackgroundWorkerPid(current_entry->db_scheduler_handle, &worker_pid) == BGWH_STOPPED)
		{
			hash_search(db_htab, &current_entry->db_oid, HASH_REMOVE, &found);
			tsbgw_total_workers_decrement();
		}
	}

}

/* actions for message types we could receive off of the tsbgw_message_queue*/
static void
message_start_action(HTAB *db_htab, TsbgwMessage * message, VirtualTransactionId vxid)
{
	TsbgwHashEntry *tsbgw_he;
	bool		found;
	bool		worker_registered;
	pid_t		worker_pid;

	tsbgw_he = hash_search(db_htab, &message->db_oid, HASH_ENTER, &found);

	/*
	 * this should be idempotent, so if we find the background worker and it's
	 * not stopped, we should just continue
	 */

	if (!found || ts_GetBackgroundWorkerPid(tsbgw_he->db_scheduler_handle, &worker_pid) == BGWH_STOPPED)
	{
		pid_t		worker_pid;

		if (!tsbgw_total_workers_increment())
		{
			ereport(LOG, (errmsg("timescale background worker could not be started consider increasing timescaledb.max_bgw_processes")));
			hash_search(db_htab, &message->db_oid, HASH_REMOVE, &found);
			tsbgw_message_send_ack(message, false);
			return;
		}
		worker_registered = register_entrypoint_for_db(tsbgw_he->db_oid, vxid, &tsbgw_he->db_scheduler_handle);
		if (!worker_registered)
		{
			ereport(LOG, (errmsg("timescale background worker could not be started")));
			tsbgw_total_workers_decrement();
			hash_search(db_htab, &message->db_oid, HASH_REMOVE, &found);
			tsbgw_message_send_ack(message, false);
			return;
		}
		ts_WaitForBackgroundWorkerStartup(tsbgw_he->db_scheduler_handle, &worker_pid);
	}
	tsbgw_message_send_ack(message, true);
	return;
}

static void
message_stop_action(HTAB *db_htab, TsbgwMessage * message)
{
	TsbgwHashEntry *tsbgw_he;
	bool		found;

	tsbgw_he = hash_search(db_htab, &message->db_oid, HASH_FIND, &found);
	if (found)
	{
		ts_TerminateBackgroundWorker(tsbgw_he->db_scheduler_handle);
		ts_WaitForBackgroundWorkerShutdown(tsbgw_he->db_scheduler_handle);
	}
	tsbgw_message_send_ack(message, true);
	stopped_db_schedulers_cleanup(db_htab);
	return;
}

/*
 * One might think that this function would simply be a combination of stop and start above, however
 * We decided against that because we want to maintain the worker's "slot" reserved by keeping the number of workers constant during stop/restart
 * when you stop and start the num_workers will be decremented and then incremented, if you restart, the count simply should remain constant.
 * We don't want a race condition where some other db steals the scheduler of the other by requesting a worker at the wrong time.
*/
static void
message_restart_action(HTAB *db_htab, TsbgwMessage * message, VirtualTransactionId vxid)
{
	TsbgwHashEntry *tsbgw_he;
	bool		found;
	bool		worker_registered;
	pid_t		worker_pid;

	tsbgw_he = hash_search(db_htab, &message->db_oid, HASH_ENTER, &found);
	if (found)
	{
		ts_TerminateBackgroundWorker(tsbgw_he->db_scheduler_handle);
		ts_WaitForBackgroundWorkerShutdown(tsbgw_he->db_scheduler_handle);
	}
	else if (!tsbgw_total_workers_increment())	/* we still need to increment
												 * if we haven't found an
												 * entry */
	{
		ereport(LOG, (errmsg("timescale background worker could not be started consider increasing timescaledb.max_bgw_processes")));
		tsbgw_message_send_ack(message, false);
		return;
	}
	worker_registered = register_entrypoint_for_db(tsbgw_he->db_oid, vxid, &tsbgw_he->db_scheduler_handle);
	if (!worker_registered)
	{
		ereport(LOG, (errmsg("timescale background worker could not be started")));
		tsbgw_total_workers_decrement();	/* couldn't register the worker,
											 * decrement and return false */
		hash_search(db_htab, &message->db_oid, HASH_REMOVE, &found);
		tsbgw_message_send_ack(message, false);
		return;
	}
	ts_WaitForBackgroundWorkerStartup(tsbgw_he->db_scheduler_handle, &worker_pid);
	tsbgw_message_send_ack(message, true);
	return;
}


/*
 * Drain queue, handle each message synchronously.
 */
static void
launcher_handle_messages(HTAB *db_htab)
{
	TsbgwMessage *message;

	for (message = tsbgw_message_receive(); message != NULL; message = tsbgw_message_receive())
	{

		PGPROC	   *sender;
		VirtualTransactionId vxid;

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
				message_start_action(db_htab, message, vxid);
				break;
			case STOP:
				message_stop_action(db_htab, message);
				break;
			case RESTART:
				message_restart_action(db_htab, message, vxid);
				break;
		}

	}
}


static void
tsbgw_sigterm(SIGNAL_ARGS)
{
	ereport(LOG, (errmsg("timescaledb launcher terminated by administrator command. Launcher will not restart after exiting.")));
	proc_exit(0);
}

static void
tsbgw_sigint(SIGNAL_ARGS)
{
	ereport(ERROR, (errmsg("timescaledb launcher canceled by administrator command. Launcher will restart after exiting")));
}

extern void
tsbgw_cluster_launcher_main(void)
{

	int			n;
	HTAB	   *db_htab;
	int			num_unreserved_workers;

	pqsignal(SIGTERM, tsbgw_sigterm);
	pqsignal(SIGINT, tsbgw_sigint);
	BackgroundWorkerUnblockSignals();

	if (!tsbgw_total_workers_increment())
	{
		/*
		 * should be the first thing happening so if we already exceeded our
		 * limits it means we have a limit of 0 and we should just exit
		 */
		ereport(LOG, (errmsg("timescale background worker limit set to %d. please set higher if you would like to use background workers.", guc_max_bgw_processes)));
		proc_exit(0);
	}
	/* Connect to the db, no db name yet, so can only access shared catalogs */
	BackgroundWorkerInitializeConnection(NULL, NULL);
	pgstat_report_appname("Timescale BGW Cluster Launcher");
	ereport(LOG, (errmsg("timesacle bgw cluster launcher started")));

	tsbgw_message_queue_set_reader();
	db_htab = populate_database_htab();
	before_shmem_exit(launcher_pre_shmem_cleanup, (Datum) db_htab);

	num_unreserved_workers = guc_max_bgw_processes - tsbgw_total_workers_get();
	if (num_unreserved_workers < hash_get_num_entries(db_htab))
		ereport(LOG, (errmsg("total databases = %ld timescale background worker limit set to %d. ", hash_get_num_entries(db_htab), guc_max_bgw_processes),
					  errhint("you may start background workers manually by using the  _timescaledb_internal.start_background_workers() function in each database you would like to have a scheduler worker in.")));
	else
	{
		for (n = 1; n <= hash_get_num_entries(db_htab); n++)
			tsbgw_total_workers_increment();
		start_db_schedulers(db_htab);
	}

	CHECK_FOR_INTERRUPTS();

	while (true)
	{
		int			wl_rc;

		stopped_db_schedulers_cleanup(db_htab);
		launcher_handle_messages(db_htab);

		wl_rc = WaitLatch(MyLatch, WL_LATCH_SET | WL_POSTMASTER_DEATH, 0, PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);
		if (wl_rc & WL_POSTMASTER_DEATH)
			tsbgw_on_postmaster_death();
		CHECK_FOR_INTERRUPTS();
	}

}


/*
 * This can be run either from the cluster launcher at db_startup time, or in the case of an install/uninstall/update of the extension,
 * in the first case, we have no vxid that we're waiting on. In the second case, we do, because we have to wait so that we see the effects of said txn.
 * So we wait for it to finish, then we  morph into the new db_scheduler worker using whatever version is now installed (or exit gracefully if no version is now installed).
 *
 * TODO: Make sure no race conditions if this is called multiple times when, say, upgrading or through the sql interface.
 */

extern void
tsbgw_db_scheduler_entrypoint(Oid db_id)
{
	bool		ts_installed = false;
	char		version[MAX_VERSION_LEN];
	VirtualTransactionId vxid;


	/* unblock signals and use default signal handlers */
	BackgroundWorkerUnblockSignals();

	BackgroundWorkerInitializeConnectionByOid(db_id, InvalidOid);
	pgstat_report_appname(MyBgworkerEntry->bgw_name);

	/*
	 * Wait until whatever vxid that potentially called us finishes before we
	 * happens in a txn so it's cleaned up correctly if we get a sigkill in
	 * the meantime, but we will need stop after and take a new txn so we can
	 * see the correct state after its effects
	 */
	StartTransactionCommand();
	(void) GetTransactionSnapshot();
	memcpy(&vxid, MyBgworkerEntry->bgw_extra, sizeof(VirtualTransactionId));
	if (VirtualTransactionIdIsValid(vxid))
		VirtualXactLock(vxid, true);
	CommitTransactionCommand();

	/*
	 * now we can start our transaction and get the version currently
	 * installed
	 */

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
			ereport(LOG, (errmsg("version %s does not have a background worker, exiting.", soname)));
		else					/* essentially we morph into the versioned
								 * worker here */
			DirectFunctionCall1(versioned_scheduler_main_loop, InvalidOid);

	}
	proc_exit(0);

}
