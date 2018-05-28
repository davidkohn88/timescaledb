#include <postgres.h>
#include <pg_config.h>
#include <access/xact.h>
#include "../compat-msvc-enter.h"
#include <commands/extension.h>
#include <miscadmin.h>
#include <parser/analyze.h>
#include "../compat-msvc-exit.h"
#include <utils/guc.h>
#include <utils/inval.h>
#include <nodes/print.h>

#include "../extension_utils.c"
#include "../timescale_bgw_utils.c" 


#define PG96 ((PG_VERSION_NUM >= 90600) && (PG_VERSION_NUM < 100000))
#define PG10 ((PG_VERSION_NUM >= 100000) && (PG_VERSION_NUM < 110000))
/*
 * Some notes on design:
 *
 * We do not check for the installation of the extension upon loading the extension and instead rely on a hook for two reasons:
 * 1) We probably can't
 *	- The shared_preload_libraries is called in PostmasterMain which is way before InitPostgres is called.
 *			(Note: This happens even before the fork of the backend) -- so we don't even know which database this is for.
 *	-- This means we cannot query for the existance of the extension yet because the caches are initialized in InitPostgres.
 * 2) We actually don't want to load the extension in two cases:
 *	  a) We are upgrading the extension.
 *	  b) We set the guc timescaledb.disable_load.
 * 
 * 3) We include a section for the bgw launcher and some workers below the rest, separated with its own notes, 
 *   some function definitions are included as they are referenced by other loader functions. 
 *
 */

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

#define GUC_DISABLE_LOAD_NAME "timescaledb.disable_load"

extern void PGDLLEXPORT _PG_init(void);
extern void PGDLLEXPORT _PG_fini(void);

/* was the versioned-extension loaded*/
static bool loaded = false;
static bool loader_present = true;


static char soversion[MAX_VERSION_LEN];

/* GUC to disable the load */
static bool guc_disable_load = false;

/* This is the hook that existed before the loader was installed */
static post_parse_analyze_hook_type prev_post_parse_analyze_hook;

/* This is timescaleDB's versioned-extension's post_parse_analyze_hook */
static post_parse_analyze_hook_type extension_post_parse_analyze_hook = NULL;

static void inline extension_check(void);
static void call_extension_post_parse_analyze_hook(ParseState *pstate,
									   Query *query);
/* BGW function definitions */
static void timescale_bgw_shmem_init(void);
static void register_timescale_bgw_cluster_launcher(void);
/*these are not defined as static as they need to be accessible for bgworker startup*/
void timescale_bgw_cluster_launcher_main(void); 
void timescale_bgw_db_scheduler_entrypoint(Oid db_id);


static void
inval_cache_callback(Datum arg, Oid relid)
{
	if (guc_disable_load)
		return;
	extension_check();
}


static bool
drop_statement_drops_extension(DropStmt *stmt)
{
	if (stmt->removeType == OBJECT_EXTENSION)
	{
		if (list_length(stmt->objects) == 1)
		{
			char	   *ext_name;
#if PG96
			List	   *names = linitial(stmt->objects);

			Assert(list_length(names) == 1);
			ext_name = strVal(linitial(names));
#elif PG10
			void	   *name = linitial(stmt->objects);

			ext_name = strVal(name);
#endif
			if (strcmp(ext_name, EXTENSION_NAME) == 0)
				return true;
		}
	}
	return false;
}

static bool
should_load_on_variable_set(Node *utility_stmt)
{
	VariableSetStmt *stmt = (VariableSetStmt *) utility_stmt;

	switch (stmt->kind)
	{
		case VAR_SET_VALUE:
		case VAR_SET_DEFAULT:
		case VAR_RESET:
			/* Do not load when setting the guc to disable load */
			return stmt->name == NULL || strcmp(stmt->name, GUC_DISABLE_LOAD_NAME) != 0;
		default:
			return true;
	}
}

static bool
should_load_on_alter_extension(Node *utility_stmt)
{
	AlterExtensionStmt *stmt = (AlterExtensionStmt *) utility_stmt;

	if (strcmp(stmt->extname, EXTENSION_NAME) != 0)
		return true;

	/* disallow loading two .so from different versions */
	if (loaded)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("extension \"%s\" cannot be updated after the old version has already been loaded", stmt->extname),
				 errhint("Start a new session and execute ALTER EXTENSION as the first command. "
						 "Make sure to pass the \"-X\" flag to psql.")));
	/* do not load the current (old) version's .so */
	return false;
}

static bool
should_load_on_create_extension(Node *utility_stmt)
{
	CreateExtensionStmt *stmt = (CreateExtensionStmt *) utility_stmt;
	bool		is_extension = strcmp(stmt->extname, EXTENSION_NAME) == 0;

	if (!is_extension)
		return false;

	if (!loaded)
		return true;

	/*
	 * If the extension exists and the create statement has an IF NOT EXISTS
	 * option, we continue without loading and let CREATE EXTENSION bail out
	 * with a standard NOTICE. We can only do this if the extension actually
	 * exists (is created), or else we might potentially load the shared
	 * library of another version of the extension. Loading typically happens
	 * on CREATE EXTENSION (via CREATE FUNCTION as SQL files are installed)
	 * even if we do not explicitly load the library here. If we load another
	 * version of the library, in addition to the currently loaded version, we
	 * might taint the backend.
	 */
	if (extension_exists() && stmt->if_not_exists)
		return false;

	/* disallow loading two .so from different versions */
	ereport(ERROR,
			(errcode(ERRCODE_DUPLICATE_OBJECT),
			 errmsg("extension \"%s\" has already been loaded with another version", stmt->extname),
			 errdetail("The loaded version is \"%s\".", soversion),
			 errhint("Start a new session and execute CREATE EXTENSION as the first command. "
					 "Make sure to pass the \"-X\" flag to psql.")));
	return false;
}

static bool
should_load_on_drop_extension(Node *utility_stmt)
{
	return !drop_statement_drops_extension((DropStmt *) utility_stmt);
}

static bool
load_utility_cmd(Node *utility_stmt)
{
	switch (nodeTag(utility_stmt))
	{
		case T_VariableSetStmt:
			return should_load_on_variable_set(utility_stmt);
		case T_AlterExtensionStmt:
			return should_load_on_alter_extension(utility_stmt);
		case T_CreateExtensionStmt:
			return should_load_on_create_extension(utility_stmt);
		case T_DropStmt:
			return should_load_on_drop_extension(utility_stmt);
		default:
			return true;
	}
}

static void
post_analyze_hook(ParseState *pstate, Query *query)
{
	if (!guc_disable_load &&
		(query->commandType != CMD_UTILITY || load_utility_cmd(query->utilityStmt)))
		extension_check();

	/*
	 * Call the extension's hook. This is necessary since the extension is
	 * installed during the hook. If we did not do this the extension's hook
	 * would not be called during the first command because the extension
	 * would not have yet been installed. Thus the loader captures the
	 * extension hook and calls it explicitly after the check for installing
	 * the extension.
	 */
	call_extension_post_parse_analyze_hook(pstate, query);

	if (prev_post_parse_analyze_hook != NULL)
	{
		prev_post_parse_analyze_hook(pstate, query);
	}
}

static void
extension_mark_loader_present()
{
	void	  **presentptr = find_rendezvous_variable(RENDEZVOUS_LOADER_PRESENT_NAME);

	*presentptr = &loader_present;
}

void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
	{
		extension_load_without_preload();
	}
	extension_mark_loader_present();

	elog(INFO, "timescaledb loaded");
	
	timescale_bgw_shmem_init();
	register_timescale_bgw_cluster_launcher();

	/* This is a safety-valve variable to prevent loading the full extension */
	DefineCustomBoolVariable(GUC_DISABLE_LOAD_NAME, "Disable the loading of the actual extension",
							 NULL,
							 &guc_disable_load,
							 false,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	/*
	 * cannot check for extension here since not inside a transaction yet. Nor
	 * do we even have an assigned database yet
	 */

	CacheRegisterRelcacheCallback(inval_cache_callback, PointerGetDatum(NULL));

	/*
	 * using the post_parse_analyze_hook since it's the earliest available
	 * hook
	 */
	prev_post_parse_analyze_hook = post_parse_analyze_hook;
	post_parse_analyze_hook = post_analyze_hook;
}

void
_PG_fini(void)
{
	post_parse_analyze_hook = prev_post_parse_analyze_hook;
	/* No way to unregister relcache callback */
}

static void inline
do_load()
{
	char	   *version = extension_version();
	char		soname[MAX_SO_NAME_LEN];
	post_parse_analyze_hook_type old_hook;

	StrNCpy(soversion, version, MAX_VERSION_LEN);
	snprintf(soname, MAX_SO_NAME_LEN, "%s-%s", EXTENSION_NAME, version);

	/*
	 * An inval_relcache callback can be called after previous checks of
	 * loaded had found it to be false. But the inval_relcache callback may
	 * load the extension setting it to true. Thus it needs to be rechecked
	 * here again by the outer call after inval_relcache completes. This is
	 * double-check locking, in effect.
	 */
	if (loaded)
		return;

	/*
	 * Set to true whether or not the load succeeds to prevent reloading if
	 * failure happened after partial load.
	 */
	loaded = true;

	/*
	 * Set the config option to let versions 0.9.0 and 0.9.1 know that the
	 * loader was preloaded, newer versions use rendezvous variables instead.
	 */
	if (strcmp(version, "0.9.0") == 0 || strcmp(version, "0.9.1") == 0)
		SetConfigOption("timescaledb.loader_present", "on", PGC_USERSET, PGC_S_SESSION);


	/*
	 * we need to capture the loaded extension's post analyze hook, giving it
	 * a NULL as previous
	 */
	old_hook = post_parse_analyze_hook;
	post_parse_analyze_hook = NULL;

	PG_TRY();
	{
		load_file(soname, false);
	}
	PG_CATCH();
	{
		extension_post_parse_analyze_hook = post_parse_analyze_hook;
		post_parse_analyze_hook = old_hook;
		PG_RE_THROW();
	}
	PG_END_TRY();

	extension_post_parse_analyze_hook = post_parse_analyze_hook;
	post_parse_analyze_hook = old_hook;
}

static void inline
extension_check()
{
	if (!loaded)
	{
		enum ExtensionState state = extension_current_state();

		switch (state)
		{
			case EXTENSION_STATE_TRANSITIONING:

				/*
				 * Always load as soon as the extension is transitioning. This
				 * is necessary so that the extension load before any CREATE
				 * FUNCTION calls. Otherwise, the CREATE FUNCTION calls will
				 * load the .so without capturing the post_parse_analyze_hook.
				 */
			case EXTENSION_STATE_CREATED:
				do_load();
				return;
			case EXTENSION_STATE_UNKNOWN:
			case EXTENSION_STATE_NOT_INSTALLED:
				return;
		}
	}
}

static void
call_extension_post_parse_analyze_hook(ParseState *pstate,
									   Query *query)
{
	if (loaded && extension_post_parse_analyze_hook != NULL)
	{
		extension_post_parse_analyze_hook(pstate, query);
	}
}


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
*/


#define TSBGW_LAUNCHER_RESTART_TIME 60


static shmem_startup_hook_type prev_shmem_startup_hook = NULL;



static void register_timescale_bgw_cluster_launcher(void) {
    BackgroundWorker worker;

    ereport(LOG, (errmsg("Registering Timescale BGW Launcher")));

    /*set up worker settings for our main worker */
	snprintf(worker.bgw_name, BGW_MAXLEN, "Timescale BGW Cluster Launcher");
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_restart_time = TSBGW_LAUNCHER_RESTART_TIME;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    worker.bgw_notify_pid = 0;
    /* TODO: maybe pass in the library name, we know it, one assumes, as we are in 
    shared-preload-libraries when we call this. The problem could be that otherwise
    a specific versioned library of timescaledb in shared preload libraries will 
    break this otherwise?*/
    sprintf(worker.bgw_library_name, "timescaledb");
    sprintf(worker.bgw_function_name , "timescale_bgw_cluster_launcher_main");
    

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
    if (prev_shmem_startup_hook)
        prev_shmem_startup_hook();

	ereport(LOG,(errmsg("In SHMEM STARTUP HOOK")));
    /* possible_restart = true in case this is a restart within the postmaster*/
    get_tsbgw_shared_state(true);
}


/* this gets called by the loader (and therefore the postmaster) at shared_preload_libraries time*/
static void timescale_bgw_shmem_init(void){
    RequestAddinShmemSpace(tsbgw_memsize());
    RequestNamedLWLockTranche(TSBGW_LW_TRANCHE_NAME, 1);
    prev_shmem_startup_hook = shmem_startup_hook;
    shmem_startup_hook = timescale_bgw_shmem_startup;
}


void timescale_bgw_cluster_launcher_main(void) {
    
    
    HASH_SEQ_STATUS         hash_seq;
    tsbgw_hash_entry        *current_entry;
    tsbgw_shared_state      *tsbgw_ss = get_tsbgw_shared_state(false);
	

    BackgroundWorkerUnblockSignals();
    increment_total_workers();  
    
    /* Connect to the db, no db name yet, so can only access shared catalogs*/
    BackgroundWorkerInitializeConnection(NULL, NULL);
	LWLockAcquire(tsbgw_ss->lock, LW_EXCLUSIVE);
	populate_database_htab();
	LWLockRelease(tsbgw_ss->lock);
    /*now scan our hash table of dbs and register a worker for each*/
    LWLockAcquire(tsbgw_ss->lock, AccessShareLock);
    hash_seq_init(&hash_seq, tsbgw_ss->hashtable);
    
    while ((current_entry = hash_seq_search(&hash_seq)) != NULL) 
    {
		BackgroundWorkerHandle 				*worker_handle = NULL;
		bool								worker_registered = false;
		pid_t								worker_pid;
		VirtualTransactionId				vxid;
		
		SetInvalidVirtualTransactionId(vxid);

		worker_registered = register_tsbgw_entrypoint_for_db(current_entry->db_oid, vxid, &worker_handle);

		if (worker_registered){
			WaitForBackgroundWorkerStartup(worker_handle, &worker_pid);
			ereport(LOG, (errmsg("Worker started with PID %d", worker_pid )));
		} 
		else 
			break; /* should we complain?*/
			

    }
    LWLockRelease(tsbgw_ss->lock);
    decrement_total_workers();
    ereport(LOG, (errmsg("Cluster Launcher shutting down")));

    
}


/*
 * This can be run either from the cluster launcher at db_startup time, or in the case of an install/uninstall/update of the extension, 
 * in the first case, we have no vxid that we're waiting on. In the second case, we do, because we have to wait to see whether the txn that did the alter extension succeeded. So we wait for it to finish, then we a)
 * check to see whether the version of Timescale shown as installed in the catalogs is different from the version we populated in 
 * our shared hash table, then if it is b) tell the old version's db_scheduler to shut down, ideally gracefully and it will cascade any 
 * shutdown events to any workers it has started then c) start a new db_scheduler worker using the updated .so  . 
 * This worker will have a restart time specced in case there's a crash during the process, but will exit 0 when it has finished its job, so won't be restarted. 
 * 
 */
void timescale_bgw_db_scheduler_entrypoint(Oid db_id){
	bool						ts_installed = false;
	char						version[MAX_VERSION_LEN];
    tsbgw_shared_state      	*tsbgw_ss = get_tsbgw_shared_state(false);
	tsbgw_hash_entry		    *tsbgw_he;
	bool						hash_found = false;
	BackgroundWorkerHandle  	*old_scheduler_handle = NULL;
	bool						ts_previously_installed = false;
	bool						ts_version_change = true; /* true also at server startup when there is no old version populated yet*/
	VirtualTransactionId		vxid;

	/* unblock signals and use default signal handlers*/
    BackgroundWorkerUnblockSignals();
    ereport(LOG, (errmsg("Worker started for Database id = %d with pid %d", db_id, MyProcPid))); 
    increment_total_workers();
    BackgroundWorkerInitializeConnectionByOid(db_id, InvalidOid);
	ereport(LOG, (errmsg("Connected to db %d", db_id)));
	/*Wait until whatever vxid that potentially called us finishes before we get a transaction so we can see the correct state after its effects */
	memcpy(&vxid, MyBgworkerEntry->bgw_extra, sizeof(VirtualTransactionId));
	if (VirtualTransactionIdIsValid(vxid))
		VirtualXactLock(vxid, true);


	/* 
     * now look up our hash_entry, make sure we take an exclusive lock on the table even though we're just getting our entry, 
     * because we might need to populate the hash table if it doesn't exist. When we're updating it later, we can use a shared 
     * lock as we're the only folks accessing. 
     */
    LWLockAcquire(tsbgw_ss->lock, LW_EXCLUSIVE);
    tsbgw_he = hash_search(tsbgw_ss->hashtable, &db_id, HASH_FIND, &hash_found);
    if (!hash_found)
    {
        populate_database_htab();
        tsbgw_he = hash_search(tsbgw_ss->hashtable, &db_id, HASH_FIND, &hash_found);
        if (!hash_found)
            ereport(LOG, (errmsg("Database with db_id = %d not found in the hashtable. This should be impossible as we are currently connected to said database. Exiting.",db_id)));
            proc_exit(0);
    }
	else
	{
		old_scheduler_handle = tsbgw_he->db_scheduler_handle;
		ts_previously_installed = tsbgw_he->ts_installed;
		ts_version_change = !strcmp(version, tsbgw_he->ts_version);
	}
	LWLockRelease(tsbgw_ss->lock);
	/* now we can start our transaction and get the version currently installed*/
	StartTransactionCommand();
	(void) GetTransactionSnapshot();
	
	ts_installed = extension_exists();
	if (ts_installed)
		StrNCpy(version, extension_version(), MAX_VERSION_LEN);
	CommitTransactionCommand();
	ereport(LOG, (errmsg("TimescaleDB %s Installed In DB ID %d", (ts_installed) ? version:"not", db_id)));

	if (ts_version_change && ts_previously_installed)
	{
		/* we need to shut down the old db scheduler if it exists because we are either uninstalling or updating*/
		if (old_scheduler_handle != NULL)
		{
			TerminateBackgroundWorker(old_scheduler_handle);
			WaitForBackgroundWorkerShutdown(old_scheduler_handle); /*should I check that the status is stopped?*/
		}
	}
	if (ts_installed && ts_version_change) /*gonna need to add a check around the extension version as well so that we make sure we're good. */
	{
		BackgroundWorker		worker;
		char					soname[MAX_SO_NAME_LEN];
        BgwHandleStatus         status;
        pid_t                   worker_pid;
		
		snprintf(soname, MAX_SO_NAME_LEN, "%s-%s", EXTENSION_NAME, version);	
		    /*common parameters for all our scheduler workers*/
		memset(&worker, 0, sizeof(worker));
		snprintf(worker.bgw_name, BGW_MAXLEN, "Timescale BGW Scheduler for DB %d ", db_id);
		worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
		worker.bgw_restart_time = TSBGW_DB_SCHEDULER_RESTART_TIME;
		worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
		StrNCpy(worker.bgw_library_name, soname, MAX_SO_NAME_LEN);
		sprintf(worker.bgw_function_name , "timescale_bgw_db_scheduler_main");
		worker.bgw_main_arg = db_id;
		worker.bgw_notify_pid = MyProcPid;

		/*this might be better to do with a mutex on the entry itself and a shared lock on the hashtable, but seems like overkill for now */
		LWLockAcquire(tsbgw_ss->lock, LW_EXCLUSIVE); /*get the hashtable lock so we can update our handle.*/
		if (RegisterDynamicBackgroundWorker(&worker, &(tsbgw_he->db_scheduler_handle)))
		{	
			status = WaitForBackgroundWorkerStartup(tsbgw_he->db_scheduler_handle, &worker_pid);
			strcpy(tsbgw_he->ts_version, version);
			ereport(LOG, (errmsg("Versioned worker started with PID %d", worker_pid)));	
		}
		LWLockRelease(tsbgw_ss->lock);
        
	}
    
	decrement_total_workers();
	proc_exit(0);

}
