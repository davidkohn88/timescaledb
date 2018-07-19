
\c single_2 :ROLE_SUPERUSER
/* 
 * Note on testing this: we had to add a short pg_sleep in a few places
 * because WaitForBackgroundWorkerStartup returns when the worker proc has started
 * not necessarily when it has actually run code or shown up in pg_stat_activity
 */

CREATE VIEW worker_counts as SELECT count(*) filter (WHERE application_name = 'Timescale BGW Cluster Launcher') as launcher,
count(*) filter (WHERE application_name = 'Timescale BGW Scheduler Entrypoint' AND datname = 'single') as single_scheduler,
count(*) filter (WHERE application_name = 'Timescale BGW Scheduler Entrypoint' AND datname = 'single_2') as single_2_scheduler
FROM pg_stat_activity;
/* 
 * when we've connected to single_2, we should be able to see the cluster launcher 
 * and the scheduler for single in pg_stat_activity
 * but single_2 shouldn't have a scheduler because ext not created yet 
 */
SELECT * FROM worker_counts;

/*Now create the extension in single_2*/
CREATE EXTENSION timescaledb CASCADE;
select pg_sleep(0.1);

/* and we should now have a scheduler for single_2*/
SELECT * FROM worker_counts;

DROP DATABASE single;

/* Now the db_scheduler for single should have disappeared*/
SELECT * FROM worker_counts;

/*now let's restart the scheduler and make sure our backend_start changed */
SELECT backend_start as orig_backend_start 
FROM pg_stat_activity 
WHERE application_name = 'Timescale BGW Scheduler Entrypoint' 
AND datname = 'single_2' \gset
/* we'll do this in a txn so that we can see that the worker locks on our txn before continuing*/
BEGIN;
SELECT _timescaledb_internal.restart_background_workers();
select pg_sleep(0.1);

SELECT * FROM worker_counts;

SELECT (backend_start > :'orig_backend_start'::timestamptz) backend_start_changed, 
(wait_event = 'virtualxid') wait_event_changed
FROM pg_stat_activity 
WHERE application_name = 'Timescale BGW Scheduler Entrypoint' 
AND datname = 'single_2';
COMMIT;

SELECT * FROM worker_counts;

SELECT (wait_event IS DISTINCT FROM 'virtualxid') wait_event_changed
FROM pg_stat_activity 
WHERE application_name = 'Timescale BGW Scheduler Entrypoint' 
AND datname = 'single_2';


/*test stop*/
SELECT _timescaledb_internal.stop_background_workers();
SELECT * FROM worker_counts;
/*make sure it doesn't break if we stop twice in a row*/
SELECT _timescaledb_internal.stop_background_workers();
SELECT * FROM worker_counts;

/*test start*/
SELECT _timescaledb_internal.start_background_workers();
select pg_sleep(0.1);

SELECT * FROM worker_counts;

/*make sure start is idempotent*/
SELECT backend_start as orig_backend_start 
FROM pg_stat_activity 
WHERE application_name = 'Timescale BGW Scheduler Entrypoint' 
AND datname = 'single_2' \gset

SELECT _timescaledb_internal.start_background_workers();
select pg_sleep(0.1);

SELECT (backend_start = :'orig_backend_start'::timestamptz) backend_start_unchanged
FROM pg_stat_activity 
WHERE application_name = 'Timescale BGW Scheduler Entrypoint' 
AND datname = 'single_2';

/*Make sure restart works from stopped worker state*/
SELECT _timescaledb_internal.stop_background_workers();
SELECT * FROM worker_counts;
SELECT _timescaledb_internal.restart_background_workers();
select pg_sleep(0.1);
SELECT * FROM worker_counts;

/*Make sure drop extension statement restarts the worker and on rollback it keeps running*/

/*now let's restart the scheduler and make sure our backend_start changed */
SELECT backend_start as orig_backend_start 
FROM pg_stat_activity 
WHERE application_name = 'Timescale BGW Scheduler Entrypoint' 
AND datname = 'single_2' \gset
/* we'll do this in a txn so that we can see that the worker locks on our txn before continuing*/
BEGIN;
DROP EXTENSION timescaledb;
select pg_sleep(0.1);

SELECT * FROM worker_counts;

SELECT (backend_start > :'orig_backend_start'::timestamptz) backend_start_changed, 
(wait_event = 'virtualxid') wait_event_changed
FROM pg_stat_activity 
WHERE application_name = 'Timescale BGW Scheduler Entrypoint' 
AND datname = 'single_2';
ROLLBACK;