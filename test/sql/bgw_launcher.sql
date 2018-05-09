\c single_2 :ROLE_SUPERUSER
/*
 * Note on testing: need a couple wrappers that pg_sleep in a loop to wait for changes
 * to appear in pg_stat_activity.
 */
CREATE FUNCTION wait_worker_counts(INTEGER, INTEGER, INTEGER) RETURNS BOOLEAN LANGUAGE PLPGSQL AS
$BODY$
DECLARE
r INTEGER;
BEGIN
FOR i in 1..10
LOOP
SELECT COUNT(*) from worker_counts where launcher=$1 AND single_scheduler=$2 AND single_2_scheduler=$3 into r;
if(r < 1) THEN
  PERFORM pg_sleep(0.1);
  PERFORM pg_stat_clear_snapshot();
ELSE
  RETURN TRUE;
END IF;
END LOOP;
RETURN FALSE;
END
$BODY$;

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
SELECT wait_worker_counts(1,1,1);

DROP DATABASE single;

/* Now the db_scheduler for single should have disappeared*/
SELECT wait_worker_counts(1,0,1);

/*now let's restart the scheduler and make sure our backend_start changed */
SELECT backend_start as orig_backend_start 
FROM pg_stat_activity 
WHERE application_name = 'Timescale BGW Scheduler Entrypoint' 
AND datname = 'single_2' \gset
/* we'll do this in a txn so that we can see that the worker locks on our txn before continuing*/
BEGIN;
SELECT _timescaledb_internal.restart_background_workers();
SELECT wait_worker_counts(1,0,1);

SELECT (backend_start > :'orig_backend_start'::timestamptz) backend_start_changed, 
(wait_event = 'virtualxid') wait_event_changed
FROM pg_stat_activity 
WHERE application_name = 'Timescale BGW Scheduler Entrypoint' 
AND datname = 'single_2';
COMMIT;

SELECT wait_worker_counts(1,0,1);
SELECT (wait_event IS DISTINCT FROM 'virtualxid') wait_event_changed
FROM pg_stat_activity
WHERE application_name = 'Timescale BGW Scheduler Entrypoint'
AND datname = 'single_2';

/*test stop*/
SELECT _timescaledb_internal.stop_background_workers();
SELECT wait_worker_counts(1,0,0);
/*make sure it doesn't break if we stop twice in a row*/
SELECT _timescaledb_internal.stop_background_workers();
SELECT wait_worker_counts(1,0,0);

/*test start*/
SELECT _timescaledb_internal.start_background_workers();
SELECT wait_worker_counts(1,0,1);

/*make sure start is idempotent*/
SELECT backend_start as orig_backend_start
FROM pg_stat_activity
WHERE application_name = 'Timescale BGW Scheduler Entrypoint'
AND datname = 'single_2' \gset

SELECT _timescaledb_internal.start_background_workers();
/*Here we're waiting to see if something shows up in pg_stat_activity, 
 * so we have to condition our loop in the opposite way. We'll only wait 
 * half a second in total as well so that tests don't take too long. */ 
CREATE FUNCTION wait_equals(TIMESTAMPTZ) RETURNS BOOLEAN LANGUAGE PLPGSQL AS
$BODY$
DECLARE
r BOOLEAN;
BEGIN
FOR i in 1..5
LOOP
SELECT (backend_start = $1::timestamptz) backend_start_unchanged
FROM pg_stat_activity
WHERE application_name = 'Timescale BGW Scheduler Entrypoint'
AND datname = 'single_2' into r;
if(r) THEN
  PERFORM pg_sleep(0.1);
  PERFORM pg_stat_clear_snapshot();
ELSE
  RETURN FALSE;
END IF;
END LOOP;
RETURN TRUE;
END
$BODY$;
select wait_equals(:'orig_backend_start');

/*Make sure restart works from stopped worker state*/
SELECT _timescaledb_internal.stop_background_workers();
SELECT wait_worker_counts(1,0,0);
SELECT _timescaledb_internal.restart_background_workers();
SELECT wait_worker_counts(1,0,1);

/*Make sure drop extension statement restarts the worker and on rollback it keeps running*/

/*now let's restart the scheduler and make sure our backend_start changed */
SELECT backend_start as orig_backend_start
FROM pg_stat_activity
WHERE application_name = 'Timescale BGW Scheduler Entrypoint'
AND datname = 'single_2' \gset

BEGIN;
DROP EXTENSION timescaledb;
SELECT wait_worker_counts(1,0,1);
ROLLBACK;

CREATE FUNCTION wait_greater(TIMESTAMPTZ) RETURNS BOOLEAN LANGUAGE PLPGSQL AS
$BODY$
DECLARE
r BOOLEAN;
BEGIN
FOR i in 1..10
LOOP
SELECT (backend_start > $1::timestamptz) backend_start_unchanged
FROM pg_stat_activity
WHERE application_name = 'Timescale BGW Scheduler Entrypoint'
AND datname = 'single_2' into r;
if(NOT r) THEN
  PERFORM pg_sleep(0.1);
  PERFORM pg_stat_clear_snapshot();
ELSE
  RETURN TRUE;
END IF;
END LOOP;
RETURN FALSE;
END
$BODY$;
SELECT wait_greater(:'orig_backend_start');

/* make sure canceling the launcher backend causes a restart of schedulers */
SELECT backend_start as orig_backend_start
FROM pg_stat_activity
WHERE application_name = 'Timescale BGW Scheduler Entrypoint'
AND datname = 'single_2' \gset

SELECT pg_cancel_backend(pid) FROM pg_stat_activity 
WHERE application_name = 'Timescale BGW Cluster Launcher';

SELECT wait_worker_counts(1,0,1);

SELECT wait_greater(:'orig_backend_start');


/* make sure dropping the extension means that the scheduler is stopped*/
BEGIN;
DROP EXTENSION timescaledb;
COMMIT;
SELECT wait_worker_counts(1,0,0);

/* make sure terminating the launcher causes it to shut down permanently */

SELECT pg_terminate_backend(pid) FROM pg_stat_activity 
WHERE application_name = 'Timescale BGW Cluster Launcher';

SELECT wait_worker_counts(0,0,0);

CREATE FUNCTION wait_no_change(INTEGER, INTEGER, INTEGER) RETURNS BOOLEAN LANGUAGE PLPGSQL AS
$BODY$
DECLARE
r INTEGER;
BEGIN
FOR i in 1..10
LOOP
SELECT COUNT(*) from worker_counts where launcher=$1 AND single_scheduler=$2 AND single_2_scheduler=$3 into r;
if(r = 1) THEN
  PERFORM pg_sleep(0.1);
  PERFORM pg_stat_clear_snapshot();
ELSE
  RETURN FALSE;
END IF;
END LOOP;
RETURN TRUE;
END
$BODY$;
SELECT wait_no_change(0,0,0);

