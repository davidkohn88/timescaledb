-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;

\set DN_DBNAME_1 :TEST_DBNAME _1
\set DN_DBNAME_2 :TEST_DBNAME _2
\set DN_DBNAME_3 :TEST_DBNAME _3

\set TEST_BASE_NAME data_fetcher
SELECT format('include/%s_load.sql', :'TEST_BASE_NAME') as "TEST_LOAD_NAME",
       format('include/%s_run.sql', :'TEST_BASE_NAME') as "TEST_QUERY_NAME",
       format('%s/results/%s_results_cursor.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') as "TEST_RESULTS_CURSOR",
       format('%s/results/%s_results_row_by_row.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') as "TEST_RESULTS_ROW_BY_ROW"
\gset
SELECT format('\! diff %s %s', :'TEST_RESULTS_CURSOR', :'TEST_RESULTS_ROW_BY_ROW') as "DIFF_CMD"
\gset

SET client_min_messages TO warning;
\ir :TEST_LOAD_NAME

\set ECHO errors
SET client_min_messages TO error;

-- Set a smaller fetch size to ensure that the result is split into
-- mutliple batches.
ALTER FOREIGN DATA WRAPPER timescaledb_fdw OPTIONS (ADD fetch_size '100');

-- run the queries using row by row fetcher
SET timescaledb.remote_data_fetcher = 'rowbyrow';
\set ON_ERROR_STOP 0
\o :TEST_RESULTS_ROW_BY_ROW
\ir :TEST_QUERY_NAME
\o
\set ON_ERROR_STOP 1

-- run queries using cursor fetcher
SET timescaledb.remote_data_fetcher = 'cursor';
\o :TEST_RESULTS_CURSOR
\ir :TEST_QUERY_NAME
\o
-- compare results
:DIFF_CMD

RESET ROLE;
DROP DATABASE :DN_DBNAME_1;
DROP DATABASE :DN_DBNAME_2;
DROP DATABASE :DN_DBNAME_3;
