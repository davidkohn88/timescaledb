CREATE OR REPLACE FUNCTION _timescaledb_internal.restart_background_workers()
RETURNS VOID 
AS '@LOADER_PATHNAME@', 'tsbgw_db_workers_restart'
LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.stop_background_workers()
RETURNS VOID 
AS '@LOADER_PATHNAME@', 'tsbgw_db_workers_stop'
LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.start_background_workers()
RETURNS VOID 
AS '@LOADER_PATHNAME@', 'tsbgw_db_workers_start'
LANGUAGE C VOLATILE;