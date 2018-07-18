CREATE OR REPLACE FUNCTION _timescaledb_internal.restart_background_workers()
RETURNS VOID 
AS '@LOADER_PATHNAME@', 'timescale_bgw_restart_db_workers'
LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.stop_background_workers()
RETURNS VOID 
AS '@LOADER_PATHNAME@', 'timescale_bgw_stop_db_workers'
LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.start_background_workers()
RETURNS VOID 
AS '@LOADER_PATHNAME@', 'timescale_bgw_start_db_workers'
LANGUAGE C VOLATILE;