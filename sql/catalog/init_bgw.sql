CREATE FUNCTION _timescaledb_internal.start_background_workers()
RETURNS VOID 
AS '@LOADER_PATHNAME@', 'timescale_bgw_start_db_workers'
LANGUAGE C VOLATILE;

SELECT _timescaledb_internal.start_background_workers();