CREATE FUNCTION _timescaledb_internal.start_background_workers()
RETURNS BOOL 
AS '@LOADER_PATHNAME@', 'tsbgw_db_workers_start'
LANGUAGE C VOLATILE;

SELECT _timescaledb_internal.start_background_workers();
