#ifndef TIMESCALE_BGW_H
#define TIMESCALE_BGW_H

#include <postgres.h>
#include <fmgr.h>

#define TIMESCALE_MAX_WORKERS_GUC_STANDIN 8

/* Interface for both the loader and the versioned extension*/


extern Datum timescale_bgw_increment_total_workers(PG_FUNCTION_ARGS);
extern Datum timescale_bgw_decrement_total_workers(PG_FUNCTION_ARGS);
extern Datum timescale_bgw_get_total_workers(PG_FUNCTION_ARGS);

extern Datum timescale_bgw_restart_db_workers(PG_FUNCTION_ARGS);

extern Datum timescale_bgw_start_db_workers(PG_FUNCTION_ARGS);

extern Datum timescale_bgw_stop_db_workers(PG_FUNCTION_ARGS);



#endif /* TIMESCALE_BGW_H*/
