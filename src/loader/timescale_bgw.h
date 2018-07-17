#ifndef TIMESCALE_BGW_H
#define TIMESCALE_BGW_H

#include <postgres.h>
#include <fmgr.h>

#define TIMESCALE_MAX_WORKERS_GUC_STANDIN 8

/* Interface for both the loader and the versioned extension*/

PG_FUNCTION_INFO_V1(timescale_bgw_increment_total_workers); 
PG_FUNCTION_INFO_V1(timescale_bgw_decrement_total_workers);
PG_FUNCTION_INFO_V1( timescale_bgw_get_total_workers);

PG_FUNCTION_INFO_V1(timescale_bgw_restart_db_workers);




#endif