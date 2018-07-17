#ifndef TIMESCALE_BGW_H
#define TIMESCALE_BGW_H

#include <postgres.h>
#include <fmgr.h>

#define TIMESCALE_MAX_WORKERS_GUC_STANDIN 8

/* Interface for both the loader and the versioned extension*/

extern bool timescale_bgw_increment_total_workers(int max_workers); 
extern void timescale_bgw_decrement_total_workers(void);
extern int timescale_bgw_get_total_workers(void);

PG_FUNCTION_INFO_V1(timescale_bgw_restart_db_workers);




#endif