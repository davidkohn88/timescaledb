#ifndef TSBGW_INTERFACE_H
#define TSBGW_INTERFACE_H

#include <postgres.h>
#include <fmgr.h>

/* This is where versioned-extension facing functions live. It shouldn't live anywhere else */

PG_FUNCTION_INFO_V1(tsbgw_worker_reserve);
PG_FUNCTION_INFO_V1(tsbgw_worker_release);
PG_FUNCTION_INFO_V1(tsbgw_num_unreserved);

PG_FUNCTION_INFO_V1(tsbgw_db_workers_start);

PG_FUNCTION_INFO_V1(tsbgw_db_workers_stop);

PG_FUNCTION_INFO_V1(tsbgw_db_workers_restart);




#endif							/* TSBGW_INTERFACE_H */
