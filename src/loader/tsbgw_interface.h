#ifndef TSBGW_INTERFACE_H
#define TSBGW_INTERFACE_H

#include <postgres.h>
#include <fmgr.h>
#include "../compat.h"

/* This is where versioned-extension facing functions live. It shouldn't live anywhere else */

TS_FUNCTION_INFO_V1(tsbgw_worker_reserve);
TS_FUNCTION_INFO_V1(tsbgw_worker_release);
TS_FUNCTION_INFO_V1(tsbgw_num_unreserved);

TS_FUNCTION_INFO_V1(tsbgw_db_workers_start);

TS_FUNCTION_INFO_V1(tsbgw_db_workers_stop);

TS_FUNCTION_INFO_V1(tsbgw_db_workers_restart);




#endif							/* TSBGW_INTERFACE_H */
