#include <postgres.h>

#include <miscadmin.h>
#include <fmgr.h>

#include "tsbgw_counter.h"
#include "tsbgw_message_queue.h"
#include "tsbgw_interface.h"



Datum
tsbgw_worker_reserve(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(tsbgw_total_workers_increment());
}
Datum
tsbgw_worker_release(PG_FUNCTION_ARGS)
{
	tsbgw_total_workers_decrement();
	PG_RETURN_VOID();
}

Datum
tsbgw_num_unreserved(PG_FUNCTION_ARGS)
{
	int			unreserved_workers;

	unreserved_workers = guc_max_bgw_processes - tsbgw_total_workers_get();
	PG_RETURN_INT32(unreserved_workers);
}

Datum
tsbgw_db_workers_start(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(tsbgw_message_send_and_wait(START, MyDatabaseId));
}

Datum
tsbgw_db_workers_stop(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(tsbgw_message_send_and_wait(STOP, MyDatabaseId));
}


Datum
tsbgw_db_workers_restart(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(tsbgw_message_send_and_wait(RESTART, MyDatabaseId));
}
