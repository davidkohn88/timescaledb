#include <postgres.h>
#include <utils/guc.h>
#include <miscadmin.h>

#include "loader_guc.h"


int			guc_max_bgw_processes = 8;
int			guc_tsbgw_launcher_restart_time = 10;


void
_loader_guc_init(void)
{


	DefineCustomIntVariable("timescaledb.max_bgw_processes",
							"Maximum background worker processes allocated to TimescaleDB",
							"Max background worker processes allocated to TimescaleDB - set to at least 1 + number of databases in Postgres instance to use background workers ",
							&guc_max_bgw_processes,
							guc_max_bgw_processes,
							0,
							max_worker_processes,
							PGC_POSTMASTER,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("timescaledb.tsbgw_launcher_restart_time",
							"Time after which Postmaster will re-launch TimescaleDB BGW launcher",
							"Time after which Postmaster will re-launch TimescaleDB BGW launcher - mainly exposed to limit time needed during testing.",
							&guc_tsbgw_launcher_restart_time,
							guc_tsbgw_launcher_restart_time,
							0,
							65536,
							PGC_POSTMASTER,
							0,
							NULL,
							NULL,
							NULL);
}

void
_loader_guc_fini(void)
{
}
