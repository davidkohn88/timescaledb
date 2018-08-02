#ifndef TIMESCALEDB_LOADER_GUC_H
#define TIMESCALEDB_LOADER_GUC_H
#include <postgres.h>

extern int			guc_max_bgw_processes;
extern int			guc_tsbgw_launcher_restart_time;


void		_loader_guc_init(void);
void		_loader_guc_fini(void);

#endif /*TIMESCALEDB_LOADER_GUC_H*/
