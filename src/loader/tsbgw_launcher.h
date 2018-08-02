
#ifndef TSBGW_LAUNCHER_H
#define TSBGW_LAUNCHER_H

#include <postgres.h>

extern void tsbgw_cluster_launcher_register(void);

/*called by postmaster at launcher bgw startup*/
extern void tsbgw_cluster_launcher_main(void);
extern void tsbgw_db_scheduler_entrypoint(Oid db_id);
extern void tsbgw_launcher_on_max_workers_guc_change(void);



#endif							/* TSBGW_LAUNCHER_H */
