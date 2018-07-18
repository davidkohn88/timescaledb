#ifndef TIMESCALE_BGW_INTERNAL_H
#define TIMESCALE_BGW_INTERNAL_H

#include <postgres.h>

/* Interface internal to the loader */ 
/* would like feedback on whether to do this or to make (some of) these static and include timescale_bgw.c in the loader*/

extern void timescale_bgw_shmem_init(void);
extern void timescale_bgw_shmem_startup(void);

extern void timescale_bgw_on_db_drop(Oid dropped_db);

extern void timescale_bgw_register_cluster_launcher(void);
extern void timescale_bgw_cluster_launcher_main(void); /* this has to be called by the postmaster at bgw startup a so must be registered non-static*/
extern void timescale_bgw_db_scheduler_entrypoint(Oid db_id); /*same*/

#endif /*TIMESCALE_BGW_INTERNAL_H*/