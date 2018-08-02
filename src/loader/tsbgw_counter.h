
#ifndef TSBGW_COUNTER_H
#define TSBGW_COUNTER_H

#include <postgres.h>



extern int			guc_max_bgw_processes;

extern void tsbgw_counter_shmem_alloc(void);
extern void tsbgw_counter_shmem_startup(void);

extern void tsbgw_counter_setup_gucs(void);

extern void tsbgw_counter_shmem_cleanup(void);
extern bool tsbgw_total_workers_increment(void);
extern void tsbgw_total_workers_decrement(void);
extern int	tsbgw_total_workers_get(void);


#endif							/* TSBGW_COUNTER_H */
