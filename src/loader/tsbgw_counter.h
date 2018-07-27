
#ifndef TSBGW_COUNTER_H
#define TSBGW_COUNTER_H

#include <postgres.h>

#define TSBGW_MAX_WORKERS_GUC_STANDIN 8
extern void tsbgw_counter_shmem_alloc(void);
extern void tsbgw_counter_shmem_startup(void);
extern bool tsbgw_total_workers_increment(void);
extern void tsbgw_total_workers_decrement(void);
extern int	tsbgw_total_workers_get(void);


#endif							/* TSBGW_COUNTER_H */
