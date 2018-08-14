#ifndef BGW_LAUNCHER_INTERFACE_H
#define BGW_LAUNCHER_INTERFACE_H
#include <postgres.h>

extern bool tsbgw_worker_reserve(void);
extern void tsbgw_worker_release(void);
extern int	tsbgw_num_unreserved(void);

#endif							/* BGW_LAUNCHER_INTERFACE_H */
