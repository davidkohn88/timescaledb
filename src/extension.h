#ifndef TIMESCALEDB_EXTENSION_H
#define TIMESCALEDB_EXTENSION_H
#include <postgres.h>


#define EXTENSION_NAME "timescaledb"
#define MAX_VERSION_LEN (NAMEDATALEN+1)
#define MAX_SO_NAME_LEN (NAMEDATALEN+1+MAX_VERSION_LEN) /* extname+"-"+version */



bool		extension_invalidate(Oid relid);
bool		extension_is_loaded(void);
void		extension_check_version(const char *so_version);


#endif							/* TIMESCALEDB_EXTENSION_H */
