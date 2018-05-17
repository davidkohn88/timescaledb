#ifndef TIMESCALEDB_EXTENSION_H
#define TIMESCALEDB_EXTENSION_H
#include <postgres.h>
#include <storage/lwlock.h>
#include <utils/hsearch.h>
#include <storage/spin.h>
#include <storage/shmem.h>       

#define EXTENSION_NAME "timescaledb"
#define MAX_VERSION_LEN (NAMEDATALEN+1)
#define MAX_SO_NAME_LEN (NAMEDATALEN+1+MAX_VERSION_LEN) /* extname+"-"+version */

/* Background Worker structs*/
typedef struct tsbgw_shared_state {
    LWLock      *lock; /*pointer to shared hashtable lock, to protect modification */
    HTAB        *hashtable;
    slock_t     mutex; /*controls modification of total_workers*/
    int         total_workers;
} tsbgw_shared_state;

typedef struct tsbgw_hash_entry {
    Oid         db_oid; /* key for the hash table, must be first */
    bool        ts_installed;
    char        ts_version[MAX_VERSION_LEN];
    int         db_launcher_pid;
    int         num_active_workers;
} tsbgw_hash_entry;

bool		extension_invalidate(Oid relid);
bool		extension_is_loaded(void);
void		extension_check_version(const char *so_version);
void		extension_check_server_version(void);
Oid			extension_schema_oid(void);


#endif							/* TIMESCALEDB_EXTENSION_H */
