/*
 *
 * Interface for the whole cluster bgw launcher, which simply looks through the dbs and
 * launches a db specific launcher in each db then dies. The db launchers will look to see
 * Timescale is installed and die if it is not. 
 *
 */

typedef struct tsbgw_shared_state {
    LWLock      *lock; /*pointer to shared hashtable lock, to protect modification */
    HTAB        *hashtable;
    slock_t     mutex; /*controls modification of total_workers*/
    int         total_workers;
} tsbgw_shared_state;

typedef struct tsbgw_hash_entry {
    Oid         db_oid; /* key for the hash table, must be first */
    bool        ts_installed;
    char        ts_version[NAMEDATALEN];
    int         db_launcher_pid;
    int         num_active_workers;
} tsbgw_hash_entry;


void timescale_bgw_shmem_init(void);
void register_timescale_bgw_launcher(void);


void timescale_bgw_launcher_main(void); 
void timescale_bgw_db_scheduler_main(Oid db_id);