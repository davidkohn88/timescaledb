#include <postgres.h>
#include <fmgr.h>
#include <commands/trigger.h>
#include <miscadmin.h>
#include <utils/hsearch.h>
#include <access/tupconvert.h>
#include <utils/rel.h>
#include <utils/relcache.h>
#include <access/xact.h>

#include "chunk.h"
#include "dimension.h"
#include "hypertable.h"
#include "export.h"
#include "utils.h"


/*
 * When tuples in a hypertable that has a continuous aggregate are modified, the
 * lowest modified value and the greatest modified value must be tracked over
 * the course of a transaction or statement. At the end of the statement these
 * values will be inserted into the proper cache invalidation log table for
 * their associated hypertable if they are below the speculative materialization
 * watermark (or, if in REPEATABLE_READ isolation level or higher, they will be
 * inserted no matter what as we cannot see if a materialization transaction has
 * started and moved the watermark during our transaction in that case).
 *
 * We accomplish this at the transaction level by keeping a hash table of each
 * hypertable that has been modified in the transaction and the lowest and
 * greatest modified values. The hashtable will be updated via a trigger that
 * will be called for every row that is inserted, updated or deleted. We use a
 * hashtable because we need to keep track of this on a per hypertable basis and
 * multiple can have tuples modified during a single transaction. (And if we
 * move to per-chunk cache-invalidation it makes it even easier).
 *
 */
typedef struct ContinuousAggsCacheInvalEntry
{
    Oid     hypertable_relid;
    int64   lowest_modified_value;
    int64   greatest_modified_value;
} ContinuousAggsCacheInvalEntry;

#define CA_CACHE_INVAL_INIT_HTAB_SIZE 64

HTAB *continuous_aggs_cache_inval_htab;

TS_FUNCTION_INFO_V1(ts_continuous_agg_trigfn);



void _continuous_aggs_cache_inval_init(void);
void _continuous_aggs_cache_inval_fini(void);

static void cache_inval_htab_init(){
    HASHCTL        ctl;

    memset(&ctl, 0, sizeof(ctl));
    ctl.keysize = sizeof(Oid);
    ctl.entrysize = sizeof(ContinuousAggsCacheInvalEntry);

    continuous_aggs_cache_inval_htab = hash_create("TS Continuous Aggs Cache Inval",CA_CACHE_INVAL_INIT_HTAB_SIZE, &ctl, HASH_ELEM | HASH_BLOBS);
};


/*
 * Take the tuple that has been modified, and the chunk it comes from and
 * extract the time value we care about in our internal format.
 */

static int64 extract_modified_value(HeapTuple modified_tup,  TupleDesc modified_tupledesc, Hypertable *modified_tuple_ht) {
    /*
     * For now this is only going to work for chunks that have the same fields
     * as the hypertable, but when we're ready to harden this we'll use
     * something like: convert_tuples_by_name() to convert tuples, then
     * potentially something like: adjust_hypertable_tlist, which we might have
     * to expose from chunk_insert_state.c. We might also have to deal with the
     * fact that some of those buggers scribble on the input tuple, so we may
     * need to copy the tuple before doing that etc.
     */

    Point   *modified_point;

    modified_point = ts_hyperspace_calculate_point(modified_tuple_ht->space, modified_tup, modified_tupledesc);
    /*
     * Thinking about this further we probably want a way to figure out which
     * key we want to keep track of for a given chunk, and then pass in the
     * attname which we can convert to an attnum and just extract the correct
     * attr here. The assumption right now that it will be the first element in
     * the hypertable's space will hold, but it's not the most efficient nor is
     * it necessarily as good for future development where we might want to do
     * this sort of cache invalidation along multiple dimensions, or potentially
     * with ranges for some dimensions and values for others in the group by.
     */
    return modified_point->coordinates[0];
};

/*
 * Update our hashtable if the value modified is either lower than the lowest or
 * greater than the greatest. Create the hash entry if it does not exist and
 * initialize the values to the current.
 */
static void cache_inval_htab_update(int64 modified_value, Oid hypertable_relid){
    ContinuousAggsCacheInvalEntry *cache_entry;
    bool    found;

    /* On first call, init the hash table*/
    if (!continuous_aggs_cache_inval_htab)
       cache_inval_htab_init();

    cache_entry = (ContinuousAggsCacheInvalEntry *) hash_search(continuous_aggs_cache_inval_htab, &hypertable_relid, HASH_ENTER, &found);
    if (!found)
    {
        cache_entry->hypertable_relid = hypertable_relid;
        cache_entry->lowest_modified_value = modified_value;
        cache_entry->greatest_modified_value = modified_value;
    }
    else
    {
        if (modified_value < cache_entry->lowest_modified_value)
            cache_entry->lowest_modified_value = modified_value;
        else if (modified_value > cache_entry->greatest_modified_value)
            cache_entry->greatest_modified_value = modified_value;
    }
};

Datum ts_continuous_agg_trigfn(PG_FUNCTION_ARGS)
{
    /*
     * Use TriggerData to determine which row to return/work with, in the case
     * of updates, we'll need to call the functions twice, once with the old
     * rows (which act like deletes) and once with the new rows.
     */

    TriggerData *trigdata = (TriggerData *) fcinfo->context;
    Chunk       *modified_tuple_chunk;
    Hypertable  *modified_tuple_ht;

    if (!CALLED_AS_TRIGGER(fcinfo))
        elog(ERROR, "continuous agg trigger function must be called by trigger manager");
    if (!TRIGGER_FIRED_AFTER(trigdata->tg_event) || !TRIGGER_FIRED_FOR_ROW(trigdata->tg_event))
        elog(ERROR, "continuous agg trigger function must be called in per row after trigger");

    modified_tuple_chunk = ts_chunk_get_by_relid(trigdata->tg_relation->rd_id, 0, false);
    if (NULL == modified_tuple_chunk)
        elog(ERROR, "continuous agg trigger function must be called on hypertable chunks only");

    modified_tuple_ht = ts_hypertable_get_by_id(ts_hypertable_relid_to_id(modified_tuple_chunk->hypertable_relid));

    cache_inval_htab_update(extract_modified_value(trigdata->tg_trigtuple, RelationGetDescr(trigdata->tg_relation), modified_tuple_ht), modified_tuple_chunk->hypertable_relid);

    if (TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event))
    {
        cache_inval_htab_update(extract_modified_value(trigdata->tg_newtuple, RelationGetDescr(trigdata->tg_relation), modified_tuple_ht), modified_tuple_chunk->hypertable_relid);
        return PointerGetDatum(trigdata->tg_newtuple);
    }
    else
        return PointerGetDatum(trigdata->tg_trigtuple);

};

/*
 * We'll probably need some helper functions for figuring out where to write
 * this and actually doing the writing, this is stubbed out for now and just raises
 * info because we want to sketch this without knowing all the things that we will
 * have in the catalog for now
 */
static void cache_inval_entry_write(ContinuousAggsCacheInvalEntry *entry){
    elog(INFO, "For hypertable id %d the LMV = %li and the GMV = %li", entry->hypertable_relid, entry->lowest_modified_value, entry->greatest_modified_value);
};
static void cache_inval_htab_cleanup(void){
    hash_destroy(continuous_aggs_cache_inval_htab);
    continuous_aggs_cache_inval_htab = NULL;
};
static void cache_inval_htab_write(void){
    HASH_SEQ_STATUS hash_seq;
	ContinuousAggsCacheInvalEntry *current_entry;

	hash_seq_init(&hash_seq, continuous_aggs_cache_inval_htab);
	while ((current_entry = hash_seq_search(&hash_seq)) != NULL)
		cache_inval_entry_write(current_entry);
};
static void continuous_aggs_pre_commit(XactEvent event, void *arg){

    /* Return quickly if we never initialize the hashtable */
    if (!continuous_aggs_cache_inval_htab)
        return;

	switch (event)
	{
        case XACT_EVENT_PRE_COMMIT:
            cache_inval_htab_write();

		case XACT_EVENT_ABORT:
		case XACT_EVENT_PARALLEL_ABORT:
            cache_inval_htab_cleanup();

		default:
			break;
	}
}
void
_continuous_aggs_cache_inval_init(void)
{
	RegisterXactCallback(continuous_aggs_pre_commit, NULL);
}


void
_continuous_aggs_cache_inval_fini(void)
{
    UnregisterXactCallback(continuous_aggs_pre_commit, NULL);
}
