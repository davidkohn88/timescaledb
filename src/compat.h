/*
 * Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
 *
 * This file is licensed under the Apache License,
 * see LICENSE-APACHE at the top level directory.
 */
#ifndef TIMESCALEDB_COMPAT_H
#define TIMESCALEDB_COMPAT_H

#include <postgres.h>
#include <pgstat.h>

#include "export.h"

#define is_supported_pg_version_96(version) ((version >= 90603) && (version < 100000))
#define is_supported_pg_version_10(version) ((version >= 100002) && (version < 110000))
#define is_supported_pg_version_11(version) ((version >= 110000) && (version < 120000))
#define is_supported_pg_version(version) (is_supported_pg_version_96(version) || is_supported_pg_version_10(version) || is_supported_pg_version_11(version))

#define PG96 is_supported_pg_version_96(PG_VERSION_NUM)
#define PG10 is_supported_pg_version_10(PG_VERSION_NUM)
#define PG11 is_supported_pg_version_11(PG_VERSION_NUM)
#define PG10_11 (is_supported_pg_version_11(PG_VERSION_NUM) || is_supported_pg_version_10(PG_VERSION_NUM))

/* TupleDescAttr was only backpatched to 9.6.5. Make it work under 9.6.3 and 9.6.4 */
#if ((PG_VERSION_NUM >= 90603) && PG_VERSION_NUM < 90605)
#define TupleDescAttrCompat(tupdesc, i) ((tupdesc)->attrs[(i)])
#else
#define TupleDescAttrCompat(tupdesc, i) TupleDescAttr(tupdesc, i)
#endif

#ifndef TupleDescAttr
/*
   backport in PG10 commit d34a74dd
*/
#define TupleDescAttr(a, i) ((a)->attrs[(i)])
#endif

/* //////////////////////////
*/
#if PG10_11

#if PG11
/* ***************************** 
commit fb466d7b5dbe73f318324cada80203522f46401f
Author: Tom Lane <tgl@sss.pgh.pa.us>
Date:   Tue Sep 4 13:45:35 2018 -0400

    Fully enforce uniqueness of constraint names.

pg compile 29 errors
*/
#define ConstraintRelidIndexId ConstraintRelidTypidNameIndexId


/* ***************************** 
commit c9c875a28fa6cbc38c227fb9e656dd7be948166f
Author: Teodor Sigaev <teodor@sigaev.ru>
Date:   Thu Apr 12 13:02:45 2018 +0300

    Rename IndexInfo.ii_KeyAttrNumbers array

pg compile 14 errors
*/
#define ii_KeyAttrNumbers ii_IndexAttrNumbers


/* ***************************** 
commit eed1ce72e1593d3e8b7461d7744808d4d6bf402b
Author: Magnus Hagander <magnus@hagander.net>
Date:   Thu Apr 5 18:59:32 2018 +0200

    Allow background workers to bypass datallowconn
    
    THis adds a "flags" field to the BackgroundWorkerInitializeConnection()
    and BackgroundWorkerInitializeConnectionByOid(). For now only one flag,
    BGWORKER_BYPASS_ALLOWCONN, is defined, which allows the worker to ignore
    datallowconn.
*/
#define BackgroundWorkerInitializeConnectionComp(a,b) \
	BackgroundWorkerInitializeConnection((a), (b), 0)
#define BackgroundWorkerInitializeConnectionByOidComp(a, b) \
	BackgroundWorkerInitializeConnectionByOid((a), (b), 0)


/* ***************************** 
commit 16828d5c0273b4fe5f10f42588005f16b415b2d8
Author: Andrew Dunstan <andrew@dunslane.net>
Date:   Wed Mar 28 10:43:52 2018 +1030

    Fast ALTER TABLE ADD COLUMN with a non-NULL default

pg compile 19 errors
*/
#define heap_attisnull(a,b) \
	heap_attisnull( a, b, NULL)


/* ***************************** 
commit 555ee77a9668e3f1b03307055b5027e13bf1a715
Author: Alvaro Herrera <alvherre@alvh.no-ip.org>
Date:   Mon Mar 26 10:43:54 2018 -0300

    Handle INSERT .. ON CONFLICT with partitioned tables

pg compile 11 errors
*/
#define ri_isvalid(a) ((a)->ri_onConflict)
#define ri_onConflictSetProj ri_onConflict->oc_ProjInfo
#define  ri_onConflictSetWhere ri_onConflict->oc_WhereClause


/* ***************************** 
commit 86f575948c773b0ec5b0f27066e37dd93a7f0a96
Author: Alvaro Herrera <alvherre@alvh.no-ip.org>
Date:   Fri Mar 23 10:48:22 2018 -0300

    Allow FOR EACH ROW triggers on partitioned tables
*/
#define  CreateTrigger(a, b, c, d, e, f, g) \
    CreateTrigger(a, b, c, d, e, f, InvalidOid, InvalidOid,NULL, g, false)


/* ***************************** 
commit 7a50bb690b4837d29e715293c156cff2fc72885c
Author: Andres Freund <andres@anarazel.de>
Date:   Fri Mar 16 23:13:12 2018 -0700

    Add 'unit' parameter to ExplainProperty{Integer,Float}.
*/
#define ExplainPropertyInteger(a, b, c) \
	ExplainPropertyInteger(a, NULL, b, c) 


/* ***************************** 
commit 04700b685f31508036456bea4d92533e5ceee9d6
Author: Peter Eisentraut <peter_e@gmx.net>
Date:   Fri Feb 16 20:44:15 2018 -0500

    Rename TransactionChain functions

pg compile 9 errors
*/
#define PreventTransactionChain PreventInTransactionBlock


/* ***************************** 
commit eb7ed3f3063401496e4aa4bd68fa33f0be31a72f
Author: Alvaro Herrera <alvherre@alvh.no-ip.org>
Date:   Mon Feb 19 16:59:37 2018 -0300

    Allow UNIQUE indexes on partitioned tables
*/
#define DefineIndex(relationId, stmt, indexRelationId, is_alter_table, check_rights, check_not_in_use, skip_build, quiet) \
	DefineIndex(relationId, stmt, indexRelationId, InvalidOid, InvalidOid, is_alter_table, check_rights, check_not_in_use, skip_build, quiet)

/* ***************************** 
commit ad7dbee368a7cd9e595d2a957be784326b08c943
Author: Andres Freund <andres@anarazel.de>
Date:   Fri Feb 16 21:17:38 2018 -0800

    Allow tupleslots to have a fixed tupledesc, use in executor nodes.

*/    
#define MakeTupleTableSlotComp() \
	MakeTupleTableSlot(NULL)

#define ExecInitExtraTupleSlotComp(a) \
	ExecInitExtraTupleSlot(a, NULL)


/* ***************************** 
commit 8237f27b504ff1d1e2da7ae4c81a7f72ea0e0e3e
Author: Alvaro Herrera <alvherre@alvh.no-ip.org>
Date:   Mon Feb 12 19:30:30 2018 -0300

    get_relid_attribute_name is dead, long live get_attname
    
    The modern way is to use a missing_ok argument instead of two separate
    almost-identical routines, so do that.

pg compile 8 errors
*/
#define get_attname(a,b) \
	get_attname(a,b, false)


/* ***************************** 
commit 8b9e9644dc6a9bd4b7a97950e6212f63880cf18b
Author: Peter Eisentraut <peter_e@gmx.net>
Date:   Sat Dec 2 09:26:34 2017 -0500

    Replace AclObjectKind with ObjectType
*/
#define ACL_KIND_CLASS OBJECT_INDEX
#define ACL_OBJECT_TABLESPACE  OBJECT_TABLESPACE


/* ***************************** 
commit 8b08f7d4820fd7a8ef6152a9dd8c6e3cb01e5f99
Author: Alvaro Herrera <alvherre@alvh.no-ip.org>
Date:   Fri Jan 19 11:49:22 2018 -0300

    Local partitioned indexes

    Revert
*/
#if 0
#define DefineIndex(relationId, stmt, indexRelationId, is_alter_table, check_rights, check_not_in_use, skip_build, quiet) \
	DefineIndex(relationId, stmt, indexRelationId, InvalidOid, is_alter_table, check_rights, check_not_in_use, skip_build, quiet)
#endif

/* *****************************
commit 19c47e7c820241e1befd975cb4411af7d43e1309
Author: Robert Haas <rhaas@postgresql.org>
Date:   Fri Jan 5 15:18:03 2018 -0500

    Factor error generation out of ExecPartitionCheck.
*/
#if 0
#define  ExecConstraints(resultRelInfo, slot, estate) \
	ExecConstraints(resultRelInfo, slot, estate, true);
#endif

/* *****************************
commit 4bd1994650fddf49e717e35f1930d62208845974
Author: Tom Lane <tgl@sss.pgh.pa.us>
Date:   Mon Sep 18 15:21:23 2017 -0400

    Make DatumGetFoo/PG_GETARG_FOO/PG_RETURN_FOO macro names more consistent.
*/
#define PG_RETURN_JSONB(x) PG_RETURN_JSONB_P(x)

/* ***************************** 
commit 398f70ec070fe60151584eaa448f04708aa77892
Author: Tom Lane <tgl@sss.pgh.pa.us>
Date:   Tue Feb 14 17:34:19 2012 -0500

    Preserve column names in the execution-time tupledesc for a RowExpr.
*/
#define adjust_appendrel_attrs(a, b, c) \
	adjust_appendrel_attrs((a), (b), 1, &(c))


#undef PG10
#define PG10 (1)

#elif PG10
/* stuff in PG11 but not in PG10 */
#define MakeTupleTableSlotComp() \
	MakeTupleTableSlot()

#define ExecInitExtraTupleSlotComp(a) \
	ExecInitExtraTupleSlot(a)

#define BackgroundWorkerInitializeConnectionComp(a,b) \
	BackgroundWorkerInitializeConnection((a), (b))
#define BackgroundWorkerInitializeConnectionByOidComp(a, b) \
	BackgroundWorkerInitializeConnectionByOid((a), (b))

#define ri_isvalid(a) (1)

#endif
#endif

#if PG10
/* stuff in PG11 and in PG10 */
#define ExecARInsertTriggersCompat(estate, result_rel_info, tuple, recheck_indexes) \
	ExecARInsertTriggers(estate, result_rel_info, tuple, recheck_indexes, NULL)
#define ExecASInsertTriggersCompat(estate, result_rel_info) \
	ExecASInsertTriggers(estate, result_rel_info, NULL)
#define InitResultRelInfoCompat(result_rel_info, result_rel_desc, result_rel_index, instrument_options) \
	InitResultRelInfo(result_rel_info, result_rel_desc, result_rel_index, NULL, instrument_options)
#define CheckValidResultRelCompat(relinfo, operation)	\
	CheckValidResultRel(relinfo, operation)
#define ParseFuncOrColumnCompat(pstate, funcname, fargs, fn, location) \
	ParseFuncOrColumn(pstate, funcname, fargs, (pstate)->p_last_srf, fn, location)
#define make_op_compat(pstate, opname, ltree, rtree, location)	\
	make_op(pstate, opname, ltree, rtree, (pstate)->p_last_srf, location)
#define get_projection_info_slot_compat(pinfo) \
	(pinfo->pi_state.resultslot)
#define map_variable_attnos_compat(returning_clauses, varno, sublevels_up, map, map_size, rowtype, found_whole_row) \
	map_variable_attnos(returning_clauses, varno, sublevels_up, map, map_size, rowtype, found_whole_row);
#define ExecBuildProjectionInfoCompat(tl, exprContext, slot, parent, inputdesc) \
	 ExecBuildProjectionInfo(tl, exprContext, slot, parent, inputdesc)
#define WaitLatchCompat(latch, wakeEvents, timeout) \
	WaitLatch(latch, wakeEvents, timeout, PG_WAIT_EXTENSION)

#elif PG96

#define ExecARInsertTriggersCompat(estate, result_rel_info, tuple, recheck_indexes) \
	ExecARInsertTriggers(estate, result_rel_info, tuple, recheck_indexes)
#define ExecASInsertTriggersCompat(estate, result_rel_info) \
	ExecASInsertTriggers(estate, result_rel_info)
#define InitResultRelInfoCompat(result_rel_info, result_rel_desc, result_rel_index, instrument_options) \
	InitResultRelInfo(result_rel_info, result_rel_desc, result_rel_index, instrument_options)
#define CheckValidResultRelCompat(relinfo, operation) \
	CheckValidResultRel((relinfo)->ri_RelationDesc, operation)
#define ParseFuncOrColumnCompat(pstate, funcname, fargs, fn, location) \
	ParseFuncOrColumn(pstate, funcname, fargs, fn, location)
#define make_op_compat(pstate, opname, ltree, rtree, location)	\
	make_op(pstate, opname, ltree, rtree, location)
#define get_projection_info_slot_compat(pinfo) \
	(pinfo->pi_slot)
#define map_variable_attnos_compat(expr, varno, sublevels_up, map, map_size, rowtype, found_whole_row) \
	map_variable_attnos(expr, varno, sublevels_up, map, map_size, found_whole_row)
#define ExecBuildProjectionInfoCompat(tl, exprContext, slot, parent, inputdesc) \
	 ExecBuildProjectionInfo((List *)ExecInitExpr((Expr *) tl, NULL), exprContext, slot, inputdesc)
#define WaitLatchCompat(latch, wakeEvents, timeout) \
	WaitLatch(latch, wakeEvents, timeout)

#define MakeTupleTableSlotComp() \
	MakeTupleTableSlot()

#define ExecInitExtraTupleSlotComp(a) \
	ExecInitExtraTupleSlot(a)

#define BackgroundWorkerInitializeConnectionComp(a,b) \
	BackgroundWorkerInitializeConnection((a), (b))
#define BackgroundWorkerInitializeConnectionByOidComp(a, b) \
	BackgroundWorkerInitializeConnectionByOid((a), (b))

#define ri_isvalid(a) (1)
#else

#error "Unsupported PostgreSQL version"

#endif							/* PG_VERSION_NUM */

#endif							/* TIMESCALEDB_COMPAT_H */
