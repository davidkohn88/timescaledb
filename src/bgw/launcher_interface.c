/*
 * Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
 *
 * This file is licensed under the Apache License,
 * see LICENSE-APACHE at the top level directory.
 */
#include <postgres.h>

#include <fmgr.h>

#include "extension.h"
#include "launcher_interface.h"
#include "compat.h"

#define MIN_LOADER_API_VERSION 1

extern bool
bgw_worker_reserve(void)
{
	PGFunction	reserve = load_external_function(EXTENSION_NAME, "ts_bgw_worker_reserve", true, NULL);

	return DatumGetBool(DirectFunctionCall1(reserve, BoolGetDatum(false))); /* no function call zero */
}

extern void
bgw_worker_release(void)
{
	PGFunction	release = load_external_function(EXTENSION_NAME, "ts_bgw_worker_release", true, NULL);

	DirectFunctionCall1(release, BoolGetDatum(false));	/* no function call zero */
}

extern int
bgw_num_unreserved(void)
{
	PGFunction	unreserved = load_external_function(EXTENSION_NAME, "ts_bgw_num_unreserved", true, NULL);

	return DatumGetInt32(DirectFunctionCall1(unreserved, BoolGetDatum(false))); /* no function call zero */
}

extern int
bgw_loader_api_version(void)
{
	void	  **versionptr = find_rendezvous_variable(RENDEZVOUS_BGW_LOADER_API_VERSION);

	if (*versionptr == NULL)
		return 0;
	return *((int32 *) *versionptr);
}

extern void
bgw_check_loader_api_version()
{
	int			version = bgw_loader_api_version();

	if (version < MIN_LOADER_API_VERSION)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("loader version out-of-date"),
				 errhint("Please restart the database to upgrade the loader version.")));
}
