/*****************************************************************************\
 *  Copyright (c) 2014 Lawrence Livermore National Security, LLC.  Produced at
 *  the Lawrence Livermore National Laboratory (cf, AUTHORS, DISCLAIMER.LLNS).
 *  LLNL-CODE-658032 All rights reserved.
 *
 *  This file is part of the Flux resource manager framework.
 *  For details, see https://github.com/flux-framework.
 *
 *  This program is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU General Public License as published by the Free
 *  Software Foundation; either version 2 of the license, or (at your option)
 *  any later version.
 *
 *  Flux is distributed in the hope that it will be useful, but WITHOUT
 *  ANY WARRANTY; without even the IMPLIED WARRANTY OF MERCHANTABILITY or
 *  FITNESS FOR A PARTICULAR PURPOSE.  See the terms and conditions of the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License along
 *  with this program; if not, write to the Free Software Foundation, Inc.,
 *  59 Temple Place, Suite 330, Boston, MA 02111-1307 USA.
 *  See also:  http://www.gnu.org/licenses/
\*****************************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <errno.h>
#include <json/json.h>
#include <time.h>
#include <inttypes.h>

#include "src/common/libutil/jsonutil.h"
#include "src/common/libutil/log.h"
#include "src/common/libutil/shortjson.h"
#include "rdl.h"
#include "scheduler.h"

const char* IDLETAG = "idle";
const char* CORETYPE = "core";

int send_rdl_update (flux_t h, struct rdl* rdl)
{
    JSON o = Jnew();

    Jadd_int64(o, "rdl_int", (int64_t) rdl);

	if (flux_event_send (h, o, "%s", "rdl.update") < 0){
		Jput(o);
		return -1;
	}

    Jput (o);
    return 0;
}

struct rdl *get_free_subset (struct rdl *rdl, const char *type)
{
	JSON tags = Jnew();
	Jadd_bool (tags, IDLETAG, true);
	JSON args = Jnew ();
	Jadd_obj (args, "tags", tags);
	Jadd_str (args, "type", type);
    double start, diff, seconds;
    start = clock();
    struct rdl *frdl = rdl_find (rdl, args);
	diff = clock() - start;
	seconds = ((double) diff) / CLOCKS_PER_SEC;
    printf("rdl_find took %f seconds\n", seconds);
	Jput (args);
	Jput (tags);
	return frdl;
}

static int64_t count_free (struct resource *r, const char *type)
{
    int64_t curr_count = 0;
    JSON o = NULL;
	const char *curr_type = NULL;
    struct resource *child = NULL;

    if (r) {
        rdl_resource_iterator_reset(r);
        while ((child = rdl_resource_next_child (r))) {
            curr_count += count_free (child, type);
            rdl_resource_destroy (child);
        }
        rdl_resource_iterator_reset(r);

        o = rdl_resource_json (r);
        Jget_str (o, "type", &curr_type);
        if (strcmp (type, curr_type)) {
            curr_count++;
        }
        Jput (o);
    }

    return curr_count;
}

int64_t get_free_count (struct rdl *rdl, const char *uri, const char *type)
{
	struct resource *fr = NULL;
    int64_t count = 0;

	if ((fr = rdl_resource_get (rdl, uri)) == NULL) {
		return -1;
	}
    count = count_free (fr, type);
    rdl_resource_destroy(fr);

    return count;
}

/*
 * Walk the tree, find the required resources and tag with the lwj_id
 * to which it is allocated.
 */
bool allocate_resources (struct rdl *rdl, const char *hierarchy,
                         struct resource *fr, struct rdl_accumulator *accum,
                         flux_lwj_t *job, zlist_t *ancestors)
{
    char *lwjtag = NULL;
    char *uri = NULL;
    const char *type = NULL;
    JSON o = NULL, o2 = NULL, o3 = NULL;
    struct resource *child, *curr;
    bool found = false;

    asprintf (&uri, "%s:%s", hierarchy, rdl_resource_path (fr));
    curr = rdl_resource_get (rdl, uri);
    free (uri);

    o = rdl_resource_json (curr);
    Jget_str (o, "type", &type);
    asprintf (&lwjtag, "lwj.%ld", job->lwj_id);
    if (job->req.nnodes && (strcmp (type, "node") == 0)) {
        job->req.nnodes--;
        job->alloc.nnodes++;
    } else if (job->req.ncores && (strcmp (type, CORETYPE) == 0) &&
               (job->req.ncores > job->req.nnodes)) {
        /* We put the (job->req.ncores > job->req.nnodes) requirement
         * here to guarantee at least one core per node. */
        Jget_obj (o, "tags", &o2);
        Jget_obj (o2, IDLETAG, &o3);
        if (o3) {
			if (allocate_bandwidth (job, curr, ancestors)) {
              job->req.ncores--;
              job->alloc.ncores++;
              rdl_resource_tag (curr, lwjtag);
              rdl_resource_set_int (curr, "lwj", job->lwj_id);
              rdl_resource_delete_tag (curr, IDLETAG);
              if (rdl_accumulator_add (accum, curr) < 0) {
                  //TODO: re-enable this error printing
                  //flux_log (h, LOG_ERR, "failed to allocate core: %s", Jtostr (o));
              }
            }
        }
    }
    free (lwjtag);
    Jput (o);

    found = !(job->req.nnodes || job->req.ncores);

	zlist_push (ancestors, curr);
    rdl_resource_iterator_reset (fr);
    while (!found && (child = rdl_resource_next_child (fr))) {
        found = allocate_resources (rdl, hierarchy, child, accum, job, ancestors);
        rdl_resource_destroy (child);
    }
    rdl_resource_iterator_reset (fr);
	zlist_pop (ancestors);
    rdl_resource_destroy (curr);

    return found;
}

static void remove_job_resources_helper (struct rdl *rdl, const char *uri,
                                         struct resource *curr_res)
{
    struct resource *rdl_res = NULL, *child_res = NULL;
    char *res_path = NULL;
    const char *type = NULL, *child_name = NULL;
    JSON o = NULL;

    rdl_resource_iterator_reset (curr_res);
    while ((child_res = rdl_resource_next_child (curr_res))) {
        o = rdl_resource_json (child_res);
        Jget_str (o, "type", &type);
        //Check if child matches, if so, unlink from hierarchy
        if (strcmp (type, CORETYPE) == 0) {
            asprintf (&res_path, "%s:%s", uri, rdl_resource_path (curr_res));
            rdl_res = rdl_resource_get (rdl, res_path);
            child_name = rdl_resource_name (child_res);
            rdl_resource_unlink_child (rdl_res, child_name);
            free (res_path);
        } else { //Else, recursive call on child
            remove_job_resources_helper (rdl, uri, child_res);
        }
        Jput (o);
        rdl_resource_destroy (child_res);
    }
    rdl_resource_iterator_reset (curr_res);
}

void remove_job_resources_from_rdl (struct rdl *rdl, const char *uri, flux_lwj_t *job)
{
    struct resource *job_rdl_root = NULL;

    job_rdl_root = rdl_resource_get (job->rdl, uri);
    remove_job_resources_helper (rdl, uri, job_rdl_root);
    rdl_resource_destroy (job_rdl_root);
}
