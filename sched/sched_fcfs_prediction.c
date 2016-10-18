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

#if HAVE_CONFIG_H
# include <config.h>
#endif
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <argz.h>
#include <errno.h>
#include <libgen.h>
#include <czmq.h>
#include <flux/core.h>

#include "src/common/libutil/log.h"
#include "src/common/libutil/shortjson.h"
#include "src/common/libutil/xzmalloc.h"
#include "resrc.h"
#include "resrc_tree.h"
#include "resrc_reqst.h"
#include "scheduler.h"

struct sched_ctx_struct {
    zlist_t *allocation_completion_times;
    zlist_t *reservation_times;
    int64_t prev_alloc_starttime ;
    int64_t prev_reservation_starttime;
    int64_t curr_reservation_depth;
    int64_t max_reservation_depth;
};

static int64_t get_max_prev_starttime (sched_ctx_t *ctx)
{
    //TODO: use a max func or ternary op
    int64_t max_prev_starttime = ctx->prev_alloc_starttime;
    if (ctx->prev_reservation_starttime > ctx->prev_alloc_starttime)
        max_prev_starttime = ctx->prev_reservation_starttime;
    return max_prev_starttime;
}

#if CZMQ_VERSION < CZMQ_MAKE_VERSION(3, 0, 1)
static bool compare_int64_ascending (void *item1, void *item2)
{
    int64_t time1 = *((int64_t *) item1);
    int64_t time2 = *((int64_t *) item2);

    return time1 > time2;
}
#else
static int compare_int64_ascending (void *item1, void *item2)
{
    int64_t time1 = *((int64_t *) item1);
    int64_t time2 = *((int64_t *) item2);

    return time1 - time2;
}
#endif

static bool select_children (flux_t h, resrc_tree_list_t *children,
                             resrc_reqst_list_t *reqst_children,
                             resrc_tree_t *selected_parent);

resrc_tree_t *select_resources (flux_t h, sched_ctx_t *ctx, resrc_tree_t *found_tree,
                                resrc_reqst_t *resrc_reqst,
                                resrc_tree_t *selected_parent);

static resrc_tree_t *internal_select_resources (flux_t h, resrc_tree_t *found_tree,
                                resrc_reqst_t *resrc_reqst,
                                resrc_tree_t *selected_parent);

int sched_loop_setup (flux_t h, sched_ctx_t* ctx)
{
    if (!ctx) {
        flux_log (h, LOG_ERR, "%s: invalid arguments", __FUNCTION__);
        return -1;
    }

    zlist_purge (ctx->reservation_times);
    ctx->prev_reservation_starttime = -1;
    ctx->curr_reservation_depth = 0;

    flux_log (h, LOG_DEBUG, "%s: %zu items in allocation_completion_times",
              __FUNCTION__, zlist_size (ctx->allocation_completion_times));

    int64_t *completion_time = NULL;
    for (completion_time = zlist_first (ctx->allocation_completion_times);
         completion_time;
         completion_time = zlist_next (ctx->allocation_completion_times)) {
        if (*completion_time < ctx->prev_alloc_starttime) {
            flux_log (h, LOG_DEBUG,
                      "%s: completion time %"PRId64" < previous job starttime %"PRId64", removing from zlist",
                      __FUNCTION__, *completion_time, ctx->prev_alloc_starttime);
            zlist_remove (ctx->allocation_completion_times, completion_time);
        }
    }

    return 0;
}

/*
 * find_resources() identifies the all of the resource candidates for
 * the job.  The set of resources returned could be more than the job
 * requires.  A later call to select_resources() will cull this list
 * down to the most appropriate set for the job.
 *
 * Inputs:  resrcs      - hash table of all resources
 *          resrc_reqst - the resources the job requests
 * Returns: nfound      - the number of resources found
 *          found_tree  - a resource tree containing resources that satisfy the
 *                        job's request or NULL if none are found
 */
int64_t find_resources (flux_t h, resrc_t *resrc, resrc_reqst_t *resrc_reqst,
                        resrc_tree_t **found_tree)
{
    int64_t nfound = 0;

    if (!resrc || !resrc_reqst) {
        flux_log (h, LOG_ERR, "%s: invalid arguments", __FUNCTION__);
        goto ret;
    }

    *found_tree = NULL;
    nfound = resrc_tree_search (resrc, resrc_reqst, found_tree, true);

    if (!nfound && *found_tree) {
        resrc_tree_destroy (*found_tree, false);
        *found_tree = NULL;
    }
ret:
    return nfound;
}

/*
 * cycles through all of the resource children and returns true when
 * the requested quantity of resources have been selected.
 */
static bool select_child (flux_t h, resrc_tree_list_t *children,
                          resrc_reqst_t *child_reqst,
                          resrc_tree_t *selected_parent)
{
    resrc_tree_t *child_tree = NULL;
    bool selected = false;

    child_tree = resrc_tree_list_first (children);
    while (child_tree) {
        if (internal_select_resources (h, child_tree, child_reqst, selected_parent) &&
            (resrc_reqst_nfound (child_reqst) >=
             resrc_reqst_reqrd_qty (child_reqst))) {
            selected = true;
            break;
        }
        child_tree = resrc_tree_list_next (children);
    }

    return selected;
}

/*
 * cycles through all of the resource requests and returns true if all
 * of the requested children were selected
 */
static bool select_children (flux_t h, resrc_tree_list_t *children,
                             resrc_reqst_list_t *reqst_children,
                             resrc_tree_t *selected_parent)
{
    resrc_reqst_t *child_reqst = NULL;
    bool selected = false;

    child_reqst = resrc_reqst_list_first (reqst_children);
    while (child_reqst) {
        resrc_reqst_clear_found (child_reqst);
        selected = false;

        if (!select_child (h, children, child_reqst, selected_parent))
            break;
        selected = true;
        child_reqst = resrc_reqst_list_next (reqst_children);
    }

    return selected;
}

static resrc_tree_t *internal_select_resources (flux_t h, resrc_tree_t *found_tree,
                                                resrc_reqst_t *resrc_reqst,
                                                resrc_tree_t *selected_parent)
{
    resrc_t *resrc;
    resrc_tree_list_t *children = NULL;
    resrc_tree_t *child_tree;
    resrc_tree_t *selected_tree = NULL;

    if (!resrc_reqst) {
        flux_log (h, LOG_ERR, "%s: called with empty request", __FUNCTION__);
        return NULL;
    }

    resrc = resrc_tree_resrc (found_tree);
    if (resrc_match_resource (resrc, resrc_reqst, true)) {
        //flux_log (h, LOG_DEBUG, "%s: Resrc matched:", __FUNCTION__);
        //resrc_flux_log (h, resrc);
        if (resrc_reqst_num_children (resrc_reqst)) {
            if (resrc_tree_num_children (found_tree)) {
                selected_tree = resrc_tree_new (selected_parent, resrc);
                if (select_children (h, resrc_tree_children (found_tree),
                                     resrc_reqst_children (resrc_reqst),
                                     selected_tree)) {
                    resrc_stage_resrc (resrc,
                                       resrc_reqst_reqrd_size (resrc_reqst),
                                       resrc_reqst_graph_reqs (resrc_reqst));
                    resrc_reqst_add_found (resrc_reqst, 1);
                } else {
                    resrc_tree_destroy (selected_tree, false);
                }
            }
        } else {
            selected_tree = resrc_tree_new (selected_parent, resrc);
            resrc_stage_resrc (resrc, resrc_reqst_reqrd_size (resrc_reqst),
                                       resrc_reqst_graph_reqs (resrc_reqst));
            resrc_reqst_add_found (resrc_reqst, 1);
        }
    } else if (resrc_tree_num_children (found_tree)) {
        /*
         * This clause visits the children of the current resource
         * searching for a match to the resource request.  The selected
         * tree must be extended to include this intermediate
         * resource.
         *
         * This also allows the resource request to be sparsely
         * defined.  E.g., it might only stipulate a node with 4 cores
         * and omit the intervening socket.
         */
        //flux_log (h, LOG_DEBUG, "%s: Resrc didn't match:", __FUNCTION__);
        //resrc_flux_log (h, resrc);
        selected_tree = resrc_tree_new (selected_parent, resrc);
        children = resrc_tree_children (found_tree);
        for (child_tree = resrc_tree_list_first (children);
             child_tree;
             child_tree = resrc_tree_list_next (children)) {
            if (internal_select_resources (h, child_tree, resrc_reqst, selected_tree) &&
                resrc_reqst_nfound (resrc_reqst) >=
                resrc_reqst_reqrd_qty (resrc_reqst)) {
                if (resrc_reqst_nfound (resrc_reqst) >
                    resrc_reqst_reqrd_qty (resrc_reqst)) {
                    flux_log (h, LOG_ERR, "%s: resrc reqst , num found: %"PRId64" > num_reqd: %"PRId64"",
                              __FUNCTION__, resrc_reqst_nfound (resrc_reqst), resrc_reqst_reqrd_qty (resrc_reqst));
                }
                break;
            }
        }
    }

    return selected_tree;
}

/*
 * select_resources() selects from the set of resource candidates the
 * best resources for the job.
 *
 * Inputs:  found_tree      - tree of resource tree candidates
 *          resrc_reqst     - the resources the job requests
 *          selected_parent - parent of the selected resource tree
 * Returns: a resource tree of however many resources were selected
 */
resrc_tree_t *select_resources (flux_t h, sched_ctx_t* ctx, resrc_tree_t *found_tree,
                                resrc_reqst_t *resrc_reqst,
                                resrc_tree_t *selected_parent)
{
    if (!ctx) {
        flux_log (h, LOG_ERR, "%s: invalid arguments", __FUNCTION__);
        return NULL;
    }

    int64_t reqst_starttime = resrc_reqst_starttime (resrc_reqst);
    int64_t max_prev_starttime = get_max_prev_starttime (ctx);

    if (reqst_starttime < max_prev_starttime) {
        flux_log (h, LOG_DEBUG, "%s: request starttime (%"PRId64") < max_prev_startttime (%"PRId64"), skipping",
                  __FUNCTION__, reqst_starttime, max_prev_starttime);
        return resrc_tree_new (selected_parent, NULL);
    }

    flux_log (h, LOG_DEBUG,
              "%s: request starttime (%"PRId64") >= max_prev_startttime (%"PRId64"), attempting to select resources",
              __FUNCTION__, reqst_starttime, max_prev_starttime);
    return internal_select_resources (h, found_tree, resrc_reqst, selected_parent);
}

int allocate_resources (flux_t h, sched_ctx_t *ctx, resrc_tree_t *selected_tree, int64_t job_id,
                        int64_t starttime, int64_t endtime)
{
    int rc = -1;

    if (!ctx) {
        flux_log (h, LOG_ERR, "%s: invalid arguments", __FUNCTION__);
        return -1;
    }

    if (selected_tree) {
        rc = resrc_tree_allocate (selected_tree, job_id, starttime, endtime);

        if (!rc) {
            int64_t *completion_time = xzmalloc (sizeof(int64_t));
            *completion_time = endtime;
            rc = zlist_append (ctx->allocation_completion_times, completion_time);
            zlist_freefn (ctx->allocation_completion_times, completion_time, free, true);
            flux_log (h, LOG_DEBUG, "Allocated job %"PRId64" from %"PRId64" to "
                      "%"PRId64"", job_id, starttime, *completion_time);
            ctx->prev_alloc_starttime = starttime;
        }
    }

    return rc;
}

/*
 * reserve_resources() reserves resources for the specified job id.
 * Unlike the FCFS version where selected_tree provides the tree of
 * resources to reserve, this backfill version will search into the
 * future to find a time window when all of the required resources are
 * available, reserve those, and return the pointer to the selected
 * tree.
 */
int reserve_resources (flux_t h, sched_ctx_t *ctx, resrc_tree_t **selected_tree, int64_t job_id,
                       int64_t starttime, int64_t walltime, resrc_t *resrc,
                       resrc_reqst_t *resrc_reqst)
{
    int rc = -1, reserve_rc = -1;
    int64_t *completion_time = NULL;
    int64_t nfound = 0;
    int64_t prev_completion_time = -1;
    resrc_tree_t *found_tree = NULL;
    zlist_t *completion_times = zlist_new ();
    int i = 0;

    if (!resrc || !resrc_reqst || !ctx) {
        flux_log (h, LOG_ERR, "%s: invalid arguments", __FUNCTION__);
        goto ret;
    }

    if (ctx->curr_reservation_depth >= ctx->max_reservation_depth) {
        goto ret;
    }
    ctx->curr_reservation_depth++;

    if (*selected_tree) {
        resrc_tree_destroy (*selected_tree, false);
        *selected_tree = NULL;
    }

    for (completion_time = zlist_first (ctx->allocation_completion_times);
         completion_time;
         completion_time = zlist_next (ctx->allocation_completion_times)) {
        zlist_append (completion_times, completion_time);
    }
    for (completion_time = zlist_first (ctx->reservation_times);
         completion_time;
         completion_time = zlist_next (ctx->reservation_times)) {
        zlist_append (completion_times, completion_time);
    }

    zlist_sort (completion_times, compare_int64_ascending);

    int64_t max_prev_starttime = get_max_prev_starttime (ctx);

    flux_log (h, LOG_DEBUG, "%s: %zu times to consider", __FUNCTION__, zlist_size (completion_times));
    for (completion_time = zlist_first (completion_times);
         completion_time && *completion_time < max_prev_starttime;
         completion_time = zlist_next (completion_times), i++) {
        // Skip past jobs that complete before this job is allowed to start
        /* flux_log (h, LOG_DEBUG, */
        /*           "%s: completion_time %"PRId64" (#%d) < max_prev_starttime %"PRId64", skipping", */
        /*           __FUNCTION__, *completion_time, i, max_prev_starttime); */
    }

    for (;
         completion_time;
         completion_time = zlist_next (completion_times), i++) {
        /* Purge past times from consideration */
        if (*completion_time < starttime) {
            /* flux_log (h, LOG_DEBUG, */
            /*           "%s: completion_time %"PRId64" (#%d) < starttime %"PRId64", skipping", */
            /*           __FUNCTION__, *completion_time, i, starttime); */
            continue;
        }

        /* Don't test the same time multiple times */
        if (prev_completion_time == *completion_time)
            continue;

        resrc_reqst_set_starttime (resrc_reqst, *completion_time + 1);
        resrc_reqst_set_endtime (resrc_reqst, *completion_time + 1 + walltime);
        flux_log (h, LOG_DEBUG, "Attempting to reserve %"PRId64" nodes for job "
                  "%"PRId64" at time %"PRId64" (#%d)",
                  resrc_reqst_reqrd_qty (resrc_reqst), job_id,
                  *completion_time + 1, i);

        resrc_reqst_clear_found (resrc_reqst);
        resrc_tree_unstage_resources (resrc_phys_tree (resrc));
        nfound = resrc_tree_search (resrc, resrc_reqst, &found_tree, true);
        flux_log (h, LOG_DEBUG, "%s: num found: %"PRId64", num requested %"PRId64"", __FUNCTION__, nfound, resrc_reqst_reqrd_qty (resrc_reqst));
        if (nfound >= resrc_reqst_reqrd_qty (resrc_reqst)) {
            resrc_reqst_clear_found (resrc_reqst);
            resrc_tree_unstage_resources (resrc_phys_tree (resrc));
            *selected_tree = internal_select_resources (h, found_tree, resrc_reqst, NULL);
            if (*selected_tree) {
                reserve_rc = resrc_tree_reserve (*selected_tree, job_id,
                                         *completion_time + 1,
                                         *completion_time + 1 + walltime);
                if (reserve_rc) {
                    flux_log (h, LOG_ERR, "%s: resrc_tree_reserve returned %d, not reserving", __FUNCTION__, rc);
                    resrc_tree_flux_log (h, *selected_tree);
                    resrc_tree_unstage_resources (*selected_tree);
                    resrc_tree_destroy (*selected_tree, false);
                    *selected_tree = NULL;
                    rc = -1;
                } else {
                    flux_log (h, LOG_DEBUG, "Reserved %"PRId64" nodes for job "
                              "%"PRId64" from %"PRId64" to %"PRId64"",
                              resrc_reqst_reqrd_qty (resrc_reqst), job_id,
                              *completion_time + 1,
                              *completion_time + 1 + walltime);
                    int64_t *time = xzmalloc (sizeof(int64_t));
                    *time = *completion_time + 1 + walltime;
                    zlist_append (ctx->reservation_times, time);
                    zlist_freefn (ctx->reservation_times, time, free, true);
                    time = xzmalloc (sizeof(int64_t));
                    *time = *completion_time + 1;
                    zlist_append (ctx->reservation_times, time);
                    zlist_freefn (ctx->reservation_times, time, free, true);
                    ctx->prev_reservation_starttime = *completion_time + 1;
                    rc = 0;
                }
                break;
            } else {
                flux_log (h, LOG_DEBUG, "%s: select_resources returned NULL, not reserving", __FUNCTION__);
            }
        }
        if (found_tree) {
            resrc_tree_destroy (found_tree, false);
            found_tree = NULL;
        }
        prev_completion_time = *completion_time;
    }

ret:
    if (completion_times) {
        zlist_destroy (&completion_times);
    }
    if (found_tree) {
        resrc_tree_destroy (found_tree, false);
    }
    return rc;
}

int process_args (flux_t h, char *argz, size_t argz_len, const sched_params_t *sp)
{
    return 0;
}

sched_ctx_t *create_ctx (flux_t h)
{
    sched_ctx_t *ctx = malloc (sizeof (sched_ctx_t));

    ctx->allocation_completion_times = zlist_new ();
    ctx->reservation_times = zlist_new ();

    ctx->curr_reservation_depth = -1;
    ctx->prev_alloc_starttime = -1;
    ctx->prev_reservation_starttime = -1;
    ctx->max_reservation_depth = SCHED_PARAM_Q_DEPTH_DEFAULT;

    return ctx;
}

void destroy_ctx (sched_ctx_t *ctx)
{
    if (!ctx)
        return;

    if (ctx->allocation_completion_times)
        zlist_destroy (&ctx->allocation_completion_times);
    if (ctx->reservation_times)
        zlist_destroy (&ctx->reservation_times);

    free (ctx);
}

MOD_NAME ("sched.fcfs-prediction");


/*
 * vi:tabstop=4 shiftwidth=4 expandtab
 */
