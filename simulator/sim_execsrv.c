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
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <json.h>
#include <flux/core.h>

#include "src/common/libutil/jsonutil.h"
#include "src/common/libutil/log.h"
#include "src/common/libutil/shortjson.h"
#include "src/common/libutil/xzmalloc.h"
#include "simulator.h"
#include "resrc.h"
#include "resrc_tree.h"

static const char *module_name = "sim_exec";
static int64_t global_curr_time = -1; // used for sorting

typedef struct {
    sim_state_t *sim_state;
    zlist_t *queued_events;  // holds int *
    zlist_t *running_jobs;  // holds job_t *
    flux_t h;
    double prev_sim_time;
    resrc_tree_t *resrc_tree;
    resources_t *resrcs;
} ctx_t;

static double determine_io_penalty (size_t job_bandwidth, size_t min_bandwidth);
static size_t *get_job_min_from_hash (zhash_t *job_hash, int job_id);
static resrc_t* get_io_resrc (resrc_tree_t *resrc_tree);

static void freectx (void *arg)
{
    ctx_t *ctx = arg;
    free_simstate (ctx->sim_state);

    while (zlist_size (ctx->queued_events) > 0)
        free (zlist_pop (ctx->queued_events));
    zlist_destroy (&ctx->queued_events);

    while (zlist_size (ctx->running_jobs) > 0)
        free_job (zlist_pop (ctx->running_jobs));
    zlist_destroy (&ctx->running_jobs);

    resrc_tree_destroy (ctx->resrc_tree, true);
    resrc_destroy_resources (ctx->resrcs);

    free (ctx);
}

static ctx_t *getctx (flux_t h)
{
    ctx_t *ctx = (ctx_t *)flux_aux_get (h, "sim_exec");

    if (!ctx) {
        ctx = xzmalloc (sizeof (*ctx));
        ctx->h = h;
        ctx->sim_state = NULL;
        ctx->queued_events = zlist_new ();
        ctx->running_jobs = zlist_new ();
        ctx->prev_sim_time = 0;
        ctx->resrc_tree = NULL;
        flux_aux_set (h, "simsrv", ctx, freectx);
    }

    return ctx;
}

// Given the kvs dir of a job, change the state of the job and
// timestamp the change
static int update_job_state (ctx_t *ctx,
                             int64_t jobid,
                             kvsdir_t *kvs_dir,
                             job_state_t new_state,
                             double update_time)
{
    char *timer_key = NULL;

    switch (new_state) {
    case J_STARTING: timer_key = "starting_time"; break;
    case J_RUNNING: timer_key = "running_time"; break;
    case J_COMPLETE: timer_key = "complete_time"; break;
    default: break;
    }
    if (timer_key == NULL) {
        flux_log (ctx->h, LOG_ERR, "Unknown state %d", (int) new_state);
        return -1;
    }

    JSON jcb = Jnew ();
    JSON o = Jnew ();

    Jadd_int64 (o, JSC_STATE_PAIR_NSTATE, (int64_t) new_state);
    Jadd_obj (jcb, JSC_STATE_PAIR, o);
    jsc_update_jcb(ctx->h, jobid, JSC_STATE_PAIR, Jtostr (jcb));

    kvsdir_put_double (kvs_dir, timer_key, update_time);
    kvs_commit (ctx->h);

    Jput (jcb);
    Jput (o);

    return 0;
}

static double calc_curr_progress (job_t *job, double sim_time)
{
    double time_passed = sim_time - job->start_time + .000001;
    //double time_necessary = job->execution_time + job->io_time;
    double time_necessary = job->execution_time;
    return time_passed / time_necessary;
}

// Calculate when the next job is going to terminate assuming no new jobs are
// added
static double determine_next_termination (ctx_t *ctx,
                                          double curr_time,
                                          zhash_t *job_hash)
{
    double next_termination = -1, curr_termination = -1;
#if IO_TME
    double projected_future_io_time, job_io_penalty, computation_time_remaining;
    size_t *job_min_bandwidth;
#endif
    zlist_t *running_jobs = ctx->running_jobs;
    job_t *job = zlist_first (running_jobs);

    while (job != NULL) {
        if (job->start_time <= curr_time) {
#if IO_TIME
            curr_termination =
                job->start_time + job->execution_time + job->io_time;
            computation_time_remaining =
                job->execution_time
                - ((curr_time - job->start_time) - job->io_time);
            job_min_bandwidth = get_job_min_from_hash (job_hash, job->id);
            job_io_penalty =
                determine_io_penalty (job->io_rate, *job_min_bandwidth);
            projected_future_io_time =
                (computation_time_remaining)*job_io_penalty;
            curr_termination += projected_future_io_time;
            flux_log (ctx->h, LOG_DEBUG,
                      "Job: %d, req_bw: %"PRId64", min_bw: %zu, io_penalty: %f, future_io_time: %f",
                      job->id, job->io_rate, *job_min_bandwidth, job_io_penalty,
                      projected_future_io_time);
#else
            curr_termination =
                job->start_time + job->execution_time;
#endif
            if (curr_termination < next_termination || next_termination < 0) {
                next_termination = curr_termination;
            }
        }
        job = zlist_next (running_jobs);
    }

    return next_termination;
}

// Set the timer for the given module
static int set_event_timer (ctx_t *ctx, char *mod_name, double timer_value)
{
    double *event_timer = zhash_lookup (ctx->sim_state->timers, mod_name);
    if (timer_value > 0 && (timer_value < *event_timer || *event_timer < 0)) {
        *event_timer = timer_value;
        flux_log (ctx->h,
                  LOG_DEBUG,
                  "next %s event set to %f",
                  mod_name,
                  *event_timer);
    }
    return 0;
}

static void release_job_tree_from_resrcs (resources_t *resrcs,
                                          resrc_tree_t *job_tree,
                                          job_t *job)
{
    resrc_t *job_resrc = resrc_tree_resrc (job_tree);
    char* job_resrc_path = resrc_path (job_resrc);
    resrc_t *resrc = resrc_lookup (resrcs, job_resrc_path);
    resrc_release_resource (resrc, job->id);

    resrc_tree_t *curr_child = NULL;
    resrc_tree_list_t *child_list = resrc_tree_children (job_tree);
    for (curr_child = resrc_tree_list_first (child_list);
         curr_child;
         curr_child = resrc_tree_list_next (child_list)) {
        release_job_tree_from_resrcs (resrcs, curr_child, job);
    }
}

static void release_job_io_from_resrcs (resources_t *resrcs,
                                        resrc_tree_t *job_tree,
                                        job_t *job)
{
    resrc_t *job_resrc = resrc_tree_resrc (job_tree);
    char* job_resrc_path = resrc_path (job_resrc);
    resrc_t *resrc = resrc_lookup (resrcs, job_resrc_path);
    resrc_tree_t *curr_ancestor = NULL;
    resrc_t *io_resrc = NULL;
    for (curr_ancestor = resrc_phys_tree (resrc);
         curr_ancestor;
         curr_ancestor = resrc_tree_parent (curr_ancestor)) {
        io_resrc = get_io_resrc (curr_ancestor);
        resrc_release_resource (io_resrc, job->id);
    }
}

static void release_job_from_resrcs (resources_t *resrcs, job_t *job)
{
    resrc_tree_t *curr_tree = NULL;
    for (curr_tree = resrc_tree_list_first (job->resrc_trees);
         curr_tree;
         curr_tree = resrc_tree_list_next (job->resrc_trees)) {
        release_job_tree_from_resrcs (resrcs, curr_tree, job);
        release_job_io_from_resrcs (resrcs, curr_tree, job);
    }
}

/*
static bool compare_jobs (void *item1, void* item2)
{
    int int1 = ((job_t*)item1)->id;
    int int2 = ((job_t*)item2)->id;

    return int1 > int2;
}

static bool compare_int_strs (void *item1, void* item2)
{
    int int1 = atoi((char*)item1);
    int int2 = atoi((char*)item2);

    return int1 > int2;
}
*/

// Update sched timer as necessary (to trigger an event in sched)
// Also change the state of the job in the KVS
static int complete_job (ctx_t *ctx, job_t *job, double completion_time)
{
    flux_t h = ctx->h;

    flux_log (h, LOG_INFO, "Job %d completed", job->id);

    update_job_state (ctx, job->id, job->kvs_dir, J_COMPLETE, completion_time);
    set_event_timer (ctx, "sched", ctx->sim_state->sim_time + .00001);
    kvsdir_put_double (job->kvs_dir, "complete_time", completion_time);
    kvsdir_put_double (job->kvs_dir, "io_time", job->io_time);
    release_job_from_resrcs(ctx->resrcs, job);
    kvs_commit (h);
    free_job (job);

    return 0;
}

// Remove completed jobs from the list of running jobs
// Update sched timer as necessary (to trigger an event in sched)
// Also change the state of the job in the KVS
static int handle_completed_jobs (ctx_t *ctx)
{
    double curr_progress;
    zlist_t *running_jobs = ctx->running_jobs;
    job_t *job = NULL;
    int num_jobs = zlist_size (running_jobs);
    double sim_time = ctx->sim_state->sim_time;

    while (num_jobs > 0) {
        job = zlist_pop (running_jobs);
        if (job->execution_time > 0) {
            curr_progress = calc_curr_progress (job, ctx->sim_state->sim_time);
        } else {
            curr_progress = 1;
            flux_log (ctx->h,
                      LOG_DEBUG,
                      "handle_completed_jobs found a job (%d) with execution "
                      "time <= 0 (%f), setting progress = 1",
                      job->id,
                      job->execution_time);
        }
        if (curr_progress < 1) {
            zlist_append (running_jobs, job);
        } else {
            flux_log (ctx->h,
                      LOG_DEBUG,
                      "handle_completed_jobs found a completed job");
            complete_job (ctx, job, sim_time);
        }
        num_jobs--;
    }

    return 0;
}

static bool inline is_node (const char *t)
{
    return (strncmp (t, "node", 5) == 0)? true: false;
}

static bool inline is_core (const char *t)
{
    return (strncmp (t, "core", 5) == 0)? true: false;
}

static bool inline is_io (const char *t)
{
    return (strncmp (t, "io", 3) == 0)? true: false;
}

static resrc_t* get_io_resrc (resrc_tree_t *resrc_tree)
{
    resrc_tree_list_t *children;
    resrc_tree_t *child;
    resrc_t *child_resrc;

    if (!resrc_tree_num_children (resrc_tree)) {
        return NULL;
    }

    children = resrc_tree_children (resrc_tree);
    for (child = resrc_tree_list_first (children);
         child;
         child = resrc_tree_list_next (children)) {
        child_resrc = resrc_tree_resrc (child);
        if (is_io (resrc_type (child_resrc))) {
            return child_resrc;
        }
    }

    printf ("%s: Failed for %s\n", __FUNCTION__,
            resrc_path (resrc_tree_resrc (resrc_tree)));
    return NULL;
}

#if 0
static size_t get_avail_bandwidth (resrc_tree_t *resrc_tree, int64_t time)
{
    resrc_t *io_resrc = get_io_resrc (resrc_tree);
    if (io_resrc)
        return resrc_available_at_time (io_resrc, time);
    else
        return 0;
}
#endif

static size_t get_alloc_bandwidth (resrc_tree_t *resrc_tree, int64_t time)
{
    resrc_t *io_resrc = get_io_resrc (resrc_tree);
    if (io_resrc)
        return resrc_size (io_resrc) - resrc_available_at_time (io_resrc, time);
    else
        return 0;
}

static int64_t get_max_bandwidth (resrc_tree_t *resrc_tree, int64_t time)
{
    resrc_t *io_resrc = get_io_resrc (resrc_tree);
    if (io_resrc)
        return resrc_size (io_resrc);
    else
        return 0;
}

#if CZMQ_VERSION < CZMQ_MAKE_VERSION(3, 0, 1)
// Compare two resources based on their alloc bandwidth
bool compare_resrc_tree_alloc_bw (void *item1, void *item2)
{
    resrc_tree_t *r1 = (resrc_tree_t *)item1;
    resrc_tree_t *r2 = (resrc_tree_t *)item2;
    size_t bw1 = get_alloc_bandwidth (r1, global_curr_time);
    size_t bw2 = get_alloc_bandwidth (r2, global_curr_time);
    return bw1 > bw2;
}
#else
// Compare two resources based on their alloc bandwidth
// Return > 0 if res1 has more alloc bandwidth than res2
//        < 0 if res1 has less alloc bandwidth than res2
//        = 0 if bandwidths are equivalent
int compare_resrc_tree_alloc_bw (void *item1, void *item2)
{
    resrc_tree_t *r1 = (resrc_tree_t *)item1;
    resrc_tree_t *r2 = (resrc_tree_t *)item2;
    size_t bw1 = get_alloc_bandwidth (r1, global_curr_time);
    size_t bw2 = get_alloc_bandwidth (r2, global_curr_time);
    return bw1 - bw2;
}
#endif

static void sort_on_alloc_bw (zlist_t *self, int64_t time)
{
    global_curr_time = time;
    zlist_sort (self, compare_resrc_tree_alloc_bw);
}

static size_t *get_job_min_from_hash (zhash_t *job_hash, int job_id)
{
    char job_id_str[100];
    sprintf (job_id_str, "%d", job_id);
    return (size_t *)zhash_lookup (job_hash, job_id_str);
}

static void determine_all_min_bandwidth_helper (resrc_tree_t *in_tree,
                                                size_t curr_min_bandwidth,
                                                zhash_t *job_hash,
                                                int64_t time)
{
    if (!in_tree) {
        return;
    }
    // Base Case:
    //
    // Check if we hit a node, if so, grab the job it is allocated to
    // currently and add the min(alloc_bw, curr_min_bw) to the job
    // hash
    //
    // TODO: make this more general
    size_t this_max_bandwidth = get_max_bandwidth (in_tree, time);
    const char *type = resrc_type (resrc_tree_resrc (in_tree));
    if (is_node (type)) {
        size_t this_alloc_bandwidth = get_alloc_bandwidth (in_tree, time);
        if (this_alloc_bandwidth > this_max_bandwidth) {
            this_alloc_bandwidth = this_max_bandwidth;
        }
        zlist_t *job_id_list = resrc_curr_job_ids (resrc_tree_resrc (in_tree), time);
        char *job_id_str = NULL;
        for (job_id_str = zlist_first (job_id_list);
             job_id_str;
             job_id_str = zlist_next (job_id_list)) {

            size_t *job_hash_bandwidth = zhash_lookup (job_hash, job_id_str);
            size_t min_bw = (this_alloc_bandwidth < curr_min_bandwidth) ?
                this_alloc_bandwidth : curr_min_bandwidth;
            min_bw = (min_bw < *job_hash_bandwidth) ? min_bw : *job_hash_bandwidth;
            *job_hash_bandwidth = min_bw;
        }
        zlist_destroy (&job_id_list);
        return;
    }

    // Sum the bandwidths of the parent's children. Simultaneously
    // create separate list of children with alloc'd bws.
    size_t child_alloc_bandwidth = 0;
    size_t total_requested_bandwidth = 0;
    resrc_tree_t *curr_child;
    zlist_t *child_list = zlist_new ();
    for (curr_child = resrc_tree_list_first (resrc_tree_children (in_tree));
         curr_child;
         curr_child = resrc_tree_list_next (resrc_tree_children (in_tree))) {

        type = resrc_type (resrc_tree_resrc (curr_child));
        if (!is_io (type)) {
            child_alloc_bandwidth = get_alloc_bandwidth (curr_child, time);
            if (child_alloc_bandwidth > 0) {
                total_requested_bandwidth += child_alloc_bandwidth;
                zlist_append (child_list, curr_child);
            }
        }
    }

    // Sort child list based on alloc bw
    sort_on_alloc_bw (child_list, time);

    size_t curr_average_bandwidth;
    size_t total_used_bandwidth = (total_requested_bandwidth > this_max_bandwidth)
                               ? this_max_bandwidth
                               : total_requested_bandwidth;
    total_used_bandwidth = (total_used_bandwidth > curr_min_bandwidth)
                               ? curr_min_bandwidth
                               : total_used_bandwidth;
    // Loop over all of the children
    int num_children;
    while (zlist_size (child_list) > 0) {
        // Determine the amount of bandwidth to allocate to each child
        num_children = zlist_size (child_list);
        curr_average_bandwidth = (total_used_bandwidth / num_children);
        curr_child = (resrc_tree_t *)zlist_pop (child_list);
        child_alloc_bandwidth = get_alloc_bandwidth (curr_child, time);
        if (child_alloc_bandwidth > 0) {
            if (child_alloc_bandwidth > curr_average_bandwidth) {
                child_alloc_bandwidth = curr_average_bandwidth;
            }

            // Subtract the allocated bandwidth from the parent's total
            total_used_bandwidth -= child_alloc_bandwidth;
            // Recurse on the child
            determine_all_min_bandwidth_helper (curr_child,
                                                child_alloc_bandwidth,
                                                job_hash, time);
        }
    }

    // Cleanup
    zlist_destroy (&child_list);

    return;
}

static zhash_t *determine_all_min_bandwidth (resrc_tree_t *root,
                                             zlist_t *running_jobs,
                                             int64_t time)
{
    size_t root_bw;
    size_t *curr_value = NULL;
    job_t *curr_job = NULL;
    char job_id_str[100];
    zhash_t *job_hash = zhash_new ();

    root_bw = get_max_bandwidth (root, time);

    curr_job = zlist_first (running_jobs);
    while (curr_job != NULL) {
        curr_value = (size_t *)malloc (sizeof (size_t));
        *curr_value = root_bw;
        sprintf (job_id_str, "%d", curr_job->id);
        zhash_insert (job_hash, job_id_str, curr_value);
        zhash_freefn (job_hash, job_id_str, free);
        curr_job = zlist_next (running_jobs);
    }

    if (!root)
        return job_hash;

    determine_all_min_bandwidth_helper (root, root_bw, job_hash, time);

    return job_hash;
}

static double determine_io_penalty (size_t job_bandwidth, size_t min_bandwidth)
{
    double io_penalty;

    if (job_bandwidth < min_bandwidth || min_bandwidth == 0) {
        return 0;
    }

    // Determine the penalty (needed rate / actual rate) - 1
    io_penalty = ((double) job_bandwidth / (double) min_bandwidth) - 1;

    return io_penalty;
}

// Model io contention that occurred between previous event and the
// curr sim time. Remove completed jobs from the list of running jobs
static int advance_time (ctx_t *ctx, zhash_t *job_hash)
{
    // TODO: Make this not static? (pass it in?, store it in ctx?)
    static double curr_time = 0;

    job_t *job = NULL;
    int num_jobs = -1;
    double next_event = -1, next_termination = -1, curr_progress = -1,
        io_penalty = 0, io_percentage = 0, io_time_delta = 0;
    size_t *job_min_bandwidth = NULL;

    zlist_t *running_jobs = ctx->running_jobs;
    double sim_time = ctx->sim_state->sim_time;

    JSON job_stat = NULL;
    JSON job_stats = NULL;
    JSON exec_stats = NULL;
    char stats_kvs_key[100];

    while (curr_time < sim_time) {
        num_jobs = zlist_size (running_jobs);
        if (num_jobs == 0) {
            curr_time = sim_time;
            break;
        }
        next_termination =
            determine_next_termination (ctx, curr_time, job_hash);
        next_event = ((sim_time < next_termination) || (next_termination < 0))
                         ? sim_time
                         : next_termination;  // min of the two
        exec_stats = Jnew ();
        job_stats = Jnew_ar ();
        while (num_jobs > 0) {
            job = zlist_pop (running_jobs);
            if (job->start_time <= curr_time) {
                // Get the minimum bandwidth between a resource in the job and
                // the pfs
                job_min_bandwidth = get_job_min_from_hash (job_hash, job->id);
                io_penalty =
                    determine_io_penalty (job->io_rate, *job_min_bandwidth);
                io_percentage = (io_penalty / (io_penalty + 1));
                io_time_delta = (next_event - curr_time) * io_percentage;
                job->io_time += io_time_delta;
                curr_progress = calc_curr_progress (job, next_event);

                // Add job stats to JSON array for logging to KVS
                job_stat = Jnew ();
                Jadd_int (job_stat, "jobid", job->id);
                Jadd_double (job_stat, "io_penalty", io_penalty);
                Jadd_double (job_stat, "io_percent", io_percentage);
                Jadd_double (job_stat, "new_io_time", job->io_time);
                Jadd_double (job_stat, "delta_io_time", io_time_delta);
                Jadd_double (job_stat, "curr_progress", curr_progress);
                json_object_array_add(job_stats, job_stat);

                if (curr_progress < 1)
                    zlist_append (running_jobs, job);
                else
                    complete_job (ctx, job, next_event);

            } else {
                zlist_append (running_jobs, job);
            }
            num_jobs--;
        }
        int64_t pfs_io_available = -1;
        resrc_t *pfs_io = resrc_lookup (ctx->resrcs, "/pfs/io");
        pfs_io_available = (int64_t) resrc_available_at_time (pfs_io, curr_time);

        json_object_object_add (exec_stats, "job_stats", job_stats);
        Jadd_int64 (exec_stats, "pfs_io_available", pfs_io_available);

        sprintf (stats_kvs_key, "sim_exec.%d-%d", (int) curr_time, (int) next_event);
        kvs_put_string (ctx->h, stats_kvs_key, Jtostr (exec_stats));
        Jput (exec_stats);
        curr_time = next_event;
    }

    return 0;
}

static int allocate_io (ctx_t *ctx, resrc_tree_t *resrc_tree, job_t *job)
{
    int rc = 0;
    size_t size = job->io_rate;
    int64_t starttime = ctx->sim_state->sim_time;
    resrc_tree_t *curr_resrc_tree = NULL;
    resrc_t *io_resrc = NULL;

    for (curr_resrc_tree = resrc_tree;
         curr_resrc_tree && !rc;
         curr_resrc_tree = resrc_tree_parent (curr_resrc_tree)) {
        io_resrc = get_io_resrc (curr_resrc_tree);
        resrc_stage_resrc (io_resrc, size);
        rc = resrc_allocate_resource_unchecked (io_resrc, job->id,
                                                starttime, job->walltime);
    }

    return rc;
}

// Walks over all resources in the job_tree and allocates them in
// sim_execsrv's copy of the resources.  Returns 0 on success, -1 on
// failure.
static int add_job_tree_to_resrcs (ctx_t *ctx,
                                    resrc_tree_t *job_tree,
                                    job_t *job)
{
    resrc_t *job_resrc = resrc_tree_resrc (job_tree);
    char *job_resrc_path = resrc_path (job_resrc);
    resrc_t *resrc = resrc_lookup (ctx->resrcs, job_resrc_path);
    if (!resrc) {
        flux_log (ctx->h, LOG_ERR, "%s: Failed to find resrc",
                  __FUNCTION__);
        return -1;
    }
    char *type = resrc_type (resrc);
    if (is_io (type)) {
        resrc_tree_t *phys_tree = resrc_phys_tree (resrc);
        resrc_tree_t *parent = resrc_tree_parent (phys_tree);
        if (parent)
            allocate_io (ctx, parent, job);
    } else if (is_node (type) || is_core (type)) {
        // TODO: fix this to read in from job resrc's size
        size_t alloc_size = 1;

        resrc_stage_resrc (resrc, alloc_size);
        if (resrc_allocate_resource (resrc, job->id,
                                     ctx->sim_state->sim_time,
                                     job->walltime)) {
            flux_log (ctx->h, LOG_ERR, "%s: Failed to alloc resrc (%s) for job %d",
                      __FUNCTION__, resrc_path (resrc), job->id);
            return -1;
        }
    }

    // Recurse on all children
    resrc_tree_t *curr_child = NULL;
    resrc_tree_list_t *child_list = resrc_tree_children (job_tree);
    for (curr_child = resrc_tree_list_first (child_list);
         curr_child;
         curr_child = resrc_tree_list_next (child_list)) {
        if (add_job_tree_to_resrcs (ctx, curr_child, job) < 0) {
            return -1;
        }
    }

    return 0;
}

// Walks over all resources allocated to job and allocates them in
// sim_execsrv's copy of the resources.  Returns 0 on success, -1 on
// failure.
static int add_job_to_resrcs (ctx_t *ctx, job_t *job)
{
    resrc_tree_t *curr_tree = NULL;
    for (curr_tree = resrc_tree_list_first (job->resrc_trees);
         curr_tree;
         curr_tree = resrc_tree_list_next (job->resrc_trees)) {
        if (add_job_tree_to_resrcs (ctx, curr_tree, job) < 0) {
            return -1;
        }
    }

    return 0;
}

// Take all of the scheduled job events that were queued up while we
// weren't running and add those jobs to the set of running jobs. This
// also requires switching their state in the kvs (to trigger events
// in the scheduler)
static int handle_queued_events (ctx_t *ctx)
{
    job_t *job = NULL;
    int *jobid = NULL;
    kvsdir_t *kvs_dir;
    flux_t h = ctx->h;
    zlist_t *queued_events = ctx->queued_events;
    zlist_t *running_jobs = ctx->running_jobs;
    double sim_time = ctx->sim_state->sim_time;

    while (zlist_size (queued_events) > 0) {
        jobid = zlist_pop (queued_events);
        if (kvs_get_dir (h, &kvs_dir, "lwj.%d", *jobid) < 0)
            err_exit ("kvs_get_dir (id=%d)", *jobid);
        job = pull_job_from_kvs (h, kvs_dir);
        if (add_job_to_resrcs (ctx, job) < 0) {
            flux_log (h, LOG_ERR,
                      "%s: failed to add job %d to sim_exec's resources",
                      __FUNCTION__, *jobid);
            return -1;
        }
        if (update_job_state (ctx, *jobid, kvs_dir, J_STARTING, sim_time) < 0) {
            flux_log (h,
                      LOG_ERR,
                      "%s: failed to set job %d's state to starting",
                      __FUNCTION__, *jobid);
            return -1;
        }
        if (update_job_state (ctx, *jobid, kvs_dir, J_RUNNING, sim_time) < 0) {
            flux_log (h,
                      LOG_ERR,
                      "%s: failed to set job %d's state to running",
                      __FUNCTION__, *jobid);
            return -1;
        }
        flux_log (h,
                  LOG_INFO,
                  "%s: job %d's state to starting then running",
                  __FUNCTION__, *jobid);
        job->start_time = sim_time;
        zlist_append (running_jobs, job);
    }

    return 0;
}

// Received an event that a simulation is starting
static void start_cb (flux_t h,
                      flux_msg_handler_t *w,
                      const flux_msg_t *msg,
                      void *arg)
{
    flux_log (h, LOG_DEBUG, "received a start event");
    if (send_join_request (h, module_name, -1) < 0) {
        flux_log (h, LOG_ERR, "failed to register with sim module");
        return;
    }
    flux_log (h, LOG_DEBUG, "sent a join request");

    if (flux_event_unsubscribe (h, "sim.start") < 0) {
        flux_log (h, LOG_ERR, "failed to unsubscribe from \"sim.start\"");
    } else {
        flux_log (h, LOG_DEBUG, "unsubscribed from \"sim.start\"");
    }
}

// TODO: make this cleaner
static const char *get_rdl_kvs_path (flux_t h)
{
    return "sim.rdl";
}

static void populate_rdl_from_kvs (ctx_t *ctx, const char *rdl_kvs_path)
{
    char *kvs_entry = NULL;
    JSON rdl_json = NULL;

    kvs_get (ctx->h, rdl_kvs_path, &kvs_entry);
    rdl_json = Jfromstr (kvs_entry);

    resrc_tree_list_t *tree_list = resrc_tree_list_deserialize (rdl_json);
    if (resrc_tree_list_size (tree_list) > 1) {
        flux_log (ctx->h, LOG_ERR,
                  "%s: Found more than one tree in serialized kvs rdl",
                  __FUNCTION__);
    }
    ctx->resrc_tree = resrc_tree_list_first (tree_list);
    ctx->resrcs = resrc_new_resources_from_tree (ctx->resrc_tree);

    resrc_tree_list_free (tree_list);
    Jput (rdl_json);
    free (kvs_entry);
}

// Handle trigger requests from the sim module ("sim_exec.trigger")
static void trigger_cb (flux_t h,
                        flux_msg_handler_t *w,
                        const flux_msg_t *msg,
                        void *arg)
{
    JSON o = NULL;
    const char *json_str = NULL;
    double next_termination = -1;
    zhash_t *job_hash = NULL;
    ctx_t *ctx = (ctx_t *)arg;

    if (flux_msg_get_payload_json (msg, &json_str) < 0 || json_str == NULL
        || !(o = Jfromstr (json_str))) {
        flux_log (h, LOG_ERR, "%s: bad message", __FUNCTION__);
        return;
    }

    // Logging
    flux_log (h,
              LOG_DEBUG,
              "received a trigger (sim_exec.trigger: %s",
              json_str);

    // Check that we have a copy of the RDL
    if (!ctx->resrc_tree || !ctx->resrcs) {
        const char *rdl_kvs_path = get_rdl_kvs_path (h);
        populate_rdl_from_kvs (ctx, rdl_kvs_path);
    }

    // Handle the trigger
    ctx->sim_state = json_to_sim_state (o);
    handle_queued_events (ctx);
    job_hash = determine_all_min_bandwidth (ctx->resrc_tree,
                                            ctx->running_jobs,
                                            ctx->sim_state->sim_time);
    advance_time (ctx, job_hash);
    handle_completed_jobs (ctx);
    next_termination =
        determine_next_termination (ctx, ctx->sim_state->sim_time, job_hash);
    set_event_timer (ctx, "sim_exec", next_termination);
    send_reply_request (h, module_name, ctx->sim_state);

    // Cleanup
    free_simstate (ctx->sim_state);
    Jput (o);
    zhash_destroy (&job_hash);
}

static void run_cb (flux_t h,
                    flux_msg_handler_t *w,
                    const flux_msg_t *msg,
                    void *arg)
{
    const char *topic;
    ctx_t *ctx = (ctx_t *)arg;
    int *jobid = (int *)malloc (sizeof (int));

    if (flux_msg_get_topic (msg, &topic) < 0) {
        flux_log (h, LOG_ERR, "%s: bad message", __FUNCTION__);
        return;
    }

    // Logging
    flux_log (h, LOG_DEBUG, "received a request (%s)", topic);

    // Handle Request
    sscanf (topic, "sim_exec.run.%d", jobid);
    zlist_append (ctx->queued_events, jobid);
    flux_log (h, LOG_DEBUG, "queued the running of jobid %d", *jobid);
}

/*
static void rdl_cb (flux_t h,
                    flux_msg_handler_t *w,
                    const flux_msg_t *msg,
                    void *arg)
{
    const char *json_str = NULL;
    JSON reply = NULL;
    ctx_t *ctx = arg;

    if (flux_msg_get_payload_json (msg, &json_str) < 0 || json_str == NULL
        || !(reply = Jfromstr (json_str))) {
        flux_log (h, LOG_ERR, "%s: bad reply message", __FUNCTION__);
        Jput (reply);
        return;
    }

    // De-serialize and get new info
    ctx->resrc_tree = resrc_tree_deserialize (reply, NULL);
    ctx->resrcs = resrc_tree_generate_resources_from_tree (ctx->resrc_tree);

    Jput (reply);
}
*/

static struct flux_msg_handler_spec htab[] = {
    {FLUX_MSGTYPE_EVENT, "sim.start", start_cb},
    {FLUX_MSGTYPE_REQUEST, "sim_exec.trigger", trigger_cb},
    {FLUX_MSGTYPE_REQUEST, "sim_exec.run.*", run_cb},
    FLUX_MSGHANDLER_TABLE_END,
};

int mod_main (flux_t h, int argc, char **argv)
{
    ctx_t *ctx = getctx (h);
    if (flux_rank (h) != 0) {
        flux_log (h, LOG_ERR, "this module must only run on rank 0");
        return -1;
    }
    flux_log (h, LOG_INFO, "module starting");

    if (flux_event_subscribe (h, "sim.start") < 0) {
        flux_log (h, LOG_ERR, "subscribing to event: %s", strerror (errno));
        return -1;
    }
    if (flux_msg_handler_addvec (h, htab, ctx) < 0) {
        flux_log (h, LOG_ERR, "flux_msg_handler_add: %s", strerror (errno));
        return -1;
    }

    send_alive_request (h, module_name);

    if (flux_reactor_start (h) < 0) {
        flux_log (h, LOG_ERR, "flux_reactor_start: %s", strerror (errno));
        return -1;
    }

    return 0;
}

MOD_NAME ("sim_exec");
