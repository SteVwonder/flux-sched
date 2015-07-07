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
#include <float.h>
#include <flux/core.h>

#include "src/common/libutil/jsonutil.h"
#include "src/common/libutil/log.h"
#include "src/common/libutil/shortjson.h"
#include "src/common/libutil/xzmalloc.h"
#include "simulator.h"
#include "rdl.h"

static const char *module_name = "sim_exec";

typedef struct {
  sim_state_t *sim_state;
  zlist_t *queued_events; //holds int *
  zlist_t *running_jobs; //holds job_t *
  flux_t h;
  double prev_sim_time;
  struct rdllib *rdllib;
  struct rdl *rdl;
} ctx_t;

//static double get_io_penalty (ctx_t *ctx, job_t *job);
static double determine_io_penalty (double job_bandwidth, double min_bandwidth);
static double* get_job_min_from_hash (zhash_t *job_hash, int job_id);

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

    rdllib_close (ctx->rdllib);
    free (ctx->rdl);
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
        ctx->rdllib = rdllib_open();
        ctx->rdl = NULL;
        flux_aux_set (h, "simsrv", ctx, freectx);
    }

    return ctx;
}

//Given the kvs dir of a job, change the state of the job and timestamp the change
static int update_job_state (ctx_t *ctx, kvsdir_t kvs_dir, char* state, double update_time)
{
	char *timer_key = NULL;

	asprintf (&timer_key, "%s_time", state);

	kvsdir_put_string (kvs_dir, "state", state);
	kvsdir_put_double (kvs_dir, timer_key, update_time);
	kvs_commit (ctx->h);

	free (timer_key);
	return 0;
}

static double calc_curr_progress (job_t *job, double sim_time)
{
	double time_passed = sim_time - job->start_time + .000001;
	double time_necessary = job->execution_time + job->io_time;
	return time_passed / time_necessary;
}

//Calculate when the next job is going to terminate assuming no new jobs are added
static double determine_next_termination (ctx_t *ctx, double curr_time, zhash_t *job_hash)
{
	double next_termination = -1, curr_termination = -1;
    double projected_future_io_time, job_io_penalty, computation_time_remaining;
    double *job_min_bandwidth;
	zlist_t *running_jobs = ctx->running_jobs;
	job_t *job = zlist_first (running_jobs);

	while (job != NULL){
        if (job->start_time <= curr_time) {
            curr_termination = job->start_time + job->execution_time + job->io_time;
            computation_time_remaining = job->execution_time - ((curr_time - job->start_time) - job->io_time);
            job_min_bandwidth = get_job_min_from_hash (job_hash, job->id);
            job_io_penalty = determine_io_penalty (job->io_rate, *job_min_bandwidth);
            projected_future_io_time = (computation_time_remaining) * job_io_penalty;
            curr_termination += projected_future_io_time;
            if (curr_termination < next_termination || next_termination < 0){
                next_termination = curr_termination;
            }
        }
        job = zlist_next (running_jobs);
	}

	return next_termination;
}

//Set the timer for the given module
static int set_event_timer (ctx_t *ctx, char *mod_name, double timer_value)
{
	double *event_timer = zhash_lookup (ctx->sim_state->timers, mod_name);
	if (timer_value > 0 && (timer_value < *event_timer || *event_timer < 0)){
		*event_timer = timer_value;
		flux_log (ctx->h, LOG_DEBUG, "next %s event set to %f", mod_name, *event_timer);
	}
	return 0;
}

//Update sched timer as necessary (to trigger an event in sched)
//Also change the state of the job in the KVS
static int complete_job (ctx_t *ctx, job_t *job, double completion_time)
{
	flux_t h = ctx->h;

	flux_log (h, LOG_INFO, "Job %d completed", job->id);
	update_job_state (ctx, job->kvs_dir, "complete", completion_time);
	set_event_timer (ctx, "sim_sched", ctx->sim_state->sim_time + .00001);
	kvsdir_put_double (job->kvs_dir, "io_time", job->io_time);
	kvs_commit (h);
	free_job (job);

	return 0;
}

//Remove completed jobs from the list of running jobs
//Update sched timer as necessary (to trigger an event in sched)
//Also change the state of the job in the KVS
static int handle_completed_jobs (ctx_t *ctx)
{
	double curr_progress;
	zlist_t *running_jobs = ctx->running_jobs;
	job_t *job = NULL;
	int num_jobs = zlist_size (running_jobs);
	double sim_time = ctx->sim_state->sim_time;

	//print_next_completing (running_jobs, ctx);

	while (num_jobs > 0){
		job = zlist_pop (running_jobs);
		if (job->execution_time > 0) {
			curr_progress = calc_curr_progress (job, ctx->sim_state->sim_time);
        } else {
			curr_progress = 1;
			flux_log (ctx->h, LOG_DEBUG, "handle_completed_jobs found a job (%d) with execution time <= 0 (%f), setting progress = 1", job->id, job->execution_time);
        }
		if (curr_progress < 1){
			zlist_append (running_jobs, job);
		} else {
			flux_log (ctx->h, LOG_DEBUG, "handle_completed_jobs found a completed job");
			complete_job (ctx, job, sim_time);
		}
		num_jobs--;
	}

	return 0;
}

static int64_t get_alloc_bandwidth (struct resource *r)
{
	int64_t alloc_bw;
	if (rdl_resource_get_int (r, "alloc_bw", &alloc_bw) == 0)
      return alloc_bw;
    else { //something got messed up, set it to zero and return zero
      rdl_resource_set_int (r, "alloc_bw", 0);
      return 0;
    }
}

static int64_t get_max_bandwidth (struct resource *r)
{
	int64_t max_bw;
	rdl_resource_get_int (r, "max_bw", &max_bw);
	return max_bw;
}

//Compare two resources based on their alloc bandwidth
//Return true if they should be swapped
//AKA r1 has more alloc bandwidth than r2
bool compare_resource_alloc_bw (void *item1, void *item2)
{
  struct resource *r1 = (struct resource*) item1;
  struct resource *r2 = (struct resource*) item2;
  return get_alloc_bandwidth(r1) > get_alloc_bandwidth(r2);
}

static double* get_job_min_from_hash (zhash_t *job_hash, int job_id) {
    char job_id_str[100];
    sprintf (job_id_str, "%d", job_id);
    return (double *) zhash_lookup (job_hash, job_id_str);
}

static void determine_all_min_bandwidth_helper (struct resource *r, double curr_min_bandwidth, zhash_t *job_hash)
{
    struct resource *curr_child;
    int64_t job_id;
    double total_requested_bandwidth, curr_average_bandwidth, child_alloc_bandwidth,
        total_used_bandwidth, this_max_bandwidth, num_children, this_alloc_bandwidth;
    JSON o;
    zlist_t *child_list;
    const char *type;

    //Check if leaf node in hierarchy (base case)
    rdl_resource_iterator_reset (r);
    curr_child = rdl_resource_next_child (r);
    if (curr_child == NULL) {
        //Check if allocated to a job
        if (rdl_resource_get_int (r, "lwj", &job_id) == 0) {
            //Determine which is less, the bandwidth currently available to this resource, or the bandwidth allocated to it by the job
            //This assumes that jobs cannot share leaf nodes in the hierarchy
            this_alloc_bandwidth = get_alloc_bandwidth (r);
            curr_min_bandwidth = (curr_min_bandwidth < this_alloc_bandwidth) ? curr_min_bandwidth : this_alloc_bandwidth;
            double *job_min_bw = get_job_min_from_hash (job_hash, job_id);
            if (job_min_bw != NULL && curr_min_bandwidth < *job_min_bw) {
                *job_min_bw = curr_min_bandwidth;
            } //if job_min_bw is NULL, the tag still exists in the RDL, but the job completed
        }
        return;
    } //else

    //Sum the bandwidths of the parent's children
    child_list = zlist_new ();
    while (curr_child != NULL) {
        o = rdl_resource_json (curr_child);
        Jget_str (o, "type", &type);
        //TODO: clean up this hardcoded value, should go away once we switch to the real
        //rdl implementation (storing a bandwidth resource at every level)
        if (strcmp (type, "memory") != 0) {
            total_requested_bandwidth += get_alloc_bandwidth (curr_child);
            zlist_append (child_list, curr_child);
        }
        Jput (o);
        curr_child = rdl_resource_next_child (r);
    }
    rdl_resource_iterator_reset (r);

    //Sort child list based on alloc bw
    zlist_sort (child_list, compare_resource_alloc_bw);

    //const char *resource_string = Jtostr(o);
    //Loop over all of the children
    this_max_bandwidth = get_max_bandwidth (r);
    total_used_bandwidth = (total_requested_bandwidth > this_max_bandwidth) ? this_max_bandwidth : total_requested_bandwidth;
    total_used_bandwidth = (total_used_bandwidth > curr_min_bandwidth) ? curr_min_bandwidth : total_used_bandwidth;
    while (zlist_size (child_list) > 0) {
        //Determine the amount of bandwidth to allocate to each child
        num_children = zlist_size (child_list);
        curr_average_bandwidth = (total_used_bandwidth / num_children);
        curr_child = (struct resource*) zlist_pop (child_list);
        child_alloc_bandwidth = get_alloc_bandwidth(curr_child);
        if (child_alloc_bandwidth > 0) {
            if (child_alloc_bandwidth > curr_average_bandwidth)
                child_alloc_bandwidth = curr_average_bandwidth;

            //Subtract the allocated bandwidth from the parent's total
            total_used_bandwidth -= child_alloc_bandwidth;
            //Recurse on the child
            determine_all_min_bandwidth_helper(curr_child, child_alloc_bandwidth, job_hash);
        }
        rdl_resource_destroy (curr_child);
    }

    //Cleanup
    zlist_destroy (&child_list); //no need to rdl_resource_destroy, done in above loop

    return;
}

static zhash_t* determine_all_min_bandwidth (struct rdl *rdl, zlist_t* running_jobs) {
    double root_bw;
    double *curr_value = NULL;
    struct resource *root = NULL;
	job_t *curr_job = NULL;
    char job_id_str[100];
    zhash_t *job_hash = zhash_new ();

    root = rdl_resource_get (rdl, "default");
    root_bw = get_max_bandwidth (root);

    curr_job = zlist_first (running_jobs);
    while (curr_job != NULL) {
        curr_value = (double*) malloc (sizeof (double));
        *curr_value = root_bw;
        sprintf (job_id_str, "%d", curr_job->id);
        zhash_insert (job_hash, job_id_str, curr_value);
        zhash_freefn (job_hash, job_id_str, free);
        curr_job = zlist_next (running_jobs);
    }

    determine_all_min_bandwidth_helper (root, root_bw, job_hash);

    return job_hash;
}

static double determine_io_penalty (double job_bandwidth, double min_bandwidth)
{
    double io_penalty;

    if (job_bandwidth < min_bandwidth) {
        return 0;
    }

    //Determine the penalty (needed rate / actual rate) - 1
    io_penalty = (job_bandwidth / min_bandwidth) - 1;

    return io_penalty;
}

//Model io contention that occurred between previous event and the curr sim time
//Remove completed jobs from the list of running jobs
static int advance_time (ctx_t *ctx, zhash_t *job_hash)
{
	//TODO: Make this not static? (pass it in?, store it in ctx?)
	static double curr_time = 0;

	job_t *job = NULL;
	int num_jobs = -1;
	double next_event = -1, next_termination = -1, curr_progress = -1,
        io_penalty = 0, io_percentage = 0;
    double *job_min_bandwidth = NULL;

	zlist_t *running_jobs = ctx->running_jobs;
	double sim_time = ctx->sim_state->sim_time;

	while (curr_time < sim_time) {
		num_jobs = zlist_size (running_jobs);
        if (num_jobs == 0) {
            curr_time = sim_time;
            break;
        }
        next_termination = determine_next_termination (ctx, curr_time, job_hash);
		next_event = ((sim_time < next_termination) || (next_termination < 0)) ? sim_time : next_termination; //min of the two
		while (num_jobs > 0) {
			job = zlist_pop (running_jobs);
            if (job->start_time <= curr_time) {
                //Get the minimum bandwidth between a resource in the job and the pfs
                job_min_bandwidth = get_job_min_from_hash (job_hash, job->id);
                io_penalty = determine_io_penalty (job->io_rate, *job_min_bandwidth);
                io_percentage = (io_penalty / (io_penalty + 1));
                job->io_time += (next_event - curr_time) * io_percentage;
                curr_progress = calc_curr_progress (job, next_event);
                if (curr_progress < 1)
                    zlist_append (running_jobs, job);
                else
                    complete_job (ctx, job, next_event);
            } else {
                    zlist_append (running_jobs, job);
            }
            num_jobs--;
		}
		curr_time = next_event;
	}

	return 0;
}

//Take all of the scheduled job eventst that were queued up while we weren't running
//and add those jobs to the set of running jobs
//This also requires switching their state in the kvs (to trigger events in the scheduler)
static int handle_queued_events (ctx_t *ctx)
{
	job_t *job = NULL;
	int *jobid = NULL;
	kvsdir_t kvs_dir;
	flux_t h = ctx->h;
	zlist_t *queued_events = ctx->queued_events;
	zlist_t *running_jobs = ctx->running_jobs;
	double sim_time = ctx->sim_state->sim_time;

	while (zlist_size (queued_events) > 0){
		jobid = zlist_pop (queued_events);
		if (kvs_get_dir (h, &kvs_dir, "lwj.%d", *jobid) < 0)
			err_exit ("kvs_get_dir (id=%d)", *jobid);
		job = pull_job_from_kvs (kvs_dir);
		if (update_job_state (ctx, kvs_dir, "starting", sim_time) < 0){
			flux_log (h, LOG_ERR, "failed to set job %d's state to starting", *jobid);
			return -1;
		}
		if (update_job_state (ctx, kvs_dir, "running", sim_time) < 0){
			flux_log (h, LOG_ERR, "failed to set job %d's state to running", *jobid);
			return -1;
		}
		flux_log (h, LOG_INFO, "job %d's state to starting then running", *jobid);
		job->start_time = ctx->sim_state->sim_time;
		zlist_append (running_jobs, job);
	}

	return 0;
}


//Request to join the simulation
static int send_join_request(flux_t h)
{
	JSON o = Jnew ();
	Jadd_str (o, "mod_name", module_name);
	Jadd_int (o, "rank", flux_rank (h));
	Jadd_double (o, "next_event", -1);
	if (flux_json_request (h, FLUX_NODEID_ANY,
                                  FLUX_MATCHTAG_NONE, "sim.join", o) < 0) {
		Jput (o);
		return -1;
	}
	Jput (o);
	return 0;
}

//Reply back to the sim module with the updated sim state (in JSON form)
static int send_reply_request(flux_t h, sim_state_t *sim_state)
{
	JSON o = sim_state_to_json (sim_state);
	Jadd_bool (o, "event_finished", true);
	if (flux_json_request (h, FLUX_NODEID_ANY,
                                  FLUX_MATCHTAG_NONE, "sim.reply", o) < 0) {
		Jput (o);
		return -1;
	}
	flux_log(h, LOG_DEBUG, "sent a reply request");
   Jput (o);
   return 0;
}

//Received an event that a simulation is starting
static int start_cb (flux_t h, int typemask, zmsg_t **zmsg, void *arg)
{
	flux_log(h, LOG_DEBUG, "received a start event");
	if (send_join_request (h) < 0){
		flux_log (h, LOG_ERR, "failed to register with sim module");
		return -1;
	}
	flux_log (h, LOG_DEBUG, "sent a join request");

	if (flux_event_unsubscribe (h, "sim.start") < 0){
		flux_log (h, LOG_ERR, "failed to unsubscribe from \"sim.start\"");
		return -1;
	} else {
		flux_log (h, LOG_DEBUG, "unsubscribed from \"sim.start\"");
	}

	//Cleanup
	zmsg_destroy (zmsg);

	return 0;
}

static int rdl_update_cb (flux_t h, int typemask, zmsg_t **zmsg, void *arg)
{
	JSON o = NULL;
	char *tag = NULL;
    const char *rdl_string = NULL;
	ctx_t *ctx = (ctx_t *) arg;
    int64_t rdl_int = 0;

	if (flux_msg_decode (*zmsg, &tag, &o) < 0 || o == NULL){
		flux_log (h, LOG_ERR, "%s: bad message", __FUNCTION__);
		Jput (o);
		return -1;
	}

    Jget_int64(o, "rdl_int", &rdl_int);
    Jget_str(o, "rdl_string", &rdl_string);

    if (rdl_int) {
        ctx->rdl = (struct rdl*) rdl_int;
    } else if (rdl_string) {
        flux_log (h, LOG_DEBUG, "resetting rdllib & rdl based on rdl.update string");
        rdllib_close(ctx->rdllib);
        ctx->rdllib = rdllib_open();
        ctx->rdl = rdl_load(ctx->rdllib, rdl_string);
    } else {
        return -1;
    }

    Jput (o);

    return 0;
}

//Handle trigger requests from the sim module ("sim_exec.trigger")
static int trigger_cb (flux_t h, int typemask, zmsg_t **zmsg, void *arg)
{
	JSON o;
	const char *json_string;
	double next_termination;
	ctx_t *ctx = (ctx_t *) arg;
    zhash_t *job_hash;

	if (flux_json_request_decode (*zmsg, &o) < 0) {
		flux_log (h, LOG_ERR, "%s: bad message", __FUNCTION__);
		return -1;
	}

//Logging
	json_string = Jtostr (o);
	flux_log(h, LOG_DEBUG, "received a trigger (sim_exec.trigger: %s", json_string);

//Handle the trigger
	ctx->sim_state = json_to_sim_state (o);
	handle_queued_events (ctx);
    job_hash = determine_all_min_bandwidth (ctx->rdl, ctx->running_jobs);
	advance_time (ctx, job_hash);
	handle_completed_jobs (ctx);
	next_termination = determine_next_termination (ctx, ctx->sim_state->sim_time, job_hash);
	set_event_timer (ctx, "sim_exec", next_termination);
	send_reply_request (h, ctx->sim_state);

//Cleanup
	free_simstate (ctx->sim_state);
	Jput (o);
    zhash_destroy (&job_hash);
	zmsg_destroy (zmsg);
	return 0;
}

static int run_cb (flux_t h, int typemask, zmsg_t **zmsg, void *arg)
{
	JSON o = NULL;
	const char *tag;
	ctx_t *ctx = (ctx_t *) arg;
	int *jobid = (int *) malloc (sizeof (int));

	if (flux_msg_get_topic (*zmsg, &tag) < 0
			|| flux_json_request_decode (*zmsg, &o) < 0) {
		flux_log (h, LOG_ERR, "%s: bad message", __FUNCTION__);
		Jput (o);
		return -1;
	}

//Logging
	flux_log(h, LOG_DEBUG, "received a request (%s)", tag);

//Handle Request
	sscanf (tag, "sim_exec.run.%d", jobid);
	zlist_append (ctx->queued_events, jobid);
	flux_log(h, LOG_DEBUG, "queued the running of jobid %d", *jobid);

//Cleanup
	Jput (o);
	zmsg_destroy (zmsg);
	return 0;
}

static msghandler_t htab[] = {
    { FLUX_MSGTYPE_EVENT,   "sim.start",        start_cb },
    { FLUX_MSGTYPE_EVENT,   "rdl.update",        rdl_update_cb },
    { FLUX_MSGTYPE_REQUEST, "sim_exec.trigger",   trigger_cb },
    { FLUX_MSGTYPE_REQUEST, "sim_exec.run.*",   run_cb },
};
const int htablen = sizeof (htab) / sizeof (htab[0]);

int mod_main(flux_t h, zhash_t *args)
{
	ctx_t *ctx = getctx (h);
	if (flux_rank (h) != 0) {
		flux_log (h, LOG_ERR, "this module must only run on rank 0");
		return -1;
	}
	flux_log (h, LOG_INFO, "module starting");

	if (flux_event_subscribe (h, "sim.start") < 0){
        flux_log (h, LOG_ERR, "subscribing to event: %s", strerror (errno));
		return -1;
	}
	if (flux_event_subscribe (h, "rdl.update") < 0){
        flux_log (h, LOG_ERR, "subscribing to event: %s", strerror (errno));
		return -1;
	}
	if (flux_msghandler_addvec (h, htab, htablen, ctx) < 0) {
		flux_log (h, LOG_ERR, "flux_msghandler_add: %s", strerror (errno));
		return -1;
	}

	send_alive_request (h, module_name);

	if (flux_reactor_start (h) < 0) {
		flux_log (h, LOG_ERR, "flux_reactor_start: %s", strerror (errno));
		return -1;
	}

	return 0;
}


MOD_NAME("sim_exec");
