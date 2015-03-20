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
#include <string.h>
#include <errno.h>
#include <libgen.h>
#include <czmq.h>
#include <json/json.h>
#include <dlfcn.h>
#include <time.h>
#include <inttypes.h>
#include <glpk.h>

#include "src/common/libutil/jsonutil.h"
#include "src/common/libutil/log.h"
#include "src/common/libutil/shortjson.h"
#include "src/common/libutil/xzmalloc.h"
#include "rdl.h"
#include "scheduler.h"
#include "simulator.h"
#include "flux_ilp.h"

#define MAX_STR_LEN 128

static const char const *module_name = "sim_sched";

/****************************************************************
 *
 *                 INTERNAL DATA STRUCTURE
 *
 ****************************************************************/
struct stab_struct {
    int i;
    const char *s;
};

typedef struct {
	char *key;
	char *val;
	int errnum;
} kvs_event_t;

/****************************************************************
 *
 *                 STATIC DATA
 *
 ****************************************************************/
static zlist_t *p_queue = NULL;
static zlist_t *r_queue = NULL;
static zlist_t *c_queue = NULL;
static zlist_t *ev_queue = NULL;
static flux_t h = NULL;
static struct rdllib *global_rdllib = NULL;
static struct rdl *global_rdl = NULL;
static char* global_rdl_resource = NULL;
static const char* IDLETAG = "idle";
static const char* CORETYPE = "core";
static const char* NODETYPE = "node";
static const char* SWITCHTYPE = "switch";

static bool in_sim = false;
static sim_state_t *sim_state = NULL;
static zlist_t *kvs_queue = NULL;
static zlist_t *timer_queue = NULL;

static struct stab_struct jobstate_tab[] = {
    { j_null,      "null" },
    { j_reserved,  "reserved" },
    { j_submitted, "submitted" },
    { j_unsched,   "unsched" },
    { j_pending,   "pending" },
    { j_runrequest,"runrequest" },
    { j_allocated, "allocated" },
    { j_starting,  "starting" },
    { j_running,   "running" },
    { j_cancelled, "cancelled" },
    { j_complete,  "complete" },
    { j_reaped,    "reaped" },
    { -1, NULL },
};

//Set the timer for "module" to happen relatively soon
//If the mod is sim_exec, it shouldn't happen immediately
//because the scheduler still needs to transition through
//3->4 states before the sim_exec module can actually "exec" a job
static void set_next_event(const char* module)
{
	double next_event;
	double *timer = zhash_lookup (sim_state->timers, module);
	next_event = sim_state->sim_time + ( (!strcmp (module, "sim_exec")) ? .0001 : .00001);
	if (*timer > next_event || *timer < 0){
		*timer = next_event;
		flux_log (h, LOG_DEBUG, "next %s event set for %f", module, next_event);
	}
}

static void handle_timer_queue ()
{
	while (zlist_size (timer_queue) > 0)
		set_next_event (zlist_pop (timer_queue));
}

static void queue_timer_change (const char* module)
{
	zlist_append (timer_queue, (void *) module);
}

/****************************************************************
 *
 *         Resource Description Library Setup
 *
 ****************************************************************/

//Reply back to the sim module with the updated sim state (in JSON form)
int send_reply_request (flux_t h, sim_state_t *sim_state)
{
    if (sim_state->rdl_string)
        free (sim_state->rdl_string);
    sim_state->rdl_string = rdl_serialize(global_rdl);
	JSON o = sim_state_to_json (sim_state);
	Jadd_bool (o, "event_finished", true);
    Jadd_str (o, "mod_name", module_name);
	if (flux_request_send (h, o, "%s", "sim.reply") < 0){
		Jput (o);
		return -1;
	}
	flux_log(h, LOG_DEBUG, "sent a reply request");
   Jput (o);
   return 0;
}

static int
signal_event ( )
{
    int rc = 0;
	if (in_sim && sim_state != NULL){
		queue_timer_change (module_name);
		//send_reply_request (h, sim_state);
		goto ret;
	}
    else if (flux_event_send (h, NULL, "sim_sched.event") < 0) {
        flux_log (h, LOG_ERR,
                 "flux_event_send: %s", strerror (errno));
        rc = -1;
        goto ret;
    }

ret:
    return rc;
}


static flux_lwj_t *
find_lwj (int64_t id)
{
    flux_lwj_t *j = NULL;

    j = zlist_first (p_queue);
    while (j) {
        if (j->lwj_id == id)
            break;
        j = zlist_next (p_queue);
    }
    if (j)
        return j;

    j = zlist_first (r_queue);
    while (j) {
        if (j->lwj_id == id)
            break;
        j = zlist_next (r_queue);
    }

    return j;
}


/****************************************************************
 *
 *              Utility Functions
 *
 ****************************************************************/

static char * ctime_iso8601_now (char *buf, size_t sz)
{
    struct tm tm;
    time_t now = time (NULL);

    memset (buf, 0, sz);

    if (!localtime_r (&now, &tm))
        err_exit ("localtime");
    strftime (buf, sz, "%FT%T", &tm);

    return buf;
}

static int
stab_lookup (struct stab_struct *ss, const char *s)
{
    while (ss->s != NULL) {
        if (!strcmp (ss->s, s))
            return ss->i;
        ss++;
    }
    return -1;
}

static const char *
stab_rlookup (struct stab_struct *ss, int i)
{
    while (ss->s != NULL) {
        if (ss->i == i)
            return ss->s;
        ss++;
    }
    return "unknown";
}

static int unwatch_lwj (flux_lwj_t *job)
{
    char *key = NULL;
    int rc = 0;

    if (asprintf (&key, "lwj.%ld.state", job->lwj_id) < 0) {
        flux_log (h, LOG_ERR, "update_job_state key create failed");
        rc = -1;
    } else if (kvs_unwatch (h, key)) {
        flux_log (h, LOG_ERR, "failed to unwatch %s", key);
        rc = -1;
    } else {
        flux_log (h, LOG_DEBUG, "unwatched %s", key);
    }

    free (key);
    return rc;
}

/*
 * Update the job's kvs entry for state and mark the time.
 * Intended to be part of a series of changes, so the caller must
 * invoke the kvs_commit at some future point.
 */
int update_job_state (flux_lwj_t *job, lwj_event_e e)
{
    char buf [64];
    char *key = NULL;
    char *key2 = NULL;
    int rc = -1;
    const char *state;

    ctime_iso8601_now (buf, sizeof (buf));

    state = stab_rlookup (jobstate_tab, e);
    if (!strcmp (state, "unknown")) {
        flux_log (h, LOG_ERR, "unknown job state %d", e);
    } else if (asprintf (&key, "lwj.%ld.state", job->lwj_id) < 0) {
        flux_log (h, LOG_ERR, "update_job_state key create failed");
    } else if (kvs_put_string (h, key, state) < 0) {
        flux_log (h, LOG_ERR, "update_job_state %ld state update failed: %s",
                  job->lwj_id, strerror (errno));
    } else if (asprintf (&key2, "lwj.%ld.%s-time", job->lwj_id, state) < 0) {
        flux_log (h, LOG_ERR, "update_job_state key2 create failed");
    } else if (kvs_put_string (h, key2, buf) < 0) {
        flux_log (h, LOG_ERR, "update_job_state %ld %s-time failed: %s",
                  job->lwj_id, state, strerror (errno));
    } else {
        rc = 0;
        flux_log (h, LOG_DEBUG, "updated job %ld's state in the kvs to %s",
                  job->lwj_id, state);
    }

    free (key);
    free (key2);

    return rc;
}

static inline void
set_event (flux_event_t *e,
           event_class_e c, int ei, flux_lwj_t *j)
{
    e->t = c;
    e->lwj = j;
    switch (c) {
    case lwj_event:
        e->ev.je = (lwj_event_e) ei;
        break;
    case res_event:
        e->ev.re = (res_event_e) ei;
        break;
    default:
        flux_log (h, LOG_ERR, "unknown ev class");
        break;
    }
    return;
}


static int
extract_lwjid (const char *k, int64_t *i)
{
    int rc = 0;
    char *kcopy = NULL;
    char *lwj = NULL;
    char *id = NULL;

    if (!k) {
        rc = -1;
        goto ret;
    }

    kcopy = strdup (k);
    lwj = strtok (kcopy, ".");
    if (strncmp(lwj, "lwj", 3) != 0) {
        rc = -1;
        goto ret;
    }
    id = strtok (NULL, ".");
    *i = strtoul(id, (char **) NULL, 10);

ret:
    return rc;
}

static int
extract_lwjinfo (flux_lwj_t *j)
{
    char *key = NULL;
    char *state;
    int64_t reqnodes = 0, reqtasks = 0, io_rate = 0;
    double walltime = 0;
    int rc = -1;

    if (asprintf (&key, "lwj.%ld.state", j->lwj_id) < 0) {
        flux_log (h, LOG_ERR, "extract_lwjinfo state key create failed");
        goto ret;
    } else if (kvs_get_string (h, key, &state) < 0) {
        flux_log (h, LOG_ERR, "extract_lwjinfo %s: %s", key, strerror (errno));
        goto ret;
    } else {
        j->state = stab_lookup (jobstate_tab, state);
        flux_log (h, LOG_DEBUG, "extract_lwjinfo got %s: %s", key, state);
        free(key);
    }

    if (asprintf (&key, "lwj.%ld.nnodes", j->lwj_id) < 0) {
        flux_log (h, LOG_ERR, "extract_lwjinfo nnodes key create failed");
        goto ret;
    } else if (kvs_get_int64 (h, key, &reqnodes) < 0) {
        flux_log (h, LOG_ERR, "extract_lwjinfo get %s: %s",
                  key, strerror (errno));
        goto ret;
    } else {
        j->req.nnodes = reqnodes;
        flux_log (h, LOG_DEBUG, "extract_lwjinfo got %s: %ld", key, reqnodes);
        free(key);
    }

    if (asprintf (&key, "lwj.%ld.ntasks", j->lwj_id) < 0) {
        flux_log (h, LOG_ERR, "extract_lwjinfo ntasks key create failed");
        goto ret;
    } else if (kvs_get_int64 (h, key, &reqtasks) < 0) {
        flux_log (h, LOG_ERR, "extract_lwjinfo get %s: %s",
                  key, strerror (errno));
        goto ret;
    } else {
        /* Assuming a 1:1 relationship right now between cores and tasks */
        j->req.ncores = reqtasks;
        flux_log (h, LOG_DEBUG, "extract_lwjinfo got %s: %ld", key, reqtasks);
        free(key);
        j->alloc.nnodes = 0;
        j->alloc.ncores = 0;
        j->rdl = NULL;
        rc = 0;
    }

    if (asprintf (&key, "lwj.%ld.io_rate", j->lwj_id) < 0) {
        flux_log (h, LOG_ERR, "extract_lwjinfo io_rate key create failed");
        goto ret;
    } else if (kvs_get_int64 (h, key, &io_rate) < 0) {
        flux_log (h, LOG_ERR, "extract_lwjinfo get %s: %s",
                  key, strerror (errno));
        goto ret;
    } else {
        j->req.io_rate = io_rate;
        j->alloc.io_rate = 0; //currently not used
        flux_log (h, LOG_DEBUG, "extract_lwjinfo got %s: %ld", key, io_rate);
        free(key);
    }

    if (asprintf (&key, "lwj.%ld.time_limit", j->lwj_id) < 0) {
        flux_log (h, LOG_ERR, "extract_lwjinfo time_limit key create failed");
        goto ret;
    } else if (kvs_get_double (h, key, &walltime) < 0) {
        flux_log (h, LOG_ERR, "extract_lwjinfo get %s: %s",
                  key, strerror (errno));
        goto ret;
    } else {
        j->req.walltime = walltime;
        j->alloc.walltime = -1; //currently not used
        flux_log (h, LOG_DEBUG, "extract_lwjinfo got %s: %f", key, walltime);
        free(key);
    }

ret:
    return rc;
}


static void
issue_lwj_event (lwj_event_e e, flux_lwj_t *j)
{
    flux_event_t *ev
        = (flux_event_t *) xzmalloc (sizeof (flux_event_t));
    ev->t = lwj_event;
    ev->ev.je = e;
    ev->lwj = j;

    if (zlist_append (ev_queue, ev) == -1) {
        flux_log (h, LOG_ERR,
                  "enqueuing an event failed");
        goto ret;
    }
    if (signal_event () == -1) {
        flux_log (h, LOG_ERR,
                  "signaling an event failed");
        goto ret;
    }

ret:
    return;
}

/****************************************************************
 *
 *         Scheduler Activities
 *
 ****************************************************************/

/*
 * Initialize the rdl resources with "idle" tags on each core
 */
static int
idlize_resources (struct resource *r)
{
    int rc = 0;
    struct resource *c;

    if (r) {
        rdl_resource_tag (r, IDLETAG);
        rdl_resource_iterator_reset(r);
        while (!rc && (c = rdl_resource_next_child (r))) {
            rc = idlize_resources (c);
            rdl_resource_destroy (c);
        }
        rdl_resource_iterator_reset(r);
    } else {
        flux_log (h, LOG_ERR, "idlize_resources passed a null resource");
        rc = -1;
    }

    return rc;
}

static void deallocate_resource_bandwidth (struct resource *r, int64_t amount)
{
	int64_t old_alloc_bw;
	int64_t new_alloc_bw;
	JSON o = rdl_resource_json (r);

    rdl_resource_get_int (r, "alloc_bw", &old_alloc_bw);
    new_alloc_bw = old_alloc_bw - amount;

    if (new_alloc_bw < 0) {
      flux_log (h, LOG_ERR, "too much bandwidth deallocated (%ld) - %s",
                amount, Jtostr (o));
    }
    rdl_resource_set_int (r, "alloc_bw", new_alloc_bw);
	Jput (o);
}

/*
static bool check_job_tag ()
{
	JSON o2;
	JSON o3;
	char *lwjtag;

	asprintf (&lwjtag, "lwj.%ld", lwj_id);
	Jget_obj (o, "tags", &o2);
	Jget_obj (o2, IDLETAG, &o3);

	if (o3) {

		Jput (o3);
	}
}
*/

//Walk the job's rdl until you reach the cores
//Keep track of ancestors the whole way down
//When you reach a core, deallocate the job's bw for that core all the way up (core -> root)
static void deallocate_bandwidth_helper (struct rdl *rdl, struct resource *jr,
										 int64_t io_rate, zlist_t *ancestors)
{
	struct resource *curr, *child, *ancestor;
    char *uri = NULL;
	const char *type = NULL;
	JSON o = NULL;

    //TODO: check if this is necessary
    asprintf (&uri, "%s:%s", global_rdl_resource, rdl_resource_path (jr));
    curr = rdl_resource_get (rdl, uri);

    if (curr) {
        o = rdl_resource_json (curr);
        Jget_str (o, "type", &type);
        if (strcmp (type, CORETYPE) == 0) {
			//Deallocate bandwidth of this resource
			deallocate_resource_bandwidth (curr, io_rate);
            //Deallocate bandwidth of this resource's ancestors
			ancestor = zlist_first (ancestors);
			while (ancestor != NULL) {
				deallocate_resource_bandwidth (ancestor, io_rate);
				ancestor = zlist_next (ancestors);
			}
        } else { //if not a core, recurse
          zlist_push (ancestors, curr);
          rdl_resource_iterator_reset (jr);
          while ((child = rdl_resource_next_child (jr))) {
            deallocate_bandwidth_helper (rdl, child, io_rate, ancestors);
            rdl_resource_destroy (child);
          }
          zlist_pop (ancestors);
        }
        Jput (o);
        rdl_resource_destroy (curr);
    } else {
        flux_log (h, LOG_ERR, "deallocate_bandwidth_helper failed to get %s", uri);
    }
    free (uri);

	return;
}

static void deallocate_bandwidth (struct rdl *rdl, const char *uri, flux_lwj_t *job)
{
	flux_log (h, LOG_DEBUG, "deallocate_bandwidth job: %ld, io_rate: %ld", job->lwj_id, job->req.io_rate);

	zlist_t *ancestors = zlist_new ();
    struct resource *jr = rdl_resource_get (job->rdl, uri);

    if (jr) {
        rdl_resource_iterator_reset (jr);
		deallocate_bandwidth_helper (rdl, jr, job->req.io_rate, ancestors);
        rdl_resource_destroy (jr);
    } else {
        flux_log (h, LOG_ERR, "deallocate_bandwidth failed to get resources: %s",
                  strerror (errno));
    }

	zlist_destroy (&ancestors);
	return;
}

static void allocate_resource_bandwidth (struct resource *r, int64_t amount)
{
	int64_t old_alloc_bw;
	int64_t new_alloc_bw;

	rdl_resource_get_int (r, "alloc_bw", &old_alloc_bw);
	new_alloc_bw = amount + old_alloc_bw;
	rdl_resource_set_int (r, "alloc_bw", new_alloc_bw);
}

static bool allocate_bandwidth (flux_lwj_t *job, struct resource *r, zlist_t *ancestors)
{
	struct resource *curr_r = NULL;

	allocate_resource_bandwidth (r, job->req.io_rate);
 	curr_r = zlist_first (ancestors);
	while (curr_r != NULL) {
		allocate_resource_bandwidth (curr_r, job->req.io_rate);
		curr_r = zlist_next (ancestors);
	}

	return true;
}

static bool allocate_core (flux_lwj_t *job, struct resource *resource,
                           zlist_t *ancestors, struct rdl_accumulator *accum,
                           char *lwjtag)
{
    bool output = false;
    JSON o;
    if (allocate_bandwidth (job, resource, ancestors)) {
        job->req.ncores--;
        job->alloc.ncores++;
        rdl_resource_tag (resource, lwjtag);
        rdl_resource_set_int (resource, "lwj", job->lwj_id);
        rdl_resource_delete_tag (resource, IDLETAG);
        if (rdl_accumulator_add (accum, resource) < 0) {
            o = rdl_resource_json (resource);
            flux_log (h, LOG_ERR, "failed to allocate core: %s", Jtostr (o));
        } else {
            output = true;
        }
    }
    return output;
}

/*
 * Walk the tree, find the required resources and tag with the lwj_id
 * to which it is allocated.
 */
static bool
allocate_resources (struct rdl *rdl, struct resource *fr,
                    struct rdl_accumulator *accum,
                    flux_lwj_t *job, zlist_t *ancestors)
{
    char *lwjtag = NULL;
    char *uri = NULL;
    const char *type = NULL;
    JSON o = NULL, o2 = NULL, o3 = NULL;
    struct resource *child, *curr;
    bool found = false;

    asprintf (&uri, "%s:%s", global_rdl_resource, rdl_resource_path (fr));
    curr = rdl_resource_get (rdl, uri);
    free (uri);

    o = rdl_resource_json (curr);
    Jget_str (o, "type", &type);
    asprintf (&lwjtag, "lwj.%ld", job->lwj_id);
    if (job->req.nnodes && (strcmp (type, NODETYPE) == 0)) {
		/*
        Jget_obj (o, "tags", &o2);
        Jget_obj (o2, IDLETAG, &o3);
        if (o3) {
		*/
			job->req.nnodes--;
			job->alloc.nnodes++;
			/*
            rdl_resource_tag (r, lwjtag);
            rdl_resource_delete_tag (r, IDLETAG);
            rdl_accumulator_add (a, r);
			}*/
    } else if (job->req.ncores && (strcmp (type, CORETYPE) == 0) &&
               (job->req.ncores > job->req.nnodes)) {
        /* We put the (job->req.ncores > job->req.nnodes) requirement
         * here to guarantee at least one core per node. */
        Jget_obj (o, "tags", &o2);
        Jget_obj (o2, IDLETAG, &o3);
        if (o3) {
            allocate_core(job, curr, ancestors, accum, lwjtag);
        }
    }

    found = !(job->req.nnodes || job->req.ncores);

	zlist_push (ancestors, curr);
    rdl_resource_iterator_reset (fr);
    while (!found && (child = rdl_resource_next_child (fr))) {
        found = allocate_resources (rdl, child, accum, job, ancestors);
        rdl_resource_destroy (child);
    }
    rdl_resource_iterator_reset (fr);
	zlist_pop (ancestors);
    rdl_resource_destroy (curr);
    free (lwjtag);
    Jput (o);

    return found;
}

static int
release_lwj_resource (struct rdl *rdl, struct resource *jr, char *lwjtag)
{
    char *uri = NULL;
    const char *type = NULL;
    int rc = 0;
    JSON o = NULL;
    struct resource *child, *curr;

    asprintf (&uri, "%s:%s", global_rdl_resource, rdl_resource_path (jr));
    curr = rdl_resource_get (rdl, uri);

    if (curr) {
        o = rdl_resource_json (curr);
        Jget_str (o, "type", &type);
        if (strcmp (type, CORETYPE) == 0) {
            rdl_resource_delete_tag (curr, lwjtag);
            rdl_resource_delete_tag (curr, "lwj");
            rdl_resource_tag (curr, IDLETAG);
        }
        Jput (o);
        rdl_resource_destroy (curr);

        rdl_resource_iterator_reset (jr);
        while (!rc && (child = rdl_resource_next_child (jr))) {
            rc = release_lwj_resource (rdl, child, lwjtag);
            rdl_resource_destroy (child);
        }
        rdl_resource_iterator_reset (jr);
    } else {
        flux_log (h, LOG_ERR, "release_lwj_resource failed to get %s", uri);
        rc = -1;
    }
    free (uri);

    return rc;
}

/*
 * Find resources allocated to this job, and remove the lwj tag.
 */
int release_resources (struct rdl *rdl, const char *uri, flux_lwj_t *job)
{
    int rc = -1;
    struct resource *jr = rdl_resource_get (job->rdl, uri);
    char *lwjtag = NULL;

    asprintf (&lwjtag, "lwj.%ld", job->lwj_id);

    if (jr) {
        rc = release_lwj_resource (rdl, jr, lwjtag);
        deallocate_bandwidth (rdl, uri, job);
        rdl_resource_destroy (jr);
    } else {
        flux_log (h, LOG_ERR, "release_resources failed to get resources: %s",
                  strerror (errno));
    }
    free (lwjtag);

    return rc;
}

/*
 * Recursively search the resource r and update this job's lwj key
 * with the core count per rank (i.e., node for the time being)
 */
static int
update_job_cores (struct resource *jr, flux_lwj_t *job,
                  uint64_t *pnode, uint32_t *pcores)
{
    bool imanode = false;
    char *key = NULL, *lwjtag = NULL;
    const char *type = NULL;
    JSON o = NULL, o2 = NULL, o3 = NULL;
    struct resource *child;
    int rc = 0;

    if (jr) {
        o = rdl_resource_json (jr);
        if (!o) {
            flux_log (h, LOG_ERR, "update_job_cores invalid resource");
            rc = -1;
            goto ret;
        }
    } else {
        flux_log (h, LOG_ERR, "update_job_cores passed a null resource");
        rc = -1;
        goto ret;
    }

    Jget_str (o, "type", &type);
    if (strcmp (type, "node") == 0) {
        *pcores = 0;
        imanode = true;
    } else if (strcmp (type, CORETYPE) == 0) {
        /* we need to limit our allocation to just the tagged cores */
        asprintf (&lwjtag, "lwj.%ld", job->lwj_id);
        Jget_obj (o, "tags", &o2);
        Jget_obj (o2, lwjtag, &o3);
        if (o3) {
            (*pcores)++;
        }
        free (lwjtag);
    }
    Jput (o);

    while ((rc == 0) && (child = rdl_resource_next_child (jr))) {
        rc = update_job_cores (child, job, pnode, pcores);
        rdl_resource_destroy (child);
    }

    if (imanode) {
        if (asprintf (&key, "lwj.%ld.rank.%ld.cores", job->lwj_id,
                      *pnode) < 0) {
            flux_log (h, LOG_ERR, "update_job_cores key create failed");
            rc = -1;
            goto ret;
        } else if (kvs_put_int64 (h, key, *pcores) < 0) {
            flux_log (h, LOG_ERR, "update_job_cores %ld node failed: %s",
                      job->lwj_id, strerror (errno));
            rc = -1;
            goto ret;
        }
        free (key);
        (*pnode)++;
    }

ret:
    return rc;
}

/*
 * Create lwj entries to tell wrexecd how many tasks to launch per
 * node.
 * The key has the form:  lwj.<jobID>.rank.<nodeID>.cores
 * The value will be the number of tasks to launch on that node.
 */
static int
update_job_resources (flux_lwj_t *job)
{
    uint64_t node = 0;
    uint32_t cores = 0;
    struct resource *jr = rdl_resource_get (job->rdl, global_rdl_resource);
    int rc = -1;

    if (jr) {
        rc = update_job_cores (jr, job, &node, &cores);
        rdl_resource_destroy (jr);
    } else {
        flux_log (h, LOG_ERR, "update_job_resources passed a null resource");
    }

    return rc;
}

/*
 * Add the allocated resources to the job, and
 * change its state to "allocated".
 */
static int
update_job (flux_lwj_t *job)
{
	flux_log (h, LOG_DEBUG, "updating job %ld", job->lwj_id);
    int rc = -1;

    if (update_job_state (job, j_allocated)) {
        flux_log (h, LOG_ERR, "update_job failed to update job %ld to %s",
                  job->lwj_id, stab_rlookup (jobstate_tab, j_allocated));
    } else if (update_job_resources(job)) {
		kvs_commit (h);
        flux_log (h, LOG_ERR, "update_job %ld resrc update failed", job->lwj_id);
    } else if (kvs_commit (h) < 0) {
        flux_log (h, LOG_ERR, "kvs_commit error!");
    } else {
        rc = 0;
    }

	flux_log (h, LOG_DEBUG, "updated job %ld", job->lwj_id);
    return rc;
}

static struct rdl *get_free_subset (struct rdl *rdl, const char *type)
{
	JSON tags = Jnew();
	Jadd_bool (tags, IDLETAG, true);
	JSON args = Jnew ();
	Jadd_obj (args, "tags", tags);
	Jadd_str (args, "type", type);
    struct rdl *frdl = rdl_find (rdl, args);
	Jput (args);
	Jput (tags);
	return frdl;
}

static int64_t get_free_count_from_res (struct resource* res, const char* type)
{
	JSON o;
	int64_t count = -1;

	o = rdl_resource_aggregate_json (res);
	if (o) {
		if (!Jget_int64(o, type, &count)) {
			flux_log (h, LOG_ERR, "get_free_count_from_res failed to get %s",
					  type);
			return -1;
		} else {
			flux_log (h, LOG_DEBUG, "get_free_count_from_res found %ld idle %ss", count, type);
		}
		Jput (o);
	}

    return count;
}

static int64_t get_free_count_from_rdl (struct rdl *rdl, const char *uri, const char *type)
{
	struct resource *fr = NULL;
	int64_t count = -1;

	if ((fr = rdl_resource_get (rdl, uri)) == NULL) {
		flux_log (h, LOG_ERR, "failed to get found resources: %s", uri);
		return -1;
	}

    count = get_free_count_from_res(fr, type);

    rdl_resource_destroy (fr);

	return count;
}

static int schedule_job_without_update (struct rdl *rdl, const char *uri, flux_lwj_t *job, struct rdl_accumulator **a)
{
    //int64_t nodes = -1;
    int rc = 0;

    int64_t cores = -1;
    struct rdl *frdl = NULL;            /* found rdl */
    struct resource *fr = NULL;         /* found resource */

    if (!job || !rdl || !uri) {
        flux_log (h, LOG_ERR, "schedule_job invalid arguments");
        goto ret;
    }

	flux_log (h, LOG_DEBUG, "schedule_job_without_update called on job %ld", job->lwj_id);

    frdl = get_free_subset (rdl, "core");
    if (frdl) {
        cores = get_free_count_from_rdl (frdl, uri, "core");
        fr = rdl_resource_get (frdl, uri);
    } else {
        flux_log (h, LOG_DEBUG, "get_free_subset returned nothing, setting cores = 0");
        cores = 0;
        fr = NULL;
    }

	if (frdl && fr && cores > 0) {
		if (cores >= job->req.ncores) {
            zlist_t *ancestors = zlist_new ();
            //TODO: revert this in the deallocation/rollback
            int old_nnodes = job->req.nnodes;
            int old_ncores = job->req.ncores;
            int old_io_rate = job->req.io_rate;
            int old_alloc_nnodes = job->alloc.nnodes;
            int old_alloc_ncores = job->alloc.ncores;

			rdl_resource_iterator_reset (fr);
			*a = rdl_accumulator_create (rdl);
			if (allocate_resources (rdl, fr, *a, job, ancestors)) {
				flux_log (h, LOG_INFO, "scheduled job %ld without update", job->lwj_id);
                if (rc == 0)
                    rc = 1;
			} else {
                if (rdl_accumulator_is_empty(*a)) {
                    flux_log (h, LOG_DEBUG, "no resources found in accumulator");
                } else {
                    job->rdl = rdl_accumulator_copy (*a);
                    release_resources (rdl, global_rdl_resource, job);
                    rdl_destroy (job->rdl);
                    rdl_accumulator_destroy (*a);
                }
            }
            job->req.io_rate = old_io_rate;
            job->req.nnodes = old_nnodes;
            job->req.ncores = old_ncores;
            job->alloc.nnodes = old_alloc_nnodes;
            job->alloc.ncores = old_alloc_ncores;

            //TODO: clear the list and free each element (or set freefn)
            zlist_destroy (&ancestors);
		} else {
            flux_log (h, LOG_DEBUG, "not enough available cores, skipping this job");
        }
        //rdl_resource_destroy (fr);
        rdl_destroy (frdl);
	}

ret:
    return rc;
}

/*
 * schedule_job() searches through all of the idle resources (cores
 * right now) to satisfy a job's requirements.  If enough resources
 * are found, it proceeds to allocate those resources and update the
 * kvs's lwj entry in preparation for job execution. Returns 1 if the
 * job was succesfully scheduled, 0 if it was not, -1 if there was an
 * error.
 */
static int schedule_job (struct rdl *rdl, const char *uri, flux_lwj_t *job)
{
    //int64_t nodes = -1;
    int rc = 0;
    struct rdl_accumulator *a = NULL;

	//The "cache"
    int64_t cores = -1;
    struct rdl *frdl = NULL;            /* found rdl */
    struct resource *fr = NULL;         /* found resource */

    if (!job || !rdl || !uri) {
        flux_log (h, LOG_ERR, "schedule_job invalid arguments");
        goto ret;
    }

	flux_log (h, LOG_DEBUG, "schedule_job called on job %ld", job->lwj_id);

    frdl = get_free_subset (rdl, "core");
    if (frdl) {
        cores = get_free_count_from_rdl (frdl, uri, "core");
        fr = rdl_resource_get (frdl, uri);
    } else {
        flux_log (h, LOG_DEBUG, "get_free_subset returned nothing, setting cores = 0");
        cores = 0;
        fr = NULL;
    }

	if (frdl && fr && cores > 0) {
		if (cores >= job->req.ncores) {
            zlist_t *ancestors = zlist_new ();
            //TODO: revert this in the deallocation/rollback
            int old_nnodes = job->req.nnodes;
            int old_ncores = job->req.ncores;
            int old_io_rate = job->req.io_rate;
            int old_alloc_nnodes = job->alloc.nnodes;
            int old_alloc_ncores = job->alloc.ncores;
			rdl_resource_iterator_reset (fr);
			a = rdl_accumulator_create (rdl);
			if (allocate_resources (rdl, fr, a, job, ancestors)) {
				flux_log (h, LOG_INFO, "scheduled job %ld", job->lwj_id);
				job->rdl = rdl_accumulator_copy (a);
				job->state = j_submitted;
				rc = update_job (job);
                if (rc == 0)
                    rc = 1;
			}
			else {
				flux_log (h, LOG_DEBUG, "not enough resources to allocate, rolling back");
                job->req.io_rate = old_io_rate;
                job->req.nnodes = old_nnodes;
                job->req.ncores = old_ncores;
                job->alloc.nnodes = old_alloc_nnodes;
                job->alloc.ncores = old_alloc_ncores;

                if (rdl_accumulator_is_empty(a)) {
                  flux_log (h, LOG_DEBUG, "no resources found in accumulator");
                } else {
                  job->rdl = rdl_accumulator_copy (a);
                  release_resources (rdl, global_rdl_resource, job);
                  rdl_destroy (job->rdl);
                }
			}
            //TODO: clear the list and free each element (or set freefn)
            zlist_destroy (&ancestors);
			rdl_accumulator_destroy (a);
		} else {
            flux_log (h, LOG_DEBUG, "not enough available cores, skipping this job");
        }
        rdl_resource_destroy (fr);
        rdl_destroy (frdl);
	}

ret:
    return rc;
}


//Return true if job1 is expected to terminate after job2
static bool job_compare_termination_fn (void *item1, void *item2)
{
    job_t *job1, *job2;
    double term1, term2;

    job1 = (job_t *) item1;
    job2 = (job_t *) item2;

    term1 = job1->start_time + job1->time_limit;
    term2 = job2->start_time + job2->time_limit;

    return (term1 > term2);
}


//Calculate the earliest point in time where the number of free cores
//is greater than the number of cores required by the reserved job.
//Output is the time at which this occurs and the number of cores that
//are free, excluding the cores that will be used by the reserved job

//The input rdl should be a copy that can be mutated to reflect the rdl's state @ shadow_time
static void calculate_shadow_info (flux_lwj_t *reserved_job, struct rdl *shadow_rdl, const char *uri, zlist_t *running_jobs, double *shadow_time, int64_t *shadow_free_cores)
{
    job_t *curr_job_t;
    flux_lwj_t *curr_lwj_job;
    struct rdl *frdl = NULL;

    if (zlist_size (running_jobs) == 0) {
        flux_log (h, LOG_ERR, "No running jobs and reserved job still doesn't fit.");
        return;
    } else {
        flux_log (h, LOG_DEBUG, "calculate_shadow_info found %zu jobs currently running", zlist_size (running_jobs));
    }

    frdl = get_free_subset (shadow_rdl, "core");
    if (frdl) {
        *shadow_free_cores = get_free_count_from_rdl (frdl, uri, "core");
        rdl_destroy (frdl);
    } else {
        flux_log (h, LOG_DEBUG, "get_free_subset returned nothing, setting shadow_free_cores = 0");
        *shadow_free_cores = 0;
    }

    zlist_sort(running_jobs, job_compare_termination_fn);

    curr_job_t = zlist_first (running_jobs);
    while ((*shadow_free_cores < reserved_job->req.ncores)) {
        if (curr_job_t == NULL) {
            flux_log (h, LOG_ERR, "Curr job is null");
            break;
        } else if (curr_job_t->ncpus < 1) {
            flux_log (h, LOG_ERR, "Curr job %d incorrectly requires < 1 cpu: %d", curr_job_t->id, curr_job_t->ncpus);
        }
        //De-allocate curr_job_t's resources from the shadow_rdl
        curr_lwj_job = find_lwj(curr_job_t->id);
        if (curr_lwj_job->alloc.ncores != curr_job_t->ncpus) {
            flux_log (h, LOG_ERR, "Job %d's ncpus don't match: %d (lwj_job) and %d (job_t)", curr_job_t->id, curr_lwj_job->alloc.ncores, curr_job_t->ncpus);
        }
        release_resources (shadow_rdl, uri, curr_lwj_job);

        *shadow_free_cores += curr_job_t->ncpus;
        *shadow_time = curr_job_t->start_time + curr_job_t->time_limit;
        curr_job_t = zlist_next (running_jobs);
    }

    flux_log (h, LOG_DEBUG, "Entering the exact shadow loop");

    //Do a loop checking if the reserved job can be scheduled (considering IO)
    struct rdl_accumulator *accum = NULL;
    while (!schedule_job_without_update (shadow_rdl, uri, reserved_job, &accum)) {
        if (curr_job_t == NULL) {
            flux_log (h, LOG_ERR, "Curr job is null");
            break;
        } else if (curr_job_t->ncpus < 1) {
            flux_log (h, LOG_ERR, "Curr job %d incorrectly requires < 1 cpu: %d", curr_job_t->id, curr_job_t->ncpus);
        }
        //De-allocate curr_job_t's resources from the shadow_rdl
        curr_lwj_job = find_lwj(curr_job_t->id);
        if (curr_lwj_job->alloc.ncores != curr_job_t->ncpus) {
            flux_log (h, LOG_ERR, "Job %d's ncpus don't match: %d (lwj_job) and %d (job_t)", curr_job_t->id, curr_lwj_job->alloc.ncores, curr_job_t->ncpus);
        }
        release_resources (shadow_rdl, uri, curr_lwj_job);

        *shadow_free_cores += curr_job_t->ncpus;
        *shadow_time = curr_job_t->start_time + curr_job_t->time_limit;
        curr_job_t = zlist_next (running_jobs);
    }
    *shadow_free_cores -= reserved_job->req.ncores;
}

static bool job_backfill_eligible (struct rdl *rdl, struct rdl *shadow_rdl, const char *uri, flux_lwj_t *job, int64_t curr_free_cores, double shadow_time)
{
    struct rdl_accumulator *accum = NULL;
    struct rdl *rdl_to_test = NULL;
    bool terminates_before_shadow_time = ((sim_state->sim_time + job->req.walltime) < shadow_time);

    flux_log (h, LOG_DEBUG, "backfill info - term_before_shadow_time: %d, curr_free_cores: %ld, job_req_cores: %d", terminates_before_shadow_time, curr_free_cores, job->req.ncores);

    if (curr_free_cores < job->req.ncores) {
        return false;
    } else if (terminates_before_shadow_time) {
        //Job ends before reserved job starts, and we have enough cores currently to schedule it
        rdl_to_test = rdl;
    } else {
        //Job ends after reserved job starts, make sure we will have
        //enough free resources (cores + IO) at shadow time to not
        //delay the reserved job
        rdl_to_test = shadow_rdl;
    }
    bool scheduled = schedule_job_without_update (rdl_to_test, uri, job, &accum);
    if (scheduled) {
        job->rdl = rdl_accumulator_copy (accum);
        release_resources(rdl_to_test, uri, job);
        rdl_destroy (job->rdl);
        rdl_accumulator_destroy (accum);
    }
    return scheduled;
}

bool get_resource_of_type_helper (const char* resource_type,
                                  struct resource* curr_resource,
                                  zlist_t* ancestors,
                                  //Output
                                  zlist_t* res_list)
{
    bool return_value = false;
    struct resource* child_resource = NULL;
    JSON curr_res_obj = rdl_resource_json (curr_resource);
    const char* curr_type;
    Jget_str (curr_res_obj, "type", &curr_type);

    //Get the 1st resource of type "resource_type"
    if (strcmp (curr_type, resource_type) == 0) {
        zlist_append (res_list, curr_resource);
        return_value = true;
    }

    zlist_push (ancestors, curr_resource);
    rdl_resource_iterator_reset (curr_resource);
    child_resource = rdl_resource_next_child(curr_resource);
    while (child_resource != NULL) {
        if (!get_resource_of_type_helper(resource_type, child_resource, ancestors, res_list)) {
            rdl_resource_destroy(child_resource);
        }
        child_resource = rdl_resource_next_child(curr_resource);
    }
    zlist_pop (ancestors);
    Jput(curr_res_obj);
    return return_value;
}

//Generates a list of all resources of a given type. e.g. core, node, switch
//Similar to doing a walk across a particular level of the rdl tree
//Algorithm does not assume all resources of a given type are on the same level
//i.e. it will traverse the entire tree
zlist_t* get_resources_of_type (struct rdl* rdl, const char* resource_type)
{
    zlist_t* ancestors = zlist_new ();
    zlist_t* res_list = zlist_new ();
    struct resource* root_resource = rdl_resource_get (rdl, global_rdl_resource);

    if (!get_resource_of_type_helper (resource_type, root_resource, ancestors, res_list)) {
        rdl_resource_destroy(root_resource);
    }
    zlist_destroy (&ancestors);
    return res_list;
}

static void generate_lp_constraints (glp_prob *lp, zlist_t *eligible_jobs,
                                     flux_ilp_matrix* ilp_mat,
                                     const char* resource_type, zlist_t* resource_list,
                                     int shadow_free_cores, double shadow_time,
                                     int jobs_on_idx)
{
    int row_idx, col_idx, num_jobs, num_rows, num_free_resources;
    double value, upper_bound;
    int64_t i, j;
    flux_lwj_t *curr_job = NULL;
    int max_name_len = 100;
    char name[max_name_len];
    struct resource* curr_resource = NULL;
    JSON o, o2;
    const char *hierarchy;

    num_free_resources = zlist_size (resource_list);
    num_jobs = zlist_size (eligible_jobs);
    num_rows = num_jobs + num_free_resources + 1; //number of "subject to" constraints, +1 due to backfilling constraint
    glp_add_rows (lp, num_rows);
    curr_job = zlist_first (eligible_jobs);
    for (i = 1; curr_job != NULL; i++) {
        snprintf (name, max_name_len, "job%03ld", curr_job->lwj_id);
        glp_set_row_name (lp, i, name);
        glp_set_row_bnds (lp, i, GLP_FX, 0.0, 0.0);
        for (j = 1; j < num_free_resources + 1; j++) {
            row_idx = i;
            col_idx = ((i-1) * num_free_resources) + j;
            value = 1;
            insert_into_ilp_matrix(ilp_mat, row_idx, col_idx, value);
        }
        curr_job = zlist_next (eligible_jobs);
    }

    for (i=1, curr_resource = zlist_first (resource_list);
         curr_resource != NULL;
         i++, curr_resource = zlist_next (resource_list))
    {
        o = rdl_resource_json (curr_resource);
        Jget_obj(o, "hierarchy", &o2);
        Jget_str (o2, "default", &hierarchy);
        printf ("Checking resource (%ld) %s\n", i, hierarchy);

        snprintf (name, max_name_len, "%s%04ld", resource_type, i);
        if (strcmp (resource_type, CORETYPE) == 0) {
            upper_bound = 1.0;
        } else if (strcmp (resource_type, SWITCHTYPE) == 0 ||
            strcmp (resource_type, NODETYPE) == 0) {
            upper_bound = get_free_count_from_res(curr_resource, CORETYPE);
        }
        row_idx = i+num_jobs;
        glp_set_row_name (lp, row_idx, name);
        glp_set_row_bnds (lp, row_idx, GLP_DB, 0.0, upper_bound);
        for (j = 1; j < num_jobs + 1; j++) {
            col_idx = ((j-1) * num_free_resources) + i;
            value = 1;
            insert_into_ilp_matrix(ilp_mat, row_idx, col_idx, value);
        }
    }

    //Set the backfill constraint and required resources per job constraint
    job_t *curr_job_t;
    kvsdir_t curr_kvs_dir;
    zlist_t *shadow_jobs = zlist_new();
    for (i = jobs_on_idx, curr_job = zlist_first (eligible_jobs);
         curr_job != NULL;
         i++, curr_job = zlist_next (eligible_jobs))
    {
        //Job resource requirement constraint
        row_idx = (i - jobs_on_idx) + 1;
        col_idx = i;
        value = -1 * (double)curr_job->req.ncores;
        insert_into_ilp_matrix(ilp_mat, row_idx, col_idx, value);

        //Backfill constraint
        kvs_get_dir (h,  &curr_kvs_dir, "lwj.%d", curr_job->lwj_id);
        curr_job_t = pull_job_from_kvs (curr_kvs_dir);
        if (curr_job_t->start_time == 0)
            curr_job_t->start_time = sim_state->sim_time;
        if (curr_job_t->start_time + curr_job_t->time_limit > shadow_time) {
            zlist_append (shadow_jobs, curr_job);
        }
    }

    //Backfill constraint
    if (zlist_size (shadow_jobs) > 0) {
        glp_set_row_name (lp, num_rows, "backfill");
        glp_set_row_bnds (lp, num_rows, GLP_DB, 0, shadow_free_cores);

        for (i = jobs_on_idx, curr_job = zlist_first (shadow_jobs);
             curr_job != NULL;
             i++, curr_job = zlist_next (shadow_jobs))
        {
            col_idx = i;
            row_idx = num_rows;
            value = -1 * (double)curr_job->req.ncores;
            insert_into_ilp_matrix(ilp_mat, row_idx, col_idx, -1*value);
        }
    }
    zlist_destroy (&shadow_jobs);
}

void generate_objective_function (glp_prob *lp, zlist_t *eligible_jobs,
                                  flux_ilp_matrix* ilp_mat, int num_cols,
                                  const char* resource_type, zlist_t* resource_list,
                                  int jobs_on_idx)
{
    int num_jobs, num_free_resources;
    double upper_bound;
    int64_t i, j, col_num;
    flux_lwj_t *curr_job = NULL;
    int max_name_len = 100;
    char name[max_name_len];
    struct resource* curr_resource = NULL;

    num_free_resources = zlist_size (resource_list);

    num_jobs = zlist_size (eligible_jobs);
    glp_set_obj_dir (lp, GLP_MAX);
    glp_add_cols (lp, num_cols);
    //I flip-flopped the nesting of the loops to only perform the strcmp's
    //and the get_free_count 1 time per resource
    for (j=1, curr_resource = zlist_first (resource_list);
         curr_resource != NULL;
         j++, curr_resource = zlist_next (resource_list))
    {
        //Get the number of free children resources that we will be mapping onto
        if (strcmp (resource_type, CORETYPE) == 0) {
            upper_bound = 1.0; //mapping directly onto core/node
        } else if (strcmp (resource_type, SWITCHTYPE) == 0 ||
                   strcmp (resource_type, NODETYPE) == 0) {
            //mapping onto free children nodes
            upper_bound = get_free_count_from_res(curr_resource, CORETYPE);
        }

        for (i = 1, curr_job = zlist_first (eligible_jobs);
             curr_job != NULL;
             i++, curr_job = zlist_next (eligible_jobs))
        {
            snprintf (name, max_name_len, "job%03ld_%s%04ld", curr_job->lwj_id, resource_type, j);
            col_num = ((i-1)*num_free_resources) + j;
            glp_set_col_name (lp, col_num, name);
            glp_set_col_bnds (lp, col_num, GLP_DB, 0.0, upper_bound);
            glp_set_obj_coef (lp, col_num, 1);
        }
    }

    curr_job = zlist_first (eligible_jobs);
    for (i = jobs_on_idx; curr_job != NULL; i++) {
        snprintf (name, max_name_len, "job%03ld_on", curr_job->lwj_id);
        glp_set_col_name (lp, i, name);
        glp_set_col_kind (lp, i, GLP_BV);
        glp_set_obj_coef (lp, i, 0);
        curr_job = zlist_next (eligible_jobs);
    }

}

static void save_ilp_to_file (glp_prob *lp)
{
    static int iter = 0;
    char* lp_filename, *sol_filename;
    int pid = getpid();
    asprintf (&lp_filename, "sched.%d.%d.lp", pid, iter);
    asprintf (&sol_filename, "sched.%d.%d.sol", pid, iter++);
    glp_write_lp(lp, NULL, lp_filename);
    glp_print_mip(lp, sol_filename);
    free (lp_filename);
    free (sol_filename);
}

static void solve_ilp (glp_prob *lp)
{
    glp_iocp params;
    glp_init_iocp (&params);
    params.presolve = GLP_ON;
    int err = glp_intopt (lp, &params);
    if (err) {
        flux_log (h, LOG_ERR, "glpk error: %d", err);
    }
}

static glp_prob* build_ilp_problem (zlist_t *eligible_jobs, struct rdl* rdl,
                                    const char* resource_type, zlist_t* res_list,
                                    int64_t shadow_free_cores, double shadow_time)
{
    int num_cols, esti_num_rows, num_jobs, jobs_on_starting_idx, num_free_resources;
    glp_prob *lp;
    flux_ilp_matrix *ilp_mat;

    num_free_resources = zlist_size (res_list);
    num_jobs = zlist_size (eligible_jobs);
    esti_num_rows = num_jobs + num_free_resources + 1; //number of "subject to" constraints, +1 due to backfilling constraint
    num_cols = (num_jobs * num_free_resources) + //jobs could be placed on any resource
        (num_jobs);  //the jobX_on decision variables
    jobs_on_starting_idx = num_cols - num_jobs + 1;

    ilp_mat = new_ilp_matrix(esti_num_rows * num_free_resources);

    lp = glp_create_prob();
    glp_set_prob_name (lp, "sched_ilp");

    generate_lp_constraints(lp, eligible_jobs, ilp_mat, resource_type, res_list,
                            shadow_free_cores, shadow_time, jobs_on_starting_idx);

    generate_objective_function(lp, eligible_jobs, ilp_mat, num_cols,
                                resource_type, res_list, jobs_on_starting_idx);

    load_matrix_into_ilp (ilp_mat, lp);
    solve_ilp(lp);
    save_ilp_to_file(lp);

    free_ilp_matrix(ilp_mat);

    return lp;
}


//The curr_resource should be from a free subset of the rdl
//This function will add the "main rdl" resource version to the accumulator
int add_child_cores_to_accum (struct rdl *rdl, struct resource* curr_resource,
                              struct rdl_accumulator *accum, int num_to_add)
{
    struct resource *curr_child, *main_rdl_resource;
    JSON o = NULL, tags = NULL;
    JSON idle = NULL;
    const char *type, *path;
    char *uri;

    if (num_to_add <= 0)
        return num_to_add;

    o = rdl_resource_json (curr_resource);
    Jget_str (o, "type", &type);
    Jget_obj (o, "tags", &tags);
    Jget_obj (tags, IDLETAG, &idle);
    if (strcmp (type, CORETYPE) == 0 && idle) {
        path = rdl_resource_path (curr_resource);
        asprintf (&uri, "%s:%s", global_rdl_resource, path);
        main_rdl_resource = rdl_resource_get(rdl, uri);
        rdl_accumulator_add (accum, main_rdl_resource);
        rdl_resource_destroy (main_rdl_resource);

        rdl_resource_delete_tag (curr_resource, IDLETAG);
        num_to_add--;
    } else {
        rdl_resource_iterator_reset(curr_resource);
        for (curr_child = rdl_resource_next_child(curr_resource);
             curr_child != NULL;
             curr_child = rdl_resource_next_child(curr_resource))
        {
            num_to_add = add_child_cores_to_accum (rdl, curr_child, accum, num_to_add);
            rdl_resource_destroy(curr_child);
        }
    }
    Jput (o);
    Jput (tags);
    return num_to_add;
}
//core num should be 0 at root of recursive tree (first call)
void schedule_jobs_from_ilp_helper (glp_prob *lp, const char* resource_type,
                                    zlist_t *res_list, struct rdl *rdl,
                                    zlist_t *scheduled_jobs)
{
    struct resource *curr_resource;
    JSON o, o2;
    const char *type, *hierarchy;
    int i, col_val, col_idx, num_remaining;
    flux_job_ilp* job_ilp;

    printf ("Num resources: %d, num jobs: %d\n",
            (int)zlist_size (res_list), (int)zlist_size (scheduled_jobs));
    for (i=0, curr_resource = zlist_first (res_list);
         curr_resource != NULL;
         i++, curr_resource = zlist_next (res_list))
    {
        o = rdl_resource_json (curr_resource);
        Jget_str (o, "type", &type);
        Jget_obj(o, "hierarchy", &o2);
        Jget_str (o2, "default", &hierarchy);
        for (job_ilp = zlist_first (scheduled_jobs); job_ilp != NULL; job_ilp = zlist_next (scheduled_jobs)) {
            col_idx = job_ilp->starting_idx + i;
            col_val = glp_mip_col_val (lp, col_idx);
            if (col_val > 0) {
                num_remaining = add_child_cores_to_accum(rdl, curr_resource, job_ilp->accum, col_val);
                if (num_remaining > 0) {
                    flux_log (h, LOG_ERR, "Failed to add enough cores to job %ld", job_ilp->job->lwj_id);
                }
            }
        }
        Jput (o);
    }
}

static zlist_t* schedule_jobs_from_ilp (struct rdl* rdl, struct rdl* free_rdl,
                                        glp_prob* lp, zlist_t* eligible_jobs,
                                        const char* resource_type, zlist_t *res_list)
{
    int num_jobs, num_cols, col_offset, i, col_idx, starting_idx, num_free_resources;
    double z, col_val;
    flux_lwj_t *curr_job = NULL;
    const char *col_name = NULL;
    zlist_t *ancestors = NULL, *scheduled_jobs = NULL, *scheduled_lwjs = NULL;
    flux_job_ilp *job_ilp = NULL;
    struct resource *job_rdl_root = NULL;
    struct rdl_accumulator* tmp_accum = NULL;

    num_free_resources = zlist_size (res_list);
    num_jobs = zlist_size (eligible_jobs);
    num_cols = glp_get_num_cols (lp);

    z = glp_mip_obj_val (lp);

    scheduled_jobs = zlist_new ();
    col_offset = num_cols - num_jobs;
    curr_job = zlist_first (eligible_jobs);
    for (i = 0; curr_job != NULL; i++) {
        col_idx = i + col_offset + 1;
        col_val = glp_mip_col_val (lp, col_idx);
        if (col_val == 1) {
            col_name = glp_get_col_name (lp, col_idx);
            starting_idx = (i * num_free_resources) + 1;
            job_ilp = new_job_ilp (curr_job, rdl, starting_idx);
            zlist_append (scheduled_jobs, job_ilp);
            zlist_freefn (scheduled_jobs, job_ilp, freefn_job_ilp, true);
        }
        curr_job = zlist_next (eligible_jobs);
    }

    schedule_jobs_from_ilp_helper(lp, resource_type, res_list, rdl, scheduled_jobs);

    scheduled_lwjs = zlist_new ();
    job_ilp = zlist_first (scheduled_jobs);
    while (job_ilp != NULL) {
        curr_job = job_ilp->job;
        flux_log (h, LOG_INFO, "scheduled job %ld", curr_job->lwj_id);
        curr_job->rdl = rdl_accumulator_copy (job_ilp->accum);
        curr_job->state = j_submitted;
        job_rdl_root = rdl_resource_get(curr_job->rdl, global_rdl_resource);
        ancestors = zlist_new();
        tmp_accum = rdl_accumulator_create(rdl);
        allocate_resources (rdl, job_rdl_root, tmp_accum, curr_job, ancestors);
        curr_job->rdl = rdl_accumulator_copy (tmp_accum);
        rdl_accumulator_destroy(tmp_accum);
        zlist_destroy (&ancestors);
        if (update_job (curr_job)) {
            flux_log (h, LOG_ERR, "error updating job %ld's state", curr_job->lwj_id);
        }
        zlist_append(scheduled_lwjs, job_ilp->job);
        job_ilp = zlist_next (scheduled_jobs);
    }

    zlist_destroy (&scheduled_jobs);
    return scheduled_lwjs;
}

static void backfill_jobs (struct rdl *rdl, const char *uri, zlist_t *queued_jobs, zlist_t *running_jobs,
                           flux_lwj_t *reserved_job)
{
    glp_prob *lp;
    double shadow_time;
    int64_t curr_free_cores = -1, shadow_free_cores = -1;
    struct rdl *shadow_rdl = NULL, *free_rdl = NULL;
    zlist_t *eligible_jobs = NULL, *scheduled_jobs = NULL, *resource_list = NULL;
    flux_lwj_t *curr_job = NULL;
    job_t *curr_job_t = NULL;
    kvsdir_t curr_kvs_dir;
    const char* resource_type = SWITCHTYPE;

    shadow_rdl = rdl_copy (rdl);
    calculate_shadow_info (reserved_job, shadow_rdl, uri, running_jobs, &shadow_time, &shadow_free_cores);

    flux_log(h, LOG_DEBUG, "Job %ld has the reservation - shadow time: %f, shadow free cores: %ld",
             reserved_job->lwj_id, shadow_time, shadow_free_cores);

    free_rdl = get_free_subset (rdl, "core");
    if (free_rdl) {
        curr_free_cores = get_free_count_from_rdl (free_rdl, uri, "core");
    } else {
        flux_log (h, LOG_DEBUG, "get_free_subset returned nothing, setting curr_free_cores = 0");
        curr_free_cores = 0;
    }

    //Queued jobs pointer might not be accurate, refind the reserved_job within the list
    curr_job = zlist_first (queued_jobs);
    while (curr_job != reserved_job) {
        curr_job = zlist_next (queued_jobs);
    }

    eligible_jobs = zlist_new();
    curr_job = zlist_next (queued_jobs); //job after the reserved_job
    while (curr_job && curr_free_cores > 0 && shadow_time > 0) {
        if (curr_job->state == j_unsched) {
            if (job_backfill_eligible (rdl, shadow_rdl, uri, curr_job, curr_free_cores, shadow_time)) {
                flux_log(h, LOG_DEBUG, "Job %ld is eligible for backfilling, adding to ILP problem.", curr_job->lwj_id);
                zlist_append(eligible_jobs, curr_job);
            } else {
                flux_log(h, LOG_DEBUG, "Job %ld is not eligible for backfilling. Not added to ILP problem.", curr_job->lwj_id);
            }
        }

        if (zlist_size (eligible_jobs) >= 5) {
            resource_list = get_resources_of_type(free_rdl, resource_type);
            lp = build_ilp_problem (eligible_jobs, rdl, resource_type, resource_list,
                                    shadow_free_cores, shadow_time);
            scheduled_jobs = schedule_jobs_from_ilp (rdl, free_rdl, lp, eligible_jobs,
                                                     resource_type, resource_list);
            glp_delete_prob (lp);
            resource_list = get_resources_of_type(free_rdl, resource_type);
            while (zlist_size (scheduled_jobs) > 0) {
                flux_lwj_t* lwj_job = zlist_pop (scheduled_jobs);
                kvs_get_dir (h,  &curr_kvs_dir, "lwj.%d", lwj_job->lwj_id);
                curr_job_t = pull_job_from_kvs (curr_kvs_dir);
                if (curr_job_t->start_time == 0)
                    curr_job_t->start_time = sim_state->sim_time;
                zlist_append (running_jobs, curr_job_t);
            }

            zlist_destroy (&eligible_jobs);
            eligible_jobs = zlist_new ();
            free_rdl = get_free_subset (rdl, "core");
            if (free_rdl) {
                curr_free_cores = get_free_count_from_rdl (free_rdl, uri, "core");
            } else {
                flux_log (h, LOG_DEBUG, "get_free_subset returned nothing, setting curr_free_cores = 0");
                curr_free_cores = 0;
            }
            rdl_destroy (shadow_rdl);
            shadow_rdl = rdl_copy (rdl);
            calculate_shadow_info (reserved_job, shadow_rdl, uri, running_jobs, &shadow_time, &shadow_free_cores);
        }
        curr_job = zlist_next (queued_jobs);
    }

    if (zlist_size (eligible_jobs) > 1) {
        resource_list = get_resources_of_type(free_rdl, resource_type);
        lp = build_ilp_problem (eligible_jobs, free_rdl, resource_type,
                                resource_list,
                                shadow_free_cores, shadow_time);
        schedule_jobs_from_ilp (rdl, free_rdl, lp, eligible_jobs,
                                resource_type, resource_list);
        glp_delete_prob (lp);
    } else if (zlist_size (eligible_jobs) == 1) {
        schedule_job (rdl, uri, zlist_pop (eligible_jobs));
    }
    zlist_destroy (&eligible_jobs);

    rdl_destroy (free_rdl);
    rdl_destroy (shadow_rdl);
}

bool job_compare_id_fn (void *item1, void *item2)
{
    flux_lwj_t *job1, *job2;
    job1 = (flux_lwj_t *) item1;
    job2 = (flux_lwj_t *) item2;

    //TODO: switch this to use submit time
    return (job1->lwj_id > job2->lwj_id);
}

int schedule_jobs (struct rdl *rdl, const char *uri, zlist_t *jobs)
{
    flux_lwj_t *curr_job = NULL, *curr_lwj_job = NULL, *reserved_job = NULL;
    job_t *curr_job_t = NULL;
    kvsdir_t curr_kvs_dir;
    int rc = 0, job_scheduled = 1;
    zlist_t *queued_jobs = zlist_dup (jobs), *running_jobs = zlist_new ();

    zlist_sort(queued_jobs, job_compare_id_fn);

    //Schedule all jobs at the front of the queue
    curr_job = zlist_first (queued_jobs);
    while (job_scheduled && curr_job) {
		if (curr_job->state == j_unsched) {
			job_scheduled = schedule_job (rdl, uri, curr_job);
            if (job_scheduled) {
                kvs_get_dir (h,  &curr_kvs_dir, "lwj.%d", curr_job->lwj_id);
                curr_job_t = pull_job_from_kvs (curr_kvs_dir);
                if (curr_job_t->start_time == 0)
                    curr_job_t->start_time = sim_state->sim_time;
                zlist_append (running_jobs, curr_job_t);
            } else {
                reserved_job = curr_job;
            }
		}
        curr_job = zlist_next (queued_jobs);
    }

    //reserved job is now set, start backfilling
    if (reserved_job) {
        curr_lwj_job = zlist_first (r_queue);
        while (curr_lwj_job != NULL) {
            kvs_get_dir (h,  &curr_kvs_dir, "lwj.%d", curr_lwj_job->lwj_id);
            curr_job_t = pull_job_from_kvs (curr_kvs_dir);
            if (curr_job_t->start_time == 0)
                curr_job_t->start_time = sim_state->sim_time;
            zlist_append (running_jobs, curr_job_t);
            curr_lwj_job = zlist_next (r_queue);
        }
        backfill_jobs(rdl, uri, queued_jobs, running_jobs, reserved_job);
    }

    //Cleanup
    curr_job_t = zlist_pop (running_jobs);
    while (curr_job_t != NULL) {
        free_job (curr_job_t);
        curr_job_t = zlist_pop (running_jobs);
    }
    zlist_destroy (&running_jobs);
    zlist_destroy (&queued_jobs);
    return rc;
}


/****************************************************************
 *
 *         Actions Led by Current State + an Event
 *
 ****************************************************************/

static int
request_run (flux_lwj_t *job)
{
    int rc = -1;

    if (update_job_state (job, j_runrequest) < 0) {
        flux_log (h, LOG_ERR, "request_run failed to update job %ld to %s",
                  job->lwj_id, stab_rlookup (jobstate_tab, j_runrequest));
    } else if (kvs_commit (h) < 0) {
        flux_log (h, LOG_ERR, "kvs_commit error!");
    } else if (!in_sim && flux_event_send (h, NULL, "rexec.run.%ld", job->lwj_id) < 0) {
        flux_log (h, LOG_ERR, "request_run event send failed: %s",
                  strerror (errno));
    } else if (in_sim && flux_request_send (h, NULL, "sim_exec.run.%ld", job->lwj_id) < 0) {
        flux_log (h, LOG_ERR, "request_run request send failed: %s",
                  strerror (errno));
    } else {
        flux_log (h, LOG_DEBUG, "job %ld runrequest", job->lwj_id);
        rc = 0;
    }

    return rc;
}


static int
issue_res_event (flux_lwj_t *lwj)
{
    int rc = 0;
    flux_event_t *newev
        = (flux_event_t *) xzmalloc (sizeof (flux_event_t));

    // TODO: how to update the status of each entry as "free"
    // then destroy zlist_t without having to destroy
    // the elements
    // release lwj->resource

    newev->t = res_event;
    newev->ev.re = r_released;
    newev->lwj = lwj;

    if (zlist_append (ev_queue, newev) == -1) {
        flux_log (h, LOG_ERR,
                  "enqueuing an event failed");
        rc = -1;
        goto ret;
    }
    if (signal_event () == -1) {
        flux_log (h, LOG_ERR,
                  "signal the event-enqueued event ");
        rc = -1;
        goto ret;
    }

ret:
    return rc;
}

static int
move_to_r_queue (flux_lwj_t *lwj)
{
    zlist_remove (p_queue, lwj);
    return zlist_append (r_queue, lwj);
}

static int
move_to_c_queue (flux_lwj_t *lwj)
{
    zlist_remove (r_queue, lwj);
    return zlist_append (c_queue, lwj);
}


static int
action_j_event (flux_event_t *e)
{
    /* e->lwj->state is the current state
     * e->ev.je      is the new state
     */
    /*
	flux_log (h, LOG_DEBUG, "attempting job %ld state change from %s to %s",
              e->lwj->lwj_id, stab_rlookup (jobstate_tab, e->lwj->state),
                              stab_rlookup (jobstate_tab, e->ev.je));
	*/
    switch (e->lwj->state) {
    case j_null:
        if (e->ev.je != j_reserved) {
            goto bad_transition;
        }
        e->lwj->state = j_reserved;
        break;

    case j_reserved:
        if (e->ev.je != j_submitted) {
            goto bad_transition;
        }
        extract_lwjinfo (e->lwj);
        if (e->lwj->state != j_submitted) {
            flux_log (h, LOG_ERR,
                      "job %ld read state mismatch ", e->lwj->lwj_id);
            goto bad_transition;
        }
        e->lwj->state = j_unsched;
        schedule_jobs (global_rdl, global_rdl_resource, p_queue);
        break;

    case j_submitted:
        if (e->ev.je != j_allocated) {
            goto bad_transition;
        }
        e->lwj->state = j_allocated;
        request_run(e->lwj);
        break;

    case j_unsched:
        /* TODO */
        goto bad_transition;
        break;

    case j_pending:
        /* TODO */
        goto bad_transition;
        break;

    case j_allocated:
        if (e->ev.je != j_runrequest) {
            goto bad_transition;
        }
        e->lwj->state = j_runrequest;
		if (in_sim)
			queue_timer_change ("sim_exec");
        break;

    case j_runrequest:
        if (e->ev.je != j_starting) {
            goto bad_transition;
        }
        e->lwj->state = j_starting;
        break;

    case j_starting:
        if (e->ev.je != j_running) {
            goto bad_transition;
        }
        e->lwj->state = j_running;
        move_to_r_queue (e->lwj);
        break;

    case j_running:
        if (e->ev.je != j_complete) {
            goto bad_transition;
        }
        /* TODO move this to j_complete case once reaped is implemented */
        move_to_c_queue (e->lwj);
        unwatch_lwj (e->lwj);
        issue_res_event (e->lwj);
        break;

    case j_cancelled:
        /* TODO */
        goto bad_transition;
        break;

    case j_complete:
        if (e->ev.je != j_reaped) {
            goto bad_transition;
        }
//        move_to_c_queue (e->lwj);
        break;

    case j_reaped:
        if (e->ev.je != j_complete) {
            goto bad_transition;
        }
        e->lwj->state = j_reaped;
        break;

    default:
        flux_log (h, LOG_ERR, "job %ld unknown state %d",
                  e->lwj->lwj_id, e->lwj->state);
        break;
    }

    return 0;

bad_transition:
    flux_log (h, LOG_ERR, "job %ld bad state transition from %s to %s",
              e->lwj->lwj_id, stab_rlookup (jobstate_tab, e->lwj->state),
                              stab_rlookup (jobstate_tab, e->ev.je));
    return -1;
}


static int
action_r_event (flux_event_t *e)
{
    int rc = -1;

    if ((e->ev.re == r_released) || (e->ev.re == r_attempt)) {
        release_resources (global_rdl, global_rdl_resource, e->lwj);
        rc = 0;
    }

    return rc;
}


static int
action (flux_event_t *e)
{
    int rc = 0;

    switch (e->t) {
    case lwj_event:
        rc = action_j_event (e);
        break;

    case res_event:
        rc = action_r_event (e);
        break;

    default:
        flux_log (h, LOG_ERR, "unknown event type");
        break;
    }

    return rc;
}


/****************************************************************
 *
 *         Abstractions for KVS Callback Registeration
 *
 ****************************************************************/
static int
wait_for_lwj_init ()
{
    int rc = 0;
    kvsdir_t dir = NULL;

    if (kvs_watch_once_dir (h, &dir, "lwj") < 0) {
        flux_log (h, LOG_ERR, "wait_for_lwj_init: %s",
                  strerror (errno));
        rc = -1;
        goto ret;
    }

    flux_log (h, LOG_DEBUG, "wait_for_lwj_init %s",
              kvsdir_key(dir));

ret:
    if (dir)
        kvsdir_destroy (dir);
    return rc;
}


static int
reg_newlwj_hdlr (KVSSetInt64F *func)
{
    if (kvs_watch_int64 (h,"lwj.next-id", func, (void *) h) < 0) {
        flux_log (h, LOG_ERR, "watch lwj.next-id: %s",
                  strerror (errno));
        return -1;
    }
    flux_log (h, LOG_DEBUG, "registered lwj creation callback");

    return 0;
}


static int
reg_lwj_state_hdlr (const char *path, KVSSetStringF *func)
{
    int rc = 0;
    char *k = NULL;

    asprintf (&k, "%s.state", path);
    if (kvs_watch_string (h, k, func, (void *)h) < 0) {
        flux_log (h, LOG_ERR,
                  "watch a lwj state in %s: %s.",
                  k, strerror (errno));
        rc = -1;
        goto ret;
    }
    flux_log (h, LOG_DEBUG, "registered lwj %s.state change callback", path);

ret:
    free (k);
    return rc;
}


/****************************************************************
 *                KVS Watch Callback Functions
 ****************************************************************/
static void
lwjstate_cb (const char *key, const char *val, void *arg, int errnum)
{
    int64_t lwj_id;
    flux_lwj_t *j = NULL;
    lwj_event_e e;

    if (errnum > 0) {
        /* Ignore ENOENT.  It is expected when this cb is called right
         * after registration.
         */
        if (errnum != ENOENT) {
            flux_log (h, LOG_ERR, "lwjstate_cb key(%s), val(%s): %s",
                      key, val, strerror (errnum));
        }
        goto ret;
    }

    if (extract_lwjid (key, &lwj_id) == -1) {
        flux_log (h, LOG_ERR, "ill-formed key: %s", key);
        goto ret;
    }
    flux_log (h, LOG_DEBUG, "lwjstate_cb: %ld, %s", lwj_id, val);

    j = find_lwj (lwj_id);
    if (j) {
        e = stab_lookup (jobstate_tab, val);
        issue_lwj_event (e, j);
    } else
        flux_log (h, LOG_ERR, "lwjstate_cb: find_lwj %ld failed", lwj_id);

ret:
    return;
}

static void queue_kvs_cb (const char *key, const char *val, void *arg, int errnum)
{
	int key_len;
	int val_len;
	char *key_copy = NULL;
	char *val_copy = NULL;
	kvs_event_t *kvs_event = NULL;

	if (key != NULL){
		key_len = strlen (key) + 1;
		key_copy = (char *) malloc (sizeof (char) * key_len);
		strncpy (key_copy, key, key_len);
	}
	if (val != NULL){
		val_len = strlen (val) + 1;
		val_copy = (char *) malloc (sizeof (char) * val_len);
		strncpy (val_copy, val, val_len);
	}

	kvs_event = (kvs_event_t *) malloc (sizeof (kvs_event_t));
	kvs_event->errnum = errnum;
	kvs_event->key = key_copy;
	kvs_event->val = val_copy;
	flux_log (h, LOG_DEBUG, "Event queued - key: %s, val: %s", kvs_event->key, kvs_event->val);
	zlist_append (kvs_queue, kvs_event);
}

/*
//Compare two kvs events based on their state
//Return true if they should be swapped
//AKA item1 is further along than item2
static bool compare_kvs_events (void *item1, void *item2)
{
	int state1 = -1;
	int state2 = -1;
	int64_t id1 = -1;
	int64_t id2 = -1;
	kvs_event_t *event1 = (kvs_event_t *) item1;
	kvs_event_t *event2 = (kvs_event_t *) item2;

	if (event1->val != NULL)
		state1 = stab_lookup (jobstate_tab, event1->val);
	if (event2->val != NULL)
		state2 = stab_lookup (jobstate_tab, event2->val);

	if (state1 != state2)
		return state1 > state2;

	extract_lwjid (event1->key, &id1);
	extract_lwjid (event2->key, &id2);
	return id1 > id2;
}
*/

static void handle_kvs_queue ()
{
	kvs_event_t *kvs_event = NULL;
	//zlist_sort (kvs_queue, compare_kvs_events);
	while (zlist_size (kvs_queue) > 0){
		kvs_event = (kvs_event_t *) zlist_pop (kvs_queue);
		flux_log (h, LOG_DEBUG, "Event to be handled - key: %s, val: %s", kvs_event->key, kvs_event->val);
		lwjstate_cb (kvs_event->key, kvs_event->val, NULL, kvs_event->errnum);
		free (kvs_event->key);
		free (kvs_event->val);
		free (kvs_event);
	}
}

/* The val argument is for the *next* job id.  Hence, the job id of
 * the new job will be (val - 1).
 */
static void
newlwj_cb (const char *key, int64_t val, void *arg, int errnum)
{
    char path[MAX_STR_LEN];
    flux_lwj_t *j = NULL;

    if (errnum > 0) {
        /* Ignore ENOENT.  It is expected when this cb is called right
         * after registration.
         */
        if (errnum != ENOENT) {
            flux_log (h, LOG_ERR, "newlwj_cb key(%s), val(%ld): %s",
                      key, val, strerror (errnum));
            goto error;
        } else {
            flux_log (h, LOG_DEBUG, "newlwj_cb key(%s), val(%ld): %s",
                      key, val, strerror (errnum));
        }
        goto ret;
    } else if (val < 0) {
        flux_log (h, LOG_ERR, "newlwj_cb key(%s), val(%ld)", key, val);
        goto error;
    } else {
        flux_log (h, LOG_DEBUG, "newlwj_cb key(%s), val(%ld)", key, val);
    }

    if ( !(j = (flux_lwj_t *) xzmalloc (sizeof (flux_lwj_t))) ) {
        flux_log (h, LOG_ERR, "oom");
        goto error;
    }
	//j->lwj_id = val;
    j->lwj_id = val - 1;
    j->state = j_null;

    if (zlist_append (p_queue, j) == -1) {
        flux_log (h, LOG_ERR,
                  "appending a job to pending queue failed");
        goto error;
    }

    snprintf (path, MAX_STR_LEN, "lwj.%ld", j->lwj_id);
    if (reg_lwj_state_hdlr (path, (KVSSetStringF *) queue_kvs_cb) == -1) {
        flux_log (h, LOG_ERR,
                  "register lwj state change "
                  "handling callback: %s",
                  strerror (errno));
        goto error;
    }
ret:
    return;

error:
	flux_log (h, LOG_ERR, "newlwj_cb failed");
    if (j)
        free (j);

    return;
}

static int newlwj_rpc (flux_t h, int typemask, zmsg_t **zmsg, void *arg)
{
	JSON o;
	JSON o_resp;
	const char *key;
	char* tag;
	int64_t id;
	int rc = 0;

	if (flux_msg_decode (*zmsg, &tag, &o) < 0
		    || o == NULL
		    || !Jget_str (o, "key", &key)
		    || !Jget_int64 (o, "val", &id)) {
		flux_log (h, LOG_ERR, "%s: bad message", __FUNCTION__);
		Jput (o);
		rc = -1;
	}
	else {
		id = id + 1; //mimics the original kvs cb
		newlwj_cb (key, id, NULL, 0);
	}

	o_resp = Jnew ();
	Jadd_int (o_resp, "rc", rc);
	flux_respond (h, zmsg, o_resp);
	Jput (o_resp);

	if (o)
		Jput (o);
	zmsg_destroy (zmsg);

	return 0;
}

#if 0
static int
event_cb (flux_t h, int typemask, zmsg_t **zmsg, void *arg)
{
    flux_event_t *e = NULL;

    while ( (e = zlist_pop (ev_queue)) != NULL) {
        action (e);
        free (e);
    }

    zmsg_destroy (zmsg);

    return 0;
}

//Compare two events based on their state
//Return true if they should be swapped
static bool compare_events (void *item1, void *item2)
{
    return false;
	flux_event_t *event1 = (flux_event_t *) item1;
	flux_event_t *event2 = (flux_event_t *) item2;

	if (event1->t != event2->t) {
        return (event1->t != res_event);
    } else if (event1->t == lwj_event) {
        return event1->ev.je > event2->ev.je;
    } else if (event1->t == res_event) {
        return event1->ev.re > event2->ev.re;
    }
    return false;
}
#endif

static void handle_event_queue ()
{
    flux_event_t *e = NULL;
    bool resources_released = false;

    for (e = (flux_event_t *) zlist_pop (ev_queue);
         e != NULL;
         e = (flux_event_t *) zlist_pop (ev_queue))
    {
        if (e->t == res_event) {
            resources_released = !action (e) || resources_released;
        } else {
            action (e);
        }
        free (e);
        //zlist_sort(ev_queue, compare_events);
    }

    if (resources_released) {
        schedule_jobs (global_rdl, global_rdl_resource, p_queue);
    }

}

static int trigger_cb (flux_t h, int typemask, zmsg_t **zmsg, void *arg)
{
	JSON o;
	char *tag;
	clock_t start, diff;
	double seconds;

	if (flux_msg_decode (*zmsg, &tag, &o) < 0 || o == NULL){
		flux_log (h, LOG_ERR, "%s: bad message", __FUNCTION__);
		Jput (o);
		return -1;
	}

	flux_log (h, LOG_DEBUG, "Setting sim_state to new values");

	sim_state = json_to_sim_state (o);

	start = clock();

	handle_kvs_queue();
    handle_event_queue ();

	diff = clock() - start;
	seconds = ((double) diff) / CLOCKS_PER_SEC;
	sim_state->sim_time += seconds;

	handle_timer_queue();

	send_reply_request (h, sim_state);

    free_simstate (sim_state);
	Jput (o);
	zmsg_destroy (zmsg);
	return 0;
}

static int send_join_request(flux_t h)
{
	JSON o = Jnew ();
	Jadd_str (o, "mod_name", module_name);
	Jadd_int (o, "rank", flux_rank (h));
	Jadd_double (o, "next_event", -1);
	if (flux_request_send (h, o, "%s", "sim.join") < 0){
		Jput (o);
		return -1;
	}
	Jput (o);
	return 0;
}

//Received an event that a simulation is starting
static int start_cb (flux_t h, int typemask, zmsg_t **zmsg, void *arg)
{
	flux_log(h, LOG_DEBUG, "received a start event");
	if (send_join_request (h) < 0){
		flux_log (h, LOG_ERR, "submit module failed to register with sim module");
		return -1;
	}

	flux_log (h, LOG_DEBUG, "sent a join request");

	//Turn off normal functionality, switch to sim_mode
	in_sim = true;
	if (flux_event_unsubscribe (h, "sim.start") < 0){
		flux_log (h, LOG_ERR, "failed to unsubscribe from \"sim.start\"");
		return -1;
	} else {
		flux_log (h, LOG_DEBUG, "unsubscribed from \"sim.start\"");
	}
	if (flux_event_unsubscribe (h, "sim_sched.event") < 0){
		flux_log (h, LOG_ERR, "failed to unsubscribe from \"sim_sched.event\"");
		return -1;
	} else {
		flux_log (h, LOG_DEBUG, "unsubscribed from \"sim_sched.event\"");
	}

	//Cleanup
	zmsg_destroy (zmsg);

	return 0;
}

/****************************************************************
 *
 *        High Level Job and Resource Event Handlers
 *
 ****************************************************************/
static msghandler_t htab[] = {
    { FLUX_MSGTYPE_EVENT,   "sim.start",     start_cb },
    { FLUX_MSGTYPE_REQUEST, "sim_sched.trigger", trigger_cb },
    //{ FLUX_MSGTYPE_EVENT,   "sim_sched.event",   event_cb },
    { FLUX_MSGTYPE_REQUEST, "sim_sched.lwj-watch",  newlwj_rpc },
};

const int htablen = sizeof (htab) / sizeof (htab[0]);

int mod_main (flux_t p, zhash_t *args)
{
    int rc = 0;
    char *path;
    struct resource *r = NULL;

    h = p;
    if (flux_rank (h) != 0) {
        flux_log (h, LOG_ERR, "sim_ched module must only run on rank 0");
        rc = -1;
        goto ret;
    }
    flux_log (h, LOG_INFO, "sim_sched comms module starting");

    if (!(path = zhash_lookup (args, "rdl-conf"))) {
        flux_log (h, LOG_ERR, "rdl-conf argument is not set");
        rc = -1;
        goto ret;
    }

    if (!(global_rdllib = rdllib_open ()) || !(global_rdl = rdl_loadfile (global_rdllib, path))) {
        flux_log (h, LOG_ERR, "failed to load resources from %s: %s",
                  path, strerror (errno));
        rc = -1;
        goto ret;
    }

    if (!(global_rdl_resource = zhash_lookup (args, "rdl-resource"))) {
        flux_log (h, LOG_INFO, "using default rdl resource");
        global_rdl_resource = "default";
    }

    if ((r = rdl_resource_get (global_rdl, global_rdl_resource))) {
        flux_log (h, LOG_DEBUG, "setting up rdl resources");
        if (idlize_resources (r)) {
            flux_log (h, LOG_ERR, "failed to idlize %s: %s", global_rdl_resource,
                      strerror (errno));
            rc = -1;
            goto ret;
        }
        flux_log (h, LOG_DEBUG, "successfully set up rdl resources");
    } else {
        flux_log (h, LOG_ERR, "failed to get %s: %s", global_rdl_resource,
                  strerror (errno));
        rc = -1;
        goto ret;
    }

    p_queue = zlist_new ();
    r_queue = zlist_new ();
    c_queue = zlist_new ();
    ev_queue = zlist_new ();
	kvs_queue = zlist_new ();
	timer_queue = zlist_new ();
    if (!p_queue || !r_queue || !c_queue || !ev_queue) {
        flux_log (h, LOG_ERR,
                  "init for queues failed: %s",
                  strerror (errno));
        rc = -1;
        goto ret;
    }
	if (flux_event_subscribe (h, "sim.start") < 0){
        flux_log (h, LOG_ERR, "subscribing to event: %s", strerror (errno));
		return -1;
	}
    if (flux_event_subscribe (h, "sim_sched.event") < 0) {
        flux_log (h, LOG_ERR,
                  "subscribing to event: %s",
                  strerror (errno));
        rc = -1;
        goto ret;
    }
	if (flux_msghandler_addvec (h, htab, htablen, NULL) < 0) {
		flux_log (h, LOG_ERR, "flux_msghandler_add: %s", strerror (errno));
		return -1;
	}

	goto skip_for_sim;

    if (wait_for_lwj_init () == -1) {
        flux_log (h, LOG_ERR, "wait for lwj failed: %s",
                  strerror (errno));
        rc = -1;
        goto ret;
    }

    if (reg_newlwj_hdlr ((KVSSetInt64F*) newlwj_cb) == -1) {
        flux_log (h, LOG_ERR,
                  "register new lwj handling "
                  "callback: %s",
                  strerror (errno));
        rc = -1;
        goto ret;
    }

skip_for_sim:
	send_alive_request (h, module_name);
    flux_log (h, LOG_DEBUG, "sent alive request");

    if (flux_reactor_start (h) < 0) {
        flux_log (h, LOG_ERR,
                  "flux_reactor_start: %s",
                  strerror (errno));
        rc =  -1;
        goto ret;
    }

    zlist_destroy (&p_queue);
    zlist_destroy (&r_queue);
    zlist_destroy (&c_queue);
    zlist_destroy (&ev_queue);
    zlist_destroy (&kvs_queue);
    zlist_destroy (&timer_queue);

    rdllib_close(global_rdllib);

ret:
    return rc;
}

MOD_NAME ("sim_sched");

/*
 * vi:tabstop=4 shiftwidth=4 expandtab
 */
