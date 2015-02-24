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

#include "src/common/libutil/jsonutil.h"
#include "src/common/libutil/log.h"
#include "src/common/libutil/shortjson.h"
#include "src/common/libutil/xzmalloc.h"
#include "rdl.h"
#include "scheduler.h"
#include "simulator.h"

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
static char* resource = NULL;
static const char* IDLETAG = "idle";
static const char* CORETYPE = "core";

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

static int unwatch_lwj (flux_lwj_t *job) {
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
    asprintf (&uri, "%s:%s", resource, rdl_resource_path (jr));
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
        flux_log (h, LOG_ERR, "deallocate_bandswith_helper failed to get %s", uri);
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

/*
 * Walk the tree, find the required resources and tag with the lwj_id
 * to which it is allocated.
 */
static bool
allocate_resources (struct resource *fr, struct rdl_accumulator *accum,
                    flux_lwj_t *job, zlist_t *ancestors)
{
    char *lwjtag = NULL;
    char *uri = NULL;
    const char *type = NULL;
    JSON o = NULL, o2 = NULL, o3 = NULL;
    struct resource *child, *curr;
    bool found = false;

    //TODO: check if this is necessary
    asprintf (&uri, "%s:%s", resource, rdl_resource_path (fr));
    curr = rdl_resource_get (global_rdl, uri);
    free (uri);

    o = rdl_resource_json (curr);
    Jget_str (o, "type", &type);
    asprintf (&lwjtag, "lwj.%ld", job->lwj_id);
    if (job->req.nnodes && (strcmp (type, "node") == 0)) {
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
			if (allocate_bandwidth (job, curr, ancestors)) {
              job->req.ncores--;
              job->alloc.ncores++;
              rdl_resource_tag (curr, lwjtag);
              rdl_resource_set_int (curr, "lwj", job->lwj_id);
              rdl_resource_delete_tag (curr, IDLETAG);
              if (rdl_accumulator_add (accum, curr) < 0) {
                  flux_log (h, LOG_ERR, "failed to allocate core: %s", Jtostr (o));
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
        found = allocate_resources (child, accum, job, ancestors);
        rdl_resource_destroy (child);
    }
    rdl_resource_iterator_reset (fr);
	zlist_pop (ancestors);
    rdl_resource_destroy (curr);

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

    asprintf (&uri, "%s:%s", resource, rdl_resource_path (jr));
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
    struct resource *jr = rdl_resource_get (job->rdl, resource);
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

static int64_t get_free_count (struct rdl *rdl, const char *uri, const char *type)
{
	JSON o;
	int64_t count = -1;
	int rc = -1;
	struct resource *fr = NULL;

	if ((fr = rdl_resource_get (rdl, uri)) == NULL) {
		flux_log (h, LOG_ERR, "failed to get found resources: %s", uri);
		return -1;
	}

	o = rdl_resource_aggregate_json (fr);
	if (o) {
		if (!Jget_int64(o, type, &count)) {
			flux_log (h, LOG_ERR, "schedule_job failed to get %s: %d",
					  type, rc);
			return -1;
		} else {
			flux_log (h, LOG_DEBUG, "schedule_job found %ld idle %ss", count, type);
		}
		Jput (o);
	}

    rdl_resource_destroy (fr);

	return count;
}

/*
 * schedule_job() searches through all of the idle resources (cores
 * right now) to satisfy a job's requirements.  If enough resources
 * are found, it proceeds to allocate those resources and update the
 * kvs's lwj entry in preparation for job execution. Returns 1 if the
 * job was succesfully scheduled, 0 if it was not, -1 if there was an
 * error.
 */
static int schedule_job (struct rdl *rdl, const char *uri, flux_lwj_t *job, bool clear_cache)
{
    //int64_t nodes = -1;
    int rc = 0;
    struct rdl_accumulator *a = NULL;
	zlist_t *ancestors = zlist_new ();

	//The "cache"
    static int64_t cores = -1;
    static struct rdl *frdl = NULL;            /* found rdl */
    static struct resource *fr = NULL;         /* found resource */
	static bool cache_valid = false;

    if (!job || !rdl || !uri) {
        flux_log (h, LOG_ERR, "schedule_job invalid arguments");
        goto ret;
    }

	flux_log (h, LOG_DEBUG, "schedule_job called on job %ld", job->lwj_id);

	//Cache results between schedule loops
	if (!cache_valid || clear_cache) {
		flux_log (h, LOG_DEBUG, "refreshing cache");
		frdl = get_free_subset (rdl, "core");
		if (frdl) {
			cores = get_free_count (frdl, uri, "core");
			fr = rdl_resource_get (frdl, uri);
		} else {
            flux_log (h, LOG_DEBUG, "get_free_subset returned nothing, setting cores = 0");
            cores = 0;
            fr = NULL;
        }
		cache_valid = true;
	}

	if (frdl && fr && cores > 0) {
		if (cores >= job->req.ncores) {
          //TODO: revert this in the deallocation/rollback
            int old_nnodes = job->req.nnodes;
            int old_ncores = job->req.ncores;
            int old_io_rate = job->req.io_rate;
            int old_alloc_nnodes = job->alloc.nnodes;
            int old_alloc_ncores = job->alloc.ncores;
			rdl_resource_iterator_reset (fr);
			a = rdl_accumulator_create (rdl);
			if (allocate_resources (fr, a, job, ancestors)) {
				flux_log (h, LOG_INFO, "scheduled job %ld", job->lwj_id);
				job->rdl = rdl_accumulator_copy (a);
				job->state = j_submitted;
				rc = update_job (job);
                if (rc == 0)
                    rc = 1;

				//Clear the "cache"
				cache_valid = false;
                rdl_resource_destroy (fr);
				fr = NULL;
                rdl_destroy (frdl);
				frdl = NULL;
				cores = -1;
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
                  release_resources (rdl, resource, job);
                  rdl_destroy (job->rdl);
                }
			}
			rdl_accumulator_destroy (a);
		} else {
            flux_log (h, LOG_DEBUG, "not enough available cores, skipping this job");
        }
	}

ret:
	//TODO: clear the list and free each element (or set freefn)
	zlist_destroy (&ancestors);
    return rc;
}


//Return true if job1 is expected to terminate after job2
static bool job_compare_termination_fn (void *item1, void *item2) {
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
;void calculate_shadow_info (flux_lwj_t *reserved_job, struct rdl *rdl, const char *uri, zlist_t *running_jobs, double *shadow_time, int64_t *free_cores) {
    job_t *curr_job_t;
    struct rdl *frdl = NULL;

    if (zlist_size (running_jobs) == 0) {
        flux_log (h, LOG_ERR, "No running jobs and reserved job still doesn't fit.");
        return;
    }

    frdl = get_free_subset (rdl, "core");
    if (frdl) {
        *free_cores = get_free_count (frdl, uri, "core");
        rdl_destroy (frdl);
    } else {
        flux_log (h, LOG_DEBUG, "get_free_subset returned nothing, setting free_cores = 0");
        *free_cores = 0;
    }

    zlist_sort(running_jobs, job_compare_termination_fn);

    curr_job_t = zlist_first (running_jobs);
    while ((*free_cores < reserved_job->req.ncores) && curr_job_t) {
        flux_log (h, LOG_DEBUG, "reserved_job_req_cores: %d, free cores: %ld, curr_job: %d, curr_job_ncores: %d", reserved_job->req.ncores, *free_cores, curr_job_t->id, curr_job_t->ncpus);
        *free_cores += curr_job_t->ncpus;
        *shadow_time = curr_job_t->start_time + curr_job_t->time_limit;
        curr_job_t = zlist_next (running_jobs);
    }

    //Subtract out the cores that will be used by reserved job
    *free_cores -= reserved_job->req.ncores;
}

//Determines if a job is eligible for backfilling or not
//Written as a series of if statements for clarity
bool job_backfill_eligible (flux_lwj_t *job, int64_t curr_free_cores, int64_t shadow_free_cores, double shadow_time) {
    bool terminates_before_shadow_time = ((sim_state->sim_time + job->req.walltime) < shadow_time);
    int64_t min_of_curr_shadow_cores = (curr_free_cores < shadow_free_cores) ? curr_free_cores : shadow_free_cores;

    flux_log (h, LOG_DEBUG, "backfill info - shadow_time: %f, curr_free_cores: %ld, shadow_free_cores: %ld, job_req_cores: %d", shadow_time, curr_free_cores, shadow_free_cores, job->req.ncores);
    flux_log (h, LOG_DEBUG, "backfill info - term_before_shadow_time: %d, min_of_curr_shadow_cores: %ld", terminates_before_shadow_time, min_of_curr_shadow_cores);

    if (terminates_before_shadow_time && curr_free_cores >= job->req.ncores) {
        //Job ends before reserved job starts, make sure we have enough cores currently to schedule it
        return true;
    } else if (!terminates_before_shadow_time && min_of_curr_shadow_cores >= job->req.ncores) {
        //Job ends after reserved job starts, make sure we will have
        //enough free cores at shadow time to not delay the reserved
        //job
        return true;
    } else {
        return false;
    }
}


bool job_compare_id_fn (void *item1, void *item2) {
    flux_lwj_t *job1, *job2;
    job1 = (flux_lwj_t *) item1;
    job2 = (flux_lwj_t *) item2;

    //TODO: switch this to use submit time
    return (job1->lwj_id > job2->lwj_id);
}

int schedule_jobs (struct rdl *rdl, const char *uri, zlist_t *jobs, bool resources_released)
{
    flux_lwj_t *curr_queued_job = NULL, *reserved_job = NULL, *curr_running_job = NULL;
    job_t *curr_job_t;
    kvsdir_t curr_kvs_dir = NULL;
    int rc = 0, job_scheduled = 1;
    bool clear_cache = resources_released;
    double shadow_time = -1;
    int64_t shadow_free_cores = -1, curr_free_cores = -1;
    struct rdl *frdl = NULL;
    zlist_t *queued_jobs = zlist_dup (jobs), *running_jobs = zlist_new ();

    zlist_sort(queued_jobs, job_compare_id_fn);

    //Schedule all jobs at the front of the queue
    curr_queued_job = zlist_first (queued_jobs);
    while (job_scheduled && curr_queued_job) {
		if (curr_queued_job->state == j_unsched) {
			job_scheduled = schedule_job(rdl, uri, curr_queued_job, clear_cache);
            if (job_scheduled) {
                if (kvs_get_dir (h,  &curr_kvs_dir, "lwj.%ld", curr_queued_job->lwj_id)) {
                    flux_log (h, LOG_ERR, "lwj.%ld kvsdir not found", curr_queued_job->lwj_id);
                } else {
                    curr_job_t = pull_job_from_kvs (curr_kvs_dir);
                    if (curr_job_t->start_time == 0)
                        curr_job_t->start_time = sim_state->sim_time;
                    zlist_append (running_jobs, curr_job_t);
                }
            } else {
                reserved_job = curr_queued_job;
            }
		}
        curr_queued_job = zlist_next (queued_jobs);
    }

    //reserved job is now set, start backfilling
    if (reserved_job && curr_queued_job) {
        curr_running_job = zlist_first (r_queue);
        while (curr_running_job != NULL) {
            if (kvs_get_dir (h,  &curr_kvs_dir, "lwj.%ld", curr_running_job->lwj_id)) {
                flux_log (h, LOG_ERR, "lwj.%ld kvsdir not found", curr_running_job->lwj_id);
            } else {
                curr_job_t = pull_job_from_kvs (curr_kvs_dir);
                if (curr_job_t->start_time == 0)
                    curr_job_t->start_time = sim_state->sim_time;
                zlist_append (running_jobs, curr_job_t);
            }
            curr_running_job = zlist_next (r_queue);
        }
        calculate_shadow_info (reserved_job, rdl, uri, running_jobs, &shadow_time, &shadow_free_cores);
        flux_log(h, LOG_DEBUG, "Job %ld has the reservation", reserved_job->lwj_id);
        flux_log(h, LOG_DEBUG, "Shadow info - shadow time: %f, shadow free cores: %ld", shadow_time, shadow_free_cores);

        frdl = get_free_subset (rdl, "core");
        if (frdl) {
            curr_free_cores = get_free_count (frdl, uri, "core");
            rdl_destroy (frdl);
        } else {
            flux_log (h, LOG_DEBUG, "get_free_subset returned nothing, setting curr_free_cores = 0");
            curr_free_cores = 0;
        }

        while (curr_queued_job && shadow_time >= 0 && curr_free_cores > 0) {
            if (curr_queued_job->state == j_unsched &&
                job_backfill_eligible (curr_queued_job, curr_free_cores, shadow_free_cores, shadow_time)) {
                flux_log(h, LOG_DEBUG, "Job %ld is eligible for backfilling. Attempting to schedule.", curr_queued_job->lwj_id);

                job_scheduled = schedule_job(rdl, uri, curr_queued_job, clear_cache);
                if (job_scheduled) {
                    curr_free_cores -= curr_queued_job->alloc.ncores;
                    if ((sim_state->sim_time + curr_queued_job->req.walltime) >= shadow_time) {
                        shadow_free_cores -= curr_queued_job->alloc.ncores;
                    }
                    flux_log(h, LOG_DEBUG, "Job %ld was backfilled.", curr_queued_job->lwj_id);
                } else  {
                    flux_log(h, LOG_DEBUG, "Job %ld was not backfillied", curr_queued_job->lwj_id);
                }
            } else {
                flux_log(h, LOG_DEBUG, "Job %ld is not eligible for backfilling, moving on to the next job.", curr_queued_job->lwj_id);
            }
            curr_queued_job = zlist_next (queued_jobs);
        }
    }

	flux_log (h, LOG_DEBUG, "Finished iterating over the queued_jobs list");
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
        schedule_jobs (global_rdl, resource, p_queue, false);
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
        release_resources (global_rdl, resource, e->lwj);
        schedule_jobs (global_rdl, resource, p_queue, true);
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
	event_cb (h, typemask, zmsg, arg);

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
    { FLUX_MSGTYPE_EVENT,   "sim_sched.event",   event_cb },
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

    if (!(resource = zhash_lookup (args, "rdl-resource"))) {
        flux_log (h, LOG_INFO, "using default rdl resource");
        resource = "default";
    }

    if ((r = rdl_resource_get (global_rdl, resource))) {
        flux_log (h, LOG_DEBUG, "setting up rdl resources");
        if (idlize_resources (r)) {
            flux_log (h, LOG_ERR, "failed to idlize %s: %s", resource,
                      strerror (errno));
            rc = -1;
            goto ret;
        }
        flux_log (h, LOG_DEBUG, "successfully set up rdl resources");
    } else {
        flux_log (h, LOG_ERR, "failed to get %s: %s", resource,
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