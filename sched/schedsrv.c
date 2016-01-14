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

/*
 * schedsrv.c - scheduler frameowrk service comms module
 *
 * Update Log:
 *       Apr 12 2015 DHA: Code refactoring including JSC API integration
 *       May 24 2014 DHA: File created.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <libgen.h>
#include <czmq.h>
#include <json.h>
#include <dlfcn.h>
#include <stdbool.h>
#include <flux/core.h>

#include "src/common/libutil/jsonutil.h"
#include "src/common/libutil/log.h"
#include "src/common/libutil/xzmalloc.h"
#include "resrc.h"
#include "resrc_tree.h"
#include "resrc_reqst.h"
#include "schedsrv.h"
#include "../simulator/simulator.h"

#define DYNAMIC_SCHEDULING 0
#define ENABLE_TIMER_EVENT 0
#define SCHED_UNIMPL -1

#if ENABLE_TIMER_EVENT
static int timer_event_cb (flux_t h, void *arg);
#endif
static void res_event_cb (flux_t h, flux_msg_handler_t *w,
                          const flux_msg_t *msg, void *arg);
static int job_status_cb (JSON jcb, void *arg, int errnum);

static char *sim_kvs_path = "sim";
static char *rdl_kvs_path;

/******************************************************************************
 *                                                                            *
 *              Scheduler Framework Service Module Context                    *
 *                                                                            *
 ******************************************************************************/

typedef resrc_tree_list_t *(*find_f) (flux_t h, resrc_t *resrc,
                                      resrc_reqst_t *resrc_reqst);

typedef resrc_tree_list_t *(*sel_f) (flux_t h, resrc_tree_list_t *resrc_trees,
                                     resrc_reqst_t *resrc_reqst);

typedef struct sched_ops {
    void         *dso;                /* Scheduler plug-in DSO handle */
    find_f        find_resources;     /* func pointer to find resources */
    sel_f         select_resources;   /* func pointer to select resources */
} sched_ops_t;

typedef struct {
    JSON jcb;
    void *arg;
    int errnum;
} jsc_event_t;

typedef struct {
    flux_t h;
    void *arg;
} res_event_t;

typedef struct {
    bool in_sim;
    sim_state_t *sim_state;
    zlist_t *res_queue;
    zlist_t *jsc_queue;
    zlist_t *timer_queue;
} simctx_t;

typedef struct {
    resrc_t      *root_resrc;         /* resrc object pointing to the root */
    char         *root_uri;           /* Name of the root of the RDL hierachy */
    resources_t  *root_resrcs;
} rdlctx_t;

/* TODO: Implement prioritization function for p_queue */
typedef struct {
    flux_t        h;
    zlist_t      *p_queue;            /* Pending job priority queue */
    zlist_t      *r_queue;            /* Running job queue */
    zlist_t      *c_queue;            /* Complete/cancelled job queue */
    rdlctx_t      rctx;               /* RDL context */
    simctx_t      sctx;               /* simulator context */
    sched_ops_t   sops;               /* scheduler plugin operations */
    char         *backfill;
    char         *io;
} ssrvctx_t;

/******************************************************************************
 *                                                                            *
 *                                 Utilities                                  *
 *                                                                            *
 ******************************************************************************/
static void freectx (void *arg)
{
    ssrvctx_t *ctx = arg;
    zlist_destroy (&(ctx->p_queue));
    zlist_destroy (&(ctx->r_queue));
    zlist_destroy (&(ctx->c_queue));
    resrc_tree_destroy (resrc_phys_tree (ctx->rctx.root_resrc), true);
    free (ctx->backfill);
    free (ctx->io);
    free (ctx->rctx.root_uri);
    free (ctx->sctx.sim_state);
    zlist_destroy (&(ctx->sctx.res_queue));
    zlist_destroy (&(ctx->sctx.jsc_queue));
    zlist_destroy (&(ctx->sctx.timer_queue));
    dlclose (ctx->sops.dso);
}

static ssrvctx_t *getctx (flux_t h)
{
    ssrvctx_t *ctx = (ssrvctx_t *)flux_aux_get (h, "schedsrv");
    if (!ctx) {
        ctx = xzmalloc (sizeof (*ctx));
        ctx->h = h;
        if (!(ctx->p_queue = zlist_new ()))
            oom ();
        if (!(ctx->r_queue = zlist_new ()))
            oom ();
        if (!(ctx->c_queue = zlist_new ()))
            oom ();
        ctx->backfill = NULL;
        ctx->io = NULL;
        ctx->rctx.root_resrc = NULL;
        ctx->rctx.root_uri = NULL;
        ctx->sctx.in_sim = false;
        ctx->sctx.sim_state = NULL;
        ctx->sctx.res_queue = NULL;
        ctx->sctx.jsc_queue = NULL;
        ctx->sctx.timer_queue = NULL;
        ctx->sops.dso = NULL;
        ctx->sops.find_resources = NULL;
        ctx->sops.select_resources = NULL;
        flux_aux_set (h, "schedsrv", ctx, freectx);
    }
    return ctx;
}

static inline void get_jobid (JSON jcb, int64_t *jid)
{
    Jget_int64 (jcb, JSC_JOBID, jid);
}

static inline void get_states (JSON jcb, int64_t *os, int64_t *ns)
{
    JSON o;
    Jget_obj (jcb, JSC_STATE_PAIR, &o);
    Jget_int64 (o, JSC_STATE_PAIR_OSTATE, os);
    Jget_int64 (o, JSC_STATE_PAIR_NSTATE, ns);
}

static inline int fill_resource_req (flux_t h, flux_lwj_t *j)
{
    int rc = -1;
    int64_t nn = 0;
    int64_t nc = 0;
    int64_t walltime = 0;
    JSON jcb = NULL;
    JSON o = NULL;

    if (!j) goto done;

    j->req = (flux_res_t *) xzmalloc (sizeof (flux_res_t));
    if ((rc = jsc_query_jcb_obj (h, j->lwj_id, JSC_RDESC, &jcb)) != 0) {
        flux_log (h, LOG_ERR, "error in jsc_query_job.");
        goto jsc_error;
    }
    if (!Jget_obj (jcb, JSC_RDESC, &o)) goto done;
    if (!Jget_int64 (o, JSC_RDESC_NNODES, &nn)) goto done;
    if (!Jget_int64 (o, JSC_RDESC_NTASKS, &nc)) goto done;
    j->req->nnodes = (uint64_t) nn;
    j->req->ncores = (uint64_t) nc;
    if (!Jget_int64 (o, JSC_RDESC_WALLTIME, &walltime)) {
        j->req->walltime = (uint64_t) 3600;
    } else {
        j->req->walltime = (uint64_t) walltime;
    }

    char *kvs_key = xasprintf ("lwj.%"PRId64".io_rate", j->lwj_id);
    int64_t io_rate = 0;
    if (kvs_get_int64 (h, kvs_key, &io_rate)) {
        flux_log (h, LOG_ERR, "%s: err=%s", __FUNCTION__, strerror(errno));
        goto kvs_error;
    }
    j->req->io_rate = (uint64_t) io_rate;
    free (kvs_key);

done:
    rc = 0;
kvs_error:
    if (jcb) {
        Jput (jcb);
    }
jsc_error:
    return rc;
}

static int update_state (flux_t h, uint64_t jid, job_state_t os, job_state_t ns)
{
    int rc = -1;
    JSON jcb = Jnew ();
    JSON o = Jnew ();
    Jadd_int64 (o, JSC_STATE_PAIR_OSTATE, (int64_t) os);
    Jadd_int64 (o, JSC_STATE_PAIR_NSTATE , (int64_t) ns);
    /* don't want to use Jadd_obj because I want to transfer the ownership */
    json_object_object_add (jcb, JSC_STATE_PAIR, o);
    rc = jsc_update_jcb_obj (h, jid, JSC_STATE_PAIR, jcb);
    Jput (jcb);
    return rc;
}

static inline bool is_newjob (JSON jcb)
{
    int64_t os = 0, ns = 0;
    get_states (jcb, &os, &ns);
    return ((os == J_NULL) && (ns == J_NULL))? true : false;
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

static int append_to_pqueue (ssrvctx_t *ctx, JSON jcb)
{
    int rc = -1;
    int64_t jid = -1;;
    flux_lwj_t *job = NULL;

    get_jobid (jcb, &jid);
    if ( !(job = (flux_lwj_t *) xzmalloc (sizeof (*job))))
        oom ();

    job->lwj_id = jid;
    job->state = J_NULL;
    job->reserved = false;
    if (zlist_append (ctx->p_queue, job) != 0) {
        flux_log (ctx->h, LOG_ERR, "failed to append to pending job queue.");
        goto done;
    }
    rc = 0;
done:
    return rc;
}

static flux_lwj_t *q_find_job (ssrvctx_t *ctx, int64_t id)
{
    flux_lwj_t *j = NULL;
    /* NOTE: performance issue when we have
     * large numbers of jobs in the system?
     */
    for (j = zlist_first (ctx->p_queue); j; j = zlist_next (ctx->p_queue)) {
        if (j->lwj_id == id)
            return j;
    }
    for (j = zlist_first (ctx->r_queue); j; j = zlist_next (ctx->r_queue)) {
        if (j->lwj_id == id)
            return j;
    }
    for (j = zlist_first (ctx->c_queue); j; j = zlist_next (ctx->c_queue)) {
        if (j->lwj_id == id)
            return j;
    }
    return NULL;
}

static int q_move_to_rqueue (ssrvctx_t *ctx, flux_lwj_t *j)
{
    zlist_remove (ctx->p_queue, j);
    return zlist_append (ctx->r_queue, j);
}

static int q_move_to_cqueue (ssrvctx_t *ctx, flux_lwj_t *j)
{
    /* NOTE: performance issue? */
    // FIXME: no transition from pending queue to cqueue yet
    //zlist_remove (ctx->p_queue, j);
    zlist_remove (ctx->r_queue, j);
    return zlist_append (ctx->c_queue, j);
}

static flux_lwj_t *fetch_job_and_event (ssrvctx_t *ctx, JSON jcb,
                                        job_state_t *ns)
{
    int64_t jid = -1, os64 = 0, ns64 = 0;
    get_jobid (jcb, &jid);
    get_states (jcb, &os64, &ns64);
    *ns = (job_state_t) ns64;
    return q_find_job (ctx, jid);
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

/******************************************************************************
 *                                                                            *
 *                            Scheduler Plugin Loader                         *
 *                                                                            *
 ******************************************************************************/

static int resolve_functions (ssrvctx_t *ctx)
{
    int rc = -1;

    ctx->sops.find_resources = dlsym (ctx->sops.dso, "find_resources");
    if (!(ctx->sops.find_resources) || !(*(ctx->sops.find_resources))) {
        flux_log (ctx->h, LOG_ERR, "can't load find_resources: %s", dlerror ());
        goto done;
    }
    ctx->sops.select_resources = dlsym (ctx->sops.dso, "select_resources");
    if (!(ctx->sops.select_resources) || !(*(ctx->sops.select_resources))) {
        flux_log (ctx->h, LOG_ERR, "can't load select_resources: %s",
                  dlerror ());
        goto done;
    }
    rc = 0;

done:
    return rc;
}

static int load_sched_plugin (ssrvctx_t *ctx, const char *pin)
{
    int rc = -1;
    flux_t h = ctx->h;
    char *path = NULL;;
    char *searchpath = getenv ("FLUX_MODULE_PATH");

    if (!searchpath) {
        flux_log (h, LOG_ERR, "FLUX_MODULE_PATH not set");
        goto done;
    }
    if (!(path = flux_modfind (searchpath, pin))) {
        flux_log (h, LOG_ERR, "%s: not found in module search path %s",
                  pin, searchpath);
        goto done;
    }
    if (!(ctx->sops.dso = dlopen (path, RTLD_NOW | RTLD_LOCAL))) {
        flux_log (h, LOG_ERR, "failed to open sched plugin: %s",
                  dlerror ());
        goto done;
    }
    flux_log (h, LOG_DEBUG, "loaded: %s", pin);
    rc = resolve_functions (ctx);

done:
    return rc;
}


/******************************************************************************
 *                                                                            *
 *                   Setting Up RDL (RFC 4)                                   *
 *                                                                            *
 ******************************************************************************/

static void setup_rdl_lua (flux_t h)
{
    flux_log (h, LOG_DEBUG, "LUA_PATH %s", getenv ("LUA_PATH"));
    flux_log (h, LOG_DEBUG, "LUA_CPATH %s", getenv ("LUA_CPATH"));
}

static int load_rdl (ssrvctx_t *ctx, char *path, char *uri)
{
    int rc = -1;

    setup_rdl_lua (ctx->h);

    if (!path) {
        flux_log (ctx->h, LOG_ERR, "rdl-conf argument is not set");
        goto done;
    }
    if (uri)
        ctx->rctx.root_uri = uri;
    else
        ctx->rctx.root_uri = xstrdup ("default");

    if (!(ctx->rctx.root_resrc = resrc_generate_resources (path,
                                                            ctx->rctx.root_uri)))
        goto done;
    ctx->rctx.root_resrcs = resrc_new_resources_from_tree (resrc_phys_tree (ctx->rctx.root_resrc));
    flux_log (ctx->h, LOG_DEBUG, "loaded %s rdl resource from %s",
              ctx->rctx.root_uri, path);
    rc = 0;
done:
    return rc;
}

/******************************************************************************
 *                                                                            *
 *                         Simulator Specific Code                            *
 *                                                                            *
 ******************************************************************************/

/*
 * Simulator Helper Functions
 */

static void queue_timer_change (ssrvctx_t *ctx, const char *module)
{
    zlist_append (ctx->sctx.timer_queue, (void *)module);
}

// Set the timer for "module" to happen relatively soon
// If the mod is sim_exec, it shouldn't happen immediately
// because the scheduler still needs to transition through
// 3->4 states before the sim_exec module can actually "exec" a job
static void set_next_event (const char *module, sim_state_t *sim_state)
{
    double next_event;
    double *timer = zhash_lookup (sim_state->timers, module);
    next_event =
        sim_state->sim_time + ((!strcmp (module, "sim_exec")) ? .0001 : .00001);
    if (*timer > next_event || *timer < 0) {
        *timer = next_event;
    }
}

static void handle_timer_queue (ssrvctx_t *ctx, sim_state_t *sim_state)
{
    while (zlist_size (ctx->sctx.timer_queue) > 0)
        set_next_event (zlist_pop (ctx->sctx.timer_queue), sim_state);

#if ENABLE_TIMER_EVENT
    // Set scheduler loop to run in next occuring scheduler block
    double *this_timer = zhash_lookup (sim_state->timers, "sched");
    double next_schedule_block =
         sim_state->sim_time
        + (SCHED_INTERVAL - ((int)sim_state->sim_time % SCHED_INTERVAL));
    if ctx->run_schedule_loop &&
        ((next_schedule_block < *this_timer || *this_timer < 0)) {
        *this_timer = next_schedule_block;
    }
    flux_log (ctx->h,
              LOG_DEBUG,
              "run_sched_loop: %d, next_schedule_block: %f, this_timer: %f",
              ctx->run_schedule_loop,
              next_schedule_block,
              *this_timer);
#endif
}

static void handle_jsc_queue (ssrvctx_t *ctx)
{
    jsc_event_t *jsc_event = NULL;

    while (zlist_size (ctx->sctx.jsc_queue) > 0) {
        jsc_event = (jsc_event_t *)zlist_pop (ctx->sctx.jsc_queue);
        flux_log (ctx->h,
                  LOG_DEBUG,
                  "JscEvent being handled - JSON: %s, errnum: %d",
                  Jtostr (jsc_event->jcb),
                  jsc_event->errnum);
        job_status_cb (jsc_event->jcb, jsc_event->arg, jsc_event->errnum);
        Jput (jsc_event->jcb);
        free (jsc_event);
    }
}

static void handle_res_queue (ssrvctx_t *ctx)
{
    res_event_t *res_event = NULL;

    while (zlist_size (ctx->sctx.res_queue) > 0) {
        res_event = (res_event_t *)zlist_pop (ctx->sctx.res_queue);
        flux_log (ctx->h,
                  LOG_DEBUG,
                  "ResEvent being handled");
        res_event_cb (res_event->h, NULL, NULL, res_event->arg);
        free (res_event);
    }
}


/*
 * Simulator Callbacks
 */

static void start_cb (flux_t h,
                 flux_msg_handler_t *w,
                 const flux_msg_t *msg,
                 void *arg)
{
    flux_log (h, LOG_DEBUG, "received a start event");
    if (send_join_request (h, "sched", -1) < 0) {
        flux_log (h,
                  LOG_ERR,
                  "submit module failed to register with sim module");
        return;
    }
    flux_log (h, LOG_DEBUG, "sent a join request");

    if (flux_event_unsubscribe (h, "sim.start") < 0) {
        flux_log (h, LOG_ERR, "failed to unsubscribe from \"sim.start\"");
        return;
    } else {
        flux_log (h, LOG_DEBUG, "unsubscribed from \"sim.start\"");
    }

    return;
}

static int sim_job_status_cb (JSON jcb, void *arg, int errnum)
{
    ssrvctx_t *ctx = getctx ((flux_t)arg);
    jsc_event_t *event = (jsc_event_t*) malloc (sizeof (jsc_event_t));

    event->jcb = Jget (jcb);
    event->arg = arg;
    event->errnum = errnum;

    flux_log (ctx->h,
              LOG_DEBUG,
              "JscEvent being queued - JSON: %s, errnum: %d",
              Jtostr (event->jcb),
              event->errnum);
    zlist_append (ctx->sctx.jsc_queue, event);

    return 0;
}

static void sim_res_event_cb (flux_t h, flux_msg_handler_t *w,
                              const flux_msg_t *msg, void *arg) {
    ssrvctx_t *ctx = getctx ((flux_t)arg);
    res_event_t *event = (res_event_t*) malloc (sizeof (res_event_t));
    const char *topic = NULL;

    event->h = h;
    event->arg = arg;

    flux_msg_get_topic (msg, &topic);
    flux_log (ctx->h,
              LOG_DEBUG,
              "ResEvent being queued - topic: %s",
              topic);
    zlist_append (ctx->sctx.res_queue, event);
}

static void trigger_cb (flux_t h,
                 flux_msg_handler_t *w,
                 const flux_msg_t *msg,
                 void *arg)
{
    clock_t start, diff;
    double seconds;
    bool sched_loop;
    const char *json_str = NULL;
    JSON o = NULL;
    ssrvctx_t *ctx = getctx (h);

    if (flux_request_decode (msg, NULL, &json_str) < 0 || json_str == NULL
        || !(o = Jfromstr (json_str))) {
        flux_log (h, LOG_ERR, "%s: bad message", __FUNCTION__);
        Jput (o);
        return;
    }

    flux_log (h, LOG_DEBUG, "Setting sim_state to new values");
    ctx->sctx.sim_state = json_to_sim_state (o);

    start = clock ();

    handle_jsc_queue (ctx);
    handle_res_queue (ctx);

    sched_loop = true;

    diff = clock () - start;
    seconds = ((double)diff) / CLOCKS_PER_SEC;
    //ctx->sctx.sim_state->sim_time += seconds;
    if (sched_loop) {
        flux_log (h,
                  LOG_DEBUG,
                  "scheduler timer: events + loop took %f seconds",
                  seconds);
    } else {
        flux_log (h,
                  LOG_DEBUG,
                  "scheduler timer: events took %f seconds",
                  seconds);
    }

    handle_timer_queue (ctx, ctx->sctx.sim_state);

    send_reply_request (h, "sched", ctx->sctx.sim_state);

    free_simstate (ctx->sctx.sim_state);
    Jput (o);
}

/*
static void sim_rdl_cb (flux_t h, flux_msg_handler_t *w,
                              const flux_msg_t *msg, void *arg)
{
    ssrvctx_t *ctx = (ssrvctx_t *) arg;
    JSON o = Jnew ();

    Jadd_str (o, "rdl_kvs_path", rdl_kvs_path);

    msg = flux_msg_create (FLUX_MSGTYPE_REQUEST);
    flux_msg_set_topic (msg, "sched.rdl.reply");
    flux_msg_set_payload_json (msg, Jtostr (o));
    if (flux_send (h, msg, 0) < 0) {
        rc = -1;
    }

    flux_log (h, LOG_DEBUG, "%s: sent a reply request", __FUNCTION__);
    Jput (o);
    return rc;
}
*/

/*
 * Simulator Initialization Functions
 */

static struct flux_msg_handler_spec sim_htab[] = {
    {FLUX_MSGTYPE_EVENT, "sim.start", start_cb},
    {FLUX_MSGTYPE_REQUEST, "sched.trigger", trigger_cb},
    {FLUX_MSGTYPE_EVENT, "sched.res.*", sim_res_event_cb},
    FLUX_MSGHANDLER_TABLE_END,
};

static int reg_sim_events (ssrvctx_t *ctx)
{
    int rc = -1;
    flux_t h = ctx->h;

    if (flux_event_subscribe (ctx->h, "sim.start") < 0) {
        flux_log (ctx->h, LOG_ERR, "subscribing to event: %s", strerror (errno));
        goto done;
    }
    if (flux_event_subscribe (ctx->h, "sched.res.") < 0) {
        flux_log (ctx->h, LOG_ERR, "subscribing to event: %s", strerror (errno));
        goto done;
    }
    if (flux_msg_handler_addvec (ctx->h, sim_htab, (void *)h) < 0) {
        flux_log (ctx->h, LOG_ERR, "flux_msg_handler_addvec: %s", strerror (errno));
        goto done;
    }
    if (jsc_notify_status_obj (h, sim_job_status_cb, (void *)h) != 0) {
        flux_log (h, LOG_ERR, "error registering a job status change CB");
        goto done;
    }

    send_alive_request (ctx->h, "sched");

    rc = 0;
 done:
    return rc;
}

static int setup_sim (ssrvctx_t *ctx, char *sim_arg)
{
    int rc = -1;

    if (sim_arg == NULL || !strncmp (sim_arg, "false", 5)) {
        rc = 0;
        goto done;
    } else if (strncmp (sim_arg, "true", 4)) {
        flux_log (ctx->h, LOG_ERR, "unknown argument (%s) for sim option", sim_arg);
        goto done;
    } else {
        flux_log (ctx->h, LOG_DEBUG, "setting up sim in scheduler");
    }

    ctx->sctx.in_sim = true;
    ctx->sctx.sim_state = NULL;
    ctx->sctx.res_queue = zlist_new ();
    ctx->sctx.jsc_queue = zlist_new ();
    ctx->sctx.timer_queue = zlist_new ();

    // Put the rdl in the KVS
    JSON o = Jnew_ar ();
    resrc_tree_serialize (o, resrc_phys_tree (ctx->rctx.root_resrc));
    kvs_mkdir (ctx->h, sim_kvs_path);
    rdl_kvs_path = xasprintf ("%s.rdl", sim_kvs_path);
    kvs_put (ctx->h, rdl_kvs_path, Jtostr (o));
    Jput (o);

    rc = 0;
 done:
    return rc;
}

/******************************************************************************
 *                                                                            *
 *                     Scheduler Event Registeration                          *
 *                                                                            *
 ******************************************************************************/

static struct flux_msg_handler_spec htab[] = {
    { FLUX_MSGTYPE_EVENT,     "sched.res.*", res_event_cb},
    FLUX_MSGHANDLER_TABLE_END
};

/*
 * Register events, some of which CAN triger a scheduling loop iteration.
 * Currently,
 *    -  Resource event: invoke the schedule loop;
 *    -  Timer event: invoke the schedule loop;
 *    -  Job event (JSC notification): triggers actions based on FSM
 *          and some state changes trigger the schedule loop.
 */
static int inline reg_events (ssrvctx_t *ctx)
{
    int rc = 0;
    flux_t h = ctx->h;

    if (flux_event_subscribe (h, "sched.res.") < 0) {
        flux_log (h, LOG_ERR, "subscribing to event: %s", strerror (errno));
        rc = -1;
        goto done;
    }
    if (flux_msg_handler_addvec (h, htab, (void *)h) < 0) {
        flux_log (h, LOG_ERR,
                  "error registering resource event handler: %s",
                  strerror (errno));
        rc = -1;
        goto done;
    }
    /* TODO: we need a way to manage environment variables or
       configrations
    */
#if ENABLE_TIMER_EVENT
    if (flux_tmouthandler_add (h, 30000, false, timer_event_cb, (void *)h) < 0) {
        flux_log (h, LOG_ERR,
                  "error registering timer event CB: %s",
                  strerror (errno));
        rc = -1;
        goto done;
    }
#endif
    if (jsc_notify_status_obj (h, job_status_cb, (void *)h) != 0) {
        flux_log (h, LOG_ERR, "error registering a job status change CB");
        rc = -1;
        goto done;
    }

done:
    return rc;
}


/********************************************************************************
 *                                                                              *
 *            Task Program Execution Service Request (RFC 8)                    *
 *                                                                              *
 *******************************************************************************/

static void inline build_contain_1node_req (int64_t nc, JSON rarr)
{
    JSON e = Jnew ();
    JSON o = Jnew ();
    Jadd_int64 (o, JSC_RDL_ALLOC_CONTAINED_NCORES, nc);
    json_object_object_add (e, JSC_RDL_ALLOC_CONTAINED, o);
    json_object_array_add (rarr, e);
}

/*
 * Because the job's rdl should only contain what's allocated to the job,
 * this traverse the entire tree post-order walk
 */
static int build_contain_req (flux_t h, flux_lwj_t *job, JSON rarr)
{
    int rc = 0;
    int64_t n;

    for (n = 0; n < job->req->nnodes; n++) {
        build_contain_1node_req (job->req->corespernode, rarr);
    }

    return rc;
}


/*
 * Once the job gets allocated to its own copy of rdl, this
 *    1) serializes the rdl and sends it to TP exec service
 *    2) builds JSC_RDL_ALLOC JCB and sends it to TP exec service
 *    3) sends JCB state update with J_ALLOCATE
 */
static int req_tpexec_allocate (ssrvctx_t *ctx, flux_lwj_t *job)
{
    int rc = -1;
    flux_t h = ctx->h;
    JSON jcb = Jnew ();
    JSON arr = Jnew_ar ();
    JSON ro = Jnew_ar ();

    if (resrc_tree_list_serialize (ro, job->resrc_trees)) {
        flux_log (h, LOG_ERR, "%"PRId64" resource serialization failed: %s",
                  job->lwj_id, strerror (errno));
        goto done;
    }
    Jadd_obj (jcb, JSC_RDL, ro);
    if (jsc_update_jcb_obj (h, job->lwj_id, JSC_RDL, jcb) != 0) {
        flux_log (h, LOG_ERR, "error jsc udpate: %"PRId64" (%s)", job->lwj_id,
                  strerror (errno));
        goto done;
    }
    Jput (jcb);
    jcb = Jnew ();
    if (build_contain_req (h, job, arr) != 0) {
        flux_log (h, LOG_ERR, "error requesting containment for job");
        goto done;
    }
    json_object_object_add (jcb, JSC_RDL_ALLOC, arr);
    if (jsc_update_jcb_obj (h, job->lwj_id, JSC_RDL_ALLOC, jcb) != 0) {
        flux_log (h, LOG_ERR, "error updating jcb");
        goto done;
    }
    Jput (arr);
    if ((update_state (h, job->lwj_id, job->state, J_ALLOCATED)) != 0) {
        flux_log (h, LOG_ERR, "failed to update the state of job %"PRId64"",
                  job->lwj_id);
        goto done;
    }
    if (ctx->sctx.in_sim) {
        queue_timer_change (ctx, "sched");
    }

    rc = 0;

done:
    if (jcb)
        Jput (jcb);
    if (ro)
      Jput (ro);
    return rc;
}

#if DYNAMIC_SCHEDULING
static int req_tpexec_grow (flux_t h, flux_lwj_t *job)
{
    /* TODO: NOT IMPLEMENTED */
    /* This runtime grow service will grow the resource set of the job.
       The special non-elastic case will be to grow the resource from
       zero to the selected RDL
    */
    return SCHED_UNIMPL;
}

static int req_tpexec_shrink (flux_t h, flux_lwj_t *job)
{
    /* TODO: NOT IMPLEMENTED */
    return SCHED_UNIMPL;
}

static int req_tpexec_map (flux_t h, flux_lwj_t *job)
{
    /* TODO: NOT IMPLEMENTED */
    /* This runtime grow service will grow the resource set of the job.
       The special non-elastic case will be to grow the resource from
       zero to RDL
    */
    return SCHED_UNIMPL;
}
#endif

static int req_tpexec_exec (flux_t h, flux_lwj_t *job)
{
    char *topic = NULL;
    flux_msg_t *msg = NULL;
    ssrvctx_t *ctx = getctx (h);
    int rc = -1;

    if ((update_state (h, job->lwj_id, job->state, J_RUNREQUEST)) != 0) {
        flux_log (h, LOG_ERR, "failed to update the state of job %"PRId64"",
                  job->lwj_id);
        goto done;
    }

    if (ctx->sctx.in_sim) {
        /* Emulation mode */
        if (asprintf (&topic, "sim_exec.run.%"PRId64"", job->lwj_id) < 0) {
            flux_log (h, LOG_ERR, "%s: topic create failed: %s",
                      __FUNCTION__, strerror (errno));
        } else if (!(msg = flux_msg_create (FLUX_MSGTYPE_REQUEST))
                            || flux_msg_set_topic (msg, topic) < 0
                            || flux_send (h, msg, 0) < 0) {
            flux_log (h, LOG_ERR, "%s: request create failed: %s",
                      __FUNCTION__, strerror (errno));
        } else {
            queue_timer_change (ctx, "sim_exec");
            flux_log (h, LOG_DEBUG, "job %"PRId64" runrequest", job->lwj_id);
            rc = 0;
        }
    } else {
        /* Normal mode */
        if (asprintf (&topic, "wrexec.run.%"PRId64"", job->lwj_id) < 0) {
            flux_log (h, LOG_ERR, "%s: topic create failed: %s",
                      __FUNCTION__, strerror (errno));
        } else if (!(msg = flux_event_encode (topic, NULL))
                            || flux_send (h, msg, 0) < 0) {
            flux_log (h, LOG_ERR, "%s: event create failed: %s",
                      __FUNCTION__, strerror (errno));
        } else {
            flux_log (h, LOG_DEBUG, "job %"PRId64" runrequest", job->lwj_id);
            rc = 0;
        }
    }

 done:
    if (msg)
        flux_msg_destroy (msg);
    if (topic)
        free (topic);
    return rc;
}

static int req_tpexec_run (flux_t h, flux_lwj_t *job)
{
    /* TODO: wreckrun does not provide grow and map yet
     *   we will switch to the following sequence under the TP exec service
     *   that provides RFC 8.
     *
     *   req_tpexec_grow
     *   req_tpexec_map
     *   req_tpexec_exec
     */
    return req_tpexec_exec (h, job);
}


/********************************************************************************
 *                                                                              *
 *           Actions on Job/Res/Timer event including Scheduling Loop           *
 *                                                                              *
 *******************************************************************************/

static resrc_t* get_io_resrc (resrc_tree_t *resrc_tree)
{
    resrc_tree_t *child;
    resrc_t *child_resrc;

    if (!resrc_tree_num_children (resrc_tree)) {
        return NULL;
    }

    for (child = resrc_tree_list_first (resrc_tree_children (resrc_tree));
         child;
         child = resrc_tree_list_next (resrc_tree_children (resrc_tree))) {
        child_resrc = resrc_tree_resrc (child);
        if (is_io (resrc_type (child_resrc))) {
            return child_resrc;
        }
    }

    return NULL;
}

static void deallocate_io (resrc_tree_t *resrc_tree, flux_lwj_t *job)
{
    resrc_t *io_resrc = NULL;
    resrc_tree_t *curr_resrc_tree = NULL;
    int64_t jobid = job->lwj_id;
    int64_t size = job->req->io_rate;

    for (curr_resrc_tree = resrc_tree;
         curr_resrc_tree;
         curr_resrc_tree = resrc_tree_parent (curr_resrc_tree)) {
        io_resrc = get_io_resrc (curr_resrc_tree);
        if (!io_resrc) {
            printf ("%s: No IO resrc available for %s\n",
                    __FUNCTION__, resrc_path (resrc_tree_resrc (curr_resrc_tree)));
        } else {
            resrc_deallocate_resource (io_resrc, jobid, size);
        }
    }
}

static void deallocate_job_io (flux_lwj_t *job)
{
    resrc_tree_t *curr_resrc_tree = NULL;
    resrc_tree_t *converted_tree = NULL;

    for (curr_resrc_tree = resrc_tree_list_first (job->resrc_trees);
         curr_resrc_tree;
         curr_resrc_tree = resrc_tree_list_next (job->resrc_trees)) {
        converted_tree = resrc_phys_tree (resrc_tree_resrc (curr_resrc_tree));
        deallocate_io (converted_tree, job);
    }
}

static int allocate_io (ssrvctx_t *ctx, resrc_tree_t *resrc_tree, flux_lwj_t *job,
                         int64_t starttime, int64_t walltime)
{
    int rc = 0;
    resrc_t *io_resrc = NULL;
    resrc_tree_t *curr_resrc_tree = NULL;
    size_t size = job->req->io_rate;
    int64_t jobid = job->lwj_id;
    zlist_t *allocd = zlist_new ();

    for (curr_resrc_tree = resrc_tree;
         curr_resrc_tree && !rc;
         curr_resrc_tree = resrc_tree_parent (curr_resrc_tree)) {
        io_resrc = get_io_resrc (curr_resrc_tree);
        io_resrc = resrc_lookup (ctx->rctx.root_resrcs, resrc_path (io_resrc));
        /*
        char *tree_path = resrc_path (resrc_tree_resrc (curr_resrc_tree));
        size_t *allocsize;
        for (allocsize = zhash_first (resrc_allocs(io_resrc));
             allocsize;
             allocsize = zhash_next (resrc_allocs(io_resrc))) {
            printf ("At %s, with %d allocd\n", tree_path, (int)*allocsize);
        }
        */
        if (io_resrc) {
            resrc_stage_resrc (io_resrc, size);
            rc = resrc_allocate_resource (io_resrc, jobid, starttime, walltime);
            if (!rc) {
                zlist_append (allocd, io_resrc);
            } else if (!strcmp (resrc_path (io_resrc), "/pfs/io")) {
                rc = -2; // special return code to signal we failed on the PFS
            }
        } else {
            printf ("No io resrc under %s\n", resrc_path (resrc_tree_resrc (curr_resrc_tree)));
        }
    }

    if (rc) {
        for (io_resrc = zlist_first (allocd);
             io_resrc;
             io_resrc = zlist_next (allocd)) {
            resrc_deallocate_resource (io_resrc, jobid, size);
        }
    }

    zlist_destroy (&allocd);

    return rc;
}

int stage_io (resrc_tree_t *resrc_tree, flux_lwj_t *job)
{
    int rc = 0;
    resrc_t *io_resrc = NULL;
    resrc_tree_t *resrc_ancestor = NULL;

    if (!resrc_tree)
        return 0;

    io_resrc = get_io_resrc (resrc_tree);
    if (io_resrc)
        resrc_stage_resrc (io_resrc, job->req->io_rate);

    resrc_ancestor = resrc_tree_parent (resrc_tree);
    if (resrc_ancestor) {
        rc = rc | stage_io (resrc_ancestor, job);
    }

    return rc;
}

static int stage_tree (resrc_tree_t *resrc_tree, flux_lwj_t *job, bool stage_io_too)
{
    int rc = 0;
    if (resrc_tree) {

        // TODO: what to do about memory or other resources?
        resrc_t *resrc = resrc_tree_resrc (resrc_tree);
        char *type = resrc_type (resrc);
        if (is_core(type)) {
            resrc_stage_resrc (resrc, 1);
        } else if (is_node(type)) {
            resrc_stage_resrc (resrc, 1);
            if (false && stage_io_too) {
                rc = stage_io (resrc_tree, job);
            }
        }

        if (resrc_tree_num_children (resrc_tree)) {
            resrc_tree_t *child = resrc_tree_list_first (resrc_tree_children (resrc_tree));
            while (!rc && child) {
                rc = stage_tree (child, job, stage_io_too);
                child = resrc_tree_list_next (resrc_tree_children (resrc_tree));
            }
        }
    }
    return rc;
}

static int stage_tree_list (resrc_tree_list_t *selected_trees, flux_lwj_t *job,
                            bool stage_io)
{
    int rc = 0;
    resrc_tree_t *rt = resrc_tree_list_first (selected_trees);

    while (rt && !rc) {
        rc = stage_tree (rt, job, stage_io);
        rt = resrc_tree_list_next (selected_trees);
    }

    return rc;
}

static int unstage_tree (resrc_tree_t *resrc_tree)
{
    int rc = 0;
    if (resrc_tree) {

        // TODO: what to do about memory or other resources?
        resrc_t *resrc = resrc_tree_resrc (resrc_tree);
        resrc_unstage_resrc (resrc);

        if (resrc_tree_num_children (resrc_tree)) {
            resrc_tree_t *child = resrc_tree_list_first (resrc_tree_children (resrc_tree));
            while (!rc && child) {
                rc = unstage_tree (child);
                child = resrc_tree_list_next (resrc_tree_children (resrc_tree));
            }
        }
    }
    return rc;
}

static int unstage_tree_list (resrc_tree_list_t *selected_trees)
{
    int rc = 0;
    resrc_tree_t *rt = resrc_tree_list_first (selected_trees);

    while (rt && !rc) {
        rc = unstage_tree (rt);
        rt = resrc_tree_list_next (selected_trees);
    }

    return rc;
}

static void io_select_children (flux_t h, resrc_tree_list_t *found_children,
                             resrc_tree_t *parent_tree)
{
    resrc_tree_t *child_tree = resrc_tree_list_first (found_children);
    resrc_tree_t *new_child_tree = NULL;

    while (child_tree) {
        new_child_tree = resrc_tree_new (parent_tree, resrc_tree_resrc (child_tree));
        io_select_children (h, resrc_tree_children(child_tree), new_child_tree);
        child_tree = resrc_tree_list_next (found_children);
    }
}

static resrc_tree_list_t *io_select_resources (ssrvctx_t *ctx, flux_lwj_t *job,
                                               resrc_tree_list_t *found_trees,
                                               int64_t time_now)
{
    int rc = 0;
    resrc_tree_list_t *selected_res = NULL;
    resrc_tree_t *curr_tree = NULL;
    resrc_tree_t *new_tree = NULL;
    resrc_tree_t *phys_tree_ptr = NULL;
    resrc_t *resrc = NULL;
    int64_t num_nodes_found = 0;

    if (!resrc_tree_list_size (found_trees)) {
        flux_log (ctx->h, LOG_ERR, "%s: called with empty found trees", __FUNCTION__);
    }

    selected_res = resrc_tree_list_new ();
    for (curr_tree = resrc_tree_list_first (found_trees);
         curr_tree && num_nodes_found < job->req->nnodes;
         curr_tree = resrc_tree_list_next (found_trees)) {
        if (!is_node (resrc_type (resrc_tree_resrc (curr_tree)))) {
            flux_log (ctx->h, LOG_ERR, "Non-node found as tree root in %s", __FUNCTION__);
        }

        resrc = resrc_lookup (ctx->rctx.root_resrcs, resrc_path (resrc_tree_resrc (curr_tree)));
        phys_tree_ptr = resrc_phys_tree (resrc);
        rc = allocate_io (ctx, phys_tree_ptr, job, time_now, job->req->walltime);
        if (!rc) {
            num_nodes_found += 1;
            resrc = resrc_tree_resrc (curr_tree);
            new_tree = resrc_tree_new (NULL, resrc);
            io_select_children (ctx->h, resrc_tree_children (curr_tree), new_tree);
            resrc_tree_list_append (selected_res, new_tree);
        } else if (rc == -2) {
            break; // failed at PFS, no further searching will help
        }
    }

    if (num_nodes_found < job->req->nnodes) {
        for (curr_tree = resrc_tree_list_first (selected_res);
             curr_tree;
             curr_tree = resrc_tree_list_next (selected_res)) {
            phys_tree_ptr = resrc_phys_tree (resrc_tree_resrc (curr_tree));
            deallocate_io (phys_tree_ptr, job);
        }
    }


    return selected_res;
}

static int schedule_io_job (ssrvctx_t *ctx, flux_lwj_t *job, int64_t time_now)
{
    JSON child_core = NULL;
    JSON node_io = NULL;
    JSON child_node = NULL;
    JSON node_children = NULL;
    flux_t h = ctx->h;
    int rc = -1;
    int64_t ntrees = 0;
    resrc_reqst_t *resrc_reqst = NULL;
    resrc_tree_list_t *found_trees = NULL;
    resrc_tree_list_t *selected_trees = NULL;

    /*
     * Require at least one task per node, and
     * Assume (for now) one task per core.
     */
    job->req->nnodes = (job->req->nnodes ? job->req->nnodes : 1);
    if (job->req->ncores < job->req->nnodes)
        job->req->ncores = job->req->nnodes;
    job->req->corespernode = (job->req->ncores + job->req->nnodes - 1) /
        job->req->nnodes;

    resrc_t *pfs_io = resrc_lookup (ctx->rctx.root_resrcs, "/pfs/io");
    int64_t pfs_io_avail = (int64_t) resrc_available_at_time (pfs_io, time_now);
    if (pfs_io_avail < job->req->nnodes * job->req->io_rate) {
        goto done;
    }

    child_core = Jnew ();
    Jadd_str (child_core, "type", "core");
    Jadd_int (child_core, "req_qty", job->req->corespernode);
    Jadd_int64 (child_core, "starttime", time_now);
    Jadd_int64 (child_core, "endtime", time_now + job->req->walltime);

    node_io = Jnew ();
    Jadd_str (node_io, "type", "io");
    Jadd_int (node_io, "req_qty", 0);
    Jadd_int64 (node_io, "starttime", time_now);
    Jadd_int64 (node_io, "endtime", time_now + job->req->walltime);

    child_node = Jnew ();
    Jadd_str (child_node, "type", "node");
    Jadd_int (child_node, "req_qty", job->req->nnodes);
    Jadd_int64 (child_node, "starttime", time_now);
    Jadd_int64 (child_node, "endtime", time_now + job->req->walltime);

    node_children = Jnew_ar ();
    json_object_array_add (node_children, child_core);
    json_object_array_add (node_children, node_io);
    json_object_object_add (child_node, "req_children", node_children);

    resrc_reqst = resrc_reqst_from_json (child_node, NULL);
    Jput (child_node);
    if (!resrc_reqst)
        goto done;

    if ((found_trees = ctx->sops.find_resources (h, ctx->rctx.root_resrc,
                                                 resrc_reqst))) {
        ntrees = resrc_tree_list_size (found_trees);
        if ((ntrees < job->req->nnodes) && !job->reserve)
            goto done;

        flux_log (h, LOG_DEBUG, "%s: Found %"PRId64" trees for job %"PRId64"",
                  __FUNCTION__, ntrees, job->lwj_id);
        if ((selected_trees = io_select_resources (ctx, job, found_trees,
                                                   time_now))) {
            ntrees = resrc_tree_list_size (selected_trees);
            flux_log (h, LOG_DEBUG, "%s: Selected %"PRId64" trees for job %"PRId64"",
                      __FUNCTION__, ntrees, job->lwj_id);
            if (ntrees == job->req->nnodes) {
                stage_tree_list (selected_trees, job, true);
                if (resrc_tree_list_allocate (selected_trees, job->lwj_id,
                                              time_now, job->req->walltime) < 0)
                    {
                        resrc_tree_list_release (selected_trees, job->lwj_id);
                        unstage_tree_list (selected_trees);
                        goto done;
                    }
                /* Scheduler specific job transition */
                // TODO: handle this some other way (JSC?)
                job->starttime = time_now;
                job->state = J_SELECTED;
                job->resrc_trees = selected_trees;
                if (req_tpexec_allocate (ctx, job) != 0) {
                    flux_log (h, LOG_ERR,
                              "failed to request allocate for job %"PRId64"",
                              job->lwj_id);
                    goto done;
                }
                rc = 0;
            }
        }
    }
done:
    if (resrc_reqst)
        resrc_reqst_destroy (resrc_reqst);
    if (found_trees)
        resrc_tree_list_destroy (found_trees, false);

    return rc;
}

/*
 * schedule_job() searches through all of the idle resources to
 * satisfy a job's requirements.  If enough resources are found, it
 * proceeds to allocate those resources and update the kvs's lwj entry
 * in preparation for job execution.  If less resources
 * are found than the job requires, and if the job asks to reserve
 * resources, then those resources will be reserved.
 */
static int schedule_noio_job (ssrvctx_t *ctx, flux_lwj_t *job, int64_t time_now)
{
    JSON child_core = NULL;
    JSON child_node = NULL;
    JSON node_io = NULL;
    JSON node_children = NULL;
    flux_t h = ctx->h;
    int rc = -1;
    int64_t ntrees = 0;
    resrc_reqst_t *resrc_reqst = NULL;
    resrc_tree_list_t *found_trees = NULL;
    resrc_tree_list_t *selected_trees = NULL;

    /*
     * Require at least one task per node, and
     * Assume (for now) one task per core.
     */
    job->req->nnodes = (job->req->nnodes ? job->req->nnodes : 1);
    if (job->req->ncores < job->req->nnodes)
        job->req->ncores = job->req->nnodes;
    job->req->corespernode = (job->req->ncores + job->req->nnodes - 1) /
        job->req->nnodes;

    child_core = Jnew ();
    Jadd_str (child_core, "type", "core");
    Jadd_int (child_core, "req_qty", job->req->corespernode);
    Jadd_int64 (child_core, "starttime", time_now);
    Jadd_int64 (child_core, "endtime", time_now + job->req->walltime);

    node_io = Jnew ();
    Jadd_str (node_io, "type", "io");
    Jadd_int (node_io, "req_qty", 0);
    Jadd_int64 (node_io, "starttime", time_now);
    Jadd_int64 (node_io, "endtime", time_now + job->req->walltime);

    child_node = Jnew ();
    Jadd_str (child_node, "type", "node");
    Jadd_int (child_node, "req_qty", job->req->nnodes);
    Jadd_int64 (child_node, "starttime", time_now);
    Jadd_int64 (child_node, "endtime", time_now + job->req->walltime);

    node_children = Jnew_ar ();
    json_object_array_add (node_children, child_core);
    json_object_array_add (node_children, node_io);
    json_object_object_add (child_node, "req_children", node_children);

    resrc_reqst = resrc_reqst_from_json (child_node, NULL);
    Jput (child_node);
    if (!resrc_reqst)
        goto done;

    if ((found_trees = ctx->sops.find_resources (h, ctx->rctx.root_resrc,
                                                 resrc_reqst))) {
        ntrees = resrc_tree_list_size (found_trees);
        if ((ntrees < job->req->nnodes) && !job->reserve)
            goto done;

        //resources_t *resrc = resrc_new_resources_from_tree_list (found_trees);
        if ((selected_trees = ctx->sops.select_resources (h, found_trees,
                                                          resrc_reqst))) {
            ntrees = resrc_tree_list_size (selected_trees);
            if (ntrees == job->req->nnodes) {
                stage_tree_list (selected_trees, job, false);
                if (resrc_tree_list_allocate (selected_trees, job->lwj_id,
                                              time_now, job->req->walltime) < 0)
                    {
                        resrc_tree_list_release (selected_trees, job->lwj_id);
                        unstage_tree_list (selected_trees);
                        goto done;
                    }
                /* Scheduler specific job transition */
                // TODO: handle this some other way (JSC?)
                job->starttime = time_now;
                job->state = J_SELECTED;
                job->resrc_trees = selected_trees;
                if (req_tpexec_allocate (ctx, job) != 0) {
                    flux_log (h, LOG_ERR,
                              "failed to request allocate for job %"PRId64"",
                              job->lwj_id);
                    goto done;
                }
                rc = 0;
            } else if (job->reserve) {
                resrc_tree_list_reserve (selected_trees, job->lwj_id,
                                         time_now, job->req->walltime);
                rc = 0;
            }
        }
    }
done:
    if (resrc_reqst)
        resrc_reqst_destroy (resrc_reqst);
    if (found_trees)
        resrc_tree_list_destroy (found_trees, false);

    return rc;
}

int schedule_job (ssrvctx_t *ctx, flux_lwj_t *job, int64_t time_now)
{
    int rc = -1;

    resrc_t *pfs_io = resrc_lookup (ctx->rctx.root_resrcs, "/pfs/io");
    int64_t pfs_io_avail_before = (int64_t) resrc_available_at_time (pfs_io, time_now);

    if (ctx->io && !strncmp (ctx->io, "true", 5)) {
        rc = schedule_io_job (ctx, job, time_now);
    } else {
        rc = schedule_noio_job (ctx, job, time_now);
    }

    int64_t pfs_io_avail_after = (int64_t) resrc_available_at_time (pfs_io, time_now);
    flux_log (ctx->h, LOG_DEBUG, "%s (%"PRId64"): pfs_io_before: %"PRId64", was_scheduled: %s, pfs_io_after: %"PRId64"",
              __FUNCTION__, job->lwj_id, pfs_io_avail_before, rc == 0 ? "true" : "false", pfs_io_avail_after);

    return rc;
}

/*
 * reserve_job searches through all of the idle resources to satisfy
 * a job's requirements.  If enough resources are found, it proceeds
 * to reserve those resources.  No state transitions are
 * made. Returns 0 if the job was allocated, -1 otherwise.
 */
static int reserve_io_job (ssrvctx_t *ctx, flux_lwj_t *job, int64_t time_now)
{
    JSON child_core = NULL;
    JSON child_node = NULL;
    JSON node_io = NULL;
    JSON node_children = NULL;
    flux_t h = ctx->h;
    int rc = -1;
    int64_t nnodes = 0;
    resrc_reqst_t *resrc_reqst = NULL;
    resrc_tree_list_t *found_trees = NULL;
    resrc_tree_list_t *selected_trees = NULL;

    /*
     * Require at least one task per node, and
     * Assume (for now) one task per core.
     */
    job->req->nnodes = (job->req->nnodes ? job->req->nnodes : 1);
    if (job->req->ncores < job->req->nnodes)
        job->req->ncores = job->req->nnodes;
    job->req->corespernode = (job->req->ncores + job->req->nnodes - 1) /
        job->req->nnodes;

    child_core = Jnew ();
    Jadd_str (child_core, "type", "core");
    Jadd_int (child_core, "req_qty", job->req->corespernode);
    Jadd_int64 (child_core, "starttime", time_now);
    Jadd_int64 (child_core, "endtime", time_now + job->req->walltime);

    node_io = Jnew ();
    Jadd_str (node_io, "type", "io");
    Jadd_int (node_io, "req_qty", 0);
    Jadd_int64 (node_io, "starttime", time_now);
    Jadd_int64 (node_io, "endtime", time_now + job->req->walltime);

    child_node = Jnew ();
    Jadd_str (child_node, "type", "node");
    Jadd_int (child_node, "req_qty", job->req->nnodes);
    Jadd_int64 (child_node, "starttime", time_now);
    Jadd_int64 (child_node, "endtime", time_now + job->req->walltime);

    node_children = Jnew_ar ();
    json_object_array_add (node_children, child_core);
    json_object_array_add (node_children, node_io);
    json_object_object_add (child_node, "req_children", node_children);

    resrc_reqst = resrc_reqst_from_json (child_node, NULL);
    Jput (child_node);
    if (!resrc_reqst)
        goto done;

    if ((found_trees = ctx->sops.find_resources (h, ctx->rctx.root_resrc,
                                                 resrc_reqst))) {
        nnodes = resrc_tree_list_size (found_trees);
        flux_log (h, LOG_DEBUG, "%"PRId64" nodes found for lwj.%"PRId64", reqrd: %"PRId64"",
                  nnodes, job->lwj_id, job->req->nnodes);
        if ((nnodes < job->req->nnodes) && !job->reserve)
            goto done;

        if ((selected_trees = io_select_resources (ctx, job, found_trees,
                                                   time_now))) {
            nnodes = resrc_tree_list_size (selected_trees);
            if (nnodes == job->req->nnodes) {
                stage_tree_list (selected_trees, job, true);
                if (resrc_tree_list_allocate (selected_trees, job->lwj_id,
                                              time_now, job->req->walltime) < 0)
                    {
                        resrc_tree_list_release (selected_trees, job->lwj_id);
                        unstage_tree_list (selected_trees);
                        goto done;
                    }
                job->resrc_trees = selected_trees;
                rc = 0;
            }
        }
    }
done:
    if (resrc_reqst)
        resrc_reqst_destroy (resrc_reqst);
    if (found_trees)
        resrc_tree_list_destroy (found_trees, false);

    return rc;
}

/*
 * reserve_job searches through all of the idle resources to satisfy
 * a job's requirements.  If enough resources are found, it proceeds
 * to reserve those resources.  No state transitions are
 * made. Returns 0 if the job was allocated, -1 otherwise.
 */
static int reserve_noio_job (ssrvctx_t *ctx, flux_lwj_t *job, int64_t time_now)
{
    JSON child_core = NULL;
    JSON child_node = NULL;
    JSON node_io = NULL;
    JSON node_children = NULL;
    flux_t h = ctx->h;
    int rc = -1;
    int64_t nnodes = 0;
    resrc_reqst_t *resrc_reqst = NULL;
    resrc_tree_list_t *found_trees = NULL;
    resrc_tree_list_t *selected_trees = NULL;

    /*
     * Require at least one task per node, and
     * Assume (for now) one task per core.
     */
    job->req->nnodes = (job->req->nnodes ? job->req->nnodes : 1);
    if (job->req->ncores < job->req->nnodes)
        job->req->ncores = job->req->nnodes;
    job->req->corespernode = (job->req->ncores + job->req->nnodes - 1) /
        job->req->nnodes;

    child_core = Jnew ();
    Jadd_str (child_core, "type", "core");
    Jadd_int (child_core, "req_qty", job->req->corespernode);
    Jadd_int64 (child_core, "starttime", time_now);
    Jadd_int64 (child_core, "endtime", time_now + job->req->walltime);

    node_io = Jnew ();
    Jadd_str (node_io, "type", "io");
    Jadd_int (node_io, "req_qty", 0);
    Jadd_int64 (node_io, "starttime", time_now);
    Jadd_int64 (node_io, "endtime", time_now + job->req->walltime);

    child_node = Jnew ();
    Jadd_str (child_node, "type", "node");
    Jadd_int (child_node, "req_qty", job->req->nnodes);
    Jadd_int64 (child_node, "starttime", time_now);
    Jadd_int64 (child_node, "endtime", time_now + job->req->walltime);

    node_children = Jnew_ar ();
    json_object_array_add (node_children, child_core);
    json_object_array_add (node_children, node_io);
    json_object_object_add (child_node, "req_children", node_children);

    resrc_reqst = resrc_reqst_from_json (child_node, NULL);
    Jput (child_node);
    if (!resrc_reqst)
        goto done;

    if ((found_trees = ctx->sops.find_resources (h, ctx->rctx.root_resrc,
                                                 resrc_reqst))) {
        nnodes = resrc_tree_list_size (found_trees);
        flux_log (h, LOG_DEBUG, "%"PRId64" nodes found for lwj.%"PRId64", reqrd: %"PRId64"",
                  nnodes, job->lwj_id, job->req->nnodes);
        if ((nnodes < job->req->nnodes) && !job->reserve)
            goto done;

        if ((selected_trees = ctx->sops.select_resources (h, found_trees,
                                                          resrc_reqst))) {
            nnodes = resrc_tree_list_size (selected_trees);
            if (nnodes == job->req->nnodes) {
                stage_tree_list (selected_trees, job, false);
                if (resrc_tree_list_allocate (selected_trees, job->lwj_id,
                                              time_now, job->req->walltime) < 0)
                    {
                        resrc_tree_list_release (selected_trees, job->lwj_id);
                        unstage_tree_list (selected_trees);
                        goto done;
                    }
                job->resrc_trees = selected_trees;
                rc = 0;
            }
        }
    }
done:
    if (resrc_reqst)
        resrc_reqst_destroy (resrc_reqst);
    if (found_trees)
        resrc_tree_list_destroy (found_trees, false);

    return rc;
}

int reserve_job (ssrvctx_t *ctx, flux_lwj_t *job, int64_t time_now)
{
    if (ctx->io) {
        return reserve_io_job (ctx, job, time_now);
    } else {
        return reserve_noio_job (ctx, job, time_now);
    }
}

static int fcfs (ssrvctx_t *ctx, bool res_changed) {
    int rc = 0;
    flux_lwj_t *job = NULL;
    /* TODO: 1. we might need to invoke prioritize_qeueu here
       or making this as a continous operation by another thread
       or comms module.
       2. when dynamic scheduling is supported, the loop
       should traverse through running job queue as well.
    */
    zlist_t *jobs = ctx->p_queue;
    int64_t time_now = (ctx->sctx.in_sim) ? (int64_t) ctx->sctx.sim_state->sim_time : -1;

    if (zlist_size (jobs) > 1 && !res_changed) {
        return rc;
    }

    job = zlist_first (jobs);
    while (!rc && job) {
        if (job->state == J_SCHEDREQ) {
            rc = schedule_job (ctx, job, time_now);
        }
        job = (flux_lwj_t*)zlist_next (jobs);
    }

    return rc;
}

static int easy_backfill (ssrvctx_t *ctx, bool res_changed)
{
    int rc = 0;
    flux_lwj_t *job = NULL, *reserved_job = NULL;
    /* TODO: 1. we might need to invoke prioritize_qeueu here
       or making this as a continous operation by another thread
       or comms module.
       2. when dynamic scheduling is supported, the loop
       should traverse through running job queue as well.
    */
    zlist_t *jobs = ctx->p_queue;
    int64_t time_now = (ctx->sctx.in_sim) ? (int64_t) ctx->sctx.sim_state->sim_time : -1;

    if (!res_changed) {
      job = (flux_lwj_t*) zlist_last (ctx->p_queue);
      return schedule_job (ctx, job, time_now);
    }

    // Build completion events for all currently running jobs
    int64_t *completion_time = NULL;
    zlist_t *completion_times = zlist_new ();
    // Need our own cursor into running list
    zlist_t *running_jobs = zlist_dup (ctx->r_queue);
    for (job = zlist_first (running_jobs); job; job = zlist_next (running_jobs)) {
        completion_time = malloc (sizeof(int64_t));
        *completion_time = job->starttime + job->req->walltime;
        zlist_append (completion_times, completion_time);
        zlist_freefn (completion_times, completion_time, free, true);
    }
    zlist_destroy (&running_jobs);

    // If the job has resources reserved then release them
    // TODO: make this method of reserving cleaner
    for (job = zlist_first (jobs);
         !rc && job;
         job = (flux_lwj_t*)zlist_next (jobs))
    {
        if (job->reserved) {
            flux_log (ctx->h, LOG_DEBUG, "Releasing a set of reserved resources");
            job->reserved = false;
            if (job->resrc_trees) {
                //resrc_tree_release (resrc_phys_tree (ctx->rctx.root_resrc), job->lwj_id);
                deallocate_job_io (job);
                resrc_tree_list_release (job->resrc_trees, job->lwj_id);
                //resrc_tree_list_destroy (job->resrc_trees, false);
            }
        }
    }


    job = zlist_first (jobs);
    while (!rc && job) {
        if (job->state == J_SCHEDREQ) {
            rc = schedule_job (ctx, job, time_now);
            if (!rc) {
                flux_log (ctx->h, LOG_DEBUG, "Scheduled job %"PRId64"", job->lwj_id);
                // Insert completion time for newly scheduled job
                completion_time = malloc (sizeof(int64_t));
                *completion_time = time_now + job->req->walltime;
                zlist_append (completion_times, completion_time);
                zlist_freefn (completion_times, completion_time, free, true);
            }
        }
        if (!rc)
            job = (flux_lwj_t*)zlist_next (jobs);
    }

    if (!job) { //No jobs remaining to backfill
        return rc;
    }

    reserved_job = job;

    flux_log (ctx->h, LOG_DEBUG, "Job %"PRId64" is now the reserved job",
              job->lwj_id);
    // Find start time of reserved job and make a reservation
    zlist_sort (completion_times, compare_int64_ascending);
    rc = -1;
    int64_t prev_completion_time = -1;
    for (completion_time = zlist_first (completion_times);
         rc && completion_time;
         completion_time = zlist_next (completion_times))
    {
        // Don't test the same time multiple times
        if (prev_completion_time != *completion_time) {
            flux_log (ctx->h, LOG_DEBUG, "Attempting to reserve job %"PRId64" at time %"PRId64"",
                      job->lwj_id, (*completion_time) + 1);
            rc = reserve_job (ctx, reserved_job, (*completion_time) + 1);
        }
        prev_completion_time = *completion_time;
    }
    job->reserved = true;
    zlist_destroy (&completion_times);

    // Begin backfilling
    zlist_t *prev_attempts = zlist_new ();
    flux_lwj_t *curr_prev_attempt;
    bool will_fail = false;
    for (job = (flux_lwj_t*)zlist_next (jobs);
           job;
           job = (flux_lwj_t*)zlist_next (jobs))
    {
        will_fail = false;
        if (job->state == J_SCHEDREQ) {
            for (curr_prev_attempt = zlist_first (prev_attempts);
                 curr_prev_attempt && !will_fail;
                 curr_prev_attempt = zlist_next (prev_attempts)) {
                if (curr_prev_attempt->req->nnodes <= job->req->nnodes &&
                    curr_prev_attempt->req->io_rate <= job->req->io_rate &&
                    curr_prev_attempt->req->walltime <= job->req->walltime) {
                    will_fail = true;
                    flux_log (ctx->h, LOG_DEBUG,
                              "Skipping %"PRId64" (%"PRId64",%"PRId64",%"PRId64") since we already failed on %"PRId64" (%"PRId64",%"PRId64",%"PRId64")",
                              job->lwj_id, job->req->nnodes, job->req->io_rate, job->req->walltime,
                              curr_prev_attempt->lwj_id, curr_prev_attempt->req->nnodes,
                              curr_prev_attempt->req->io_rate, curr_prev_attempt->req->walltime);
                }
            }
            if (!will_fail) {
                flux_log (ctx->h, LOG_DEBUG, "Attempting to backfill job %"PRId64" (%"PRId64",%"PRId64",%"PRId64")",
                          job->lwj_id, job->req->nnodes, job->req->io_rate, job->req->walltime);
                rc = schedule_job (ctx, job, time_now);
                if (rc) {
                    zlist_append (prev_attempts, job);
                }
            }
        }
    }
    zlist_destroy (&prev_attempts);

    return rc;
}

static int schedule_jobs (ssrvctx_t *ctx, bool res_changed)
{
    int rc = -1;

    if (ctx->backfill && !strncmp(ctx->backfill, "easy", 5)) {
      rc = easy_backfill (ctx, res_changed);
    } else {
        rc = fcfs (ctx, res_changed);
    }

    return rc;
}


/********************************************************************************
 *                                                                              *
 *                        Scheduler Event Handling                              *
 *                                                                              *
 ********************************************************************************/

#define VERIFY(rc) if (!(rc)) {goto bad_transition;}
static inline bool trans (job_state_t ex, job_state_t n, job_state_t *o)
{
    if (ex == n) {
        *o = n;
        return true;
    }
    return false;
}

/*
static bool compare_int_strs (void *item1, void* item2)
{
    int int1 = atoi((char*)item1);
    int int2 = atoi((char*)item2);

    return int1 > int2;
}
*/

/*
 * Following is a state machine. action is invoked when an external job
 * state event is delivered. But in action, certain events are also generated,
 * some events are realized by falling through some of the case statements.
 */
static int action (ssrvctx_t *ctx, flux_lwj_t *job, job_state_t newstate)
{
    flux_t h = ctx->h;
    job_state_t oldstate = job->state;

    flux_log (h, LOG_DEBUG, "attempting job %"PRId64" state change from "
              "%s to %s", job->lwj_id, jsc_job_num2state (oldstate),
              jsc_job_num2state (newstate));

    switch (oldstate) {
    case J_NULL:
        VERIFY (trans (J_NULL, newstate, &(job->state))
                || trans (J_RESERVED, newstate, &(job->state)));
        break;
    case J_RESERVED:
        VERIFY (trans (J_SUBMITTED, newstate, &(job->state)));
        fill_resource_req (h, job);
        /* fall through for implicit event generation */
    case J_SUBMITTED:
        VERIFY (trans (J_PENDING, J_PENDING, &(job->state)));
        /* fall through for implicit event generation */
    case J_PENDING:
        VERIFY (trans (J_SCHEDREQ, J_SCHEDREQ, &(job->state)));
        schedule_jobs (ctx, false); /* includes request allocate if successful */
        break;
    case J_SCHEDREQ:
        /* A schedule reqeusted should not get an event. */
        /* SCHEDREQ -> SELECTED happens implicitly within schedule jobs */
        VERIFY (false);
        break;
    case J_SELECTED:
        VERIFY (trans (J_ALLOCATED, newstate, &(job->state)));
        req_tpexec_run (h, job);
        break;
    case J_ALLOCATED:
        VERIFY (trans (J_RUNREQUEST, newstate, &(job->state)));
        break;
    case J_RUNREQUEST:
        VERIFY (trans (J_STARTING, newstate, &(job->state)));
        break;
    case J_STARTING:
        VERIFY (trans (J_RUNNING, newstate, &(job->state)));
        q_move_to_rqueue (ctx, job);
        break;
    case J_RUNNING:
        VERIFY (trans (J_COMPLETE, newstate, &(job->state))
                || trans (J_CANCELLED, newstate, &(job->state)));
        q_move_to_cqueue (ctx, job);

        deallocate_job_io (job);

        if ((resrc_tree_list_release (job->resrc_trees, job->lwj_id))) {
            flux_log (h, LOG_ERR, "%s: failed to release resources for job "
                      "%"PRId64"", __FUNCTION__, job->lwj_id);
        } else {
            flux_msg_t *msg = flux_event_encode ("sched.res.freed", NULL);

            if (!msg || flux_send (h, msg, 0) < 0) {
                flux_log (h, LOG_ERR, "%s: error sending event: %s",
                          __FUNCTION__, strerror (errno));
            } else
                flux_msg_destroy (msg);
            queue_timer_change (ctx, "sched");
        }
    case J_CANCELLED:
      //VERIFY (trans (J_REAPED, newstate, &(job->state)));
        zlist_remove (ctx->c_queue, job);
        if (job->req)
            free (job->req);
        if (job->resrc_trees) {
          resrc_tree_list_destroy (job->resrc_trees, false);
        }
        free (job);
        break;
    case J_COMPLETE:
        VERIFY (trans (J_REAPED, newstate, &(job->state)));
        zlist_remove (ctx->c_queue, job);
        if (job->req)
            free (job->req);
        free (job);
        break;
    case J_REAPED:
    default:
        VERIFY (false);
        break;
    }
    return 0;

bad_transition:
    flux_log (h, LOG_ERR, "job %"PRId64" bad state transition from %s to %s",
              job->lwj_id, jsc_job_num2state (oldstate),
              jsc_job_num2state (newstate));
    return -1;
}

/* TODO: we probably need to abstract out resource status & control  API
 * For now, the only resource event is raised when a job releases its
 * RDL allocation.
 */
static void res_event_cb (flux_t h, flux_msg_handler_t *w,
                          const flux_msg_t *msg, void *arg)
{
  schedule_jobs (getctx ((flux_t)arg), true);
    return;
}

#if ENABLE_TIMER_EVENT
static int timer_event_cb (flux_t h, void *arg)
{
    //flux_log (h, LOG_ERR, "TIMER CALLED");
    schedule_jobs (getctx ((flux_t)arg));
    return 0;
}
#endif

static int job_status_cb (JSON jcb, void *arg, int errnum)
{
    ssrvctx_t *ctx = getctx ((flux_t)arg);
    flux_lwj_t *j = NULL;
    job_state_t ns = J_FOR_RENT;

    if (errnum > 0) {
        flux_log (ctx->h, LOG_ERR, "job_status_cb: errnum passed in");
        return -1;
    }
    if (is_newjob (jcb))
        append_to_pqueue (ctx, jcb);
    if ((j = fetch_job_and_event (ctx, jcb, &ns)) == NULL) {
        flux_log (ctx->h, LOG_ERR, "error fetching job and event");
        return -1;
    }
    Jput (jcb);
    return action (ctx, j, ns);
}

/******************************************************************************
 *                                                                            *
 *                     Scheduler Service Module Main                          *
 *                                                                            *
 ******************************************************************************/

int mod_main (flux_t h, int argc, char **argv)
{
    int rc = -1, i = 0;
    ssrvctx_t *ctx = NULL;
    char *schedplugin = NULL, *userplugin = NULL;
    char *uri = NULL, *path = NULL, *sim = NULL;

    if (!(ctx = getctx (h))) {
        flux_log (h, LOG_ERR, "can't find or allocate the context");
        goto done;
    }

    for (i = 0; i < argc; i++) {
        if (!strncmp ("rdl-conf=", argv[i], sizeof ("rdl-conf"))) {
            path = xstrdup (strstr (argv[i], "=") + 1);
        } else if (!strncmp ("rdl-resource=", argv[i], sizeof ("rdl-resource"))) {
            uri = xstrdup (strstr (argv[i], "=") + 1);
        } else if (!strncmp ("in-sim=", argv[i], sizeof ("in-sim"))) {
            sim = xstrdup (strstr (argv[i], "=") + 1);
        } else if (!strncmp ("plugin=", argv[i], sizeof ("plugin"))) {
            userplugin = xstrdup (strstr (argv[i], "=") + 1);
        } else if (!strncmp ("backfill=", argv[i], sizeof ("backfill"))) {
            ctx->backfill = xstrdup (strstr (argv[i], "=") + 1);
        } else if (!strncmp ("io=", argv[i], sizeof ("io"))) {
            ctx->io = xstrdup (strstr (argv[i], "=") + 1);
        } else {
            flux_log (ctx->h, LOG_ERR, "module load option %s invalid", argv[i]);
            goto done;
        }
    }

    if (userplugin == NULL) {
        schedplugin = "sched.plugin1";
    } else {
        schedplugin = userplugin;
    }

    if (flux_rank (h) != 0) {
        flux_log (h, LOG_ERR, "sched module must only run on rank 0");
        goto done;
    }
    flux_log (h, LOG_INFO, "sched comms module starting");
    if (load_sched_plugin (ctx, schedplugin) != 0) {
        flux_log (h, LOG_ERR, "failed to load scheduler plugin");
        goto done;
    }
    flux_log (h, LOG_INFO, "scheduler plugin loaded");
    if (load_rdl (ctx, path, uri) != 0) {
        flux_log (h, LOG_ERR, "failed to setup and load RDL");
        goto done;
    }
    flux_log (h, LOG_INFO, "RDL loaded");
    if (setup_sim (ctx, sim) != 0) {
        flux_log (h, LOG_INFO, "failed to setup sim");
    }
    if (ctx->sctx.in_sim) {
        if (reg_sim_events (ctx) != 0) {
            flux_log (h, LOG_ERR, "failed to reg events");
            goto done;
        }
        flux_log (h, LOG_INFO, "sim events registered");
    } else {
        if (reg_events (ctx) != 0) {
            flux_log (h, LOG_ERR, "failed to reg events");
            goto done;
        }
        flux_log (h, LOG_INFO, "events registered");
    }
    if (flux_reactor_start (h) < 0) {
        flux_log (h, LOG_ERR, "flux_reactor_start: %s", strerror (errno));
        rc =  -1;
        goto done;
    }
    rc = 0;

done:
    free (path);
    free (uri);
    free (sim);
    free (userplugin);
    return rc;
}

MOD_NAME ("sched");

/*
 * vi:tabstop=4 shiftwidth=4 expandtab
 */
