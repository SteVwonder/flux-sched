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
 * sched.c - scheduler framework service comms module
 */

#if HAVE_CONFIG_H
# include <config.h>
#endif
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <libgen.h>
#include <errno.h>
#include <libgen.h>
#include <czmq.h>
#include <dlfcn.h>
#include <stdbool.h>
#include <flux/core.h>

#include "src/common/libutil/log.h"
#include "src/common/libutil/shortjson.h"
#include "src/common/libutil/xzmalloc.h"
#include "resrc.h"
#include "resrc_tree.h"
#include "resrc_reqst.h"
#include "rs2rank.h"
#include "rsreader.h"
#include "scheduler.h"
#include "plugin.h"

#include "../simulator/simulator.h"

#define ENABLE_TIMER_EVENT 0
#define SCHED_UNIMPL -1

//#undef LOG_DEBUG
//#define LOG_DEBUG LOG_INFO


static void timer_event_cb (flux_reactor_t *r, flux_watcher_t *w,
                           int revents, void *arg);
static void res_event_cb (flux_t h, flux_msg_handler_t *w,
                          const flux_msg_t *msg, void *arg);
static int job_status_cb (const char *jcbstr, void *arg, int errnum);



static void req_qstat_cb (flux_t h, flux_msg_handler_t *w, const flux_msg_t *msg, void *arg);
static void req_alljobssubmitted_cb (flux_t h, flux_msg_handler_t *w, const flux_msg_t *msg, void *arg);
static void childbirth_cb (flux_t h, flux_msg_handler_t *w, const flux_msg_t *msg, void *arg);
static void req_death_cb (flux_t h, flux_msg_handler_t *w, const flux_msg_t *msg, void *arg);

static void req_addresources_cb (flux_t h, flux_msg_handler_t *w, const flux_msg_t *msg, void *arg);
static void req_askresources_cb (flux_t h, flux_msg_handler_t *w, const flux_msg_t *msg, void *arg);
static void req_cslack_cb (flux_t h, flux_msg_handler_t *w, const flux_msg_t *msg, void *arg);
static void req_cslack_invalid_cb (flux_t h, flux_msg_handler_t *w, const flux_msg_t *msg, void *arg);

flux_msg_t *init_msg = NULL;
static int jobcount = 0;
static int jobsubcount = 0;
char *resultsfolder = NULL;


/******************************************************************************
 *                                                                            *
 *              Scheduler Framework Service Module Context                    *
 *                                                                            *
 ******************************************************************************/

typedef struct {
    JSON          jcb;
    void         *arg;
    int           errnum;
} jsc_event_t;

typedef struct {
    flux_t        h;
    void         *arg;
} res_event_t;

typedef struct {
    flux_t h;
    void *arg;
    char *topic;
    flux_msg_t *msg;
} slack_event_t;

typedef struct {
    flux_t h;
    void *arg;
} timer_event_t;

typedef struct {
    bool in_sim;
    int     token;
    char  *module_name;        /* Sim module name */
    char  *next_prefix;
    char  *parent_id;
    sim_state_t *sim_state;
    zlist_t *res_queue;
    zlist_t *jsc_queue;
    zlist_t *timer_queue;
    zlist_t *slack_queue;
    int     cdeathcount;        /* number of children dying */
    int     ccompletecount;
    zlist_t *dying_jobs;
    flux_t  sim_h;
    char    *sim_uri;
} simctx_t;

typedef struct {
    resrc_t      *root_resrc;         /* resrc object pointing to the root */
    char         *root_uri;           /* Name of the root of the RDL hierachy */
    zhash_t      *resrc_id_hash;      /* All root resources hashed by id */
} rdlctx_t;

typedef struct {
    char         *path;
    char         *uri;
    char         *userplugin;
    bool          sim;
    bool          schedonce;          /* Use resources only once */
    bool          fail_on_error;      /* Fail immediately on error */
    int           verbosity;
    rsreader_t    r_mode;
    char         *module_name_prefix;
    char         *sim_uri;
    char         *resultsfolder;
    int64_t       my_job_id;
} ssrvarg_t;

typedef struct {
    slack_state_t slack_state;        /* My slack state */
    int64_t       resource_out_count; /* Number of resources given out */
    int           wait_until_return;  /* If 1, surplus = 0 until all own resources are returned */
    int64_t       jobid;
    int           psize;
    int           rsize;
} slackctx_t;

/* TODO: Implement prioritization function for p_queue */
typedef struct {
    flux_t        h;
    zlist_t      *p_queue;            /* Pending job priority queue */
    zlist_t      *r_queue;            /* Running job queue */
    zlist_t      *c_queue;            /* Complete/cancelled job queue */
    zlist_t      *i_queue;            /* Instance queue with running jobs */
    machs_t      *machs;              /* Helps resolve resources to ranks */
    ssrvarg_t     arg;                /* args passed to this module */
    rdlctx_t      rctx;               /* RDL context */
    simctx_t      sctx;               /* simulator context */
    struct sched_plugin_loader *loader; /* plugin loader */
    int64_t       my_job_id;          /* My job id in my parent */
    int64_t       my_walltime;        /* My walltime */
    char         *parent_uri;         /* My parent's URI */
    slackctx_t    slctx;              /* Slack info context */
    zlist_t      *prev_attempts;           /* Cached previous attempts at scheduling */
} ssrvctx_t;

static void set_next_event_slack_wrapper (ssrvctx_t *ctx, int jobid);
static void set_next_event_timer_wrapper (ssrvctx_t *ctx);
static int schedule_jobs (ssrvctx_t *ctx);

static char *my_simexec_module;

/******************************************************************************
 *                                                                            *
 *                                 Utilities                                  *
 *                                                                            *
 ******************************************************************************/

static inline void ssrvarg_init (ssrvarg_t *arg)
{
    arg->path = NULL;
    arg->uri = NULL;
    arg->userplugin = NULL;
    arg->sim = false;
    arg->schedonce = false;
    arg->fail_on_error = false;
    arg->verbosity = 0;
    arg->module_name_prefix = NULL;
    arg->sim_uri = NULL;
}

static inline void ssrvarg_free (ssrvarg_t *arg)
{
    if (arg->path)
        free (arg->path);
    if (arg->uri)
        free (arg->uri);
    if (arg->userplugin)
        free (arg->userplugin);
    if (arg->module_name_prefix)
        free (arg->module_name_prefix);
    if (arg->sim_uri)
        free (arg->sim_uri);
}

static inline int ssrvarg_process_args (int argc, char **argv, ssrvarg_t *a)
{
    int i = 0, rc = 0;
    char *schedonce = NULL;
    char *immediate = NULL;
    char *vlevel= NULL;
    char *sim = NULL;
    char *jobid_str = NULL;

    for (i = 0; i < argc; i++) {
        if (!strncmp ("rdl-conf=", argv[i], sizeof ("rdl-conf"))) {
            a->path = xstrdup (strstr (argv[i], "=") + 1);
        } else if (!strncmp ("sched-once=", argv[i], sizeof ("sched-once"))) {
            schedonce = xstrdup (strstr (argv[i], "=") + 1);
        } else if (!strncmp ("fail-on-error=", argv[i],
                    sizeof ("fail-on-error"))) {
            immediate = xstrdup (strstr (argv[i], "=") + 1);
        } else if (!strncmp ("verbosity=", argv[i], sizeof ("verbosity"))) {
            vlevel = xstrdup (strstr (argv[i], "=") + 1);
        } else if (!strncmp ("rdl-resource=", argv[i], sizeof ("rdl-resource"))) {
            a->uri = xstrdup (strstr (argv[i], "=") + 1);
        } else if (!strncmp ("in-sim=", argv[i], sizeof ("in-sim"))) {
            sim = xstrdup (strstr (argv[i], "=") + 1);
        } else if (!strncmp ("plugin=", argv[i], sizeof ("plugin"))) {
            a->userplugin = xstrdup (strstr (argv[i], "=") + 1);
        } else if (!strncmp ("jobid=", argv[i], sizeof ("jobid"))) {
            jobid_str = xstrdup (strstr (argv[i], "=") + 1);
        } else if (!strncmp ("prefix=", argv[i], sizeof ("prefix"))) {
            a->module_name_prefix = xstrdup (strstr (argv[i], "=") + 1);
        } else if (!strncmp ("sim_uri=", argv[i], sizeof ("sim_uri"))) {
            a->sim_uri = xstrdup (strstr (argv[i], "=") + 1);
        } else if (!strncmp ("resultsfolder=", argv[i], sizeof("resultsfolder"))) {
            a->resultsfolder = xstrdup (strstr (argv[i], "=") + 1);
        } else {
            rc = -1;
            errno = EINVAL;
            goto done;
        }
    }

    if (!(a->userplugin))
        a->userplugin = xstrdup ("sched.fcfs");

    if (sim && !strncmp (sim, "true", sizeof ("true"))) {
        a->sim = true;
        free (sim);
    }
    if (schedonce && !strncmp (schedonce, "true", sizeof ("true"))) {
        a->schedonce = true;
        free (schedonce);
    }
    if (immediate && !strncmp (immediate, "true", sizeof ("true"))) {
        a->fail_on_error = true;
        free (immediate);
    }
    if (vlevel) {
         a->verbosity = strtol(vlevel, (char **)NULL, 10);
         free (vlevel);
    }
    if (a->sim) {
        a->r_mode = RSREADER_RESRC_EMUL;
    } else if (a->path) {
        a->r_mode = RSREADER_RESRC;
    } else {
        a->r_mode = RSREADER_HWLOC;
    }
    if (jobid_str) {
        a->my_job_id = strtol (jobid_str, NULL, 10);
    }
done:
    return rc;
}

static void freectx (void *arg)
{
    ssrvctx_t *ctx = arg;
    zlist_destroy (&(ctx->p_queue));
    zlist_destroy (&(ctx->r_queue));
    zlist_destroy (&(ctx->c_queue));
    zlist_destroy (&(ctx->i_queue));
    rs2rank_tab_destroy (ctx->machs);
    ssrvarg_free (&(ctx->arg));
    resrc_resource_destroy (ctx->rctx.root_resrc);
    free (ctx->rctx.root_uri);
    free_simstate (ctx->sctx.sim_state);
    if (ctx->sctx.res_queue)
        zlist_destroy (&(ctx->sctx.res_queue));
    if (ctx->sctx.jsc_queue)
        zlist_destroy (&(ctx->sctx.jsc_queue));
    if (ctx->sctx.timer_queue)
        zlist_destroy (&(ctx->sctx.timer_queue));
    if (ctx->loader)
        sched_plugin_loader_destroy (ctx->loader);
    if (ctx->prev_attempts)
        zlist_destroy (&ctx->prev_attempts);
    free (ctx);
}

static ssrvctx_t *getctx (flux_t h)
{
    ssrvctx_t *ctx = (ssrvctx_t *)flux_aux_get (h, "sched");
    if (!ctx) {
        ctx = xzmalloc (sizeof (*ctx));
        ctx->h = h;
        if (!(ctx->p_queue = zlist_new ()))
            oom ();
        if (!(ctx->r_queue = zlist_new ()))
            oom ();
        if (!(ctx->c_queue = zlist_new ()))
            oom ();
        if (!(ctx->i_queue = zlist_new ()))
            oom ();
        if (!(ctx->machs = rs2rank_tab_new ()))
            oom ();
        ssrvarg_init (&(ctx->arg));
        flux_aux_set (h, "sched", ctx, freectx);

        ctx->loader = NULL;

        ctx->rctx.root_resrc = NULL;
        ctx->rctx.root_uri = NULL;
        ctx->rctx.resrc_id_hash = zhash_new ();

        ctx->sctx.in_sim = false;
        ctx->sctx.sim_state = NULL;
        ctx->sctx.res_queue = NULL;
        ctx->sctx.jsc_queue = NULL;
        ctx->sctx.timer_queue = NULL;
        ctx->sctx.slack_queue = NULL;

        ctx->slctx.slack_state = false;
        ctx->slctx.jobid = 0;
        ctx->slctx.psize = 0;
        ctx->slctx.rsize = 0;
        ctx->slctx.resource_out_count = 0;

        ctx->my_job_id = 0;
        ctx->my_walltime = 0;
        ctx->parent_uri = NULL;

        ctx->prev_attempts = zlist_new ();
    }
    return ctx;
}

static inline void get_jobid (JSON jcb, int64_t *jid)
{
    Jget_int64 (jcb, JSC_JOBID, jid);
}

static inline void get_states (JSON jcb, int64_t *os, int64_t *ns)
{
    JSON o = NULL;
    Jget_obj (jcb, JSC_STATE_PAIR, &o);
    Jget_int64 (o, JSC_STATE_PAIR_OSTATE, os);
    Jget_int64 (o, JSC_STATE_PAIR_NSTATE, ns);
}

static inline void get_walltime (flux_t h, flux_lwj_t *j, int64_t *walltime)
{
    kvsdir_t *dir;
    *walltime = INT64_MAX;

    if (kvs_get_dir (h, &dir, "lwj.%lu", j->lwj_id) < 0) {
        flux_log (h, LOG_ERR, "Couldn't get job kvs dir. getting walltime failed");
        return;
    }

    if (kvsdir_get_int64 (dir, "walltime", walltime) < 0) {
        flux_log (h, LOG_DEBUG, ": No walltime set");
        return;
    }

    if (*walltime == 0)
        *walltime = INT64_MAX;

}

static inline int fill_resource_req (flux_t h, flux_lwj_t *j)
{
    char *jcbstr = NULL;
    int rc = -1;
    int64_t nn = 0;
    int64_t nc = 0;
    int64_t walltime = 0;
    JSON jcb = NULL;
    JSON o = NULL;

    if (!j) goto done;

    j->req = (flux_res_t *) xzmalloc (sizeof (flux_res_t));
    if ((rc = jsc_query_jcb (h, j->lwj_id, JSC_RDESC, &jcbstr)) != 0) {
        flux_log (h, LOG_ERR, "error in jsc_query_jcb.");
        goto done;
    }
    if (!(jcb = Jfromstr (jcbstr))) {
        flux_log (h, LOG_ERR, "fill_resource_req: error parsing JSON string");
        goto done;
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
    j->req->node_exclusive = false;
    rc = 0;
done:
    if (jcb)
        Jput (jcb);
    return rc;
}


static inline int fill_hierarchical_req (flux_t h, flux_lwj_t *j)
{
    //ssrvctx_t *ctx = getctx (h);
    int rc = -1;
    char *cmdline_str;
    kvsdir_t *dir;

    if (kvs_get_dir (h, &dir, "lwj.%lu", j->lwj_id) < 0) {
        flux_log (h, LOG_ERR, "Couldn't get job kvs dir. Hierarchy addons failed");
        goto done;
    }

    kvsdir_get (dir, "cmdline", &cmdline_str);
    flux_log (h, LOG_DEBUG, "cmdline from sched = %s", cmdline_str);


    if (kvsdir_get_int (dir, "is_hierarchical", &(j->is_hierarchical)) < 0) {
        flux_log (h, LOG_DEBUG, "Failed to find \"is_hierarchical\" entry in kvs");
        j->is_hierarchical = 0;
    }

    if (j->is_hierarchical) {
        flux_log (h, LOG_DEBUG, "Job is hierarchical");
    } else {
        flux_log (h, LOG_DEBUG, "Job is NOT hierarchical");
    }

    /* Set job slack state as false */
    j->slackinfo = (flux_slackinfo_t*) malloc (sizeof(flux_slackinfo_t));
    j->slackinfo->slack_state = false;
    j->slackinfo->need = NULL;

done:
    return rc;
}

static int update_state (flux_t h, uint64_t jid, job_state_t os, job_state_t ns)
{
    const char *jcbstr = NULL;
    int rc = -1;
    JSON jcb = Jnew ();
    JSON o = Jnew ();
    Jadd_int64 (o, JSC_STATE_PAIR_OSTATE, (int64_t) os);
    Jadd_int64 (o, JSC_STATE_PAIR_NSTATE , (int64_t) ns);
    /* don't want to use Jadd_obj because I want to transfer the ownership */
    json_object_object_add (jcb, JSC_STATE_PAIR, o);
    jcbstr = Jtostr (jcb);
    rc = jsc_update_jcb (h, jid, JSC_STATE_PAIR, jcbstr);
    Jput (jcb);
    return rc;
}

static inline bool is_newjob (JSON jcb)
{
    int64_t os = J_NULL, ns = J_NULL;
    get_states (jcb, &os, &ns);
    return ((os == J_NULL) && (ns == J_NULL))? true : false;
}

/********************************************************************************
 *                                                                              *
 *                          Simple Job Queue Methods                            *
 *                                                                              *
 *******************************************************************************/

static int append_to_pqueue (ssrvctx_t *ctx, JSON jcb)
{
    int rc = -1;
    int64_t jid = -1;
    flux_lwj_t *job = NULL;

    get_jobid (jcb, &jid);
    if ( !(job = (flux_lwj_t *) xzmalloc (sizeof (*job))))
        oom ();

    job->lwj_id = jid;
    job->state = J_NULL;
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
    for (j = zlist_first (ctx->p_queue); j; j = zlist_next (ctx->p_queue))
        if (j->lwj_id == id)
            return j;
    for (j = zlist_first (ctx->r_queue); j; j = zlist_next (ctx->r_queue))
        if (j->lwj_id == id)
            return j;
    for (j = zlist_first (ctx->c_queue); j; j = zlist_next (ctx->c_queue))
        if (j->lwj_id == id)
            return j;
    return NULL;
}

static flux_lwj_t* find_job_in_queue (zlist_t *queue, int64_t jobid)
{
    flux_lwj_t *job = NULL;
    job = zlist_first (queue);
    while (job) {
        if (job->lwj_id == jobid)
            break;
        job = zlist_next (queue);
    }
    return job;
}

static int q_move_to_rqueue (ssrvctx_t *ctx, flux_lwj_t *j)
{
    zlist_remove (ctx->p_queue, j);
    if (j->is_hierarchical)
        zlist_append (ctx->i_queue, j);
    return zlist_append (ctx->r_queue, j);
}

static int q_move_to_cqueue (ssrvctx_t *ctx, flux_lwj_t *j)
{
    /* NOTE: performance issue? */
    // FIXME: no transition from pending queue to cqueue yet
    //zlist_remove (ctx->p_queue, j);
    zlist_remove (ctx->r_queue, j);
    if (j->is_hierarchical)
        zlist_remove (ctx->i_queue, j);
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

static void retrieve_queue_info (ssrvctx_t *ctx, int64_t *jobid, int *psize, int *rsize)
{
    flux_lwj_t *job;
    *psize = zlist_size (ctx->p_queue);
    *rsize = zlist_size (ctx->r_queue);
    if (*psize) {
        job = zlist_first (ctx->p_queue);
        *jobid = job->lwj_id;
    } else {
        *jobid = 0;
    }
    return;
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

/* Block until value of 'key' becomes non-NULL.
 * It is an EPROTO error if value is type other than json_type_string.
 * On success returns value, otherwise NULL with errno set.
 */
static json_object *get_string_blocking (flux_t h, const char *key)
{
    char *json_str = NULL; /* initial value for watch */
    json_object *o = NULL;
    int saved_errno;

    if (kvs_watch_once (h, key, &json_str) < 0) {
        saved_errno = errno;
        goto error;
    }
    if (!json_str || !(o = json_tokener_parse (json_str))
                  || json_object_get_type (o) != json_type_string) {
        saved_errno = EPROTO;
        goto error;
    }
    free (json_str);
    return o;
error:
    if (json_str)
        free (json_str);
    if (o)
        json_object_put (o);
    errno = saved_errno;
    return NULL;
}

static int build_hwloc_rs2rank (ssrvctx_t *ctx, rsreader_t r_mode)
{
    int rc = -1;
    uint32_t rank = 0, size = 0;

    if (flux_get_size (ctx->h, &size) == -1) {
        flux_log_error (ctx->h, "flux_get_size");
        goto done;
    }
    for (rank=0; rank < size; rank++) {
        json_object *o;
        char k[64];
        int n = snprintf (k, sizeof (k), "resource.hwloc.xml.%"PRIu32"", rank);
        assert (n < sizeof (k));
        if (!(o = get_string_blocking (ctx->h, k))) {
            flux_log_error (ctx->h, "kvs_get %s", k);
            goto done;
        }
        const char *s = json_object_get_string (o);
        char *err_str = NULL;
        size_t len = strlen (s);
        if (rsreader_hwloc_load (s, len, rank, r_mode, &(ctx->rctx.root_resrc),
                                 ctx->machs, &err_str)) {
            json_object_put (o);
            flux_log_error (ctx->h, "can't load hwloc data: %s", err_str);
            free (err_str);
            goto done;
        }
        json_object_put (o);
    }
    rc = 0;

done:
    return rc;
}

static int load_rdl_from_parent_kvs (ssrvctx_t *ctx)
{
    int rc = -1;
    flux_t h = ctx->h;
    char *rdlstr;
    kvsdir_t *dir;

    flux_t parent_h = flux_open (ctx->parent_uri, 0);
    if (!parent_h) {
        flux_log (h, LOG_ERR, "Could not open connection to parent");
        goto ret;
    }
    flux_log (h, LOG_DEBUG, "%s: LoadRDLkvs: Opened parent handle", __FUNCTION__);

    if (kvs_get_dir (parent_h, &dir, "lwj.%lu", ctx->my_job_id) < 0) {
        flux_log (h, LOG_ERR, "Couldn't get job kvs dir. Hierarchy addons failed");
        goto ret;
    }
    flux_log (h, LOG_DEBUG, "%s: LoadRDLkvs: Opened job directory", __FUNCTION__);

    if (kvsdir_get_string (dir, "rdl", &rdlstr) < 0) {
        flux_log (h, LOG_ERR, "Couldn't get rdl from parent kvs: %s", strerror (errno));
        goto ret;
    }
    flux_log (h, LOG_DEBUG, "%s: LoadRDLkvs: got rdlstring from parent", __FUNCTION__);

    if (kvsdir_get_int64 (dir, "walltime", &ctx->my_walltime) < 0) {
        flux_log (h, LOG_ERR, "Couldn't get rdl from parent kvs: %s", strerror (errno));
        ctx->my_walltime = INT64_MAX;
    }
    flux_log (h, LOG_DEBUG, "%s: LoadRDLkvs: Walltime obtained from parent", __FUNCTION__); fflush (0);

    flux_close (parent_h);

    /* Create a cluster resource */
    //TODO: set the root_uri
    ctx->rctx.root_resrc = resrc_create_cluster ("cluster");
    flux_log (h, LOG_DEBUG, "%s: LoadRDLkvs: root cluster created", __FUNCTION__);

    /* deserialize and add it to cluster */
    JSON ro = Jfromstr (rdlstr);
    //flux_log (h, LOG_DEBUG, "%s: LoadRDLkvs: ro obtained from json: %s", __FUNCTION__, Jtostr(ro));
    resrc_add_resources_from_json (h, ctx->rctx.root_resrc, ctx->rctx.resrc_id_hash, ro, 0);
    //resrc_hash_by_uuid (resrc_phys_tree (ctx->rctx.root_resrc), ctx->rctx.resrc_id_hash);
    //Jput (ro);

    rc = 0;

ret:
    return rc;
}

static int load_resources (ssrvctx_t *ctx)
{
    int rc = -1;
    char *e_str = NULL;
    char *turi = NULL;
    resrc_t *tres = NULL;
    char *path = ctx->arg.path;
    char *uri = ctx->arg.uri;
    rsreader_t r_mode = ctx->arg.r_mode;

    setup_rdl_lua (ctx->h);

    switch (r_mode) {
    case RSREADER_RESRC_EMUL:
        if (ctx->my_job_id > 0) {
            flux_log (ctx->h, LOG_DEBUG, "Going to load RDL from parent KVS");
            rc = load_rdl_from_parent_kvs (ctx);
        } else {
            flux_log (ctx->h, LOG_DEBUG, "Loading RDL from a conf file in emu mode");
            if (rsreader_resrc_bulkload (path, uri, &turi, &tres) != 0) {
                flux_log (ctx->h, LOG_ERR, "failed to load resrc");
                goto done;
            }
            /*
            } else if (build_hwloc_rs2rank (ctx, r_mode) != 0) {
                flux_log (ctx->h, LOG_ERR, "failed to build rs2rank");
                goto done;
            } else if (rsreader_force_link2rank (ctx->machs, tres) != 0) {
                flux_log (ctx->h, LOG_ERR, "failed to force a link to a rank");
                goto done;
            }
            */
            ctx->rctx.root_uri = turi;
            ctx->rctx.root_resrc = tres;
            resrc_tree_hash_by_uuid (resrc_phys_tree (ctx->rctx.root_resrc), ctx->rctx.resrc_id_hash);
            rc = 0;
        }
        resrc_tree_idle_resources (resrc_phys_tree (ctx->rctx.root_resrc));
        flux_log (ctx->h, LOG_INFO, "%s: loaded resrc", __FUNCTION__);
        //flux_log (ctx->h, LOG_DEBUG, "%s: Printing resource in resrc_id_hash", __FUNCTION__);
        //resrc_hash_flux_log (ctx->h, ctx->rctx.resrc_id_hash);
        int num_nodes = 0;
        resrc_t *curr_resrc = NULL;
        for (curr_resrc = zhash_first(ctx->rctx.resrc_id_hash);
             curr_resrc;
             curr_resrc = zhash_next(ctx->rctx.resrc_id_hash)) {
            if (!strncmp (resrc_type(curr_resrc), "node", 5)) {
                num_nodes++;
            }
        }
        flux_log (ctx->h, LOG_DEBUG, "%s: number of nodes: %d", __FUNCTION__, num_nodes);
        if (ctx->arg.verbosity > 0) {
            flux_log (ctx->h, LOG_DEBUG, "%s: Printing resource in root_resrc tree", __FUNCTION__);
            resrc_tree_flux_log (ctx->h, resrc_phys_tree (ctx->rctx.root_resrc));
        }
        break;

    case RSREADER_RESRC:
        if (rsreader_resrc_bulkload (path, uri, &turi, &tres) != 0) {
            flux_log (ctx->h, LOG_ERR, "failed to load resrc");
            goto done;
        } else if (build_hwloc_rs2rank (ctx, r_mode) != 0) {
            flux_log (ctx->h, LOG_ERR, "failed to build rs2rank");
            goto done;
        }
        if (ctx->arg.verbosity > 0) {
            flux_log (ctx->h, LOG_INFO, "resrc state after resrc read");
            resrc_tree_flux_log (ctx->h, resrc_phys_tree (tres));
        }
        if (rsreader_link2rank (ctx->machs, tres, &e_str) != 0) {
            flux_log (ctx->h, LOG_INFO, "RDL(%s) inconsistent w/ hwloc!", path);
            if (e_str) {
                flux_log (ctx->h, LOG_INFO, "%s", e_str);
                free (e_str);
            }
            if (ctx->arg.fail_on_error)
                goto done;
            flux_log (ctx->h, LOG_INFO, "rebuild resrc using hwloc");
            if (turi)
                free (turi);
            if (tres)
                resrc_resource_destroy (tres);
            r_mode = RSREADER_HWLOC;
            /* deliberate fall-through to RSREADER_HWLOC! */
        } else {
            ctx->rctx.root_uri = turi;
            ctx->rctx.root_resrc = tres;
            flux_log (ctx->h, LOG_INFO, "loaded resrc");
            rc = 0;
            break;
        }

    case RSREADER_HWLOC:
        if (!(ctx->rctx.root_resrc = resrc_create_cluster ("cluster"))) {
            flux_log (ctx->h, LOG_ERR, "failed to create cluster resrc");
            goto done;
        } else if (build_hwloc_rs2rank (ctx, r_mode) != 0) {
            flux_log (ctx->h, LOG_ERR, "failed to load resrc using hwloc");
            goto done;
        }
        /* linking has already been done by build_hwloc_rs2rank above */
        resrc_tree_idle_resources (resrc_phys_tree (ctx->rctx.root_resrc));
        if (ctx->arg.verbosity > 0) {
            flux_log (ctx->h, LOG_INFO, "resrc state after hwloc read");
            resrc_tree_flux_log (ctx->h, resrc_phys_tree (ctx->rctx.root_resrc));
        }
        rc = 0;
        break;

    default:
        flux_log (ctx->h, LOG_ERR, "unkwown resource reader type");
        break;
    }

done:
    return rc;
}


/******************************************************************************
 *                                                                            *
 *       Scheduler Communication for Hierarchical/Slack Scheduling            *
 *                                                                            *
 ******************************************************************************/

static void job_cslack_set (flux_t h, flux_lwj_t *job, JSON needobj)
{
    job->slackinfo->slack_state = CSLACK;
    if (job->slackinfo->need) {
        Jput (job->slackinfo->need);
        job->slackinfo->need = NULL;
    }
    flux_log (h, LOG_DEBUG, "SO FAR SO GOOD");
    job->slackinfo->need = Jdup (needobj);
    if (job->slackinfo == NULL)
        flux_log (h, LOG_DEBUG, "AGAIN IT IS NULL");
}

static void job_cslack_invalidate (flux_lwj_t *job)
{
    job->slackinfo->slack_state = INVALID;
    if (job->slackinfo->need != NULL) {
        Jput (job->slackinfo->need);
        job->slackinfo->need = NULL;
    }
}

static void ctx_slack_invalidate (ssrvctx_t *ctx)
{
    ctx->slctx.slack_state = INVALID;
    ctx->slctx.psize = 0;
    ctx->slctx.rsize = 0;
    ctx->slctx.jobid = 0;
    set_next_event_timer_wrapper (ctx);
}

bool all_children_cslack (ssrvctx_t *ctx)
{
    bool rc = true;
    flux_lwj_t *job = NULL;
    zlist_t *jobs = ctx->i_queue;
    job = zlist_first (jobs);
    while (job) {
        if (job->slackinfo->slack_state != CSLACK) {
            rc = false;
            break;
        }
        job = (flux_lwj_t*)zlist_next (jobs);
    }
    return rc;
}

static int accumulate_surplus_need (ssrvctx_t *ctx, int k_needs, JSON o, int *rcount)
{
    int rc = -1;
    int jcount = 0;
    flux_t h = ctx->h;
    JSON j = Jnew (); // uuid : endtime, uuid : endtime
    JSON r = Jnew_ar ();

    flux_log (h, LOG_DEBUG, "%s: Computing surplus/need", __FUNCTION__);
    /* If all my resources are not yet with me, do not send surplus */
    if (ctx->slctx.wait_until_return) {
        *rcount = 0;
        flux_log (h, LOG_DEBUG, "%s: Wait until return set to true. Skipping surplus collection. Goto needs.", __FUNCTION__);
        goto needs;
    }

    flux_log (h, LOG_DEBUG, "%s: Proceeding to compute surplus", __FUNCTION__);
    /* Calculate surplus */
    resrc_t *resrc = zhash_first (ctx->rctx.resrc_id_hash);
    while (resrc) {
        //flux_log (h, LOG_DEBUG, "%s: Checking resource %s and type %s", __FUNCTION__, resrc_name (resrc), resrc_type (resrc));
        int64_t endtime = 0;
        if (!(resrc_check_slacksub_ready (h, resrc, &endtime))) {  // ensures only cores are included. even the ones that I am not the owner of.
            //flux_log (h, LOG_DEBUG, "%s: Resource not eligible for slacksub", __FUNCTION__);
            goto next;
        }
        //flux_log (h, LOG_DEBUG, "%s: Resource is eligible", __FUNCTION__);
        char ruuid[40];
        resrc_uuid (resrc, ruuid);
        Jadd_int64 (j, ruuid, endtime);
        flux_log (h, LOG_DEBUG, "Accumulated resource with uuid: %s of type: %s, endtime:%"PRId64", owner: %"PRId64"", ruuid, resrc_type (resrc), endtime, resrc_owner (resrc));
        if (resrc_owner (resrc) == 0)
            (*rcount)++;
next:
        resrc = zhash_next (ctx->rctx.resrc_id_hash);
    }
    flux_log (h, LOG_DEBUG, "final value of rcount: %d", *rcount);

    /* Add surplus to message */
    Jadd_obj (o, "surplus", j);

needs:
    if (k_needs == 0)
        goto ret;

    /* Calculate needs */
    flux_lwj_t *job = zlist_first (ctx->p_queue);
    while (job) {
        if ((K_NEEDS > -1) && (jcount >= K_NEEDS)) {
            break;
        }

        //TODO: freeing this memory of needs
        JSON req_res = Jnew ();
        Jadd_int64 (req_res, "nnodes", job->req->nnodes);
        Jadd_int64 (req_res, "ncores", job->req->ncores);
        Jadd_int64 (req_res, "walltime", job->req->walltime);

        flux_log (h, LOG_DEBUG, "%s: Added need: = %s, ncores was: %ld, and walltime was= %ld", __FUNCTION__, Jtostr (req_res), job->req->ncores, job->req->walltime);

        char *key;
        if (asprintf (&key, "need%d", jcount) < 0) {
            flux_log (ctx->h, LOG_ERR, "(%s): create need key failed: %s",
                      __FUNCTION__, strerror (errno));
        }
        Jadd_ar_obj (r, req_res);

        //Jput (req_res);

        jcount++;

        job = zlist_next (ctx->p_queue);
    }
    flux_log (h, LOG_DEBUG, "%s: added %d needs from self", __FUNCTION__, jcount);

    flux_log (h, LOG_DEBUG, "%s: total needs added: %d", __FUNCTION__, jcount);

    /* Add need to message */
    if (jcount > 0) {
        Jadd_obj (o, "need", r);
    }

ret:
    Jput (j);
    Jput (r);
    return rc;
}

/*
 * Send complete slack status to another instance
 */
int send_cslack (ssrvctx_t *ctx, JSON jmsg)
{
    int rc = -1;
    flux_t h = ctx->h;
    JSON o = Jnew ();
    flux_rpc_t *rpc = NULL;
    flux_log (h, LOG_DEBUG, "%s: Going to send cslack", __FUNCTION__);

    Jadd_int64 (o, "jobid", ctx->my_job_id);
    flux_log (h, LOG_DEBUG, "%s: My job id = %ld", __FUNCTION__, ctx->my_job_id);

    if (jmsg != NULL) {
        flux_log (h, LOG_DEBUG, "%s: Msg has been given: %s", __FUNCTION__, Jtostr (jmsg));
        //Jadd_obj (o, "msg", jmsg);
        Jadd_str (o, "msg", Jtostr (jmsg));
    } else {
        flux_log (h, LOG_DEBUG, "%s: No msg has been given", __FUNCTION__);
    }

    if (ctx->parent_uri) {
        flux_log (h, LOG_DEBUG, "%s: Parent URI still exists", __FUNCTION__);
    } else {
        flux_log (h, LOG_DEBUG, "%s: Parent URI does not exist", __FUNCTION__);
    }

    flux_t parent_h = flux_open (ctx->parent_uri, 0);
    if (!parent_h) {
        flux_log (h, LOG_ERR, "couldn't open handle to parent");
        return rc;
    }

    flux_log (h, LOG_DEBUG, "%s: Parent handle successfully obtained, x = %s", __FUNCTION__, Jtostr (o));
    rpc = flux_rpc (parent_h, "sched.slack.cslack", (const char *)Jtostr (o), FLUX_NODEID_ANY, 0);
    flux_log (h, LOG_DEBUG, "%s: Message sent to parent", __FUNCTION__);
    flux_rpc_destroy (rpc);
    flux_log (h, LOG_DEBUG, "%s: destroyed rpc", __FUNCTION__);
    flux_close (parent_h);
    flux_log (h, LOG_DEBUG, "%s: Closed parent handle", __FUNCTION__);
    Jput (o);
    flux_log (h, LOG_DEBUG, "%s: Cleared JSON", __FUNCTION__);

    rc = 0;

    if ((ctx->my_job_id) && (ctx->sctx.in_sim)) {
        flux_log (h, LOG_DEBUG, "%s: setting slack event for parent", __FUNCTION__);
        set_next_event_slack_wrapper (ctx, -1);
    }

    return rc;
}


static int send_cslack_invalid (ssrvctx_t *ctx, JSON jmsg)
{
    int rc = -1;
    flux_t remote_h;
    JSON o = Jnew ();
    flux_rpc_t *rpc;
    flux_t h = ctx->h;

    flux_log (h, LOG_DEBUG, "%s: Going to send cslack invalid", __FUNCTION__);
    Jadd_int64 (o, "jobid", ctx->my_job_id);

    if (jmsg)
        Jadd_obj (o, "ask", jmsg);
    flux_log (h, LOG_DEBUG, "%s: Final message to be sent = %s", __FUNCTION__, Jtostr (o));

    if (!ctx->parent_uri) {
        flux_log (h, LOG_DEBUG, "%s: No parent for sending cslack invalid", __FUNCTION__);
        Jput (o);
        rc = 0;
        return rc;
    }

    remote_h = flux_open (ctx->parent_uri, 0);
    if (!remote_h) {
        flux_log (h, LOG_DEBUG, "%s: Could not get parent handle.", __FUNCTION__);
        Jput (o);
        return rc;
    }
    flux_log (h, LOG_DEBUG, "%s: Parent handle has been obtained", __FUNCTION__);

    rpc = flux_rpc (remote_h, "sched.slack.cslack_invalidate", Jtostr (o), FLUX_NODEID_ANY, 0);
    if (!rpc) {
        flux_log (h, LOG_DEBUG, "%s: Sending cslack_invalid unsuccessful", __FUNCTION__);
        flux_close (remote_h);
        Jput (o);
        return rc;
    }
    flux_log (h, LOG_DEBUG, "%s: Sending cslack_invalid successful", __FUNCTION__);
    flux_close (remote_h);

    Jput (o);
    rc = 0;

    if ((ctx->my_job_id) && (ctx->sctx.in_sim)) {
        flux_log (h, LOG_DEBUG, "%s: setting slack event for parent", __FUNCTION__);
        set_next_event_slack_wrapper (ctx, -1);
    }

    return rc;
}

static int send_to_child (ssrvctx_t *ctx, flux_lwj_t *job, JSON jmsg, char *service)
{
    int rc = -1;
    flux_t h = ctx->h;
    flux_rpc_t *rpc;

    JSON o = Jnew ();
    Jadd_int64 (o, "jobid", -1);
    Jadd_obj (o, "msg", jmsg);

    flux_log (h, LOG_DEBUG, "%s: Going to send following to child: %s", __FUNCTION__, Jtostr (o));
    flux_t child_h = flux_open (job->contact, 0);
    if (!child_h) {
        flux_log (h, LOG_ERR, "Couldn't open handle to child");
        return rc;
    }
    rpc = flux_rpc (child_h, service, Jtostr (o), FLUX_NODEID_ANY, 0);
    if (!rpc) {
        flux_log (h, LOG_DEBUG, "%s: rpc did not complete", __FUNCTION__);
    }
    flux_close (child_h);

    Jput (o);
    rc = 0;

    if (ctx->sctx.in_sim) {
        flux_log (h, LOG_DEBUG, "%s: setting slack event for child - %"PRId64"", __FUNCTION__, job->lwj_id);
        set_next_event_slack_wrapper (ctx, job->lwj_id);
    }

    return rc;
}

static int send_to_parent (ssrvctx_t *ctx, JSON jmsg, char *service)
{
    int rc = -1;
    flux_t h = ctx->h;

    JSON o = Jnew ();
    Jadd_int64 (o, "jobid", ctx->my_job_id);
    Jadd_obj (o, "msg", jmsg);

    flux_t parent_h = flux_open (ctx->parent_uri, 0);
    if (!parent_h) {
        flux_log (h, LOG_ERR, "Couldn't open connection to parent");
        return rc;
    }
    flux_rpc (parent_h, service, Jtostr (o), FLUX_NODEID_ANY, 0);
    flux_close (parent_h);

    Jput (o);
    rc = 0;

    if ((ctx->my_job_id) && (ctx->sctx.in_sim)) {
        flux_log (h, LOG_DEBUG, "%s: setting slack event for parent", __FUNCTION__);
        set_next_event_slack_wrapper (ctx, -1);
    }

    return rc;
}

/*
 * Find any slack resources that were given to a child job that
 * haven't been returned yet.  To be run after a child job has
 * completed.
 */
static int reap_child_slack_resources (ssrvctx_t *ctx, flux_lwj_t *job)
{
#ifdef DYNAMIC_SCHEDULING
    /*
     * TODO: this performance stinks, utilize a structure in the job's
     * slackinfo to avoid having to search the entire resrc tree, and
     * instead just iterate over the slack resources of the job
     * directly
     */

    flux_log (ctx->h, LOG_DEBUG,
              "%s: just entered - job #%"PRId64", "
              "current resource_out_count is %"PRId64"",
              __FUNCTION__, job->lwj_id,
              ctx->slctx.resource_out_count);
    resrc_t *curr_resrc = NULL;
    for (curr_resrc = zhash_first (ctx->rctx.resrc_id_hash);
         curr_resrc;
         curr_resrc = zhash_next (ctx->rctx.resrc_id_hash))
        {
            if ((resrc_owner (curr_resrc) == 0) &&
                (!strncmp (resrc_type (curr_resrc), "core", 5)) &&
                // TODO: verify that this check is sufficient
                (resrc_leasee (curr_resrc) == job->lwj_id))
                {
                    resrc_mark_resource_return_received (curr_resrc, job->lwj_id);
                    // TODO: verify if this should only happen for cores
                    ctx->slctx.resource_out_count--;
                    flux_log (ctx->h, LOG_DEBUG,
                              "%s: reaping a child's slack resources: %s, "
                              "current resource_out_count is %"PRId64"",
                              __FUNCTION__, resrc_path (curr_resrc),
                              ctx->slctx.resource_out_count);
                }
        }
#endif
    return 0;
}

static int return_idle_slack_resources (ssrvctx_t *ctx)
{

    int rc = 0;
    int64_t jobid = 0;
    flux_t h = ctx->h;
    // Contains an entry for every owner that is being returned resources
    zhash_t *jobid_uuid_hash = zhash_new ();
    flux_log (h, LOG_DEBUG, "%s: Entered return idle slack resources", __FUNCTION__);
    resrc_t *resrc = zhash_first (ctx->rctx.resrc_id_hash);
    while (resrc) {
        //flux_log (h, LOG_DEBUG, "%s: Considering resource = %s, %s, state=%s,", __FUNCTION__, resrc_type (resrc), resrc_path (resrc), resrc_state (resrc));
        if (resrc_check_return_ready (h ,resrc, &jobid) == false)  {// jobid is the ownerid
            //flux_log (h, LOG_DEBUG, "%s: Resource not return ready", __FUNCTION__);
            goto next;
        }
        flux_log (h, LOG_DEBUG, "%s: Resource is return ready - %ss", __FUNCTION__, resrc_path (resrc));
        char *jobid_str = NULL;
        if (asprintf (&jobid_str, "%ld", jobid) < 0) {
            flux_log (ctx->h, LOG_ERR, "(%s): create jobid str failed: %s",
                      __FUNCTION__, strerror (errno));
        }
        char *juidarry = zhash_lookup (jobid_uuid_hash, jobid_str);
        if (juidarry) {
            JSON j = Jfromstr (juidarry);
            Jadd_ar_str (j, (const char *)zhash_cursor (ctx->rctx.resrc_id_hash));
            char *x = xstrdup (Jtostr (j));
            zhash_update (jobid_uuid_hash, jobid_str, (void *) x);
            Jput (j);
            flux_log (h, LOG_DEBUG, "%s: Resource entry added to existing array", __FUNCTION__);
        } else {
            JSON j = Jnew_ar ();
            Jadd_ar_str (j, (const char *)zhash_cursor (ctx->rctx.resrc_id_hash));
            char *x = xstrdup (Jtostr (j));
            zhash_insert (jobid_uuid_hash, jobid_str, (void *)x);
            Jput (j);
            flux_log (h, LOG_DEBUG,
                      "%s: New array created in idle hashtable for job %s",
                      __FUNCTION__, jobid_str);
        }
next:
        resrc = zhash_next (ctx->rctx.resrc_id_hash);
    }

    /* Now we know what jobs have to get what resources back */
    char *jstr = zhash_first (jobid_uuid_hash);
    while (jstr) {
        const char *jobid_str = zhash_cursor (jobid_uuid_hash);
        JSON uo = Jfromstr (jstr);
        jobid = strtol (jobid_str, NULL, 10);
        if (jobid == -1) {
            JSON r = Jnew ();
            Jadd_obj (r, "return", uo);
            if ((send_to_parent (ctx, r, "sched.slack.addresources")) < 0) {
                flux_log (h, LOG_ERR, "couldn't send resources back to parent");
                goto nextone;
            }
            flux_log (h, LOG_DEBUG,
                      "%s: Sent idle slack resources back to parent: %s",
                      __FUNCTION__, Jtostr (r));
            Jput (r);
        } else {
            flux_lwj_t *job = find_job_in_queue (ctx->i_queue, jobid);
            if (!job) {
                flux_log (h, LOG_ERR, "job in resrc not found in i_queue: %ld", job->lwj_id);
                //resrc_own_resources (r);
                goto nextone;
            }
            JSON r = Jnew ();
            Jadd_obj (r, "return", uo);
            send_to_child (ctx, job, r, "sched.slack.addresources");
            flux_log (h, LOG_DEBUG,
                      "%s: Sent idle slack resources back to child - %s",
                      __FUNCTION__, Jtostr (r));
            Jput (r);
        }
        resrc_mark_resources_returned (ctx->rctx.resrc_id_hash, uo);
nextone:
        jstr = zhash_next (jobid_uuid_hash);
    }

    flux_log (h, LOG_DEBUG, "%s: destroying returned resources", __FUNCTION__);
    resrc_tree_destroy_returned_resources (resrc_phys_tree(ctx->rctx.root_resrc), ctx->rctx.resrc_id_hash);
    return rc;
}

/******************************************************************************
 *                                                                            *
 *                         Emulator Specific Code                             *
 *                                                                            *
 ******************************************************************************/

/******************************************************************************
 *                                                                            *
 *                          Emulator Specific Code                            *
 *                                                                            *
 ******************************************************************************/

static int add_job_complete_time (ssrvctx_t *ctx, flux_lwj_t *job)
{
    int rc = -1;
    char *complete_key = NULL;
    double sim_time = ctx->sctx.sim_state->sim_time;

    if (asprintf (&complete_key, "lwj.%"PRId64".complete_time", job->lwj_id) < 0) {
            flux_log (ctx->h, LOG_ERR, "(%s): create complete_key failed: %s",
                      __FUNCTION__, strerror (errno));
    }
    if (kvs_put_double (ctx->h, complete_key, sim_time) < 0) {
        flux_log_error (ctx->h, "%s: kvs_commit", __FUNCTION__);
        goto out;
    }
    if (kvs_commit(ctx->h) < 0) {
        flux_log_error (ctx->h, "%s: kvs_commit", __FUNCTION__);
        goto out;
    }

out:
    free (complete_key);
    return (rc);
}

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
    double next_event = sim_state->sim_time;
    if (!strncmp (module, "sim_exec", 8)) {
        next_event += .0001;
    } else if (!strncmp (module, "sim_timer", 9)) {
        next_event += .01;
    } else {
        next_event += .00001;
    }

    double *timer = zhash_lookup (sim_state->timers, module);
    if (timer) {
        if (*timer > next_event || *timer < 0) {
            *timer = next_event;
        }
    } else {
        timer = (double *)malloc (sizeof (double));
        *timer = next_event;
        zhash_insert (sim_state->timers, module, timer);
    }

}

/*
 * Set next event for sched module based on a slack event
 * If job id < 0, then set for the parent's sched module
 * If job is >= 0, then set for the sched module of the child with id == jobid
 */
static void set_next_event_slack_wrapper (ssrvctx_t *ctx, int jobid)
{
#ifdef DYNAMIC_SCHEDULING
    char *module = NULL;
    if ((jobid < 0) && (ctx->my_job_id == 0)) return;

    if (jobid < 0) {
        if (asprintf (&module, "sched.%s", ctx->sctx.parent_id) < 0) {
            flux_log (ctx->h, LOG_ERR, "(%s): create module str failed: %s",
                      __FUNCTION__, strerror (errno));
        }
    } else {
        if (asprintf (&module, "sched.%s.%d", ctx->sctx.next_prefix, jobid) < 0) {
            flux_log (ctx->h, LOG_ERR, "(%s): create module str failed: %s",
                      __FUNCTION__, strerror (errno));
        }
    }
    flux_log (ctx->h, LOG_DEBUG,
              "%s: setting up next event for %s based on slack event",
              __FUNCTION__, module);

    set_next_event (module, ctx->sctx.sim_state);
    flux_log (ctx->h, LOG_DEBUG, "setup next timer for module %s", module);
    free (module);
#endif
    return;
}

/*
 * Set next event for the timer module in THIS instance
 */
static void set_next_event_timer_wrapper (ssrvctx_t *ctx)
{
#ifdef DYNAMIC_SCHEDULING
    char *module = NULL;
    flux_log (ctx->h, LOG_DEBUG, "%s: setting up next timer for timer module", __FUNCTION__);

    if (asprintf (&module, "sim_timer.%s", ctx->sctx.next_prefix) < 0) {
        flux_log (ctx->h, LOG_ERR, "(%s): create module str failed: %s",
                  __FUNCTION__, strerror (errno));
    }
    set_next_event (module, ctx->sctx.sim_state);

    flux_log (ctx->h, LOG_DEBUG, "%s: setup next timer for module %s", __FUNCTION__, module);
    free (module);
#endif
    return;
}

static void handle_timer_queue (ssrvctx_t *ctx, sim_state_t *sim_state)
{
    while (zlist_size (ctx->sctx.timer_queue) > 0)
        set_next_event (zlist_pop (ctx->sctx.timer_queue), sim_state);
}

static void handle_jsc_queue (ssrvctx_t *ctx)
{
    jsc_event_t *jsc_event = NULL;

    flux_log (ctx->h, LOG_DEBUG, "%s: Handling jsc_queue", __FUNCTION__);

    while (zlist_size (ctx->sctx.jsc_queue) > 0) {
        jsc_event = (jsc_event_t *)zlist_pop (ctx->sctx.jsc_queue);
        flux_log (ctx->h,
                  LOG_DEBUG,
                  "JscEvent being handled - JSON: %s, errnum: %d",
                  Jtostr (jsc_event->jcb),
                  jsc_event->errnum);
        job_status_cb (Jtostr(jsc_event->jcb), jsc_event->arg,
                       jsc_event->errnum);
        Jput (jsc_event->jcb);
        free (jsc_event);
    }
}

static void handle_res_queue (ssrvctx_t *ctx)
{
    bool should_schedule_jobs = (zlist_size (ctx->sctx.res_queue) > 0);
    res_event_t *res_event = NULL;

    flux_log (ctx->h, LOG_DEBUG,
              "%s: Handling res_queue and %s schedule jobs after since number of events = %zu",
              __FUNCTION__,
              should_schedule_jobs ? "will" : "will not",
              zlist_size (ctx->sctx.res_queue));

    while (zlist_size (ctx->sctx.res_queue) > 0) {
        res_event = (res_event_t *)zlist_pop (ctx->sctx.res_queue);
        flux_log (ctx->h,
                  LOG_DEBUG,
                  "ResEvent being handled");
        res_event_cb (res_event->h, NULL, NULL, res_event->arg);
        free (res_event);
    }

    if (should_schedule_jobs) {
        int num_jobs_scheduled = schedule_jobs (ctx);
        if (num_jobs_scheduled > 0 && ctx->arg.verbosity > 0) {
            flux_log (ctx->h, LOG_DEBUG, "%s: jobs were scheduled, printing rdl:", __FUNCTION__);
            resrc_tree_flux_log (ctx->h, resrc_phys_tree (ctx->rctx.root_resrc));
        }
    }
}

static void handle_slack_queue (ssrvctx_t *ctx)
{
    bool should_schedule_jobs = false;
    slack_event_t *slack_event = NULL;
    flux_log (ctx->h, LOG_DEBUG, "%s: Handling slack_queue", __FUNCTION__);
    while (zlist_size (ctx->sctx.slack_queue) > 0) {
        slack_event = (slack_event_t *)zlist_pop (ctx->sctx.slack_queue);
        flux_log (ctx->h,
                  LOG_DEBUG,
                  "SlackEvent being handled for topic: %s", slack_event->topic);
        if (!strcmp (slack_event->topic, "sched.slack.cslack")) {
            req_cslack_cb (slack_event->h, NULL, slack_event->msg, slack_event->arg);
            should_schedule_jobs = true;
        } else if (!strcmp (slack_event->topic, "sched.slack.cslack_invalidate")) {
            req_cslack_invalid_cb (slack_event->h, NULL, slack_event->msg, slack_event->arg);
        } else if (!strcmp (slack_event->topic, "sched.slack.addresources")) {
            req_addresources_cb (slack_event->h, NULL, slack_event->msg, slack_event->arg);
            should_schedule_jobs = true;
        } else if (!strcmp (slack_event->topic, "sched.slack.askresources")) {
            req_askresources_cb (slack_event->h, NULL, slack_event->msg, slack_event->arg);
        }

        flux_msg_destroy (slack_event->msg);
        free (slack_event->topic);
        free (slack_event);
    }

    if (should_schedule_jobs) {
        flux_log (ctx->h, LOG_DEBUG,
                  "%s: resources were added and/or sched is now in cslack, printing rdl:",
                  __FUNCTION__);
        resrc_tree_flux_log (ctx->h, resrc_phys_tree (ctx->rctx.root_resrc));
        flux_log (ctx->h, LOG_DEBUG, "%s: will schedule jobs", __FUNCTION__);
        int num_jobs_scheduled = schedule_jobs (ctx);
        if (num_jobs_scheduled > 0 && ctx->arg.verbosity > 0) {
            flux_log (ctx->h, LOG_DEBUG, "%s: jobs were scheduled, printing rdl:", __FUNCTION__);
            resrc_tree_flux_log (ctx->h, resrc_phys_tree (ctx->rctx.root_resrc));
        }
    } else {
        flux_log (ctx->h, LOG_DEBUG, "%s: will not schedule jobs", __FUNCTION__);
    }
}

/******************************************************************************
 *                                                                            *
 *                   Scheduler Eventing For Emulation Mode                    *
 *                                                                            *
 ******************************************************************************/

/*
 * Simulator Callbacks
 */

static void check_completion (ssrvctx_t *ctx)
{
    if ((zlist_size (ctx->p_queue) == 0) && (zlist_size (ctx->r_queue) == 0)) {
        if (ctx->my_job_id) {
            set_next_event_slack_wrapper (ctx, -1);
        }
        char *module = NULL;
        if (asprintf (&module, "init_prog.%s", ctx->sctx.next_prefix) < 0) {
            flux_log (ctx->h, LOG_ERR, "(%s): create module str failed: %s",
                      __FUNCTION__, strerror (errno));
}
        flux_log (ctx->h, LOG_DEBUG, "%s: setting timer event for %s", __FUNCTION__, module);
        set_next_event(module, ctx->sctx.sim_state);
        free (module);
    }
}



static void start_cb (flux_t h,
                      flux_msg_handler_t *w,
                      const flux_msg_t *msg,
                      void *arg)
{
    ssrvctx_t *ctx = getctx (h);
    flux_log (h, LOG_DEBUG, "received a start event");
    if (send_join_request (h, ctx->sctx.sim_h, ctx->sctx.module_name, 0) < 0) {
        flux_log (h,
                  LOG_ERR,
                  "submit module failed to register with sim module");
        return;
    }
    flux_log (h, LOG_DEBUG, "%s: sent a join request", __FUNCTION__);

    if (flux_event_unsubscribe (h, "sim.start") < 0) {
        flux_log (h, LOG_ERR, "failed to unsubscribe from \"sim.start\"");
        return;
    } else {
        flux_log (h, LOG_DEBUG, "unsubscribed from \"sim.start\"");
    }

    return;
}

static int sim_job_status_cb (const char *jcbstr, void *arg, int errnum)
{
    JSON jcb = NULL;
    ssrvctx_t *ctx = getctx ((flux_t)arg);
    jsc_event_t *event = (jsc_event_t*) xzmalloc (sizeof (jsc_event_t));

    if (!(jcb = Jfromstr (jcbstr))) {
        flux_log (ctx->h, LOG_ERR, "sim_job_status_cb: error parsing JSON string");
        return -1;
    }

    event->jcb = Jget (jcb);
    event->arg = arg;
    event->errnum = errnum;

    flux_log (ctx->h,
              LOG_DEBUG,
              "JscEvent being queued - JSON: %s, errnum: %d",
              Jtostr (event->jcb),
              event->errnum);
    zlist_append (ctx->sctx.jsc_queue, event);

    if (ctx->sctx.token) {
        handle_jsc_queue (ctx);
        check_completion (ctx);
        if ((ctx->sctx.cdeathcount == 0) && (ctx->sctx.ccompletecount == 0)) {
            ctx->sctx.token = 0;
            set_next_event (ctx->sctx.module_name, ctx->sctx.sim_state);
            send_reply_request (ctx->h, ctx->sctx.sim_h, ctx->sctx.module_name, ctx->sctx.sim_state);
            free_simstate (ctx->sctx.sim_state);
            flux_log (ctx->h, LOG_DEBUG, "Token sent back from job status cb sim");
        }
    }

    return 0;
}

static void sim_res_event_cb (flux_t h, flux_msg_handler_t *w,
                              const flux_msg_t *msg, void *arg) {
    ssrvctx_t *ctx = getctx ((flux_t)arg);
    res_event_t *event = (res_event_t*) xzmalloc (sizeof (res_event_t));
    const char *topic = NULL;

    event->h = h;
    event->arg = arg;

    flux_msg_get_topic (msg, &topic);
    flux_log (ctx->h,
              LOG_DEBUG,
              "ResEvent being queued - topic: %s",
              topic);
    zlist_append (ctx->sctx.res_queue, event);

    ctx->sctx.ccompletecount--;
    flux_log (h, LOG_DEBUG, "%s: ++++++++++++ completecount decremented = %d", __FUNCTION__, ctx->sctx.ccompletecount);

    if (ctx->sctx.token) {
        handle_res_queue (ctx);
        check_completion (ctx);
        if ((ctx->sctx.cdeathcount == 0) && (ctx->sctx.ccompletecount == 0)) {
            ctx->sctx.token = 0;
            set_next_event (ctx->sctx.module_name, ctx->sctx.sim_state);
            send_reply_request (ctx->h, ctx->sctx.sim_h, ctx->sctx.module_name, ctx->sctx.sim_state);
            free_simstate (ctx->sctx.sim_state);
            flux_log (ctx->h, LOG_DEBUG, "Token sent back from res event cb sim");
        }
    }

}

static void sim_slack_event_cb (flux_t h, flux_msg_handler_t *w, const flux_msg_t *msg, void *arg)
{
    ssrvctx_t *ctx = getctx ((flux_t)arg);
    slack_event_t *event = (slack_event_t*) xzmalloc (sizeof (slack_event_t));
    const char *topic = NULL;

    event->h = h;
    event->arg = arg;
    flux_msg_get_topic (msg, &topic);
    event->msg = flux_msg_copy (msg, true);
    event->topic = xstrdup (topic);
    flux_log (ctx->h,
              LOG_DEBUG,
              "SlackEvent being queued - topic: %s",
              topic);
    zlist_append (ctx->sctx.slack_queue, event);

    return;
}

static void sim_timer_cb (flux_t h, flux_msg_handler_t *w, const flux_msg_t *msg, void *arg)
{
/*
    ssrvctx_t *ctx = getctx ((flux_t)arg);
    const char *topic = NULL;
    timer_event_t *event = (timer_event_t *) xzmalloc (sizeof (timer_event_t));
    event->h = h;
    event->arg = arg;
    flux_msg_get_topic (msg, &topic);
    flux_log (ctx->h,
              LOG_DEBUG,
              "ResEvent being queued - topic: %s",
              topic);
    zlist_append (ctx->sctx.timer_queue, event);
*/
    ssrvctx_t *ctx = getctx (h);
    JSON o = NULL;
    JSON out = NULL;
    const char *json_str = NULL;

    if (flux_msg_get_payload_json (msg, &json_str) < 0 || json_str == NULL
        || !(o = Jfromstr (json_str))) {
        flux_log (h, LOG_ERR, "%s: bad message", __FUNCTION__);
        Jput (o);
        return;
    }

    ctx->sctx.sim_state = json_to_sim_state (o);
    resrc_set_sim_time (floor(ctx->sctx.sim_state->sim_time));
    flux_log (h, LOG_DEBUG, "resrc_epochtime: %"PRId64", epochtime: %"PRId64"", resrc_epochtime(), epochtime());

    timer_event_cb (flux_get_reactor(h), NULL, 0, h);
    out = sim_state_to_json (ctx->sctx.sim_state);

    Jadd_int (out, "slack_state", ctx->slctx.slack_state);

    flux_log (h, LOG_DEBUG, "%s: sending response to timer - %s", __FUNCTION__, Jtostr(out));
    if (flux_respond (h, msg, 0, Jtostr(out)) < 0) {
        flux_log (h, LOG_ERR, "%s: failed to respond to timer", __FUNCTION__);
    }

    free_simstate (ctx->sctx.sim_state);
    Jput (o);
    Jput (out);

    return;
}

static void trigger_cb (flux_t h,
                        flux_msg_handler_t *w,
                        const flux_msg_t *msg,
                        void *arg)
{
    clock_t start, diff;
    double seconds;
    const char *json_str = NULL;
    JSON o = NULL;
    ssrvctx_t *ctx = getctx (h);
    flux_log (h, LOG_DEBUG, "%s: Received a trigger in %s", __FUNCTION__, ctx->sctx.module_name);

    if (flux_request_decode (msg, NULL, &json_str) < 0 || json_str == NULL
        || !(o = Jfromstr (json_str))) {
        flux_log (h, LOG_ERR, "%s: bad message", __FUNCTION__);
        Jput (o);
        return;
    }

    //flux_log (h, LOG_DEBUG, "%s: Setting sim_state to new values", __FUNCTION__);
    ctx->sctx.sim_state = json_to_sim_state (o);
    resrc_set_sim_time (floor(ctx->sctx.sim_state->sim_time));
    flux_log (h, LOG_DEBUG, "resrc_epochtime: %"PRId64", epochtime: %"PRId64"", resrc_epochtime(), epochtime());
    ctx->sctx.token = 1;

    start = clock ();

    handle_jsc_queue (ctx);
    handle_res_queue (ctx);

#if 0
    flux_log (h, LOG_DEBUG, "%s: will now schedule jobs", __FUNCTION__);
    schedule_jobs (ctx);
#endif

    handle_slack_queue (ctx);
    handle_timer_queue (ctx, ctx->sctx.sim_state);

    check_completion (ctx);

    if ((ctx->sctx.cdeathcount == 0) && (ctx->sctx.ccompletecount == 0)) {
        ctx->sctx.token = 0;
        send_reply_request (h, ctx->sctx.sim_h, ctx->sctx.module_name, ctx->sctx.sim_state);
        free_simstate (ctx->sctx.sim_state);
    }

    diff = clock () - start;
    seconds = ((double)diff) / CLOCKS_PER_SEC;

    flux_log (h,
              LOG_DEBUG,
              "scheduler trigger took %f seconds",
              seconds);

    Jput (o);

    return;
}

/*
 * Simulator Initialization Functions
 */

static int reg_sim_events (ssrvctx_t *ctx)
{
    int rc = -1;
    flux_t h = ctx->h;

    if (flux_event_subscribe (ctx->h, "sim.start") < 0) {
        flux_log (ctx->h, LOG_ERR, "subscribing to event: %s", strerror (errno));
        goto done;
    }
    if (flux_event_subscribe (ctx->h, "sched.childbirth") < 0) {
        flux_log (ctx->h, LOG_ERR, "subscribing to event: %s", strerror (errno));
        goto done;
    }
    if (flux_event_subscribe (ctx->h, "sched.res.") < 0) {
        flux_log (ctx->h, LOG_ERR, "subscribing to event: %s", strerror (errno));
        goto done;
    }

    char *schedtrigger, *schedtimer;
    if ((asprintf (&schedtrigger, "%s.trigger", ctx->sctx.module_name) < 0) ||
        (asprintf (&schedtimer, "%s.timer", ctx->sctx.module_name) < 0)) {
        flux_log (ctx->h, LOG_ERR, "(%s): create sched trigger str failed: %s",
                  __FUNCTION__, strerror (errno));
    }

    // TODO: move this htab outside of this function with topics we know a-priori
    // Then use add the dynamic ones one-by-one in this function
    struct flux_msg_handler_spec sim_htab[] = {
        {FLUX_MSGTYPE_EVENT, "sim.start", start_cb},
        {FLUX_MSGTYPE_EVENT, "sched.childbirth", childbirth_cb},
        {FLUX_MSGTYPE_EVENT, "sched.res.*", sim_res_event_cb},
        {FLUX_MSGTYPE_REQUEST, "sched.slack.*", sim_slack_event_cb},
        {FLUX_MSGTYPE_REQUEST, "sched.death", req_death_cb},
        {FLUX_MSGTYPE_REQUEST, "sched.qstat", req_qstat_cb},
        {FLUX_MSGTYPE_REQUEST, "sched.alljobssubmitted", req_alljobssubmitted_cb},
        {FLUX_MSGTYPE_REQUEST, schedtrigger, trigger_cb},
        {FLUX_MSGTYPE_REQUEST, schedtimer, sim_timer_cb},
        FLUX_MSGHANDLER_TABLE_END,
    };

    if (flux_msg_handler_addvec (ctx->h, sim_htab, (void *)h) < 0) {
        flux_log (ctx->h, LOG_ERR, "flux_msg_handler_addvec: %s", strerror (errno));
        goto done;
    }
    if (jsc_notify_status (h, sim_job_status_cb, (void *)h) != 0) {
        flux_log (h, LOG_ERR, "error registering a job status change CB");
        goto done;
    }

    if (ctx->sctx.parent_id == NULL) {
        flux_log (h, LOG_DEBUG, "%s: No parent id, so sending alive request", __FUNCTION__);
        send_alive_request (h, ctx->sctx.sim_h, ctx->sctx.module_name);
    } else  {

        flux_log (h, LOG_DEBUG, "%s: Going to send join request from child", __FUNCTION__);

        flux_t sim_h = flux_open (ctx->sctx.sim_uri, 0);
        if (sim_h == NULL) {
            flux_log (h, LOG_ERR, "Could not open connection to simulator");
            goto done;
        }

        if (send_join_request (h, sim_h, ctx->sctx.module_name, -1) < 0) {
            flux_log (h, LOG_ERR, "Unable to join simulation from a child instance");
            goto done;
        }
        flux_log (h, LOG_DEBUG, "%s: sent a join request", __FUNCTION__);

        if (flux_event_unsubscribe (h, "sim.start") < 0) {
            flux_log (h, LOG_ERR, "failed to unsubscribe from \"sim.start\"");
            goto done;
        } else {
            flux_log (h, LOG_DEBUG, "unsubscribed from \"sim.start\"");
        }

        flux_close(sim_h);
    }

    rc = 0;
    free (schedtrigger);
    free (schedtimer);
    //free (schedresstar);
    //free (schedslackstar);
done:
    return rc;
}

static int setup_child (ssrvctx_t *ctx)
{
    int rc = -1;
    flux_t h = ctx->h;

    if (ctx->my_job_id == 0) {
        flux_log (h, LOG_INFO, "Instance does not have parent");
        rc = 0;
        goto ret;
    }

    const char *parent_uri = flux_attr_get (h, "parent-uri", NULL);
    if (!parent_uri) {
        flux_log (h, LOG_ERR, "Instance is child, but could not get parent URI");
        goto ret;
    }

    ctx->parent_uri = xstrdup (parent_uri);
    flux_log (h, LOG_DEBUG, "Instance has a parent with uri: %s", ctx->parent_uri);

    rc = 0;
ret:
    return rc;
}

static int setup_sim (ssrvctx_t *ctx, bool sim)
{
    int rc = -1;

    if (!sim) {
        return rc;
    }

    flux_log (ctx->h, LOG_INFO, "setting up sim in scheduler");
    ctx->sctx.in_sim = true;
    ctx->sctx.token = 0;
    ctx->sctx.sim_state = NULL;
    ctx->sctx.res_queue = zlist_new ();
    ctx->sctx.jsc_queue = zlist_new ();
    ctx->sctx.timer_queue = zlist_new ();
    ctx->sctx.slack_queue = zlist_new ();
    ctx->sctx.dying_jobs = zlist_new ();
    ctx->sctx.cdeathcount = 0;
    ctx->sctx.ccompletecount = 0;
    ctx->sctx.sim_h = NULL;
    resrc_set_sim_mode();

    if (ctx->arg.module_name_prefix) {
        if ((asprintf (&(ctx->sctx.module_name), "sched.%s.%ld", ctx->arg.module_name_prefix, ctx->my_job_id) < 0) ||
            (asprintf (&(ctx->sctx.next_prefix), "%s.%ld", ctx->arg.module_name_prefix, ctx->my_job_id) < 0) ||
            (asprintf (&(ctx->sctx.parent_id), "%s", ctx->arg.module_name_prefix) < 0)) {
            flux_log (ctx->h, LOG_ERR, "(%s): setup module names failed: %s",
                      __FUNCTION__, strerror (errno));
        }
    } else {
        if ((asprintf (&(ctx->sctx.module_name), "sched.%ld", ctx->my_job_id) < 0) ||
            (asprintf (&(ctx->sctx.next_prefix), "%ld", ctx->my_job_id) < 0)) {
            flux_log (ctx->h, LOG_ERR, "(%s): setup module names failed: %s",
                      __FUNCTION__, strerror (errno));
        }
        ctx->sctx.parent_id = NULL;
    }

    if (ctx->arg.sim_uri) {
        ctx->sctx.sim_uri = xstrdup (ctx->arg.sim_uri);
        ctx->sctx.sim_h = flux_open (ctx->sctx.sim_uri, 0);
        if (!ctx->sctx.sim_h) {
            flux_log (ctx->h, LOG_ERR, "Could not open handle to simsrv");
            goto done;
        }
        flux_log (ctx->h, LOG_DEBUG, "-===== URI = %s and sim_h has been successfully opened", ctx->sctx.sim_uri);
    } else {
        const char *local_uri = flux_attr_get (ctx->h, "local-uri", NULL);
        ctx->sctx.sim_uri = xstrdup (local_uri);
        ctx->sctx.sim_h = ctx->h;
    }

    if (asprintf (&my_simexec_module, "sim_exec.%s", ctx->sctx.next_prefix) < 0) {
        flux_log (ctx->h, LOG_ERR, "(%s): create sim_exec module str failed: %s",
                  __FUNCTION__, strerror (errno));
    }

    rc = 0;
done:
    return rc;
}

/******************************************************************************
 *                                                                            *
 *                     Scheduler Eventing For Normal Mode                     *
 *                                                                            *
 ******************************************************************************/


/******************************************************************************
 *                                                                            *
 *                     Scheduler Event Registeration                          *
 *                                                                            *
 ******************************************************************************/

static struct flux_msg_handler_spec htab[] = {
    { FLUX_MSGTYPE_EVENT,     "sched.res.*", res_event_cb},
    {FLUX_MSGTYPE_EVENT, "sched.childbirth", childbirth_cb},
    {FLUX_MSGTYPE_REQUEST, "sched.death", req_death_cb},
    {FLUX_MSGTYPE_REQUEST, "sched.qstat", req_qstat_cb},
    {FLUX_MSGTYPE_REQUEST, "sched.slack.cslack", req_cslack_cb},
    {FLUX_MSGTYPE_REQUEST, "sched.slack.cslack_invalidate", req_cslack_invalid_cb},
    {FLUX_MSGTYPE_REQUEST, "sched.slack.addresources", req_addresources_cb},
    {FLUX_MSGTYPE_REQUEST, "sched.slack.askresources", req_askresources_cb},
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
    if (flux_timer_watcher_create(flux_get_reactor (h), 300.0, 300.0, timer_event_cb, (void *)h) < 0) {
        flux_log (h, LOG_ERR,
                  "error registering timer event CB: %s",
                  strerror (errno));
        rc = -1;
        goto done;
    }
#endif
    if (jsc_notify_status (h, job_status_cb, (void *)h) != 0) {
        flux_log (h, LOG_ERR, "error registering a job status change CB");
        rc = -1;
        goto done;
    }

done:
    return rc;
}

/******************************************************************************
 *                                                                            *
 *            Mode Bridging Layer to Hide Emulation vs. Normal Mode           *
 *                                                                            *
 ******************************************************************************/

static inline int bridge_set_execmode (ssrvctx_t *ctx)
{
    int rc = 0;
    if (ctx->arg.my_job_id) {
        ctx->my_job_id = ctx->arg.my_job_id;
        flux_log (ctx->h, LOG_DEBUG, "my_job_id has been successfully set: %ld", ctx->my_job_id);
    } else {
        flux_log (ctx->h, LOG_DEBUG, "my_job_id was not set at the command line");
    }

    if (ctx->arg.sim && setup_sim (ctx, ctx->arg.sim) != 0) {
        flux_log (ctx->h, LOG_ERR, "failed to setup sim mode");
        rc = -1;
        goto done;
    }
done:
    return rc;
}

static inline int bridge_set_events (ssrvctx_t *ctx)
{
    int rc = -1;
    if (ctx->sctx.in_sim) {
        if (reg_sim_events (ctx) != 0) {
            flux_log (ctx->h, LOG_ERR, "failed to reg sim events");
            goto done;
        }
        flux_log (ctx->h, LOG_INFO, "sim events registered");
    } else {
        if (reg_events (ctx) != 0) {
            flux_log (ctx->h, LOG_ERR, "failed to reg events");
            goto done;
        }
        flux_log (ctx->h, LOG_INFO, "events registered");
    }
    rc = 0;

done:
    return rc;
}

static inline int bridge_send_runrequest (ssrvctx_t *ctx, flux_lwj_t *job)
{
    int rc = -1;
    flux_t h = ctx->h;
    char *topic = NULL;
    flux_msg_t *msg = NULL;

    if (ctx->sctx.in_sim) {
        /* Emulation mode */
        if (asprintf (&topic, "%s.run.%"PRId64"", my_simexec_module, job->lwj_id) < 0) {
            flux_log (h, LOG_ERR, "(%s): topic create failed: %s",
                      __FUNCTION__, strerror (errno));
        }
        // TODO: move the "rpc_get" to before the simulator reply is
        // sent (to make asynchronous and improve throughput)
        flux_rpc_t *rpc = flux_rpc (h, topic, NULL, FLUX_NODEID_ANY, 0);
        if (rpc == NULL) {
            flux_log (h, LOG_ERR, "(%s): rpc failed: %s",
                      __FUNCTION__, strerror (errno));
        } else if (flux_rpc_get (rpc, NULL, NULL) < 0) {
            flux_log (h, LOG_ERR, "(%s): rpc_get failed: %s",
                      __FUNCTION__, strerror (errno));
        } else {
            queue_timer_change (ctx, my_simexec_module);
            flux_log (h, LOG_DEBUG, "job %"PRId64" runrequest", job->lwj_id);
            rc = 0;
        }
    } else {
        /* Normal mode */
        if (asprintf (&topic, "wrexec.run.%"PRId64"", job->lwj_id) < 0) {
            flux_log (h, LOG_ERR, "(%s): topic create failed: %s",
                      __FUNCTION__, strerror (errno));
        } else if (!(msg = flux_event_encode (topic, NULL))
                   || flux_send (h, msg, 0) < 0) {
            flux_log (h, LOG_ERR, "(%s): event create failed: %s",
                      __FUNCTION__, strerror (errno));
        } else {
            flux_log (h, LOG_DEBUG, "job %"PRId64" runrequest", job->lwj_id);
            rc = 0;
        }
    }

    if (msg)
        flux_msg_destroy (msg);
    if (topic)
        free (topic);
    return rc;
}

static inline void bridge_update_timer (ssrvctx_t *ctx)
{
    if (ctx->sctx.in_sim)
        queue_timer_change (ctx, ctx->sctx.module_name);
}

static inline int bridge_rs2rank_tab_query (ssrvctx_t *ctx, resrc_t *r,
                                            uint32_t *rank)
{
    int rc = -1;
    if (ctx->sctx.in_sim) {
        /*
        rc = rs2rank_tab_query_by_none (ctx->machs, resrc_digest (r),
                                        false, rank);
        */
        rc = 0;
    } else {
        flux_log (ctx->h, LOG_DEBUG, "hostname: %s, digest: %s", resrc_name (r),
                                     resrc_digest (r));
        rc = rs2rank_tab_query_by_sign (ctx->machs, resrc_name (r), resrc_digest (r),
                                        false, rank);
    }
    if (rc == 0)
        flux_log (ctx->h, LOG_DEBUG, "broker found, rank: %"PRIu32, *rank);
    else
        flux_log (ctx->h, LOG_ERR, "controlling broker not found!");

    return rc;
}

/********************************************************************************
 *                                                                              *
 *            Task Program Execution Service Request (RFC 8)                    *
 *                                                                              *
 *******************************************************************************/

static void inline build_contain_1node_req (int64_t nc, int64_t rank, JSON rarr)
{
    JSON e = Jnew ();
    JSON o = Jnew ();
    Jadd_int64 (o, JSC_RDL_ALLOC_CONTAINING_RANK, rank);
    Jadd_int64 (o, JSC_RDL_ALLOC_CONTAINED_NCORES, nc);
    json_object_object_add (e, JSC_RDL_ALLOC_CONTAINED, o);
    json_object_array_add (rarr, e);
}

/*
 * Because the job's rdl should only contain what's allocated to the job,
 * this traverse the entire tree post-order walk
 */
static int build_contain_req (ssrvctx_t *ctx, flux_lwj_t *job, JSON arr)
{
    int rc = -1;
    uint32_t rank = 0;
    resrc_tree_t *nd = NULL;
    resrc_t *r = NULL;
    int64_t corespernode = -1;
    char cpumask_key[100];
    char cpumask_val[100];
    char *cpumask_str_ptr = NULL;
    JSON reqobj= NULL;
    resrc_reqst_t *req = NULL;
    resrc_tree_list_t *core_list = NULL;
    resrc_tree_t *curr_core = NULL;
    int num_cores = 0;

    for (nd = resrc_tree_list_first (job->resrc_trees); nd;
            nd = resrc_tree_list_next (job->resrc_trees)) {
        r = resrc_tree_resrc (nd);
        if (strcmp (resrc_type (r), "node") != 0
            || bridge_rs2rank_tab_query (ctx, r, &rank) != 0)
            goto done;

        if (job->is_hierarchical) {
            corespernode = 1;
        } else {
            corespernode = job->req->corespernode;
        }
        build_contain_1node_req (corespernode, rank, arr);
        flux_log (ctx->h, LOG_DEBUG, "%s: hostname=%s, corespernode=%"PRId64", rank=%u",
                  __FUNCTION__, resrc_name (r), job->req->corespernode, rank);
        // code that inserts the cpumask at lwj.<lwjid>.rank.<rank>.cpumask
        // Leverages snippets from cpuset-str.[ch] to turn core ids into cstr
        // Leverages snippets from rsreader.c to find all cores under the given node
        sprintf (cpumask_key, "lwj-active.%"PRId64".rank.%u.cpumask", job->lwj_id, rank);

        reqobj = Jnew ();
        Jadd_str (reqobj, "type", "core");
        Jadd_int (reqobj, "req_qty", 1);
        req = resrc_reqst_from_json (reqobj, NULL);
        core_list = resrc_tree_list_new ();
        num_cores = resrc_tree_search (resrc_tree_children (nd), req, core_list, false);
        resrc_reqst_destroy (req);
        Jput (reqobj);

        assert (num_cores > 0);

        // TODO: can be optimized to aggregate runs using "-". Could
        // create a cpu_set_t then convert to cstr or just directly
        // build the cstr in a smarter way.
        cpumask_str_ptr = cpumask_val;
        for (curr_core = resrc_tree_list_first (core_list);
             curr_core;
             curr_core = resrc_tree_list_next (core_list)) {

             sprintf (cpumask_str_ptr, "%"PRId64",",
                      resrc_id (resrc_tree_resrc (curr_core)));
             // Move ptr to the end of the str we just sprint'd
             while (*cpumask_str_ptr != 0) {
                  cpumask_str_ptr++;
             }
        }
        // Delete the trailing comma
        cpumask_str_ptr--;
        *cpumask_str_ptr = 0;

        resrc_tree_list_destroy (core_list, false);
        kvs_put_string (ctx->h, cpumask_key, cpumask_val);
        flux_log (ctx->h, LOG_DEBUG, "%s: CPUMASK, key: %s, val: %s",
                  __FUNCTION__, cpumask_key, cpumask_val);
    }
    // kvs commit shouldn't be necessary here since a kvs commit will
    // be necessary later on when the job state is updated
    rc = 0;
done:
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
    const char *jcbstr = NULL;
    int rc = -1;
    flux_t h = ctx->h;
    JSON jcb = Jnew ();
    JSON arr = Jnew_ar ();
    JSON ro = Jnew_ar ();

    if (resrc_tree_list_serialize (ro, job->resrc_trees, job->lwj_id)) {
        flux_log (h, LOG_ERR, "%"PRId64" resource serialization failed: %s",
                  job->lwj_id, strerror (errno));
        goto done;
    }

    json_object_object_add (jcb, JSC_RDL, ro);
    jcbstr = Jtostr (jcb);
    if (jsc_update_jcb (h, job->lwj_id, JSC_RDL, jcbstr) != 0) {
        flux_log (h, LOG_ERR, "error jsc udpate: %"PRId64" (%s)", job->lwj_id,
                  strerror (errno));
        goto done;
    }
    Jput (jcb);
    jcb = Jnew ();
    if (build_contain_req (ctx, job, arr) != 0) {
        flux_log (h, LOG_ERR, "error requesting containment for job");
        goto done;
    }
    json_object_object_add (jcb, JSC_RDL_ALLOC, arr);
    jcbstr = Jtostr (jcb);
    if (jsc_update_jcb (h, job->lwj_id, JSC_RDL_ALLOC, jcbstr) != 0) {
        flux_log (h, LOG_ERR, "error updating jcb");
        goto done;
    }
    if (job->is_hierarchical) {
        JSON o = Jnew ();
        Jput (jcb);
        jcb = Jnew ();

        Jadd_int64 (o, JSC_RDESC_NNODES, job->req->nnodes);
        Jadd_int64 (o, JSC_RDESC_NTASKS, job->req->nnodes);
        Jadd_int64 (o, JSC_RDESC_WALLTIME, job->req->walltime);
        json_object_object_add (jcb, JSC_RDESC, o);
        jcbstr = Jtostr (jcb);

        if (jsc_update_jcb (h, job->lwj_id, JSC_RDESC, jcbstr) != 0) {
            flux_log (h, LOG_ERR, "error updating jcb");
            goto done;
        }
    }

    //Jput (arr);
    if ((update_state (h, job->lwj_id, job->state, J_ALLOCATED)) != 0) {
        flux_log (h, LOG_ERR, "failed to update the state of job %"PRId64"",
                  job->lwj_id);
        goto done;
    }
    bridge_update_timer (ctx);
    rc = 0;
done:
    if (jcb)
        Jput (jcb);
    return rc;
}

#if 0
static int req_tpexec_grow (flux_t h, flux_lwj_t *job, resrc_tree_list_t *resrc_tree_list)
{
    /* TODO: NOT IMPLEMENTED */
    /* This runtime grow service will grow the resource set of the job.
       The special non-elastic case will be to grow the resource from
       zero to the selected RDL
    */

    /* Add resources
     * 1. take each resrc_tree from resrc_tree_list.
     * 2. Compare that with each of job's tree_list.
     * 3. If there is a match, then do the add child special
     * 4. Otherwise, append it to the list */
    resrc_tree_t *resrc_tree = resrc_tree_list_first (resrc_tree_list);
    while (resrc_tree) {
        resrc_t *resrc = resrc_tree_resrc (resrc_tree);
        resrc_tree_t *job_resrc_tree = resrc_tree_list_first (job->resrc_trees);
        while (job_resrc_tree) {
            resrc_t *job_resrc = resrc_tree_resrc (resrc_tree);
            char uuid[40] = {0}, juuid[40] = {0};
            resrc_uuid (resrc, uuid);
            resrc_uuid (resrc, juuid);
            if (!(strcmp (uuid, juuid))) {
                // add children
                resrc_tree_add_child_special (job_resrc_tree, resrc_tree);
                goto next;
            }
            resrc_tree_list_next (job->resrc_trees);
        }
        resrc_tree_list_append (job->resrc_trees, resrc_tree);
next:
        resrc_tree_list_next (resrc_tree_list);
    }

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
    ssrvctx_t *ctx = getctx (h);
    int rc = -1;

    if ((update_state (h, job->lwj_id, job->state, J_RUNREQUEST)) != 0) {
        flux_log (h, LOG_ERR, "failed to update the state of job %"PRId64"",
                  job->lwj_id);
        goto done;
    } else if (bridge_send_runrequest (ctx, job) != 0) {
        flux_log (h, LOG_ERR, "failed to send runrequest for job %"PRId64"",
                  job->lwj_id);
        goto done;
    }
    rc = 0;
done:
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

/*
 * schedule_job() searches through all of the idle resources to
 * satisfy a job's requirements.  If enough resources are found, it
 * proceeds to allocate those resources and update the kvs's lwj entry
 * in preparation for job execution.  If less resources
 * are found than the job requires, and if the job asks to reserve
 * resources, then those resources will be reserved.
 */
int schedule_job (ssrvctx_t *ctx, flux_lwj_t *job, int64_t starttime)
{
    JSON child_core = NULL;
    JSON req_res = NULL;
    flux_t h = ctx->h;
    int rc = -1;
    int64_t nnodes = 0;
    resrc_reqst_t *resrc_reqst = NULL;
    resrc_tree_list_t *found_trees = NULL;
    resrc_tree_list_t *selected_trees = NULL;
    struct sched_plugin *plugin = sched_plugin_get (ctx->loader);

    if (!plugin)
        return rc;

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
    Jadd_int64 (child_core, "req_qty", job->req->corespernode);
    /* setting size == 1 devotes (all of) the core to the job */
    Jadd_int64 (child_core, "req_size", 1);
    /* setting exclusive to true prevents multiple jobs per core */
    Jadd_bool (child_core, "exclusive", true);
    Jadd_int64 (child_core, "starttime", starttime);
    Jadd_int64 (child_core, "endtime", starttime + job->req->walltime);

    req_res = Jnew ();
    Jadd_str (req_res, "type", "node");
    Jadd_int64 (req_res, "req_qty", job->req->nnodes);
    if (job->req->node_exclusive) {
        Jadd_int64 (req_res, "req_size", 1);
        Jadd_bool (req_res, "exclusive", true);
    } else {
        Jadd_int64 (req_res, "req_size", 0);
        Jadd_bool (req_res, "exclusive", false);
    }
    Jadd_int64 (req_res, "starttime", starttime);
    Jadd_int64 (req_res, "endtime", starttime + job->req->walltime);

    flux_log (h, LOG_DEBUG, "%s: id = %ld, nnodes = %ld, corespernode = %ld, starttime = %"PRId64", walltime = %ld", __FUNCTION__, job->lwj_id, job->req->nnodes, job->req->corespernode, starttime, job->req->walltime);

    json_object_object_add (req_res, "req_child", child_core);

    resrc_reqst = resrc_reqst_from_json (req_res, NULL);
    flux_log (h, LOG_DEBUG, "%s: resrc_reqst: %s", __FUNCTION__, Jtostr (req_res));
    Jput (req_res);
    if (!resrc_reqst)
        goto done;

    int i = 0;
    flux_lwj_t *curr_prev_attempt = NULL;
    for (curr_prev_attempt = (flux_lwj_t*) zlist_first (ctx->prev_attempts);
         curr_prev_attempt;
         curr_prev_attempt = (flux_lwj_t*) zlist_next (ctx->prev_attempts), i++) {

        if ((curr_prev_attempt->req->nnodes <= job->req->nnodes) &&
            (curr_prev_attempt->req->corespernode <= job->req->corespernode) &&
            (curr_prev_attempt->req->walltime <= job->req->walltime)) {

            flux_log (h, LOG_DEBUG,
                      ": Job %ld will fail, therefore going for reservation directly (had to go %d deep)",
                      job->lwj_id, i);
            zlist_append (ctx->prev_attempts, job);
            plugin->reserve_resources (h, NULL, job->lwj_id,
                                       starttime, job->req->walltime,
                                       ctx->rctx.root_resrc,
                                       resrc_reqst);
            rc = 0;
            goto done;
        }
    }



    flux_log (h, LOG_DEBUG, "%s: Starting finding resources", __FUNCTION__);
    if ((found_trees = plugin->find_resources (h, ctx->rctx.root_resrc,
                                                 resrc_reqst))) {
        nnodes = resrc_tree_list_size (found_trees);
        flux_log (h, LOG_DEBUG, "%"PRId64" nodes found for job %"PRId64", "
                  "reqrd: %"PRId64"", nnodes, job->lwj_id, job->req->nnodes);

        resrc_tree_list_unstage_resources (found_trees);
        flux_log (h, LOG_DEBUG, "%s: Starting select resources", __FUNCTION__);
        if ((selected_trees = plugin->select_resources (h, found_trees,
                                                          resrc_reqst))) {
            nnodes = resrc_tree_list_size (selected_trees);
            if (nnodes == job->req->nnodes) {
                flux_log (h, LOG_DEBUG, "%s: Starting allocate resources from %"PRId64" to %"PRId64"", __FUNCTION__, starttime, starttime + job->req->walltime);
                plugin->allocate_resources (h, selected_trees, job->lwj_id,
                                              starttime, starttime +
                                              job->req->walltime);
                /* Scheduler specific job transition */
                // TODO: handle this some other way (JSC?)
                job->starttime = starttime;
                job->state = J_SELECTED;
                job->resrc_trees = selected_trees;
                if (req_tpexec_allocate (ctx, job) != 0) {
                    flux_log (h, LOG_ERR,
                              "failed to request allocate for job %"PRId64"",
                              job->lwj_id);
                    resrc_tree_list_destroy (job->resrc_trees, false);
                    job->resrc_trees = NULL;
                    goto done;
                }
                flux_log (h, LOG_DEBUG, "Allocated %"PRId64" nodes for job "
                          "%"PRId64", reqrd: %"PRId64"", nnodes, job->lwj_id,
                          job->req->nnodes);
            } else {
                flux_log (h, LOG_DEBUG, "%s: Couldn't select trees, only found %"PRId64" nodes", __FUNCTION__, nnodes);
                zlist_append (ctx->prev_attempts, job);
                plugin->reserve_resources (h, selected_trees, job->lwj_id,
                                           starttime, job->req->walltime,
                                           ctx->rctx.root_resrc,
                                           resrc_reqst);
           }
        }
    } else {
        flux_log (h, LOG_DEBUG, "%s: Couldn't find trees at all", __FUNCTION__);
        zlist_append (ctx->prev_attempts, job);
        plugin->reserve_resources (h, selected_trees, job->lwj_id,
                                   starttime, job->req->walltime,
                                   ctx->rctx.root_resrc,
                                   resrc_reqst);
    }

    rc = 0;
done:
    if (resrc_reqst)
        resrc_reqst_destroy (resrc_reqst);
    if (found_trees)
        resrc_tree_list_destroy (found_trees, false);

    return rc;
}


int schedule_dynamic_job (ssrvctx_t *ctx, flux_lwj_t *job)
{
    int i = 0;
    int rc = -1;
    int len = 0;
    resrc_reqst_t *resrc_reqst = NULL;
    resrc_tree_list_t *found_trees = NULL;
    resrc_tree_list_t *selected_trees = NULL;

    flux_t h = ctx->h;
    int64_t nnodes = 0;

    int64_t reqd_nodes = 0;
    int64_t reqd_cores = 0;
    int64_t reqd_corespernode = 0;
    int64_t walltime = 0;

    struct sched_plugin *plugin = sched_plugin_get (ctx->loader);

    flux_log (h, LOG_DEBUG, "%s: Scheduling dynamic instance job : %ld", __FUNCTION__, job->lwj_id);

    if (!(job->slackinfo->need)) {
        flux_log (h, LOG_DEBUG, "%s: Job does not have any need", __FUNCTION__);
        return rc;
    }
    flux_log (h, LOG_DEBUG, "%s: Job has needs", __FUNCTION__);

    if (!(Jget_ar_len (job->slackinfo->need, &len))) {
        flux_log (h, LOG_ERR, "Could not job needs len");
        return rc;
    }
    flux_log (h, LOG_DEBUG, "%s: Job needs len = %d", __FUNCTION__, len);

    for (i = 0; i < len; i++) {

        int64_t starttime = resrc_epochtime ();
        JSON needobj = NULL;
        JSON req_res = Jnew ();
        JSON child_core = Jnew ();

        if (!(Jget_ar_obj (job->slackinfo->need, i, &needobj))) {
            flux_log (h, LOG_ERR, "Could not fetch need i = %d", i);
            goto next;
        } else if (!(Jget_int64 (needobj, "nnodes", &reqd_nodes))) {
            flux_log (h, LOG_ERR, "Could not fetch the required nodes");
            goto next;
        } else if (!(Jget_int64 (needobj, "ncores", &reqd_cores))) {
            flux_log (h, LOG_ERR, "Could not fetch the required nodes");
            goto next;
        } else if (!(Jget_int64 (needobj, "walltime", &walltime))) {
            flux_log (h, LOG_ERR, "Could not fetch walltime");
            goto next;
        }

        //create req_res
        reqd_nodes = (reqd_nodes ? reqd_nodes : 1);
        if (reqd_cores < reqd_nodes)
            reqd_cores = reqd_nodes;
        reqd_corespernode = (reqd_cores + reqd_nodes - 1) / reqd_nodes;
        Jadd_str (child_core, "type", "core");
        Jadd_int64 (child_core, "req_qty", reqd_corespernode);
        Jadd_int64 (child_core, "req_size", 1);
        Jadd_bool (child_core, "exclusive", true);
        Jadd_int64 (child_core, "starttime", starttime);
        Jadd_int64 (child_core, "endtime", starttime + walltime);

        Jadd_str (req_res, "type", "node");
        Jadd_int (req_res, "req_qty", reqd_nodes);
        Jadd_int64 (req_res, "req_size", 0);
        Jadd_int64 (req_res, "starttime", starttime);
        Jadd_int64 (req_res, "endtime", starttime + walltime);
        json_object_object_add (req_res, "req_child", child_core);

        flux_log (h, LOG_DEBUG,
                  "%s: need #%d, nnodes = %"PRId64", corespernode = %"PRId64", starttime = %"PRId64", walltime = %"PRId64"",
                  __FUNCTION__, i, reqd_nodes, reqd_corespernode,
                  starttime, walltime);
        resrc_reqst = resrc_reqst_from_json (req_res, NULL);
        //Jput (req_res);
        if (!resrc_reqst) {
            flux_log (h, LOG_DEBUG, "%s: Could not form resrc_reqst from json", __FUNCTION__);
            goto next;
        }
        flux_log (h, LOG_DEBUG, "%s: Created resrc_reqst:", __FUNCTION__);
        if (ctx->arg.verbosity > 0) {
            resrc_reqst_print (resrc_reqst);
        }
        if ((found_trees = plugin->find_resources (h, ctx->rctx.root_resrc,
                                                     resrc_reqst))) {
            nnodes = resrc_tree_list_size (found_trees);
            flux_log (h, LOG_DEBUG, "%"PRId64" nodes found for job %"PRId64"", nnodes, job->lwj_id);
            resrc_tree_list_unstage_resources (found_trees);
            flux_log (h, LOG_DEBUG, "%s: Starting select resources", __FUNCTION__);
            if ((selected_trees = plugin->select_resources (h, found_trees,
                                                              resrc_reqst))) {
                nnodes = resrc_tree_list_size (selected_trees);
                flux_log (h, LOG_DEBUG, "%s: nnodes found = %ld", __FUNCTION__, nnodes);
                if (nnodes == reqd_nodes) {
                    flux_log (h, LOG_DEBUG, "%s: Found the nodes required", __FUNCTION__);
                    //resrc_tree_list_flux_log (h, found_trees);

                    //Mark/Allocate these resources
                    flux_log (h, LOG_DEBUG,
                              "%s: Starting allocate resources from %"PRId64" to %"PRId64"",
                              __FUNCTION__, starttime, starttime + walltime + SLACK_BUFFER_TIME);
                    resrc_tree_list_allocate_dynamic (selected_trees, job->lwj_id, starttime, starttime + walltime + SLACK_BUFFER_TIME);

                    //Split new and old resources, in other words, new and return resources
                    resrc_tree_list_t *to_serialize_tree_list = NULL;
                    JSON ret_array = Jnew_ar ();
                    to_serialize_tree_list = resrc_tree_split_resources (h, selected_trees,
                                                                         job->resrc_trees,
                                                                         ret_array, job->lwj_id);

                    flux_log (h, LOG_DEBUG, "%s: return array = %s", __FUNCTION__, Jtostr (ret_array));
                    JSON new_serialized = Jnew_ar ();
                    if (resrc_tree_list_size (to_serialize_tree_list) > 0) {
                        flux_log (h, LOG_DEBUG, " Printing the list of resources to serialize");
                        resrc_tree_list_flux_log (h, to_serialize_tree_list);
                        flux_log (h, LOG_DEBUG, " Finished printing the list of resources to serialize");
                        resrc_tree_list_serialize (new_serialized, to_serialize_tree_list, job->lwj_id);
                        flux_log (h, LOG_DEBUG, "%s: Serialized dynamic new: %s", __FUNCTION__, Jtostr(new_serialized));
                    }

                    /*
                     * TODO: add selected_trees to job's slackinfo
                     *       struct for easy reaping once the child
                     *       job completes and just all around better
                     *       accounting
                     */

                    //Send resources to job
                    JSON j = Jnew ();
                    Jadd_obj (j, "return", ret_array);
                    Jadd_obj (j, "new", new_serialized);
                    send_to_child (ctx, job, j, "sched.slack.addresources");
                    Jput (j);

                    Jput (new_serialized);

                    if (to_serialize_tree_list) {
                        resrc_tree_list_destroy (to_serialize_tree_list, false);
                    }

                    resrc_tree_list_unstage_resources (found_trees);
                }
            }

        } else {
            flux_log (h, LOG_DEBUG, "%s: Couldn't find trees for need", __FUNCTION__);
        }

next:
        nnodes = 0;
        reqd_nodes = 0;
        reqd_cores = 0;
        //Jput (child_core);
        //Jput (req_res);
        if (resrc_reqst)
            resrc_reqst_destroy (resrc_reqst);
        if (found_trees)
            resrc_tree_list_destroy (found_trees, false);
        continue;
    }

    return rc;
}

static int schedule_jobs (ssrvctx_t *ctx)
{
    int rc = 0;
    int c = 0;
    flux_lwj_t *job = NULL;
    flux_t h = ctx->h;
    struct sched_plugin *plugin = sched_plugin_get (ctx->loader);

    zlist_purge (ctx->prev_attempts);
    /* Prioritizing the job queue is left to an external agent.  In
     * this way, the scheduler's activities are pared down to just
     * scheduling activies.
     * TODO: when dynamic scheduling is supported, the loop should
     * traverse through running job queue as well.
     */

    if (!plugin)
        return -1;

    zlist_t *jobs = ctx->p_queue;
    int64_t starttime = (ctx->sctx.in_sim) ?
        (int64_t) floor(ctx->sctx.sim_state->sim_time) : resrc_epochtime();

    resrc_tree_release_all_reservations (resrc_phys_tree (ctx->rctx.root_resrc));
    rc = plugin->sched_loop_setup (ctx->h);
    job = zlist_first (jobs);
    flux_log (h, LOG_DEBUG, "%s: Released reservations, about to start scheduling with starttime = %"PRId64"", __FUNCTION__, starttime);
    while (!rc && job) {
        if (job->state == J_SCHEDREQ) {
            rc = schedule_job (ctx, job, starttime);
            if (rc < 0) {
                flux_log (h, LOG_ERR, "Could not schedule job : %ld", job->lwj_id);
            }
            if (job->state == J_SELECTED) {
                flux_log (ctx->h, LOG_DEBUG, "Job %ld has changed to state selected", job->lwj_id);
                c++;
            }
        }
        job = (flux_lwj_t*)zlist_next (jobs);
    }

    return c;
}

static int dynamic_action (ssrvctx_t *ctx)
{
    int sc = 0;
    int rc = 0;
    flux_lwj_t *job = NULL;
    flux_t h = ctx->h;

    /* schedule own jobs */
    sc = schedule_jobs (ctx);
    if (sc > 0) {
        flux_log (h, LOG_DEBUG, "%s: a job in this instance was scheduled using slack resources", __FUNCTION__);
    }
    /* try to schedule needs */
    job = zlist_first (ctx->i_queue);
    while (!rc && job) {
        flux_log (h, LOG_DEBUG, "%s: Considering expansion of job: %ld", __FUNCTION__, job->lwj_id);
        if (job->slackinfo->slack_state != CSLACK) {
            flux_log (h, LOG_DEBUG, "%s: Instance job %ld not in CLSACK. Not scheduling.", __FUNCTION__, job->lwj_id);
            goto next;
        }
        rc = schedule_dynamic_job (ctx, job);
        if (rc <= 0) {
            flux_log (h, LOG_DEBUG, "%s: Job : %ld, not expanded", __FUNCTION__, job->lwj_id);
            rc = 0;
            goto next;
        }
        flux_log (h, LOG_DEBUG, "%s: Job: %ld has been expanded", __FUNCTION__, job->lwj_id);
        sc++;
next:
        job = (flux_lwj_t*)zlist_next (ctx->i_queue);
    }

    if (sc > 0 && ctx->arg.verbosity > 0) {
        flux_log (h, LOG_DEBUG, "%s: jobs were scheduled, printing rdl:", __FUNCTION__);
        resrc_tree_flux_log (h, resrc_phys_tree (ctx->rctx.root_resrc));
    }

    return sc;
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
        flux_log (h, LOG_INFO, "Job %ld going to submitted", job->lwj_id);
        VERIFY (trans (J_PENDING, J_PENDING, &(job->state)));
        fill_hierarchical_req (h, job);
        /* fall through for implicit event generation */
    case J_PENDING:
        flux_log (h, LOG_INFO, "Job %ld going from submitted to pending", job->lwj_id);
        VERIFY (trans (J_SCHEDREQ, J_SCHEDREQ, &(job->state)));
        jobsubcount++;
        if (ctx->sctx.in_sim && jobsubcount == jobcount) {
            flux_log (h, LOG_DEBUG, "%s: All jobs (%d) submitted, scheduling jobs",
                      __FUNCTION__, jobsubcount);
            int num_jobs_scheduled = schedule_jobs (ctx);
            if (num_jobs_scheduled > 0&& ctx->arg.verbosity > 0) {
                flux_log (h, LOG_DEBUG, "%s: jobs were scheduled, printing rdl:", __FUNCTION__);
                resrc_tree_flux_log (h, resrc_phys_tree (ctx->rctx.root_resrc));
            }
        } else if (!ctx->sctx.in_sim) {
            int num_jobs_scheduled = schedule_jobs (ctx);
            if (ctx->arg.verbosity > 0) {
                flux_log (h, LOG_DEBUG, "%s: %d jobs were scheduled", __FUNCTION__, num_jobs_scheduled);
            }
        }
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
        flux_log (h, LOG_INFO, "Total number of jobs in pending queue: %zu", zlist_size (ctx->p_queue));
        break;
    case J_RUNNING:
        VERIFY (trans (J_COMPLETE, newstate, &(job->state))
                || trans (J_CANCELLED, newstate, &(job->state)));
        if (!ctx->arg.schedonce && (resrc_tree_list_release (job->resrc_trees, job->lwj_id))) {
            flux_log (h, LOG_ERR, "(%s): failed to release resources for job "
                      "%"PRId64"", __FUNCTION__, job->lwj_id);
        } else if (reap_child_slack_resources (ctx, job) < 0) {
            flux_log (h, LOG_ERR, "(%s): failed to reap slack resources of child job #%"PRId64"",
                      __FUNCTION__, job->lwj_id);
        } else {
            flux_msg_t *msg = flux_event_encode ("sched.res.freed", NULL);

            if (!msg || flux_send (h, msg, 0) < 0) {
                flux_log (h, LOG_ERR, "(%s): error sending event: %s",
                          __FUNCTION__, strerror (errno));
            } else {
                flux_msg_destroy (msg);
            }
            if (ctx->arg.verbosity > 0) {
                flux_log (h, LOG_DEBUG,
                          "Released resources for job %"PRId64", printing root resrc tree:",
                          job->lwj_id);
                resrc_tree_flux_log (ctx->h, resrc_phys_tree (ctx->rctx.root_resrc));
            }
        }
        q_move_to_cqueue (ctx, job);
        if (ctx->sctx.in_sim) {
            //queue_timer_change (ctx, ctx->sctx.module_name);
            ctx->sctx.ccompletecount++;
            flux_log (h, LOG_DEBUG, "%s: ++++++++++++ completecount incremented = %d", __FUNCTION__, ctx->sctx.ccompletecount);
            if (job->is_hierarchical) {
                // TODO: move this code over to sim_execsrv
                add_job_complete_time (ctx, job);
            }
            if (ctx->sctx.cdeathcount > 0) {
                int64_t *jobid = zlist_first (ctx->sctx.dying_jobs);
                while (jobid) {
                    if (job->lwj_id == *jobid) {
                        zlist_remove (ctx->sctx.dying_jobs, jobid);
                        free (jobid);
                        ctx->sctx.cdeathcount--;
                        flux_log (h, LOG_DEBUG, "%s: Job expected to die died: %ld, deathcount = %d", __FUNCTION__, job->lwj_id, ctx->sctx.cdeathcount);
                        break;
                    }
                    jobid = zlist_next (ctx->sctx.dying_jobs);
                }
            }
        }
        flux_log (h, LOG_INFO, "Total number of jobs in pending queue: %zu", zlist_size (ctx->p_queue));
        break;
    case J_CANCELLED:
        VERIFY (trans (J_REAPED, newstate, &(job->state)));
        zlist_remove (ctx->c_queue, job);
        if (job->req)
            free (job->req);
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
    ssrvctx_t *ctx = getctx (h);
#ifdef DYNAMIC_SCHEDULING

    flux_log (h, LOG_DEBUG, "%s: Resource event, job completion detected", __FUNCTION__);

    JSON ao = Jnew_ar ();
    JSON jao = Jnew ();
    resrc_collect_own_resources_unasked (h, ctx->rctx.resrc_id_hash, ao);  // uuid_array only
    resrc_retrieve_lease_information (ctx->rctx.resrc_id_hash, ao, jao); // jobid : uuid_array, jobid : uuid_array

    flux_log (h, LOG_DEBUG, "%s: Lease information : %s", __FUNCTION__, Jtostr (jao));

    /* If in cslack, send invalid to parent and ask resources back if any */
    if (ctx->slctx.slack_state == CSLACK) {
        JSON pao = NULL;
        if (Jget_obj (jao, "-1", &pao)) {
            flux_log (h, LOG_DEBUG, "%s: Sending slack invalid and asking for resources to parent: %s", __FUNCTION__, Jtostr (pao));
            send_cslack_invalid (ctx, pao);
            resrc_mark_resources_asked (ctx->rctx.resrc_id_hash, pao);
        } else {
            flux_log (h, LOG_DEBUG, "%s: Sending slack invalid and NOT asking anything", __FUNCTION__);
            send_cslack_invalid (ctx, NULL);
        }
    }

    flux_log (h, LOG_DEBUG, "%s: Will check if anything to be asked from children", __FUNCTION__);
    /* ask resources if any have been leased to children, i.key is string, i.val is json array */
    json_object_iter i;
    json_object_object_foreachC (jao, i) {

        flux_log (h, LOG_DEBUG, "%s: Dealing with resource from %s with %s", __FUNCTION__, i.key, Jtostr (i.val));

        /* ask parent only if not already asked due to cslack state */
        if (!(strcmp (i.key, "-1"))) {
            if (ctx->slctx.slack_state != CSLACK) {
                send_to_parent (ctx, i.val, "sched.slack.askresources");
                resrc_mark_resources_asked (ctx->rctx.resrc_id_hash, i.val);
            }
            continue;
        }

        int64_t jobid = strtol (i.key, NULL, 10);
        flux_lwj_t *job = find_job_in_queue (ctx->i_queue, jobid);
        if (!job) {
            flux_log (h, LOG_ERR, "FATAL resource owner does not exist");
            continue;
        }

        if (send_to_child (ctx, job, i.val, "sched.slack.askresources") < 0) {
            flux_log (h, LOG_ERR, "Could not send askresources to %ld", jobid);
            continue;
        }

        resrc_mark_resources_asked  (ctx->rctx.resrc_id_hash, i.val);
        flux_log (h, LOG_DEBUG, "%s: resources asked to job : %ld", __FUNCTION__, jobid);
    }

    /* change slack state */
    ctx_slack_invalidate (ctx);
    if (ctx->slctx.resource_out_count > 0) {
        flux_log (h, LOG_DEBUG, "%s: wait until return is being set as true, current resource_out_count is %"PRId64"",
                  __FUNCTION__, ctx->slctx.resource_out_count);
        ctx->slctx.wait_until_return = 1;
    }
#else
    schedule_jobs (ctx);
#endif
    //Jput (ao);
    //Jput (jao);


    return;
}

static void timer_event_cb (flux_reactor_t *r, flux_watcher_t *w,
                           int revents, void *arg)
{
    clock_t start, diff;
    double seconds;
    flux_t h = (flux_t) arg;
    ssrvctx_t *ctx = getctx (h);
    int64_t jobid;
    int psize, rsize;
    int rc = 0;


    start = clock ();
    flux_log (h, LOG_DEBUG, "TIMER CALLED: %ld", resrc_epochtime());
    //schedule_jobs(ctx);

    /* 1. If there are any resources overtime, return them */
    return_idle_slack_resources (ctx);
    flux_log (h, LOG_DEBUG, "%s: return idle resources successful", __FUNCTION__);

    /* 2. If already in cslack, nothing to do */
    if (ctx->slctx.slack_state == CSLACK) {
        flux_log (h, LOG_ERR, "%s: instance already in cslack. doing nothing", __FUNCTION__);
        goto ret;
    }
    flux_log (h, LOG_DEBUG, "%s: instance not in cslack", __FUNCTION__);
    //schedule_jobs (ctx);

    /* 3. If last 2 timers, no change, then slack */
    retrieve_queue_info (ctx, &jobid, &psize, &rsize);
    if (ctx->slctx.slack_state == SLACK) {
        flux_log (h, LOG_DEBUG, "%s: Instance already in slack", __FUNCTION__);
    } else if ((ctx->slctx.jobid == jobid) && (ctx->slctx.psize == psize) && (ctx->slctx.rsize == rsize)) {
        flux_log (h, LOG_DEBUG, "%s: Instance moving to SLACK state", __FUNCTION__);
        ctx->slctx.slack_state = SLACK;
    } else {
        flux_log (h, LOG_DEBUG, "%s: updating recent job information", __FUNCTION__);
        ctx->slctx.jobid = jobid;
        ctx->slctx.psize = psize;
        ctx->slctx.rsize = rsize;
    }

    /* 4. If in slack and all children cslack, then action */
    if ((ctx->slctx.slack_state == SLACK)
        // TODO: determine if this is a good idea or not
        //&& (all_children_cslack(ctx))
        ) {
        flux_log (h, LOG_DEBUG, "%s: instance in slack. taking dynamic action", __FUNCTION__);
        rc = dynamic_action(ctx);  // returns number of jobs started/expanded
        if (rc > 0) {
            flux_log (h, LOG_DEBUG, "%s: started/expanded %d jobs, not changing to cslack", __FUNCTION__, rc);
            goto ret;
        } else {
            flux_log (h, LOG_DEBUG, "%s: no jobs started/expanded. going into cslack", __FUNCTION__);
            if (all_children_cslack (ctx)) {
                ctx->slctx.slack_state = CSLACK;
            }
        }
    }

    /* 5. If in cslack, submit to parent */
    //if ((ctx->slctx.slack_state == CSLACK)) {
    if ((ctx->parent_uri) && (ctx->slctx.slack_state == CSLACK)) {
        JSON jmsg = Jnew ();
        int rcount = 0;
        flux_log (h, LOG_DEBUG, "%s: going to compute surplus", __FUNCTION__);
        accumulate_surplus_need (ctx, 1, jmsg, &rcount);
        flux_log (h, LOG_DEBUG, "%s: surplus accumulated, rcount = %d", __FUNCTION__, rcount);
        if (send_cslack (ctx, jmsg) < 0) {
            flux_log (h, LOG_DEBUG, "%s: Could not send slack to parent. Error or parent does not implement push model.", __FUNCTION__);
            goto ret;
        } else {
            flux_log (h, LOG_DEBUG, "%s: CSLACK successfully sent to parent", __FUNCTION__);
            JSON so = NULL;
            if ((Jget_obj (jmsg, "surplus", &so))) {
                flux_log (h, LOG_DEBUG, "%s: Going to mark resource slacksub/returned", __FUNCTION__);
                resrc_mark_resources_slacksub_or_returned (ctx->rctx.resrc_id_hash, so); // uuid : endtime uuid: endtime
                flux_log (h, LOG_DEBUG, "%s: Marked slacksub/retured. Going to destroy returned resources", __FUNCTION__);
                resrc_tree_destroy_returned_resources (resrc_phys_tree (ctx->rctx.root_resrc), ctx->rctx.resrc_id_hash);
                flux_log (h, LOG_DEBUG, "%s: Destroyed returned resources", __FUNCTION__);
            }
            ctx->slctx.resource_out_count += rcount;
            flux_log (h, LOG_DEBUG, "%s: current resource_out_count is %"PRId64"",
                      __FUNCTION__, ctx->slctx.resource_out_count);
        }
        Jput (jmsg);
    }
    rc = 0;

ret:
    diff = clock () - start;
    seconds = ((double)diff) / CLOCKS_PER_SEC;

    flux_log (h,
              LOG_DEBUG,
              "timer trigger took %f seconds",
              seconds);
    flux_log (h, LOG_DEBUG, "%s: exiting timer", __FUNCTION__);
    return;
}

static int job_status_cb (const char *jcbstr, void *arg, int errnum)
{
    JSON jcb = NULL;
    ssrvctx_t *ctx = getctx ((flux_t)arg);
    flux_lwj_t *j = NULL;
    job_state_t ns = J_FOR_RENT;

    if (errnum > 0) {
        flux_log (ctx->h, LOG_ERR, "job_status_cb: errnum passed in");
        return -1;
    }
    if (!(jcb = Jfromstr (jcbstr))) {
        flux_log (ctx->h, LOG_ERR, "job_status_cb: error parsing JSON string");
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

static void req_death_cb (flux_t h, flux_msg_handler_t *w, const flux_msg_t *msg, void *arg)
{
    ssrvctx_t *ctx = getctx (h);
    JSON in = NULL;
    if (ctx->sctx.in_sim) {
        const char *json_str = NULL;
        if (flux_request_decode (msg, NULL, &json_str) < 0 || json_str == NULL
            || !(in = Jfromstr (json_str))) {
            flux_log (h, LOG_ERR, "%s: bad message", __FUNCTION__);
            Jput (in);
            return;
        }
        int64_t jobid = -1;
        Jget_int64 (in, "jobid", &jobid);
        flux_log (h, LOG_DEBUG, "%s: Received a death message from Job: %ld", __FUNCTION__, jobid);
        flux_lwj_t *job = q_find_job (ctx, jobid);
        if (job) {
            int64_t *xjobid = malloc (sizeof(int64_t));
            *xjobid = jobid;
            zlist_append (ctx->sctx.dying_jobs, xjobid);
            ctx->sctx.cdeathcount++;
            flux_log (h, LOG_DEBUG, "%s: Deathcount increased to %d", __FUNCTION__, ctx->sctx.cdeathcount);
        }
    }

    flux_respond (h, msg, 0, NULL);
    flux_log (h, LOG_DEBUG, "%s: Response to death message sent", __FUNCTION__);
}

static void childbirth_cb (flux_t h, flux_msg_handler_t *w, const flux_msg_t *msg, void *arg)
{
    int64_t jobid = -1;
    JSON in = NULL;
    const char *child_uri = NULL;
    //const char *json_in = NULL;
    ssrvctx_t *ctx = getctx (h);
    flux_lwj_t *job = NULL;
    const char *json_str = NULL;

    flux_log (h, LOG_DEBUG, "%s: Child has scheduler!", __FUNCTION__);
    if (flux_msg_get_payload_json (msg, &json_str) < 0 || json_str == NULL
        || !(in = Jfromstr (json_str))) {
        flux_log (h, LOG_ERR, "%s: bad message", __FUNCTION__);
        goto done;
    }
    flux_log (h, LOG_DEBUG, "%s: Message received = %s", __FUNCTION__, Jtostr(in));

    /* Decode information */
    Jget_int64 (in, "jobid", &jobid);
    Jget_str (in, "child-uri", &child_uri);
    job = q_find_job (ctx, jobid);
    if (job)
        job->contact = xstrdup (child_uri);

 done:
    Jput (in);
}

static void req_qstat_cb (flux_t h, flux_msg_handler_t *w, const flux_msg_t *msg, void *arg)
{
    int psize = -1;
    int rsize = -1;
    ssrvctx_t *ctx = NULL;
    JSON out = Jnew ();

    /* debug */
    //flux_log (h, LOG_DEBUG, "%s: Received qstat request: %s ================ %s", __FUNCTION__, (char *) *zmsg, (char *) arg);

    /* get the context */
    if (!(ctx = getctx (h))) {
        flux_log (h, LOG_ERR, "cannot find context");
        goto send;
    }

    /* get size of the pending and running queues */
    psize = zlist_size (ctx->p_queue);
    rsize = zlist_size (ctx->r_queue);

    /* send the size as json */
send:
    Jadd_int (out, "psize", psize);
    Jadd_int (out, "rsize", rsize);

    char *jstr = xstrdup (Jtostr (out));
    flux_respond (h, msg, 0, jstr);

    free (jstr);
    Jput (out);
}

/*
 * receive_resources_parent_from_child: Parent receives surplus resources from child
 */
static void req_cslack_cb (flux_t h, flux_msg_handler_t *w, const flux_msg_t *msg, void *arg)
{
    flux_log (h, LOG_DEBUG, "%s: Callback on reporting surplus/need called", __FUNCTION__);

    int64_t jobid = -1;
    flux_lwj_t *job = NULL;
    ssrvctx_t *ctx = getctx (h);
    const char *json_str = NULL;

    JSON in = NULL;
    JSON jmsg = NULL;
    JSON surobj = NULL;
    JSON needobj = NULL;

    if (flux_request_decode (msg, NULL, &json_str) < 0 || json_str == NULL
        || !(in = Jfromstr (json_str))) {
        flux_log (h, LOG_ERR, "%s: bad message", __FUNCTION__);
        Jput (in);
        return;
    }

    /* get information */
    Jget_int64 (in, "jobid", &jobid);

    /* get job */
    job = find_job_in_queue (ctx->i_queue, jobid);
    if (!job) {
        flux_log (h, LOG_ERR, "Job not found");
        goto ret;
    }
    flux_log (h, LOG_DEBUG, "%s: Job has been found", __FUNCTION__);

    /* Update surplus and need */
    const char *jmsg_str = NULL;
    if (!(Jget_str (in, "msg", &jmsg_str))) {
        flux_log (h, LOG_DEBUG, "%s: No surplus or need provided", __FUNCTION__);
        goto ret;
    }
    jmsg = Jfromstr (jmsg_str);
    flux_log (h, LOG_DEBUG, "%s: surplus or need information provided", __FUNCTION__);

    if (Jget_obj (jmsg, "surplus", &surobj)) {
        flux_log (h, LOG_DEBUG, "%s: jobid = %ld and surplus = %s", __FUNCTION__, jobid, Jtostr (surobj));
        /* Update slack reservation */
        json_object_iter iter;
        json_object_object_foreachC (surobj, iter) {
            resrc_t *resrc = zhash_lookup (ctx->rctx.resrc_id_hash, iter.key);
            if (!resrc) {
                flux_log (h, LOG_ERR, "Resource not found - %s", iter.key);
                flux_log (h, LOG_ERR, "Printing resource in resrc_id_hash");
                resrc_t *curr_resrc = NULL;
                char *resrc_str = NULL;
                for (curr_resrc = zhash_first (ctx->rctx.resrc_id_hash);
                     curr_resrc;
                     curr_resrc = zhash_next (ctx->rctx.resrc_id_hash))
                    {
                        resrc_str = resrc_to_string (curr_resrc);
                        flux_log (h, LOG_ERR, "curr_resrc: %s", resrc_str);
                        free (resrc_str);
                    }
                continue;
            }
            //flux_log (h, LOG_DEBUG, "%s: Resource type: %s, uuid: %s is being reported as slack", __FUNCTION__, resrc_type (resrc), iter.key);
            if (!(!(strncmp (resrc_type (resrc), "core", 4)))) {
                flux_log (h, LOG_ERR, "FATAL: non core resource slacksubbed by child");
                continue;
            }
            const char *endtime_str = json_object_get_string (iter.val);
            //flux_log (h, LOG_DEBUG, "CLSACK: passed checks, endtime_str = %s", endtime_str);

            if (!endtime_str) {
                flux_log (h, LOG_ERR, "Value invalid");
                goto ret;
            }
            //flux_log (h, LOG_DEBUG, "%s: slack endtime obtained as: %s", __FUNCTION__, endtime_str);

            int64_t endtime = strtol (endtime_str, NULL, 10);
            if (endtime == 0) {
                // a slack resource that was originally sent from me
                resrc_mark_resource_return_received (resrc, jobid);
                if (resrc_owner(resrc) == 0) {
                    ctx->slctx.resource_out_count--;
                    flux_log (h, LOG_DEBUG, "%s: current resource_out_count is %"PRId64"",
                              __FUNCTION__, ctx->slctx.resource_out_count);
                }
                flux_log (h, LOG_DEBUG, "%s: Return resource received during slacksub :%s", __FUNCTION__, resrc_path (resrc));
            } else {
                // a slack resource from job's allocation
                resrc_mark_resource_slack (resrc, jobid, endtime);
                //flux_log (h, LOG_DEBUG, "CSLACK: Resource marked as slack: %s", resrc_name (resrc));
           }
        }
    } else {
        flux_log (h, LOG_DEBUG, "%s: Surplus = 0", __FUNCTION__);
    }

    if (Jget_obj (jmsg, "need", &needobj)) {
        flux_log (h, LOG_DEBUG, "%s: needob set: %s", __FUNCTION__, Jtostr (needobj));
    } else {
        flux_log (h, LOG_DEBUG, "%s: Need = 0", __FUNCTION__);
    }

    /* update job slack state */
    job_cslack_set (h, job, needobj);
    flux_log (h, LOG_DEBUG, "%s: job %"PRId64", cslack set", __FUNCTION__, job->lwj_id);

    /* return idle resources */
    if (return_idle_slack_resources(ctx) < 0) {
        flux_log (h, LOG_ERR, "error in returning idle slack resources");
    }
    flux_log (h, LOG_DEBUG, "%s: Return idle resources successful", __FUNCTION__);

    if (ctx->slctx.slack_state == CSLACK) {
        //this should never happen
        flux_log (h, LOG_DEBUG, "%s: FATAL: in cslack when child is sending surplus", __FUNCTION__);
        send_cslack_invalid (ctx, NULL);
        ctx->slctx.slack_state = INVALID;
    } else if (ctx->slctx.slack_state == SLACK) {
        flux_log (h, LOG_DEBUG, "%s: SLACK state staying as SLACK", __FUNCTION__);
        //flux_log (h, LOG_DEBUG, "%s: SLACK state going back to invalid", __FUNCTION__);
        //ctx->slctx.slack_state = INVALID;
    } else {
        flux_log (h, LOG_DEBUG, "%s: Slack state in parent already not reached yet", __FUNCTION__);
        ctx->slctx.slack_state = INVALID;
    }

    //schedule_jobs (ctx);

ret:
    Jput (in);
}

/*
 * resources added could be new or returned.
 * TODO: Replace blank returns with responses containing the error?
 */
static void req_addresources_cb (flux_t h, flux_msg_handler_t *w, const flux_msg_t *msg, void *arg)
{
    int i = 0;
    int len = 0;
    JSON id_array;
    int64_t jobid = -1;
    JSON in = NULL;
    JSON new_res = NULL;
    JSON jmsg = NULL;
    const char *json_str = NULL;

    flux_log (h, LOG_DEBUG, "%s: !!!!!!!!!!!!!!!!!!!!!!!!!Received add resources message!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!", __FUNCTION__);

    /* get context */
    ssrvctx_t *ctx = getctx (h);
    if (flux_request_decode (msg, NULL, &json_str) < 0 || json_str == NULL
        || !(in = Jfromstr (json_str))) {
        flux_log (h, LOG_ERR, "%s: bad message", __FUNCTION__);
        Jput (in);
        return;
    }
    flux_log (h, LOG_DEBUG, "%s: Message decode succeeded in addresources", __FUNCTION__);

    if (!(Jget_int64 (in, "jobid", &jobid))) {
        flux_log (h, LOG_ERR, "Unable to determine who added resources");
        goto ret;
    }
    flux_log (h, LOG_DEBUG, "%s: Resources added by %ld", __FUNCTION__, jobid);

    if (!(Jget_obj (in, "msg", &jmsg))) {
        flux_log (h, LOG_ERR, "No message given in addresources");
        goto ret;
    }
    flux_log (h, LOG_DEBUG, "%s: Message in add resources retrieved: %s", __FUNCTION__, Jtostr (jmsg));

    if (!(Jget_obj (jmsg, "return", &id_array))) {
        flux_log (h, LOG_DEBUG, "%s: no resources returned", __FUNCTION__);
    } else {
        // collect all returned resources
        flux_log (h, LOG_DEBUG, "%s: resource return included in addresources", __FUNCTION__);
        if (!(Jget_ar_len (id_array, &len))) {
            flux_log (h, LOG_ERR, "%s: FATAL: Could not get length of id array. skipping return resources", __FUNCTION__);
            goto new;
        }

        for (i = 0; i < len; i++) {
            const char *uuid = NULL;
            Jget_ar_str (id_array, i, &uuid);
            resrc_t *resrc = zhash_lookup (ctx->rctx.resrc_id_hash, uuid);
            if (!resrc) {
                flux_log (h, LOG_ERR, "stub resource in return resource");
                continue;
            }
            if (!(!(strcmp (resrc_type (resrc), "core")))) {
                flux_log (h, LOG_ERR, "%s: non-core resource returned by job %"PRId64" dropping it:", __FUNCTION__, jobid);
                resrc_flux_log (h, resrc);
                continue;
            }
            resrc_mark_resource_return_received (resrc, jobid);
            flux_log (h, LOG_DEBUG, "%s: resource returned by job adding it:", __FUNCTION__);
            resrc_flux_log(h, resrc);
            if (resrc_owner (resrc) == 0) {
                ctx->slctx.resource_out_count--;
                flux_log (h, LOG_DEBUG, "%s: current resource_out_count is %"PRId64"",
                          __FUNCTION__, ctx->slctx.resource_out_count);
            }
            flux_log (h, LOG_DEBUG, "%s: resource return = %s", __FUNCTION__, resrc_name (resrc));
        }
    }

new:
    if (!(Jget_obj (jmsg, "new", &new_res))) {
        flux_log (h, LOG_DEBUG, "%s: no new resources added", __FUNCTION__);
    } else {
        flux_log (h, LOG_DEBUG, "%s: new resources provided", __FUNCTION__);
        if (resrc_add_resources_from_json (h, ctx->rctx.root_resrc, ctx->rctx.resrc_id_hash, new_res, jobid) < 0) {
            flux_log (h, LOG_ERR, "error in adding new resources");
            return;
        }
        flux_log (h, LOG_DEBUG, "%s: new resources added succesfully", __FUNCTION__);
    }

    /* update slack state */
    if (ctx->slctx.slack_state == CSLACK) {
        /* Invalidate cslack asking nothing for return */
        send_cslack_invalid (ctx, NULL);
        ctx_slack_invalidate (ctx);
    } else if (ctx->slctx.slack_state == SLACK) {
        /* only change the state */
        ctx_slack_invalidate (ctx);
    }
    flux_log (h, LOG_DEBUG, "%s: slack state update successful", __FUNCTION__);

    /* return any idle slack resource */
    if (return_idle_slack_resources (ctx) < 0) {
        flux_log (h, LOG_ERR, "error in returning idle slack resources");
        return;
    }
    flux_log (h, LOG_DEBUG, "%s: slack resources return successful", __FUNCTION__);

    if ((ctx->slctx.wait_until_return) && (ctx->slctx.resource_out_count == 0)) {
        ctx->slctx.wait_until_return = 0;
        flux_log (h, LOG_DEBUG,
                  "%s: Was waiting for all resources to return, "
                  "all resources have been returned, setting wait flag to 0",
                  __FUNCTION__);
    }

    /* schedule jobs */
#if 0
    flux_log (h, LOG_DEBUG, "%s: will now schedule jobs", __FUNCTION__);
    int num_jobs_scheduled = schedule_jobs (ctx);
    if (num_jobs_scheduled > 0 && ctx->arg.verbosity > 0) {
        flux_log (h, LOG_DEBUG, "%s: jobs were scheduled, printing rdl:", __FUNCTION__);
        resrc_tree_flux_log (h, resrc_phys_tree (ctx->rctx.root_resrc));
    }
#endif

ret:
    Jput (in);
}

static void req_cslack_invalid_cb (flux_t h, flux_msg_handler_t *w, const flux_msg_t *msg, void *arg)
{
    int64_t jobid;
    flux_lwj_t *job;
    ssrvctx_t *ctx = getctx (h);
    JSON lao = Jnew ();
    JSON in = Jnew ();
    const char *json_str = NULL;

    flux_log (h, LOG_DEBUG, "%s: 6666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666 SLACK INVALIDATE MESSAGE received", __FUNCTION__);
    if (flux_request_decode (msg, NULL, &json_str) < 0 || json_str == NULL
        || !(in = Jfromstr (json_str))) {
        flux_log (h, LOG_ERR, "%s: bad message", __FUNCTION__);
        Jput (in);
        return;
    }
    flux_log (h, LOG_DEBUG, "%s: Message in req_cslack_invalid_cb decoded", __FUNCTION__);

    if (!(Jget_int64 (in, "jobid", &jobid))) {
        flux_log (h, LOG_ERR, "Could not fetch jobid on receiving slack invalidate");
        goto ret;
    }
    flux_log (h, LOG_DEBUG, "Slack invalide message from job: %"PRId64"", jobid);

    /* 1. Find the job */
    job = find_job_in_queue (ctx->r_queue, jobid);
    if (!job) {
        flux_log (h, LOG_ERR, "job %ld not found in running queue", jobid);
        goto ret;
    }
    flux_log (h, LOG_DEBUG, "%s: job found in running queue", __FUNCTION__);

    /* 2. Set slack invalid in job */
    job_cslack_invalidate (job);

    /* 3. Have resources been asked for? lao contains "jobid": "uuid_array" -1 = parent, 0 = me, jobid = children */
    JSON askobj = NULL;
    if (!(Jget_obj (in, "ask", &askobj))) {
        flux_log (h, LOG_DEBUG, "%s: No resources have been asked", __FUNCTION__);
    } else {
        flux_log (h, LOG_DEBUG, "%s: Resources have been asked", __FUNCTION__);
        resrc_retrieve_lease_information (ctx->rctx.resrc_id_hash, askobj, lao);
        flux_log (h, LOG_DEBUG, "%s: resources asked = %s", __FUNCTION__, Jtostr (lao));
    }

    /* 4. Update my slack state ask parent */
    if (ctx->slctx.slack_state == CSLACK) {
        /* Invalidate slack and ask resources from parent */
        JSON pao = NULL;
        if ((lao) && (Jget_obj (lao, "-1", &pao))) {    // pao contains {-1 : uuid_array}
            send_cslack_invalid (ctx, pao);
            resrc_mark_resources_asked (ctx->rctx.resrc_id_hash, pao);
            resrc_mark_resources_to_be_returned (ctx->rctx.resrc_id_hash, pao);
        } else {
            send_cslack_invalid (ctx, NULL);
        }
    } else {
        /* Just check if we need to ask anything to the parent */
        JSON pao = NULL;
        if ((lao) && (Jget_obj (lao, "-1", &pao))) {
            send_to_parent (ctx, pao, "sched.slack.askresources");
            resrc_mark_resources_asked (ctx->rctx.resrc_id_hash, pao);
            resrc_mark_resources_to_be_returned (ctx->rctx.resrc_id_hash, pao);
        }
    }

    /* 5. ask rest of the resources */
    json_object_iter i;
    json_object_object_foreachC (lao, i) {
        flux_log (h, LOG_DEBUG, "%s: Dealing with resource from %s with %s", __FUNCTION__, i.key, Jtostr (i.val));
        if (!(strcmp (i.key, "-1")))
            continue;
        if (!(strcmp (i.key, "0"))) {
            resrc_mark_resources_to_be_returned (ctx->rctx.resrc_id_hash, i.val);
            continue;
        }
        int64_t jobid = strtol (i.key, NULL, 10);
        flux_lwj_t *job = find_job_in_queue (ctx->i_queue, jobid);
        flux_log (h, LOG_DEBUG, "%s: Job currently using this set of resources is not an instance. Just marking as to_be_returned", __FUNCTION__);
        if ((job) && (send_to_child (ctx, job, i.val, "sched.slack.askresources") < 0)) {
            flux_log (h, LOG_ERR, "Could not send askresources to %ld", jobid);
        }
        resrc_mark_resources_asked (ctx->rctx.resrc_id_hash, i.val);
        resrc_mark_resources_to_be_returned (ctx->rctx.resrc_id_hash, i.val); //i.val is json array
    }

    /* 6. Return resources and set events to send other resources later */
    return_idle_slack_resources (ctx);

ret:
    Jput (in);
    Jput (lao);
}

static void req_askresources_cb (flux_t h, flux_msg_handler_t *w, const flux_msg_t *msg, void *arg)
{
    int64_t jobid;
    ssrvctx_t *ctx = getctx (h);
    int len = 0, i = 0;
    const char *json_str = NULL;
    JSON in = NULL;

    if (flux_request_decode (msg, NULL, &json_str) < 0 || json_str == NULL
        || !(in = Jfromstr (json_str))) {
        flux_log (h, LOG_ERR, "%s: bad message", __FUNCTION__);
        Jput (in);
        goto ret;
    }
    flux_log (h, LOG_DEBUG, "%s: Message in req_askresources_cb decoded", __FUNCTION__);

    if (!(Jget_int64 (in, "jobid", &jobid))) {
        flux_log (h, LOG_ERR, "Could not fetch jobid on receiving askresources");
        goto ret;
    }
    flux_log (h, LOG_DEBUG, "Ask resources from %"PRId64"", jobid);

    JSON id_ar = NULL;
    if (!(Jget_obj (in, "msg", &id_ar))) {
        flux_log (h, LOG_ERR, "No message given in askresources");
        goto ret;
    }

    if (!(Jget_ar_len (id_ar, &len))) {
        flux_log (h, LOG_ERR, "Message received not valid uuid array. Could not retrieve length");
        goto ret;
    }
    flux_log (h, LOG_DEBUG, "%s: resources asked length = %d and resources = %s", __FUNCTION__, len, Jtostr (id_ar));

    resrc_mark_resources_to_be_returned (ctx->rctx.resrc_id_hash, id_ar);
    flux_log (h, LOG_DEBUG, "%s: all resources marked to be returned", __FUNCTION__);

    JSON juid = Jnew (); //jobid: uuidi_array, jobid:uuid_array

    for (i = 0; i < len; i++) {
        const char *uuid = NULL;
        Jget_ar_str (id_ar, i, &uuid);
        resrc_t *resrc = zhash_lookup (ctx->rctx.resrc_id_hash, uuid);
        if (!resrc) {
            flux_log (h, LOG_ERR, "FATAL: Obtained ask for resource that does not exist in my list");
            goto ret;
        }
        int64_t leasee = resrc_leasee (resrc);
        if (leasee == jobid) {
            flux_log (h, LOG_ERR, "FATAL: resource is leased to the instance that asked");
            goto ret;
        }
        if (leasee != 0) {
            char *leasee_str;
            if (asprintf (&leasee_str, "%ld", leasee) < 0) {
                flux_log (ctx->h, LOG_ERR, "(%s): create leasee_str failed: %s",
                          __FUNCTION__, strerror (errno));
            }
            JSON da = NULL;
            if (!(Jget_obj (juid, leasee_str, &da))) {
                // add new entry
                da = Jnew_ar ();
                Jadd_ar_str (da, uuid);
                Jadd_obj (juid, leasee_str, da);
                Jput (da);
            } else {
                // append to entry
                Jadd_ar_str (da, uuid);
                Jadd_obj (juid, leasee_str, da);
            }
        }
    }

    /* ask resources from others from the juid */
    json_object_iter iter;
    //iter.key = NULL;
    json_object_object_foreachC (juid, iter)
    {
        // i.key is the leasee_str and i.val is the uuid_array
        int64_t jid = strtol (iter.key, NULL, 10);
        flux_lwj_t *job;
        if (jid != -1) {
            // ask child
            job = find_job_in_queue (ctx->i_queue, jid);
            if (!job) {
                flux_log (h, LOG_ERR, "FATAL: Could not find job in instance queue askresources");
                goto ret;
            }
            if (send_to_child (ctx, job, iter.val, "sched.slack.askresources") < 0) {
                flux_log (h, LOG_ERR, "Could not send askresources to child from askresources");
                goto ret;
            }
            resrc_mark_resources_asked (ctx->rctx.resrc_id_hash, iter.val);
        } else {
            // ask parent: this should ideally not happen
            if (send_to_parent (ctx, iter.val, "sched.slack.askresources") < 0) {
                flux_log (h, LOG_ERR, "Could not send askresources to parent from askresources");
                goto ret;
            }
            resrc_mark_resources_asked (ctx->rctx.resrc_id_hash, iter.val);
        }
    }

    return_idle_slack_resources (ctx);

ret:
    return;
}

static void req_alljobssubmitted_cb (flux_t h, flux_msg_handler_t *w, const flux_msg_t *msg, void *arg)
{
    flux_log (h, LOG_INFO, "All jobs submitted message received");
    const char *json_str = NULL;
    JSON in = NULL;

    if (flux_request_decode (msg, NULL, &json_str) < 0 || json_str == NULL
        || !(in = Jfromstr (json_str))) {
        flux_log (h, LOG_ERR, "%s: bad message", __FUNCTION__);
        Jput (in);
        return;
    }

    Jget_int (in, "count", &jobcount);
    flux_log (h, LOG_INFO, "submitted jobs count = %d", jobcount);

    init_msg = flux_msg_copy (msg, true);
    if (!init_msg) {
        flux_log (h, LOG_DEBUG, ": flux_msg could not be copied alljobssubmitted");
    }
}

/******************************************************************************
 *                                                                            *
 *                     Scheduler Service Module Main                          *
 *                                                                            *
 ******************************************************************************/


int mod_main (flux_t h, int argc, char **argv)
{
    int rc = -1;
    ssrvctx_t *ctx = NULL;
    uint32_t rank = 1;

    if (!(ctx = getctx (h))) {
        flux_log (h, LOG_ERR, "can't find or allocate the context");
        goto done;
    }

    if (flux_get_rank (h, &rank)) {
        flux_log (h, LOG_ERR, "failed to determine rank");
        goto done;
    } else if (rank) {
        flux_log (h, LOG_ERR, "sched module must only run on rank 0");
        goto done;
    } else if (ssrvarg_process_args (argc, argv, &(ctx->arg)) != 0) {
        flux_log (h, LOG_ERR, "can't process module args");
        goto done;
    }
    flux_log (h, LOG_INFO, "sched comms module starting");
    if (!(ctx->loader = sched_plugin_loader_create (h))) {
        flux_log_error (h, "failed to initialize plugin loader");
        goto done;
    }
    if (ctx->arg.userplugin) {
        if (sched_plugin_load (ctx->loader, ctx->arg.userplugin) < 0) {
            flux_log_error (h, "failed to load %s", ctx->arg.userplugin);
            goto done;
        }
        flux_log (h, LOG_INFO, "%s plugin loaded", ctx->arg.userplugin);
    }
    if (bridge_set_execmode (ctx) != 0) {
        flux_log (h, LOG_ERR, "failed to setup execution mode");
        goto done;
    }
    if (setup_child (ctx) != 0) {
        flux_log (h, LOG_ERR, "Child setup unsuccessful");
        goto done;
    }
    flux_log (h, LOG_INFO, "Setup child stuff");
    if (load_resources (ctx) != 0) {
        flux_log (h, LOG_ERR, "failed to load resources");
        goto done;
    }
    flux_log (h, LOG_INFO, "resources loaded");
    if (bridge_set_events (ctx) != 0) {
        flux_log (h, LOG_ERR, "failed to set events");
        goto done;
    }

    /* if ((!ctx->sctx.in_sim) && (send_childbirth_msg (ctx) != 0)) { */
    /*     flux_log (h, LOG_ERR, "Instance is child and sending childbirth message failed"); */
    /*     goto done; */
    /* } */
    flux_log (h, LOG_INFO, "Instance birth successful");

    if (flux_reactor_run (flux_get_reactor (h), 0) < 0) {
        flux_log (h, LOG_ERR, "flux_reactor_run: %s", strerror (errno));
        goto done;
    }
    rc = 0;
done:
    return rc;
}

MOD_NAME ("sched");

/*
 * vi:tabstop=4 shiftwidth=4 expandtab
 */
