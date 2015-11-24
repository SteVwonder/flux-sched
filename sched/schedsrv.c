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
#define ENABLE_TIMER_EVENT 1
#define SCHED_UNIMPL -1

//#undef LOG_DEBUG
//#define LOG_DEBUG LOG_INFO

#if ENABLE_TIMER_EVENT
static int timer_event_cb (flux_t h, void *arg);
#endif
static void res_event_cb (flux_t h, flux_msg_handler_t *w,
                          const flux_msg_t *msg, void *arg);
static int job_status_cb (JSON jcb, void *arg, int errnum);

static int req_childbirth_cb (flux_t h, int typemask,
                              zmsg_t **zmsg, void *arg);
static int req_qstat_cb (flux_t h, int typemask,
                         zmsg_t **zmsg, void *arg);
static int req_addresources_cb (flux_t h, int typemask, zmsg_t **zmsg, void *arg);
static int req_askresources_cb (flux_t h, int typemask, zmsg_t **zmsg, void *arg);
static int req_cslack_cb (flux_t h, int typemask, zmsg_t **zmsg, void *arg);
static int req_cslack_invalid_cb (flux_t h, int typemask, zmsg_t **zmsg, void *arg);

/******************************************************************************
 *                                                                            *
 *              Scheduler Framework Service Module Context                    *
 *                                                                            *
 ******************************************************************************/

typedef int (*setup_f) (flux_t h);

typedef resrc_tree_list_t *(*find_f) (flux_t h, resrc_t *resrc,
                                      resrc_reqst_t *resrc_reqst);

typedef resrc_tree_list_t *(*sel_f) (flux_t h, resrc_tree_list_t *resrc_trees,
                                     resrc_reqst_t *resrc_reqst);

typedef int (*alloc_f) (flux_t h, resrc_tree_list_t *rtl, int64_t job_id,
                        int64_t starttime, int64_t endtime);

typedef int (*reserv_f) (flux_t h, resrc_tree_list_t *rtl, int64_t job_id,
                         int64_t starttime, int64_t walltime, resrc_t *resrc,
                         resrc_reqst_t *resrc_reqst);

typedef struct sched_ops {
    void         *dso;                /* Scheduler plug-in DSO handle */
    setup_f       sched_loop_setup;   /* prepare to run through a sched cycle */
    find_f        find_resources;     /* find resources that match request */
    sel_f         select_resources;   /* select the best resources */
    alloc_f       allocate_resources; /* allocate those resources */
    reserv_f      reserve_resources;  /* or reserve them */
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
    zhash_t      *resrc_id_hash;      /* All root resources hashed by id */
} rdlctx_t;

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
    rdlctx_t      rctx;               /* RDL context */
    simctx_t      sctx;               /* simulator context */
    sched_ops_t   sops;               /* scheduler plugin operations */
    int64_t       my_job_id;          /* My job id in my parent */
    int64_t       my_walltime;        /* My walltime */
    char         *parent_uri;         /* My parent's URI */
    slackctx_t    slctx;              /* Slack info context */ 
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
    zlist_destroy (&(ctx->i_queue));
    resrc_tree_destroy (resrc_phys_tree (ctx->rctx.root_resrc), true);
    zhash_destroy (&(ctx->rctx.resrc_id_hash));
    free (ctx->rctx.root_uri);
    free (ctx->sctx.sim_state);
    if (ctx->sctx.res_queue)
        zlist_destroy (&(ctx->sctx.res_queue));
    if (ctx->sctx.jsc_queue)
        zlist_destroy (&(ctx->sctx.jsc_queue));
    if (ctx->sctx.timer_queue)
        zlist_destroy (&(ctx->sctx.timer_queue));
    if (ctx->sops.dso)
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
        if (!(ctx->i_queue = zlist_new ()))
            oom ();
        ctx->rctx.root_resrc = NULL;
        ctx->rctx.root_uri = NULL;
        ctx->sctx.in_sim = false;
        ctx->sctx.sim_state = NULL;
        ctx->sctx.res_queue = NULL;
        ctx->sctx.jsc_queue = NULL;
        ctx->sctx.timer_queue = NULL;
        ctx->sops.dso = NULL;
        ctx->sops.sched_loop_setup = NULL;
        ctx->sops.find_resources = NULL;
        ctx->sops.select_resources = NULL;
        ctx->sops.allocate_resources = NULL;
        ctx->sops.reserve_resources = NULL;
        flux_aux_set (h, "schedsrv", ctx, freectx);
 
        ctx->rctx.resrc_id_hash = zhash_new ();
        ctx->slctx.slack_state = false;
        ctx->slctx.jobid = 0;
        ctx->slctx.psize = 0;
        ctx->slctx.rsize = 0;
        ctx->slctx.resource_out_count = 0; 
        ctx->my_job_id = 0;
        ctx->my_walltime = 0;
        ctx->parent_uri = NULL;
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

static inline void get_walltime (flux_t h, flux_lwj_t *j, int64_t *walltime)
{
    kvsdir_t *dir;
    *walltime = INT64_MAX;

    if (kvs_get_dir (h, &dir, "lwj.%lu", j->lwj_id) < 0) {
        flux_log (h, LOG_ERR, "Couldn't get job kvs dir. getting walltime failed");
        return; 
    }

    if (kvsdir_get_int64 (dir, "walltime", walltime) < 0) {
        flux_log (h, LOG_DEBUG, "No walltime set");
        return;
    }   

    if (*walltime == 0) 
        *walltime = INT64_MAX; 

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
    rc = 0;
done:
    if (jcb)
        Jput (jcb);
    return rc;
}


static inline int fill_hierarchical_req (flux_t h, flux_lwj_t *j)
{

    int rc = -1;
    char *cmdline_str;
    kvsdir_t *dir;
    char *hfile = NULL;

    if (kvs_get_dir (h, &dir, "lwj.%lu", j->lwj_id) < 0) {
        flux_log (h, LOG_ERR, "Couldn't get job kvs dir. Hierarchy addons failed");
        goto done;
    }

    kvsdir_get (dir, "cmdline", &cmdline_str);
    flux_log (h, LOG_INFO, "cmdline from sched = %s\n", cmdline_str);


    if (kvsdir_get_int (dir, "is_hierarchical", &(j->is_hierarchical)) < 0) {
        flux_log (h, LOG_ERR, "Job is not hierarchical");
        j->is_hierarchical = 0;
        goto done;
    }

    if (kvsdir_get_string (dir, "hfile", &hfile) < 0) {
        flux_log (h, LOG_ERR, "Will launch hierarchical job with no hfile");
        hfile = NULL;
    }

    if (j->is_hierarchical) {

        char jobid[22];
        sprintf(jobid, "%lu", j->lwj_id);

        JSON aro = Jnew_ar ();
        Jadd_ar_str (aro, "/g/g92/surajpkn/flux-core/src/cmd/flux");
        Jadd_ar_str (aro, "-M");
        Jadd_ar_str (aro, "/g/g92/surajpkn/flux-sched/");
        Jadd_ar_str (aro, "broker");
        Jadd_ar_str (aro, "/g/g92/surajpkn/initial_program");
        Jadd_ar_str (aro, "-j");
        Jadd_ar_str (aro, jobid);
        if ((hfile) && (strcmp (hfile, "0") != 0)) {
            Jadd_ar_str (aro, "-f");
            Jadd_ar_str (aro, hfile);
        }

        JSON o = Jnew ();
        Jadd_obj (o, "cmdline", aro);
        json_object_iter i;
        json_object_object_foreachC (o, i);
        {
            flux_log (h, LOG_DEBUG, "New cmdline: %s:%s\n",i.key, json_object_to_json_string(i.val));
            if (kvsdir_put (dir, i.key, json_object_to_json_string (i.val)) < 0) {
                flux_log (h, LOG_ERR, "Could not modify cmdline of hierarchical job: %lu", j->lwj_id);
            }
        }
        kvs_commit (h);
        Jput (aro);

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
    int64_t os, ns;
    get_states (jcb, &os, &ns);
    return ((os == J_NULL) && (ns == J_NULL))? true : false;
}

/* clang warning:  error: unused function */
#if 0
static bool inline is_node (const char *t)
{
    return (strcmp (t, "node") == 0)? true: false;
}

static bool inline is_core (const char *t)
{
    return (strcmp (t, "core") == 0)? true: false;
}
#endif

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

void retrieve_stat_info (ssrvctx_t *ctx, int64_t *jobid, int *psize, int *rsize)
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

static void job_cslack_set (flux_lwj_t *job, JSON needobj)
{
    job->slackinfo->slack_state = CSLACK;
    if (job->slackinfo->need) {
        Jput (job->slackinfo->need);
        job->slackinfo->need = NULL;
    }
    printf ("SO FAR SO GOOD\n");
    fflush(0);
    job->slackinfo->need = Jdup (needobj);
    if (job->slackinfo == NULL) 
        printf ("AGAIN IT IS NULL\n");
    else 
        printf ("IT IS NOT NULL\n");
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

/******************************************************************************
 *                                                                            *
 *                            Scheduler Communication                         *
 *                                                                            *
 ******************************************************************************/
static int send_childbirth_msg (ssrvctx_t *ctx)
{
    int rc = -1;
    flux_t h = ctx->h;
    flux_rpc_t *rpc;

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

    const char *local_uri = flux_attr_get (h, "local-uri", NULL);
    if (!local_uri) {
        flux_log (h, LOG_ERR, "Could not get local URI");
        goto ret;
    }

    flux_t parent_h = flux_open (parent_uri, 0);
    if (parent_h == NULL) {
        flux_log (h, LOG_ERR, "Could not open connection to parent");
        goto ret;
    }

    JSON in = Jnew();
    Jadd_str (in, "child-uri", local_uri);
    Jadd_int64 (in, "jobid", ctx->my_job_id);
    rpc = flux_rpc (parent_h, "sched.childbirth", Jtostr(in), FLUX_NODEID_ANY, 0);
    if (!rpc) {
        flux_log (h, LOG_ERR, "Could not send childbirth to parent");
        goto ret; 
    }

    flux_close (parent_h);

    Jput (in);

    ctx->parent_uri = xstrdup (parent_uri);
    
    rc = 0;

ret:
    return rc;
}


static int accumulate_surplus_need (ssrvctx_t *ctx, int k_needs, JSON o, int *rcount)
{
    int rc = -1;
    int jcount = 0;
    flux_t h = ctx->h;
    JSON j = Jnew (); // uuid : endtime, uuid : endtime
    JSON r = Jnew_ar ();

    flux_log (h, LOG_DEBUG, "Computing surplus/need");
    /* If all my resources are not yet with me, do not send surplus */
    if (ctx->slctx.wait_until_return) {
        *rcount = 0;
        flux_log (h, LOG_DEBUG, "Wait until return set to trure. Skipping surplus collection. Goto needs.");
        goto needs;
    }

    flux_log (h, LOG_DEBUG, "Proceeding to compute surplus");
    /* Calculate surplus */
    resrc_t *resrc = zhash_first (ctx->rctx.resrc_id_hash);
    while (resrc) {
        flux_log (h, LOG_DEBUG, "Checking resource %s and type %s", resrc_name (resrc), resrc_type (resrc));
        int64_t endtime = 0;
        if (!(resrc_check_slacksub_ready (resrc, &endtime))) {  // ensures only cores are included. even the ones that I am not the owner of. 
            flux_log (h, LOG_DEBUG, "Resource not eligible for slacksub");
            goto next;
        }
        flux_log (h, LOG_DEBUG, "Resource is eligible");
        char ruuid[40];
        resrc_uuid (resrc, ruuid);
        Jadd_int64 (j, ruuid, endtime);
        flux_log (h, LOG_ERR, "Accumulated resource with uuid: %s of type: %s and endtime:%ld", ruuid, resrc_type (resrc), endtime);
        if (resrc_owner (resrc) == 0)
            (*rcount)++;
next:
        resrc = zhash_next (ctx->rctx.resrc_id_hash);
    }
    flux_log (h, LOG_ERR, "final value of rcount: %d", *rcount);
    
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
        Jadd_int (req_res, "nnodes", job->req->nnodes);
        Jadd_int (req_res, "ncores", job->req->corespernode);
        Jadd_int64 (req_res, "walltime", job->req->walltime);

        flux_log (h, LOG_DEBUG, "8888888888 Added need: = %s, corespernode was: %ld, and walltime was= %ld\n", Jtostr (req_res), job->req->corespernode, job->req->walltime);

        char *key;
        asprintf (&key, "need%d", jcount);
        Jadd_ar_obj (r, req_res);
       
        //Jput (req_res);
 
        jcount++;
    
        job = zlist_next (ctx->p_queue);
    }
    flux_log (h, LOG_DEBUG, "added %d needs from self", jcount);

#if 0
    /* add children needs */
    job = zlist_first (ctx->i_queue);
    while ((job) && (jcount < K_NEEDS)) {
        
        if (job->slackinfo->need == NULL)
            continue;
        
        json_object_iter i;
        json_object_object_foreachC (job->slackinfo->need, i) { 
            char *key;
            asprintf (&key, "need%d", jcount);
            Jadd_ar_obj (r, i.val);

            jcount++;

            if (jcount == K_NEEDS)
                break;
        }
        flux_log (h, LOG_DEBUG, "needs increased to %d after adding a child's needs", jcount);
        job = zlist_next (ctx->i_queue);
    }
#endif

    flux_log (h, LOG_DEBUG, "total needs added: %d", jcount); 
    
    /* Add need to message */ 
    if (jcount > 0) {
        Jadd_obj (o, "need", r);
    }

#if 0
    /* alternate way of collecting needs. First need from my job and each child job */
    flux_lwj_t *job = zlist_first (ctx->p_queue);
    if (job) {
        JSON child_core = Jnew ();
        Jadd_str (child_core, "type", "core");
        Jadd_int (child_core, "req_qty", job->req->corespernode);
        Jadd_int64 (child_core, "walltime", job->req->walltime);

        JSON req_res = Jnew ();
        Jadd_str (req_res, "type", "node");
        Jadd_int (req_res, "req_qty", job->req->nnodes);
        Jadd_int (req_res, "walltime", job->req->walltime);

        json_object_object_add (req_res, "req_child", child_core);

        char *key;
        asprintf (&key, "need%d", jcount);
        Jadd_obj (r, key, req_res);

        Jput (child_core);
        Jput (req_res);        
        jcount++;
    }
    job = zlist_first (i_queue);
    while (job) {
        if (job->slackinfo->need == NULL)
            continue;
        json_object_iter i;
        json_object_object_foreachC (job->slackinfo->need, i) {
            char *key;
            asprintf (&key, "need%ld", jcount);
            Jadd_obj (r, key, i.val);

            jcount++;
            break;
        }
        job = zlist_next (i_queue);     
    } 
#endif 

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
    flux_rpc_t *rpc;
    flux_log (h, LOG_DEBUG, "Going to send cslack");

    Jadd_int64 (o, "jobid", ctx->my_job_id);
    flux_log (h, LOG_DEBUG, "My job id = %ld", ctx->my_job_id);

    if (jmsg != NULL) {
        flux_log (h, LOG_DEBUG, "Msg has been given: %s", Jtostr (jmsg));
        //Jadd_obj (o, "msg", jmsg);
        Jadd_str (o, "msg", Jtostr (jmsg));
    } else {
        flux_log (h, LOG_DEBUG, "No msg has been given");
    }
   
    if (ctx->parent_uri) {
        flux_log (h, LOG_DEBUG, "Parent URI still exists");
    } else {
        flux_log (h, LOG_DEBUG, "Parent URI does not exist");
    }
 
    flux_t parent_h = flux_open (ctx->parent_uri, 0);
    if (!parent_h) {
        flux_log (h, LOG_ERR, "couldn't open handle to parent");
        return rc;
    }
    
    flux_log (h, LOG_DEBUG, "Parent handle successfully obtained, x = %s", Jtostr (o));
    rpc = flux_rpc (parent_h, "sched.cslack", (const char *)Jtostr (o), FLUX_NODEID_ANY, 0);
    flux_log (h, LOG_DEBUG, "Message sent to parent");
    flux_close (parent_h);
    flux_log (h, LOG_DEBUG, "Closed parent handle");
    Jput (o);
    flux_log (h, LOG_DEBUG, "Cleared JSON");

    rc = 0;
    return rc;
}


static int send_cslack_invalid (ssrvctx_t *ctx, JSON jmsg)
{
    int rc = -1;
    flux_t remote_h;
    JSON o = Jnew ();
    flux_rpc_t *rpc;
    flux_t h = ctx->h;

    flux_log (h, LOG_DEBUG, "Going to send cslack invalid");
    Jadd_int64 (o, "jobid", ctx->my_job_id);

    if (jmsg)
        Jadd_obj (o, "ask", jmsg); 
    flux_log (h, LOG_DEBUG, "Final message to be sent = %s", Jtostr (o)); 

    if (!ctx->parent_uri) {
        flux_log (h, LOG_DEBUG, "No parent for sending cslack invalid");
        Jput (o);
        rc = 0;
        return rc;
    }
 
    remote_h = flux_open (ctx->parent_uri, 0);
    if (!remote_h) {        
        flux_log (h, LOG_DEBUG, "Could not get parent handle.");
        Jput (o);
        return rc;
    }
    flux_log (h, LOG_DEBUG, "Parent handle has been obtained");
    
    rpc = flux_rpc (remote_h, "sched.cslack_invalidate", Jtostr (o), FLUX_NODEID_ANY, 0);
    if (!rpc) {
        flux_log (h, LOG_DEBUG, "Sending cslack_invalid unsuccessful");
        flux_close (remote_h);
        Jput (o);
        return rc;
    }
    flux_log (h, LOG_DEBUG, "Sending cslack_invalid successful");
    flux_close (remote_h);
 
    Jput (o);
    rc = 0;
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

    flux_log (h, LOG_DEBUG, "Going to send following to child: %s", Jtostr (o));
    flux_t child_h = flux_open (job->contact, 0);
    if (!child_h) {
        flux_log (h, LOG_ERR, "Couldn't open handle to child");
        return rc;
    }
    rpc = flux_rpc (child_h, service, Jtostr (o), FLUX_NODEID_ANY, 0);
    if (!rpc) {
        flux_log (h, LOG_DEBUG, "rpc did not complete");
    }
    flux_close (child_h);

    Jput (o);
    rc = 0;
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
    return rc;
}

static int return_idle_slack_resources (ssrvctx_t *ctx)
{
    
    int rc = 0;
    int64_t jobid = 0;
    flux_t h = ctx->h;
    zhash_t *jobid_uuid_hash = zhash_new ();
    flux_log (h, LOG_DEBUG, "Entered return idle slack resources");
    resrc_t *resrc = zhash_first (ctx->rctx.resrc_id_hash);
    while (resrc) { 
        flux_log (h, LOG_DEBUG, "Considering resource = %s, %s", resrc_type (resrc), resrc_path (resrc));
        if (resrc_check_return_ready (resrc, &jobid) == false)  {// jobid is the ownerid
            flux_log (h, LOG_DEBUG, "Resource not return ready");
            goto next;
        }
        flux_log (h, LOG_DEBUG, "Resource is return ready");
        char *jobid_str = NULL; 
        asprintf (&jobid_str, "%ld", jobid);
        char *juidarry = zhash_lookup (jobid_uuid_hash, jobid_str);
        if (juidarry) {
            JSON j = Jfromstr (juidarry);
            Jadd_ar_str (j, (const char *)zhash_cursor (ctx->rctx.resrc_id_hash));
            char *x = xstrdup (Jtostr (j));
            zhash_update (jobid_uuid_hash, jobid_str, (void *) x);
            Jput (j);
            flux_log (h, LOG_DEBUG, "Resource entry added to existing array");
        } else {
            JSON j = Jnew_ar ();
            Jadd_ar_str (j, (const char *)zhash_cursor (ctx->rctx.resrc_id_hash));
            char *x = xstrdup (Jtostr (j));
            zhash_insert (jobid_uuid_hash, jobid_str, (void *)x); 
            Jput (j);
            flux_log (h, LOG_DEBUG, "New resource entry added");
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
            flux_log (h, LOG_DEBUG, "Sending something to parent");
            JSON r = Jnew ();
            Jadd_obj (r, "return", uo);
            if ((send_to_parent (ctx, uo, "sched.addresources")) < 0) {
                flux_log (h, LOG_ERR, "couldn't send resources back to parent");
                goto nextone;
            }
            Jput (r);
        } else { 
            flux_log (h, LOG_DEBUG, "Sending back to child");
            flux_lwj_t *job = find_job_in_queue (ctx->i_queue, jobid);
            if (!job) {
                flux_log (h, LOG_ERR, "job in resrc not found in i_queue: %ld", job->lwj_id);
                //resrc_own_resources (r);
                goto nextone;
            }
            JSON r = Jnew ();
            Jadd_obj (r, "return", uo);
            send_to_child (ctx, job, r, "sched.addresources");
            Jput (r);
        }
        resrc_mark_resources_returned (ctx->rctx.resrc_id_hash, uo);
nextone:
        jstr = zhash_next (jobid_uuid_hash);
    }

    flux_log (h, LOG_DEBUG, "destroying returned resources");
    resrc_tree_destroy_returned_resources (resrc_phys_tree(ctx->rctx.root_resrc), ctx->rctx.resrc_id_hash);
    return rc;
}

/******************************************************************************
 *                                                                            *
 *                            Scheduler Plugin Loader                         *
 *                                                                            *
 ******************************************************************************/

static int resolve_functions (ssrvctx_t *ctx)
{
    int rc = -1;

    ctx->sops.sched_loop_setup = dlsym (ctx->sops.dso, "sched_loop_setup");
    if (!(ctx->sops.sched_loop_setup) || !(*(ctx->sops.sched_loop_setup))) {
        flux_log (ctx->h, LOG_ERR, "can't load sched_loop_setup: %s",
                  dlerror ());
        goto done;
    }
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
    ctx->sops.allocate_resources = dlsym (ctx->sops.dso, "allocate_resources");
    if (!(ctx->sops.allocate_resources) || !(*(ctx->sops.allocate_resources))) {
        flux_log (ctx->h, LOG_ERR, "can't load allocate_resources: %s",
                  dlerror ());
        goto done;
    }
    ctx->sops.reserve_resources = dlsym (ctx->sops.dso, "reserve_resources");
    if (!(ctx->sops.reserve_resources) || !(*(ctx->sops.reserve_resources))) {
        flux_log (ctx->h, LOG_ERR, "can't load reserve_resources: %s",
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

static int retrieve_instance_jobid (ssrvctx_t *ctx, int argc, char **argv)
{
    int i;
    int rc = 0;
    char *jobid_str = NULL;

    for (i = 0; i < argc; i++) {
        if (!strncmp ("jobid=", argv[i], sizeof ("jobid")))
            jobid_str = xstrdup (strstr (argv[i], "=") + 1);
    }

    if (jobid_str != NULL)
        ctx->my_job_id = strtol (jobid_str, NULL, 10);

    return rc;
}

static void print_rdl (ssrvctx_t *ctx)
{
    //resrc_print_resource (ctx->rctx.root_resrc);
    resrc_tree_print (resrc_phys_tree (ctx->rctx.root_resrc));
}

static int load_rdl_from_parent_kvs (ssrvctx_t *ctx) 
{
    int rc = -1;
    flux_t h = ctx->h;
    char *rdlstr;
    kvsdir_t *dir;
    
    flux_t parent_h = flux_open (ctx->parent_uri, 0);
    if (!parent_h) {
        flux_log (h, LOG_ERR, "Could not open connection to parent"); fflush(0);
        goto ret;
    }
    flux_log (h, LOG_DEBUG, "LoadRDLkvs: Opened parent handle"); fflush(0);


    if (kvs_get_dir (parent_h, &dir, "lwj.%lu", ctx->my_job_id) < 0) {
        flux_log (h, LOG_ERR, "Couldn't get job kvs dir. Hierarchy addons failed"); fflush(0);
        goto ret;
    }        
    flux_log (h, LOG_DEBUG, "LoadRDLkvs: Opened job directory"); fflush(0);

    if (kvsdir_get_string (dir, "rdl", &rdlstr) < 0) {
        flux_log (h, LOG_ERR, "Couldn't get rdl from parent kvs: %s", strerror (errno)); fflush(0);
        goto ret;
    }
    flux_log (h, LOG_DEBUG, "LoadRDLkvs: got rdlstring from parent"); fflush(0);

    if (kvsdir_get_int64 (dir, "walltime", &ctx->my_walltime) < 0) {
        flux_log (h, LOG_ERR, "Couldn't get rdl from parent kvs: %s", strerror (errno));
        ctx->my_walltime = INT64_MAX;
    }
    flux_log (h, LOG_DEBUG, "LoadRDLkvs: Walltime obtained from parent"); fflush (0);

    flux_close (parent_h);

    /* Create a cluster resource */
    ctx->rctx.root_resrc = resrc_create_cluster ("cluster");
    flux_log (h, LOG_DEBUG, "LoadRDLkvs: root cluster created");   
 
    /* deserialize and add it to cluster */
    JSON ro = Jfromstr (rdlstr);
    flux_log (h, LOG_DEBUG, "LoadRDLkvs: ro obtained from json: %s", Jtostr(ro));
    resrc_add_resources_from_json (ctx->rctx.root_resrc, ctx->rctx.resrc_id_hash, ro, 0);
    //resrc_hash_by_uuid (resrc_phys_tree (ctx->rctx.root_resrc), ctx->rctx.resrc_id_hash);
    //Jput (ro);
     
    rc = 0;

ret:
    return rc;
}

static int load_resources (ssrvctx_t *ctx, char *path, char *uri)
{
    int rc = -1;

    if (ctx->my_job_id !=0) {
        flux_log (ctx->h, LOG_DEBUG, "Going to load RDL from parent KVS");
        rc = load_rdl_from_parent_kvs (ctx);
        return rc;
    }
    
    setup_rdl_lua (ctx->h);
    if (path) {
        if (uri)
            ctx->rctx.root_uri = uri;
        else
            ctx->rctx.root_uri = xstrdup ("default");

        if ((ctx->rctx.root_resrc =
             resrc_generate_rdl_resources (path, ctx->rctx.root_uri))) {
            flux_log (ctx->h, LOG_DEBUG, "loaded %s rdl resource from %s",
                      ctx->rctx.root_uri, path);
            rc = 0;
        } else {
            flux_log (ctx->h, LOG_ERR, "failed to load %s rdl resource from %s",
                      ctx->rctx.root_uri, path);
        }
    } else if ((ctx->rctx.root_resrc = resrc_create_cluster ("cluster"))) {
        char    *buf = NULL;
        char    *key;
        int64_t i = 0;
        size_t  buflen = 0;

        rc = 0;
        while (1) {
            key = xasprintf ("resource.hwloc.xml.%"PRIu64"", i++);
            if (kvs_get_string (ctx->h, key, &buf)) {
                /* no more nodes to load - normal exit */
                free (key);
                break;
            }
            buflen = strlen (buf);
            if ((resrc_generate_xml_resources (ctx->rctx.root_resrc, buf,
                                               buflen))) {
                flux_log (ctx->h, LOG_DEBUG, "loaded %s", key);
            } else {
                free (buf);
                free (key);
                rc = -1;
                break;
            }
            free (buf);
            free (key);
        }
        flux_log (ctx->h, LOG_INFO, "loaded resrc using hwloc (status %d)", rc);
    }

    resrc_hash_by_uuid (resrc_phys_tree (ctx->rctx.root_resrc), ctx->rctx.resrc_id_hash);

    


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
#if 0
#if ENABLE_TIMER_EVENT
    // Set scheduler loop to run in next occuring scheduler block
    double *this_timer = zhash_lookup (sim_state->timers, "sched");
    double next_schedule_block =
        sim_state->sim_time
        + (SCHED_INTERVAL - ((int)sim_state->sim_time % SCHED_INTERVAL));
    if (ctx->run_schedule_loop &&
        ((next_schedule_block < *this_timer || *this_timer < 0))) {
        *this_timer = next_schedule_block;
    }
    flux_log (ctx->h,
              LOG_DEBUG,
              "run_sched_loop: %d, next_schedule_block: %f, this_timer: %f",
              ctx->run_schedule_loop,
              next_schedule_block,
              *this_timer);
#endif
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
    jsc_event_t *event = (jsc_event_t*) xzmalloc (sizeof (jsc_event_t));

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
    ctx->sctx.sim_state->sim_time += seconds;
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
        flux_log (ctx->h, LOG_ERR, "unknown argument (%s) for sim option",
                  sim_arg);
        errno = EINVAL;
        goto done;
    } else {
        flux_log (ctx->h, LOG_INFO, "setting up sim in scheduler");
    }

    ctx->sctx.in_sim = true;
    ctx->sctx.sim_state = NULL;
    ctx->sctx.res_queue = zlist_new ();
    ctx->sctx.jsc_queue = zlist_new ();
    ctx->sctx.timer_queue = zlist_new ();

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
    if (flux_tmouthandler_add (h, 10000, false, timer_event_cb, (void *)h) < 0) {
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

/*
 * Register request/responses
 */
static int inline reg_requests (ssrvctx_t *ctx)
{
    int rc = 0;
    flux_t h = ctx->h;

    if (flux_msghandler_add (h, FLUX_MSGTYPE_REQUEST, "sched.childbirth",
                             (FluxMsgHandler)req_childbirth_cb, (void *)h) < 0) {
        flux_log (h, LOG_ERR,
                  "error registering childbirth request handler: %s",
                  strerror (errno));

        rc = -1;
        goto done;
    }

    if (flux_msghandler_add (h, FLUX_MSGTYPE_REQUEST, "sched.qstat",
                             (FluxMsgHandler)req_qstat_cb, (void *)h) < 0) {

        flux_log (h, LOG_ERR,
                  "error registering qstat request handler: %s",
                  strerror (errno));

        rc = -1;
        goto done;
    }

    if (flux_msghandler_add (h, FLUX_MSGTYPE_REQUEST, "sched.cslack",
                             (FluxMsgHandler)req_cslack_cb, (void *)h) < 0) {

        flux_log (h, LOG_ERR,
                  "error registering cslack request handler: %s",
                  strerror (errno));

        rc = -1;
        goto done;
    }

    if (flux_msghandler_add (h, FLUX_MSGTYPE_REQUEST, "sched.cslack_invalidate",
                             (FluxMsgHandler)req_cslack_invalid_cb, (void *)h) < 0) {

        flux_log (h, LOG_ERR,
                  "error registering cslack_invalid request handler: %s",
                  strerror (errno));

        rc = -1;
        goto done;
    }

    if (flux_msghandler_add (h, FLUX_MSGTYPE_REQUEST, "sched.addresources",
                             (FluxMsgHandler)req_addresources_cb, (void *)h) < 0) {

        flux_log (h, LOG_ERR,
                  "error registering addresources request handler: %s",
                  strerror (errno));

        rc = -1;
        goto done;
    }

    if (flux_msghandler_add (h, FLUX_MSGTYPE_REQUEST, "sched.askresources",
                             (FluxMsgHandler)req_askresources_cb, (void *)h) < 0) {

        flux_log (h, LOG_ERR,
                  "error registering askresources request handler: %s",
                  strerror (errno));

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

    if (resrc_tree_list_serialize (ro, job->resrc_trees, job->lwj_id)) {
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
    return rc;
}

#if DYNAMIC_SCHEDULING
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
    resrc_tree_t *resrc_tree = zlist_first (resrc_tree_list);
    while (resrc_tree) {
        resrc_t *resrc = resrc_tree_resrc (resrc_tree);
        resrc_tree_t *job_resrc_tree = zlist_first (job->resrc_trees);
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
            zlist_next (job_resrc_tree);
        }
        resrc_tree_list_append (job->resrc_trees, resrc_tree);
next:
        zlist_next (resrc_tree_list);
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
    bool allocated = false;
    
    flux_log (h, LOG_DEBUG, "scheduling job: %ld", job->lwj_id);

    

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
    Jadd_int64 (child_core, "starttime", starttime);
    Jadd_int64 (child_core, "endtime", starttime + job->req->walltime);

    req_res = Jnew ();
    Jadd_str (req_res, "type", "node");
    Jadd_int (req_res, "req_qty", job->req->nnodes);
    Jadd_int64 (req_res, "starttime", starttime);
    Jadd_int64 (req_res, "endtime", starttime + job->req->walltime);

    json_object_object_add (req_res, "req_child", child_core);

    resrc_reqst = resrc_reqst_from_json (req_res, NULL);
    Jput (req_res);
    if (!resrc_reqst)
        goto done;

    flux_log (h, LOG_DEBUG, "Starting finding resources");
    if ((found_trees = ctx->sops.find_resources (h, ctx->rctx.root_resrc,
                                                 resrc_reqst))) {
        nnodes = resrc_tree_list_size (found_trees);
        flux_log (h, LOG_DEBUG, "%"PRId64" nodes found for job %"PRId64", "
                  "reqrd: %"PRId64"", nnodes, job->lwj_id, job->req->nnodes);

        resrc_tree_list_unstage_resources (found_trees);
        flux_log (h, LOG_DEBUG, "Starting select resources");
        if ((selected_trees = ctx->sops.select_resources (h, found_trees,
                                                          resrc_reqst))) {
            nnodes = resrc_tree_list_size (selected_trees);
            if (nnodes == job->req->nnodes) {
                flux_log (h, LOG_DEBUG, "Starting allocate resources");
                ctx->sops.allocate_resources (h, selected_trees, job->lwj_id,
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
                    goto done;
                }
                allocated = true;
                flux_log (h, LOG_DEBUG, "Allocated %"PRId64" nodes for job "
                          "%"PRId64", reqrd: %"PRId64"", nnodes, job->lwj_id,
                          job->req->nnodes);

            } else {
                flux_log (h, LOG_DEBUG, "Couldn't select trees");
           }
        }
    } else {
        flux_log (h, LOG_DEBUG, "Couldn't find trees at all");
    }

    if (!allocated) {
        ctx->sops.reserve_resources (h, NULL, job->lwj_id,
                                     starttime, job->req->walltime,
                                     ctx->rctx.root_resrc, resrc_reqst);
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

    int reqd_nodes = 0;
    int reqd_cores = 0;
    int reqd_corespernode = 0;
    int64_t walltime = 0;

    flux_log (h, LOG_DEBUG, "Scheduling dynamic instance job : %ld", job->lwj_id);
    
    if (!(job->slackinfo->need)) {
        flux_log (h, LOG_DEBUG, "Job does not have any need");
        return rc;
    }
    flux_log (h, LOG_DEBUG, "Job has needs");

    if (!(Jget_ar_len (job->slackinfo->need, &len))) {
        flux_log (h, LOG_ERR, "Could not job needs len");
        return rc;   
    }
    flux_log (h, LOG_DEBUG, "Job needs len = %d", len);

    for (i = 0; i < len; i++) {

        int64_t starttime = epochtime ();
        JSON needobj = NULL;
        JSON req_res = Jnew ();
        JSON child_core = Jnew ();
        
        flux_log (h, LOG_DEBUG, "Considering need i = %d", i);
        if (!(Jget_ar_obj (job->slackinfo->need, i, &needobj))) {
            flux_log (h, LOG_ERR, "Could not fetch need i = %d", i);
            goto next;
        }
        flux_log (h, LOG_DEBUG, "Fetched the need %d: %s", i, Jtostr (req_res));

        if (!(Jget_int (needobj, "nnodes", &reqd_nodes))) {
            flux_log (h, LOG_ERR, "Could not fetch the required nodes");
            goto next;       
        }
        flux_log (h, LOG_ERR, "Required quantity of nodes = %d", reqd_nodes);

        if (!(Jget_int (needobj, "ncores", &reqd_cores))) {
            flux_log (h, LOG_ERR, "Could not fetch the required nodes");
            goto next;
        }
        flux_log (h, LOG_ERR, "Required quantity of cores = %d", reqd_cores);

        if (!(Jget_int64 (needobj, "walltime", &walltime))) {
            flux_log (h, LOG_ERR, "Could not fetch walltime");
            goto next;
        }
        flux_log (h, LOG_ERR, "Required quantity of walltime = %ld", walltime);
       
        //create req_res
        reqd_corespernode = (reqd_cores + reqd_nodes - 1) / reqd_nodes;
        Jadd_str (child_core, "type", "core");
        Jadd_int (child_core, "req_qty", reqd_corespernode);
        Jadd_int64 (child_core, "starttime", starttime);
        Jadd_int64 (child_core, "endtime", starttime + walltime);         
        Jadd_str (req_res, "type", "node");
        Jadd_int (req_res, "req_qty", reqd_nodes);
        Jadd_int64 (req_res, "starttime", starttime);
        Jadd_int64 (req_res, "endtime", starttime + walltime);
        json_object_object_add (req_res, "req_child", child_core); 
        
        resrc_reqst = resrc_reqst_from_json (req_res, NULL);

        if (!resrc_reqst) {
            flux_log (h, LOG_DEBUG, "Could not form resrc_reqst from json");
            goto next;
        } 
        flux_log (h, LOG_DEBUG, "Created resrc_reqst");

        resrc_reqst_print (resrc_reqst);

        if ((found_trees = ctx->sops.find_resources (h, ctx->rctx.root_resrc,
                                                     resrc_reqst))) {
            flux_log (h, LOG_DEBUG, "%"PRId64" nodes found for job %"PRId64"", nnodes, job->lwj_id);
            resrc_tree_list_unstage_resources (found_trees);
            flux_log (h, LOG_DEBUG, "Starting select resources");
            if ((selected_trees = ctx->sops.select_resources (h, found_trees,
                                                              resrc_reqst))) {
                nnodes = resrc_tree_list_size (selected_trees);
                flux_log (h, LOG_DEBUG, "nnodes found = %ld", nnodes);
                if (nnodes == reqd_nodes) {
                    flux_log (h, LOG_DEBUG, "Found the nodes required");
                    resrc_tree_list_print (found_trees);
                   
                    //Split new and old resources, in other words, new and return resources
                    resrc_tree_list_t *to_serialize_tree_list = NULL;
                    JSON ret_array = Jnew_ar ();
                    to_serialize_tree_list = resrc_split_resources (selected_trees, job->resrc_trees, ret_array, job->lwj_id);

                    flux_log (h, LOG_DEBUG, "RESULTTTTTTTTTTTTT: return array = %s\n", Jtostr (ret_array));
                    fflush(0); printf(" PRINTING THE LISTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTT\n"); fflush(0);
                    resrc_tree_list_print (to_serialize_tree_list);
                    fflush(0); printf(" END: PRINTING THE LISTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTT\n"); fflush(0);
                    JSON new_serialized = Jnew_ar ();
                    resrc_tree_list_serialize (new_serialized, to_serialize_tree_list, 0);
                    flux_log (h, LOG_DEBUG, "Serialized dynamic new: %s", Jtostr(new_serialized));

                    //Mark/Allocate these resources
                    resrc_tree_list_allocate_dynamic (selected_trees, job->lwj_id, starttime, starttime + walltime + SLACK_BUFFER_TIME);

#if 1
                    //Send resources to job
                    JSON j = Jnew ();
                    Jadd_obj (j, "return", ret_array);
                    Jadd_obj (j, "new", new_serialized);
                    send_to_child (ctx, job, j, "sched.addresources");
                    Jput (j);
#endif
                    
                    Jput (new_serialized);
                    
                    if (to_serialize_tree_list) {
                        resrc_tree_list_destroy (to_serialize_tree_list, false);
                    }

                    resrc_tree_list_unstage_resources (found_trees);                    
                }              
            }

        } else {
            flux_log (h, LOG_DEBUG, "Couldn't find trees for need");
        }

next:
#if 0
        nnodes = 0;
        reqd_nodes = 0;
        reqd_cores = 0;
        Jput (child_core);
        Jput (req_res);
        if (resrc_reqst)
            resrc_reqst_destroy (resrc_reqst);
        if (found_trees)
            resrc_tree_list_destroy (found_trees, false);    
#endif
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
    /* Prioritizing the job queue is left to an external agent.  In
     * this way, the scheduler's activities are pared down to just
     * scheduling activies.
     * TODO: when dynamic scheduling is supported, the loop should
     * traverse through running job queue as well.
     */
    zlist_t *jobs = ctx->p_queue;
    int64_t starttime = (ctx->sctx.in_sim) ?
        (int64_t) ctx->sctx.sim_state->sim_time : epochtime();

    resrc_tree_release_all_reservations (resrc_phys_tree (ctx->rctx.root_resrc));
    rc = ctx->sops.sched_loop_setup (ctx->h);
    job = zlist_first (jobs);
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
#if 1
    /* try to schedule needs */
    job = zlist_first (ctx->i_queue);
    while (!rc && job) {
        flux_log (h, LOG_DEBUG, "Considering expansion of job: %ld\n", job->lwj_id);
        if (job->slackinfo->slack_state != CSLACK) {
            flux_log (h, LOG_DEBUG, "Instance job %ld not in CLSACK. Not scheduling.\n", job->lwj_id);
            goto next;
        }
        rc = schedule_dynamic_job (ctx, job);
        if (rc <= 0) {
            flux_log (h, LOG_DEBUG, "Job : %ld, not expanded", job->lwj_id);
            rc = 0;
            goto next;
        }
        flux_log (h, LOG_DEBUG, "Job: %ld has been expanded", job->lwj_id);
        sc++;
next:
        job = (flux_lwj_t*)zlist_next (ctx->i_queue);
    }    
#endif
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
        flux_log (h, LOG_INFO, "Job going to submitted");
        VERIFY (trans (J_PENDING, J_PENDING, &(job->state)));
        fill_hierarchical_req (h, job);
        /* fall through for implicit event generation */
    case J_PENDING:
        flux_log (h, LOG_INFO, "Job going from submitted to pending");
        VERIFY (trans (J_SCHEDREQ, J_SCHEDREQ, &(job->state)));
        //set_slack_state (ctx, false);
        schedule_jobs (ctx); /* includes request allocate if successful */
        //if (!ctx->resource_out_count)
        //    set_slack_state (ctx, true);
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
            flux_log (h, LOG_DEBUG, "Released resources for job %"PRId64"",
                      job->lwj_id);
        }
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
#if 1
    ssrvctx_t *ctx = getctx (h);

    flux_log (h, LOG_DEBUG, "Resource event, job completion detected");
 
    JSON ao = Jnew_ar (); 
    JSON jao = Jnew ();
    resrc_collect_own_resources_unasked (ctx->rctx.resrc_id_hash, ao);  // uuid_array only
    resrc_retrieve_lease_information (ctx->rctx.resrc_id_hash, ao, jao); // jobid : uuid_array, jobid : uuid_array

    flux_log (h, LOG_DEBUG, "Lease information : %s", Jtostr (jao));

    /* If in cslack, send invalid to parent and ask resources back if any */
    if (ctx->slctx.slack_state == CSLACK) {
        JSON pao = NULL;
        if (Jget_obj (jao, "-1", &pao)) {
            flux_log (h, LOG_DEBUG, "Sending slack invalid and asking for resources to parent: %s", Jtostr (pao));
            send_cslack_invalid (ctx, pao);
            resrc_mark_resources_asked (ctx->rctx.resrc_id_hash, pao);
        } else {
            flux_log (h, LOG_DEBUG, "Sending slack invalid and NOT asking anything");
            send_cslack_invalid (ctx, NULL);
        }
    } 

    flux_log (h, LOG_DEBUG, "Will check if anything to be asked from children");
    /* ask resources if any have been leased to children, i.key is string, i.val is json array */
    json_object_iter i;
    json_object_object_foreachC (jao, i) {

        flux_log (h, LOG_DEBUG, "Dealing with resource from %s with %s", i.key, Jtostr (i.val)); 

        /* ask parent only if not already asked due to cslack state */
        if (!(strcmp (i.key, "-1"))) {
            if (ctx->slctx.slack_state != CSLACK) {
                send_to_parent (ctx, i.val, "askresources");
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

        if (send_to_child (ctx, job, i.val, "askresources") < 0) {
            flux_log (h, LOG_ERR, "Could not send askresources to %ld", jobid);
            continue;
        }

        resrc_mark_resources_asked  (ctx->rctx.resrc_id_hash, i.val);
        flux_log (h, LOG_DEBUG, "resources asked to job : %ld", jobid);
    }         
 
    /* change slack state */
    ctx_slack_invalidate (ctx);
    if (ctx->slctx.resource_out_count > 0) {
        flux_log (h, LOG_DEBUG, "wait until return is being set as true");
        ctx->slctx.wait_until_return = 1;
    }
 
    //Jput (ao);
    //Jput (jao);    

#endif

    schedule_jobs (getctx ((flux_t)arg));

    return;
}

#if ENABLE_TIMER_EVENT
static int timer_event_cb (flux_t h, void *arg)
{
    ssrvctx_t *ctx = getctx ((flux_t)arg);
    int64_t jobid;
    int psize, rsize;
    int rc = 0;
    fflush(0);
    flux_log (h, LOG_ERR, "TIMER CALLED: %ld", epochtime()); fflush(0);

    /* 1. If there are any resources overtime, return them */
    return_idle_slack_resources (ctx);
    flux_log (h, LOG_DEBUG, "return idle resources successful");

    /* 2. If already in cslack, nothing to do */
    if (ctx->slctx.slack_state == CSLACK) {
        flux_log (h, LOG_DEBUG, "instance already in cslack. doing nothing");
        goto ret;
    }
    flux_log (h, LOG_DEBUG, "instance not in cslack");
    //schedule_jobs (ctx);

    /* 3. If last 2 timers, no change, then slack */
    retrieve_stat_info (ctx, &jobid, &psize, &rsize);
    if (ctx->slctx.slack_state == SLACK) {  
        flux_log (h, LOG_DEBUG, "Instance already in slack");
    } else if ((ctx->slctx.jobid == jobid) && (ctx->slctx.psize == psize) && (ctx->slctx.rsize == rsize)) {
        flux_log (h, LOG_DEBUG, "slack detected in timer");
        ctx->slctx.slack_state = SLACK;
    } else {
        flux_log (h, LOG_DEBUG, "updating recent job information");
        ctx->slctx.jobid = jobid;
        ctx->slctx.psize = psize;
        ctx->slctx.rsize = rsize;
    }

    /* 4. If in slack and all children cslack, then action */
    if ((ctx->slctx.slack_state == SLACK) && (all_children_cslack(ctx))) {
        flux_log (h, LOG_DEBUG, "instance in slack and all children in cslack. taking dynamic action");
        rc = dynamic_action(ctx);  // returns number of jobs started/expanded
        if (rc > 0) {
            flux_log (h, LOG_DEBUG, "started/expanded %d jobs, not changing to cslack", rc);
            goto ret;
        } else {
            flux_log (h, LOG_DEBUG, "no jobs started/expanded. going into cslack");
            ctx->slctx.slack_state = CSLACK;
        }
    }
    
    /* 5. If in cslack, submit to parent */
    //if ((ctx->slctx.slack_state == CSLACK)) {
    if ((ctx->parent_uri) && (ctx->slctx.slack_state == CSLACK)) {
        JSON jmsg = Jnew ();
        int rcount = 0;
        flux_log (h, LOG_DEBUG, "going to compute surplus");
        accumulate_surplus_need (ctx, 1, jmsg, &rcount);
        flux_log (h, LOG_DEBUG, "surplus accumulated, rcount = %d\n", rcount);
#if 1
        if (send_cslack (ctx, jmsg) < 0) {
            flux_log (h, LOG_DEBUG, "Could not send slack to parent. Error or parent does not implement push model.");
            goto ret;
        } else {
            flux_log (h, LOG_DEBUG, "CSLACK successfully sent to parent");
            JSON so = NULL;
            if ((Jget_obj (jmsg, "surplus", &so))) {
                flux_log (h, LOG_DEBUG, "Going to mark resource slacksub/returned");
                resrc_mark_resources_slacksub_or_returned (ctx->rctx.resrc_id_hash, so); // uuid : endtime uuid: endtime
                flux_log (h, LOG_DEBUG, "Marked slacksub/retured. Going to destroy returned resources");
                resrc_tree_destroy_returned_resources (resrc_phys_tree (ctx->rctx.root_resrc), ctx->rctx.resrc_id_hash); 
                flux_log (h, LOG_DEBUG, "Destroyed returned resources");
            }
            ctx->slctx.resource_out_count += rcount;
        }
        Jput (jmsg);
#endif
    }
    rc = 0;
ret:
    flux_log (h, LOG_DEBUG, "exiting timer");
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

static int req_childbirth_cb (flux_t h, int typemask,  zmsg_t **zmsg, void *arg)
{
    int rc = -1;
    int64_t jobid = -1;
    JSON in = NULL;
    const char *child_uri = NULL;
    //const char *json_in = NULL;
    ssrvctx_t *ctx = getctx (h);
    flux_lwj_t *job = NULL;

    flux_log (h, LOG_DEBUG, "Child has scheduler!\n");

    /* decode msg */
    if (flux_json_request_decode (*zmsg, &in) < 0)
      flux_log (h, LOG_ERR, "Unable to decode message into json\n");

#if 0
    const char *header = flux_msg_get_route_string (*zmsg);
    flux_log (h, LOG_DEBUG, "zmsg = %s\n",header);
#endif

    /* Decode information */
    Jget_int64 (in, "jobid", &jobid);
    Jget_str (in, "child-uri", &child_uri);
    job = q_find_job (ctx, jobid);
    if (job)
        job->contact = xstrdup (child_uri);
    Jput (in);

#if 0
    /* reply to msg */
    JSON out = Jnew ();
    Jadd_str(out, "test", "teststring");
    rc = flux_json_respond (h, out, zmsg);
    Jput (out);
#endif

    /* open a handle and send a message */
#if 0
    flux_rpc_t *rpc;
    flux_t child_h = flux_open (child_uri, 0);
    if (child_h == NULL) {
        flux_log (h, LOG_INFO, " ERROR Could not do child open");
        //return rc;
    }
    flux_log (h, LOG_DEBUG, "child connection opened\n");
    rpc = flux_rpc (child_h, "sched.addresources", json_in, FLUX_NODEID_ANY, 0);
    flux_close (child_h);
#endif

#if 0
    zmsg_t *newmsg = flux_msg_create (FLUX_MSGTYPE_REQUEST);
    flux_msg_enable_route (newmsg);
    flux_msg_push_route (newmsg, header);
    char *top = NULL;
    //flux_msg_pop_route (newmsg, &top);
    flux_msg_get_route_last (newmsg, &top);
    flux_log (h, LOG_INFO, "top = %s\n",top);
    flux_msg_set_topic (newmsg, "sched.addresources");
    int routecount = flux_msg_get_route_count (newmsg);
    flux_log (h, LOG_INFO, "route count = %d\n", routecount);
    flux_send (h, newmsg, 0);
    zmsg_destroy (&newmsg);
#endif

    rc = 0;
    return rc;
}

static int req_qstat_cb (flux_t h, int typemask, zmsg_t **zmsg, void *arg)
{
    int rc = -1;
    int psize = -1;
    int rsize = -1;
    ssrvctx_t *ctx = NULL;
    JSON out = Jnew ();

    /* debug */
    flux_log (h, LOG_DEBUG, "Received qstat request\n");

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
    flux_json_respond (h, out, zmsg);
    Jput (out);

    /* return success always*/
    rc = 0;
    return rc;
}

/* 
 * receive_resources_parent_from_child: Parent receives surplus resources from child 
 */
static int req_cslack_cb (flux_t h, int typemask, zmsg_t **zmsg, void *arg)
{
    flux_log (h, LOG_DEBUG, "Callback on reporting surplus/need called");

    int rc = 0;
    int64_t jobid, id;
    flux_lwj_t *job = NULL;
    ssrvctx_t *ctx = getctx (h);

    JSON in = Jnew ();
    JSON jmsg = NULL;
    JSON surobj = NULL;
    JSON needobj = NULL;

    /* decode msg */ 
    if (flux_json_request_decode (*zmsg, &in) < 0) {
        flux_log (h, LOG_ERR, "Unable to decode message into json");
        return rc;
    }
    flux_log (h, LOG_DEBUG, "Message received: %s", Jtostr (in));

    /* get information */
    Jget_int64 (in, "jobid", &jobid);
   
    /* get job */
    job = find_job_in_queue (ctx->i_queue, jobid);
    if (!job) {
        flux_log (h, LOG_ERR, "Job not found");
        goto ret;
    }    
    flux_log (h, LOG_DEBUG, "Job has been found");
    
    /* Update surplus and need */
    const char *jmsg_str = NULL;
    if (!(Jget_str (in, "msg", &jmsg_str))) {
        flux_log (h, LOG_DEBUG, "No surplus or need provided");
        goto ret;
    }
    jmsg = Jfromstr (jmsg_str);
    flux_log (h, LOG_DEBUG, "surplus or need information provided");

    fflush(0);

    if (Jget_obj (jmsg, "surplus", &surobj)) {
        flux_log (h, LOG_DEBUG, "jobid = %ld and surplus = %s", jobid, Jtostr (surobj));
        /* Update slack reservation */
        json_object_iter iter;
        json_object_object_foreachC (surobj, iter) {

            id = strtol (iter.key, NULL, 10);
            resrc_t *resrc = zhash_lookup (ctx->rctx.resrc_id_hash, iter.key);
            if (!resrc) {
                flux_log (h, LOG_ERR, "Resource not found");
                continue;
            }
            flux_log (h, LOG_DEBUG, "Resource type: %s, uuid: %s is being reported as slack", resrc_type (resrc), iter.key);
            if (!(!(strncmp (resrc_type (resrc), "core", 4)))) {
                flux_log (h, LOG_ERR, "FATAL: non core resource slacksubbed by child");
                continue;
            }
            printf("CSLACK: passsed checks\n");
            const char *endtime_str = json_object_get_string (iter.val);
            printf ("CLSACK: endtime_str = %s\n", endtime_str);
            fflush(0);

            if (!endtime_str) {
                flux_log (h, LOG_ERR, "Value invalid");
                goto ret;
            }
            flux_log (h, LOG_DEBUG, "slack endtime obtained as: %s", endtime_str);

            int64_t endtime = strtol (endtime_str, NULL, 10);
            if (endtime == 0) {
                // a slack resource that was originally sent from me
                resrc_mark_resource_return_received (resrc, jobid);
                if (resrc_owner(resrc) == 0)
                    ctx->slctx.resource_out_count--;
                flux_log (h, LOG_DEBUG, "Return resource received during slacksub :%s\n", resrc_name (resrc));
            } else {
                // a slack resource from job's allocation
                resrc_mark_resource_slack (resrc, jobid, endtime);    
                printf ("CSLACK: Resource marked as slack: %s\n", resrc_name (resrc));
           }
        }
    } else {    
        flux_log (h, LOG_DEBUG, "Surplus = 0");
    } 

    if (Jget_obj (jmsg, "need", &needobj)) {
        flux_log (h, LOG_DEBUG, "needob set: %s", Jtostr (needobj));
    } else {
        flux_log (h, LOG_DEBUG, "Need = 0");
    }
#if 1
    /* update job slack state */
    job_cslack_set (job, needobj);
    flux_log (h, LOG_DEBUG, "job cslack set");
#endif

#if 1
    /* return idle resources */
    if (return_idle_slack_resources(ctx) < 0) {
        flux_log (h, LOG_ERR, "error in returning idle slack resources");
    }
    flux_log (h, LOG_DEBUG, "Return idle resources successful"); 
#endif
    
    if (ctx->slctx.slack_state == CSLACK) {
        //this should never happen
        flux_log (h, LOG_DEBUG, "FATAL: in cslack when child is sending surplus");
        send_cslack_invalid (ctx, NULL);
        ctx->slctx.slack_state = INVALID;        
    } else if (ctx->slctx.slack_state == SLACK) {
        flux_log (h, LOG_DEBUG, "SLACK state going back to invalid");
        ctx->slctx.slack_state = INVALID;
    } else { 
        flux_log (h, LOG_DEBUG, "Slack state in parent already not reached yet");
        ctx->slctx.slack_state = INVALID;
    }
   
    fflush (0);

    schedule_jobs (ctx);
 
ret:
    Jput (in);
    rc = 0;
    return rc;
}

/*
 * resources added could be new or returned. 
 */
static int req_addresources_cb (flux_t h, int typemask, zmsg_t **zmsg, void *arg)
{
    int rc = -1;
    int i = 0;
    int len = 0;
    JSON id_array;
    int64_t jobid;
    JSON new_res = NULL;
    JSON jmsg = NULL;

    flux_log (h, LOG_DEBUG, "!!!!!!!!!!!!!!!!!!!!!!!!!Received add resources message!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");

    /* get context */
    ssrvctx_t *ctx = getctx (h);

    /* decode msg */
    JSON in = Jnew ();
    if (flux_json_request_decode (*zmsg, &in) < 0) {
        flux_log (h, LOG_ERR, "Unable to decode message into json");
        goto ret;
    }
    flux_log (h, LOG_DEBUG, "Message decode succeeded in addresources");
    
    if (!(Jget_int64 (in, "jobid", &jobid))) {
        flux_log (h, LOG_ERR, "Unable to determine who added resources");
        goto ret;
    }
    flux_log (h, LOG_DEBUG, "Resources added by %ld", jobid);
   
    if (!(Jget_obj (in, "msg", &jmsg))) {
        flux_log (h, LOG_ERR, "No message given in addresources");
        goto ret;
    }
    flux_log (h, LOG_DEBUG, "Message in add resources retrieved: %s", Jtostr (jmsg));

#if 1
    if (!(Jget_obj (jmsg, "return", &id_array))) {
        flux_log (h, LOG_DEBUG, "no resources returned");
    } else {
        // collect all returned resources
        flux_log (h, LOG_DEBUG, "resource return included in addresources");
        if (!(Jget_ar_len (id_array, &len))) {
            flux_log (h, LOG_ERR, "FATAL: Could not get length of id array. skipping return resources");
            goto new;        
        }

        for (i = 0; i < len; i++) {
            const char *uuid;
            Jget_ar_str (id_array, i, &uuid);
            resrc_t *resrc = zhash_lookup (ctx->rctx.resrc_id_hash, uuid);
            if (!resrc) {
                flux_log (h, LOG_ERR, "stub resource in return resource");
                continue;
            }
            if (!(!(strcmp (resrc_type (resrc), "core")))) {
                flux_log (h, LOG_ERR, "non-core resource returned by job dropping it: %ld\n", jobid);
                continue;
            }
            resrc_mark_resource_return_received (resrc, jobid);
            if (resrc_owner (resrc) == 0)
                ctx->slctx.resource_out_count--;      
            flux_log (h, LOG_DEBUG, "resource return = %s\n", resrc_name (resrc)); 
        }    
    }
#endif

new:
    if (!(Jget_obj (jmsg, "new", &new_res))) {
        flux_log (h, LOG_DEBUG, "no new resources added");
    } else {
        flux_log (h, LOG_DEBUG, "new resources provided");
        if (resrc_add_resources_from_json (ctx->rctx.root_resrc, ctx->rctx.resrc_id_hash, new_res, jobid) < 0) {
            flux_log (h, LOG_ERR, "error in adding new resources");
            return rc;
        } 
        flux_log (h, LOG_DEBUG, "new resources added succesfully");
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
    flux_log (h, LOG_DEBUG, "slack state update successful");

    /* return any idle slack resource */
    if (return_idle_slack_resources (ctx) < 0) {
        flux_log (h, LOG_ERR, "error in returning idle slack resources");
        return rc;
    }
    flux_log (h, LOG_DEBUG, "slack resources return successful");

    if ((ctx->slctx.wait_until_return) && (ctx->slctx.resource_out_count == 0)) 
        ctx->slctx.wait_until_return = 0;

    /* schedule jobs */
    schedule_jobs (ctx);
    
    rc = 0;
ret:
    Jput (in);
    return rc;
}

static int req_cslack_invalid_cb (flux_t h, int typemask, zmsg_t **zmsg, void *arg)
{
    int rc = 0;
    int64_t jobid;
    flux_lwj_t *job;
    ssrvctx_t *ctx = getctx (h);
    JSON lao = Jnew ();
    JSON in = Jnew ();

    flux_log (h, LOG_DEBUG, "6666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666 SLACK INVALIDATE MESSAGE received");

    if (flux_json_request_decode (*zmsg, &in) < 0) {
        flux_log (h, LOG_ERR, "Unable to decode message into json");
        goto ret;
    }
    flux_log (h, LOG_DEBUG, "Message in req_cslack_invalid_cb decoded");

    if (!(Jget_int64 (in, "jobid", &jobid))) {
        flux_log (h, LOG_ERR, "Could not fetch jobid on receiving slack invalidate");
        goto ret;
    }
    flux_log (h, LOG_DEBUG, "Slack invalide message from job: %"PRId64"\n", jobid);

    /* 1. Find the job */
    job = find_job_in_queue (ctx->r_queue, jobid);
    if (!job) {
        flux_log (h, LOG_ERR, "job %ld not found in running queue", jobid);
        goto ret;
    }   
    flux_log (h, LOG_DEBUG, "job found in running queue");

    /* 2. Set slack invalid in job */
    job_cslack_invalidate (job);

    /* 3. Have resources been asked for? lao contains "jobid": "uuid_array" -1 = parent, 0 = me, jobid = children */
    JSON askobj = NULL;
    if (!(Jget_obj (in, "ask", &askobj))) {
        flux_log (h, LOG_DEBUG, "No resources have been asked");
    } else {
        flux_log (h, LOG_DEBUG, "Resources have been asked");
        resrc_retrieve_lease_information (ctx->rctx.resrc_id_hash, askobj, lao);
        flux_log (h, LOG_DEBUG, "resources asked = %s", Jtostr (lao));
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
            send_to_parent (ctx, pao, "askresources");
            resrc_mark_resources_asked (ctx->rctx.resrc_id_hash, pao);
            resrc_mark_resources_to_be_returned (ctx->rctx.resrc_id_hash, pao);
        }
    }

    /* 5. ask rest of the resources */
    json_object_iter i;
    json_object_object_foreachC (lao, i) {
        flux_log (h, LOG_DEBUG, "Dealing with resource from %s with %s", i.key, Jtostr (i.val));
        if (!(strcmp (i.key, "-1")))
            continue;
        if (!(strcmp (i.key, "0"))) {
            resrc_mark_resources_to_be_returned (ctx->rctx.resrc_id_hash, i.val);
            continue;
        } 
        int64_t jobid = strtol (i.key, NULL, 10);
        flux_lwj_t *job = find_job_in_queue (ctx->i_queue, jobid);
        flux_log (h, LOG_DEBUG, "Job currently using this set of resources is not an instance. Just marking as to_be_returned");
        if ((job) && (send_to_child (ctx, job, i.val, "askresources") < 0)) {
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
    return rc;
}

static int req_askresources_cb (flux_t h, int typemask, zmsg_t **zmsg, void *arg)
{
    int rc = -1;
    int64_t jobid;
    ssrvctx_t *ctx = getctx (h);
    int len = 0, i = 0;

    JSON in = Jnew ();
    if (flux_json_request_decode (*zmsg, &in) < 0) {
        flux_log (h, LOG_ERR, "Unable to decode message into json");
        goto ret;
    }
    flux_log (h, LOG_DEBUG, "Message in req_askresources_cb decoded");    

    if (!(Jget_int64 (in, "jobid", &jobid))) {
        flux_log (h, LOG_ERR, "Could not fetch jobid on receiving askresources");
        goto ret;
    }
    flux_log (h, LOG_DEBUG, "Ask resources from %"PRId64"\n", jobid);    
    
    JSON id_ar = NULL;
    if (!(Jget_obj (in, "msg", &id_ar))) {
        flux_log (h, LOG_ERR, "No message given in askresources");
        goto ret;
    }

    if (!(Jget_ar_len (id_ar, &len))) {
        flux_log (h, LOG_ERR, "Message received not valid uuid array. Could not retrieve length");
        goto ret;
    }
    flux_log (h, LOG_DEBUG, "resources asked length = %d and resources = %s", len, Jtostr (id_ar));

    resrc_mark_resources_to_be_returned (ctx->rctx.resrc_id_hash, id_ar);
    flux_log (h, LOG_DEBUG, "all resources marked to be returned");

    JSON juid = Jnew (); //jobid: uuidi_array, jobid:uuid_array

    for (i = 0; i < len; i++) {
        const char *uuid;
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
            asprintf (&leasee_str, "%ld", leasee);
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
    json_object_object_foreachC (juid, iter);
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
            if (send_to_child (ctx, job, iter.val, "askresources") < 0) {
                flux_log (h, LOG_ERR, "Could not send askresources to child from askresources");
                goto ret;
            }
            resrc_mark_resources_asked (ctx->rctx.resrc_id_hash, iter.val);
        } else {
            // ask parent: this should ideally not happen
            if (send_to_parent (ctx, iter.val, "askresources") < 0) {
                flux_log (h, LOG_ERR, "Could not send askresources to parent from askresources");
                goto ret;
            }
            resrc_mark_resources_asked (ctx->rctx.resrc_id_hash, iter.val);
        }    
    }   
    
    return_idle_slack_resources (ctx);

    rc = 0;
ret:
    return rc;
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
    uint32_t rank = 1;

    if (!(ctx = getctx (h))) {
        flux_log (h, LOG_ERR, "can't find or allocate the context");
        goto done;
    }

    flux_log (h, LOG_DEBUG, "argc = %d", argc);

    for (i = 0; i < argc; i++) {
        flux_log (h, LOG_DEBUG, "argv[%d] = %s", i, argv[i]);
        if (!strncmp ("rdl-conf=", argv[i], sizeof ("rdl-conf"))) {
            path = xstrdup (strstr (argv[i], "=") + 1);
            flux_log (h, LOG_DEBUG, "RDL given");
        } else if (!strncmp ("rdl-resource=", argv[i], sizeof ("rdl-resource"))) {
            uri = xstrdup (strstr (argv[i], "=") + 1);
        } else if (!strncmp ("in-sim=", argv[i], sizeof ("in-sim"))) {
            sim = xstrdup (strstr (argv[i], "=") + 1);
        } else if (!strncmp ("plugin=", argv[i], sizeof ("plugin"))) {
            userplugin = xstrdup (strstr (argv[i], "=") + 1);
        } else if (!strncmp ("jobid=", argv[i], sizeof ("jobid"))) {
            char *jobid_str = xstrdup (strstr (argv[i], "=") + 1);
            ctx->my_job_id = strtol (jobid_str, NULL, 10);
            flux_log (h, LOG_DEBUG, "Job is an instance with jobid = %ld", ctx->my_job_id); 
        } else {
            flux_log (ctx->h, LOG_ERR, "module load option %s invalid", argv[i]);
            errno = EINVAL;
            goto done;
        }
    }

    if (userplugin == NULL) {
        schedplugin = "backfill.plugin1";
    } else {
        schedplugin = userplugin;
    }

    if (flux_get_rank (h, &rank)) {
        flux_log (h, LOG_ERR, "failed to determine rank");
        goto done;
    } else if (rank) {
        flux_log (h, LOG_ERR, "sched module must only run on rank 0");
        goto done;
    }
    flux_log (h, LOG_INFO, "sched comms module starting");
    if (load_sched_plugin (ctx, schedplugin) != 0) {
        flux_log (h, LOG_ERR, "failed to load scheduler plugin");
        goto done;
    }
    flux_log (h, LOG_INFO, "%s plugin loaded", schedplugin);

    if (send_childbirth_msg (ctx) != 0) {
        flux_log (h, LOG_ERR, "Instance is child and sending childbirth message failed");
        goto done;
    }
    flux_log (h, LOG_INFO, "Instance birth successful");
 
   if (load_resources (ctx, path, uri) != 0) {
        flux_log (h, LOG_ERR, "failed to load resources");
        goto done;
    }
    flux_log (h, LOG_INFO, "resources loaded");

    print_rdl (ctx); fflush(0);

    if ((sim) && setup_sim (ctx, sim) != 0) {
        flux_log (h, LOG_ERR, "failed to setup sim");
        goto done;
    }
    if (ctx->sctx.in_sim) {
        if (reg_sim_events (ctx) != 0) {
            flux_log (h, LOG_ERR, "failed to reg sim events");
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
    flux_log (h, LOG_INFO, "events registered");

    if (reg_requests (ctx) != 0) {
        flux_log (h, LOG_ERR, "failed to reg requests");
        goto done;
    }
    flux_log (h, LOG_INFO, "requests registered");
    
    if (flux_reactor_start (h) < 0) {
        flux_log (h, LOG_ERR, "flux_reactor_start: %s", strerror (errno));
        goto done;
    }
    rc = 0;

done:
    fflush(0);
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
