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
#include <getopt.h>
#include <errno.h>
#include <czmq.h>
#include <json.h>
#include <flux/core.h>

#include "src/common/libutil/shortjson.h"
#include "src/common/libutil/jsonutil.h"
#include "src/common/libutil/log.h"
#include "src/common/libutil/xzmalloc.h"

typedef struct {
    flux_t h;
    int64_t jobid;
    char *start;
    char *complete;
} wjctx_t;

static flux_t sig_flux_h;

#define OPTIONS "h:c:s:"
static const struct option longopts[] = {
    {"help",          no_argument,        0, 'h'},
    {"sync-start",    required_argument,  0, 's'},
    {"sync-complete", required_argument,  0, 'c'},
    { 0, 0, 0, 0 },
};

static void usage (void)
{
    fprintf (stderr,
"Usage: flux-waitjob [OPTIONS] jobid\n"
" Block waiting until the job specified by jobid completes.\n"
" The OPTIONS are:\n"
"  -h, --help                    Display this message\n"
"  -s, --sync-start=filename1    Create an empty file (filename1) right after the reactor starts\n"
"  -c, --sync-complete=filename2 Create an empty file (filename2) right after jobid completed\n"
);
    exit (1);
}

static void freectx (void *arg)
{
    wjctx_t *ctx = arg;
    if (ctx->start)
        free (ctx->start);
    if (ctx->complete)
        free (ctx->complete);
    free (ctx);
    ctx = NULL;
}

static wjctx_t *getctx (flux_t h)
{
    wjctx_t *ctx = (wjctx_t *)flux_aux_get (h, "waitjob");
    if (!ctx) {
        ctx = xzmalloc (sizeof (*ctx));
        ctx->jobid = -1;
        ctx->h = h;
        ctx->start = NULL;
        ctx->complete = NULL;
        flux_aux_set (h, "waitjob", ctx, freectx);
    }
    return ctx;
}

static void sig_handler (int s)
{
    if (s == SIGINT) {
        fprintf (stdout, "Exit on INT\n\n");
        /* this will call freectx */
        flux_close (sig_flux_h);
        log_fini ();
        exit (0);
    }
}

static void create_outfile (const char *fn)
{
    FILE *fp = NULL;
    if (fn && !(fp = fopen (fn, "w"))) {
        fprintf (stderr, "Failed to open %s\n", fn);
        fclose (fp);
    }
}

static inline void get_jobid (JSON jcb, int64_t *j)
{
    Jget_int64 (jcb, JSC_JOBID, j);
}

static inline void get_states (JSON jcb, int64_t *os, int64_t *ns)
{
    JSON o = NULL;
    Jget_obj (jcb, JSC_STATE_PAIR, &o);
    Jget_int64 (o, JSC_STATE_PAIR_OSTATE, os);
    Jget_int64 (o, JSC_STATE_PAIR_NSTATE, ns);
}

static bool complete_job (wjctx_t *ctx)
{
    JSON jcb = NULL;
    JSON o = NULL;
    bool rc = false;
    char *json_str = NULL;
    int64_t state = J_NULL;

    if (jsc_query_jcb (ctx->h, ctx->jobid, JSC_STATE_PAIR, &json_str) == 0) {
        jcb = Jfromstr (json_str);
        Jget_obj (jcb, JSC_STATE_PAIR, &o);
        Jget_int64 (o, JSC_STATE_PAIR_NSTATE, &state);
        Jput (jcb);
        free (json_str);
        flux_log (ctx->h, LOG_INFO, "%"PRId64" already started (%s)",
                     ctx->jobid, jsc_job_num2state (state));
        if (state == J_COMPLETE) {
            flux_log (ctx->h, LOG_INFO, "%"PRId64" already completed", ctx->jobid);
            rc = true;
        }
    }
    return rc;
}

static int waitjob_cb (const char *jcbstr, void *arg, int errnum)
{
    JSON jcb = NULL;
    int64_t os = 0, ns = 0, j = 0;
    flux_t h = (flux_t)arg;
    wjctx_t *ctx = getctx (h);

    if (errnum > 0) {
        flux_log (ctx->h, LOG_ERR, "waitjob_cb: errnum passed in");
        return -1;
    }

    if (!(jcb = Jfromstr (jcbstr))) {
        flux_log (ctx->h, LOG_ERR, "waitjob_cb: error parsing JSON string");
        return -1;
    }
    get_jobid (jcb, &j);
    get_states (jcb, &os, &ns);
    Jput (jcb);
    if ((j == ctx->jobid) && (ns == J_COMPLETE)) {
        if (ctx->complete)
            create_outfile (ctx->complete);
        flux_log (ctx->h, LOG_INFO, "waitjob_cb: completion notified");
        raise (SIGINT);
    }

    return 0;
}

static void sync_event_cb (flux_t h, flux_msg_handler_t *w,
                           const flux_msg_t *msg, void *arg)
{
    wjctx_t *ctx = getctx (h);

    if (ctx->start)
        create_outfile (ctx->start);

    if (flux_event_unsubscribe (h, "hb") < 0) {
        flux_log (h, LOG_ERR, "%s: flux_event_unsubscribe hb: %s",
                 __FUNCTION__, strerror (errno));
    }
    if (jsc_notify_status (h, waitjob_cb, (void *)h) != 0) {
        flux_log (h, LOG_ERR, "failed to reg a waitjob CB");
    }
    if (complete_job (ctx)) {
        if (ctx->complete)
            create_outfile (ctx->complete);
        flux_log (ctx->h, LOG_INFO, "sync_event_cb: completion detected");
    }
    return;
}

static struct flux_msg_handler_spec htab[] = {
    { FLUX_MSGTYPE_EVENT,     "hb", sync_event_cb},
    FLUX_MSGHANDLER_TABLE_END
};

static int wait_job_complete (flux_t h)
{
    int rc = -1;
    sig_flux_h = h;

    if (signal (SIGINT, sig_handler) == SIG_ERR) 
        goto done;
    if (flux_event_subscribe (h, "hb") < 0) {
        flux_log (h, LOG_ERR, "flux_event_subscribe: %s",
                  strerror (errno));
        goto done;
    }
    if (flux_msg_handler_addvec (h, htab, (void *)h) < 0) {
        flux_log (h, LOG_ERR,
                  "error registering sync event handler: %s",
                  strerror (errno));
        goto done;
    }
    if (flux_reactor_start (h) < 0) {
        flux_log (h, LOG_ERR, "error in flux_reactor_start");
        goto done;
    }
    rc = 0;
done:
    return rc;
}

/******************************************************************************
 *                                                                            *
 *                            Main entry point                                *
 *                                                                            *
 ******************************************************************************/

int main (int argc, char *argv[])
{
    flux_t h;
    int ch = 0;
    int64_t jobid = -1;
    char *sfn = NULL;
    char *cfn = NULL;
    wjctx_t *ctx = NULL;

    log_init ("flux-waitjob");
    while ((ch = getopt_long (argc, argv, OPTIONS, longopts, NULL)) != -1) {
        switch (ch) {
            case 'h': /* --help */
                usage ();
                break;
            case 's': /* --sync-start */
                sfn = strdup (optarg);
                break;
            case 'c': /* --sync-complete */
                cfn = strdup (optarg);
                break;
            default:
                usage ();
                break;
        }
    }
    if (optind == argc)
        usage ();

    jobid = strtol (argv[optind], NULL, 10);
    if (jobid <= 0)
        err_exit ("jobid must be a positive number");
    else if (!(h = flux_open  (NULL, 0)))
        err_exit ("flux_open");

    ctx = getctx (h);
    if (sfn)
        ctx->start = sfn;
    if (cfn)
        ctx->complete = cfn;
    ctx->jobid = jobid;

    flux_log_set_facility (h, "waitjob");
    wait_job_complete (h);

    flux_close (h);
    log_fini ();

    return 0;
}

/*
 * vi:tabstop=4 shiftwidth=4 expandtab
 */
