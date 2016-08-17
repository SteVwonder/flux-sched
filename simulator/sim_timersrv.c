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

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <json.h>
#include <flux/core.h>

#include "src/common/libutil/log.h"
#include "src/common/libutil/shortjson.h"
#include "src/common/libutil/xzmalloc.h"
#include "simulator.h"

// TODO: grab the actual enum in the scheduler.h header
typedef enum {
    INVALID = 0,
    SLACK,
    CSLACK
} slack_state_t;

typedef struct {
    flux_t h;
    flux_t sim_h;
    char *sim_uri;
    char *my_sim_id;
    char *module_name;
    char *sched_timer_topic;
    int timer_interval;
    bool should_trigger_sched;
    bool is_root;
} ctx_t;

static ctx_t *getctx (flux_t h)
{
    ctx_t *ctx = (ctx_t *)flux_aux_get (h, "timer");
    if (!ctx) {
        ctx = xzmalloc(sizeof(ctx_t));
        ctx->h = h;
        ctx->sim_h = NULL;
        ctx->sim_uri = NULL;
        ctx->my_sim_id = NULL;
        ctx->module_name = NULL;
        ctx->sched_timer_topic = NULL;
        ctx->should_trigger_sched = false;
        ctx->is_root = false;
        // Set to -2 for static scheduling
        // Set to positive integer for dynamic scheduling
#ifdef DYNAMIC_SCHEDULING
        ctx->timer_interval = 100;
        //ctx->timer_interval = 30;
#else
        ctx->timer_interval = -2;
#endif
    }

    return ctx;
}

// Received an event that a simulation is starting
static void start_cb (flux_t h,
                      flux_msg_handler_t *w,
                      const flux_msg_t *msg,
                      void *arg)
{
    ctx_t *ctx = (ctx_t*) arg;
    int next_event = -1;

    if (ctx->is_root) {
        next_event = 1;
    }

    flux_log (h, LOG_DEBUG, "received a start event");

    if (send_join_request (h, ctx->sim_h, ctx->module_name, next_event) < 0) {
        flux_log (h,
                  LOG_ERR,
                  "timer module failed to register with sim module");
        return;
    }
    flux_log (h, LOG_DEBUG, "sent a join request");

    if (flux_event_unsubscribe (h, "sim.start") < 0) {
        flux_log (h, LOG_ERR, "failed to unsubscribe from \"sim.start\"");
    } else {
        flux_log (h, LOG_DEBUG, "unsubscribed from \"sim.start\"");
    }
}

static void trigger_cb (flux_t h,
                        flux_msg_handler_t *w,
                        const flux_msg_t *msg,
                        void *arg)
{
    ctx_t *ctx = (ctx_t*) arg;
    JSON o = NULL;
    const char *json_str = NULL;
    const char *json_out = NULL;
    sim_state_t *sim_state = NULL;
    int slack_state = 0;

    flux_log (h, LOG_DEBUG, "received a trigger");

    if (flux_msg_get_payload_json (msg, &json_str) < 0 || json_str == NULL
        || !(o = Jfromstr (json_str))) {
        flux_log (h, LOG_ERR, "%s: bad message", __FUNCTION__);
        Jput (o);
        return;
    }

    // Logging
    flux_log (h,
              LOG_DEBUG,
              "received a trigger (timer.trigger): %s",
              json_str);

    if (ctx->should_trigger_sched) {
        // invoke timer event in sched
        Jput (o);
        flux_log (h, LOG_DEBUG, "sched timer topic: %s", ctx->sched_timer_topic);
        flux_rpc_t *rpc = flux_rpc (h, ctx->sched_timer_topic, json_str, FLUX_NODEID_ANY, 0);
        if (flux_rpc_get (rpc, NULL, &json_out) < 0 || !json_out) {
            flux_log (h, LOG_ERR, "%s: failed to extract payload from sched response", __FUNCTION__);
            return;
        }
        if (!(o = Jfromstr (json_out))) {
            flux_log (h, LOG_ERR, "%s: bad message has been given by the sched module to timer", __FUNCTION__);
            return;
        }
    } else {
        ctx->should_trigger_sched = true;
    }

    sim_state = json_to_sim_state (o);
    Jget_int (o, "slack_state", &slack_state);

    if (slack_state == 0) {
        double *new_time = (double *)zhash_lookup (sim_state->timers, ctx->module_name);
        if (new_time && ctx->timer_interval > 0) {
            *new_time = sim_state->sim_time + ctx->timer_interval;
        } // else: dynamic scheduling is turned off or the timer module isn't in the zhash
    } else {
        flux_log (h, LOG_DEBUG, "%s: sched is in a slack state, not setting the timer", __FUNCTION__);
        ctx->should_trigger_sched = false;
    }

    send_reply_request (h, ctx->sim_h, ctx->module_name, sim_state);

    // Cleanup
    free_simstate (sim_state);
    Jput (o);
}

int mod_main (flux_t h, int argc, char **argv)
{
    int64_t jobid = 0;
    char *prefix = NULL;
    int i;

    ctx_t *ctx = getctx (h);
    zhash_t *args = zhash_fromargv (argc, argv);
    if (!args)
        oom ();

    flux_log (h, LOG_INFO, "Timer module starting");

    for (i = 0; i < argc; i++) {
        if (!strncmp ("jobid=", argv[i], sizeof ("jobid"))) {
            char *jobid_str = xstrdup (strstr (argv[i], "=") + 1);
            jobid = strtol (jobid_str, NULL, 10);
            free (jobid_str);
        }
        if (!strncmp ("time=", argv[i], sizeof ("time"))) {
            char *time_str = xstrdup (strstr (argv[i], "=") + 1);
            ctx->timer_interval = strtol (time_str, NULL, 10);
            free (time_str);
        }
        if (!strncmp ("prefix=", argv[i], sizeof ("prefix"))) {
            prefix = xstrdup (strstr (argv[i], "=") + 1);
        }
        if (!strncmp ("sim_uri=", argv[i], sizeof ("sim_uri"))) {
            ctx->sim_uri = xstrdup (strstr (argv[i], "=") + 1);
        }
    }

    ctx->is_root = (prefix == NULL);

    if (prefix) {
        asprintf (&ctx->module_name, "sim_timer.%s.%ld", prefix, jobid);
        asprintf (&ctx->my_sim_id, "%s.%ld", prefix, jobid);
        asprintf (&ctx->sched_timer_topic, "sched.%s.%ld.timer", prefix, jobid);
        free (prefix);
    } else {
        asprintf (&ctx->module_name, "sim_timer.%ld", jobid);
        asprintf (&ctx->my_sim_id, "%ld", jobid);
        asprintf (&ctx->sched_timer_topic, "sched.%ld.timer", jobid);
    }

    if (flux_event_subscribe (h, "sim.start") < 0) {
        flux_log (h, LOG_ERR, "subscribing to event: %s", strerror (errno));
        return -1;
    }

    char *timertrigger;
    asprintf (&timertrigger, "%s.trigger", ctx->module_name);
    flux_log (h, LOG_DEBUG, "trigger function : %s", timertrigger);

    struct flux_msg_handler_spec htab1[] = {
        {FLUX_MSGTYPE_EVENT, "sim.start", start_cb},
        {FLUX_MSGTYPE_REQUEST, timertrigger, trigger_cb},
        FLUX_MSGHANDLER_TABLE_END,
    };

    if (flux_msg_handler_addvec (h, htab1, ctx) < 0) {
        flux_log (h, LOG_ERR, "flux_msg_handler_addvec: %s", strerror (errno));
        return -1;
    }

    free (timertrigger);

    if (ctx->sim_uri) {
        ctx->sim_h = flux_open (ctx->sim_uri, 0);
        if (!ctx->sim_h) {
            flux_log (h, LOG_ERR, "Could not open handle to sim");
            return -1;
        }
    } else {
        ctx->sim_h = h;
    }
    if (ctx->is_root) {
        send_alive_request (h, ctx->sim_h, ctx->module_name);
    } else {
        if (send_join_request (h, ctx->sim_h, ctx->module_name, -1) < 0) {
            flux_log (h,
                      LOG_ERR,
                      "timer module failed to register with sim module");
            return -1;
        }
        flux_log (h, LOG_DEBUG, "sent a join request");

        if (flux_event_unsubscribe (h, "sim.start") < 0) {
            flux_log (h, LOG_ERR, "failed to unsubscribe from \"sim.start\"");
        } else {
            flux_log (h, LOG_DEBUG, "unsubscribed from \"sim.start\"");
        }
    }

    if (flux_reactor_run (flux_get_reactor (h), 0) < 0) {
        flux_log (h, LOG_ERR, "flux_reactor_run: %s", strerror (errno));
        return -1;
    }

    return 0;
}

MOD_NAME ("sim_timer");
