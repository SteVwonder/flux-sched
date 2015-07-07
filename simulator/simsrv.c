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
#include <stdint.h>
#include <string.h>
#include <errno.h>
#include <json.h>
#include <flux/core.h>

#include "src/common/libutil/jsonutil.h"
#include "src/common/libutil/log.h"
#include "src/common/libutil/shortjson.h"
#include "src/common/libutil/xzmalloc.h"
#include "simulator.h"

typedef struct {
	sim_state_t *sim_state;
    flux_t h;
    FILE *output_file;
    bool rdl_changed;
    char* rdl_string;
    bool exit_on_complete;
} ctx_t;

static void freectx (void *arg)
{
    ctx_t *ctx = arg;
	free_simstate (ctx->sim_state);
    if (ctx->output_file)
        fclose (ctx->output_file);
    free (ctx->rdl_string);
    free (ctx);
}

static ctx_t *getctx (flux_t h, char* save_path, bool exit_on_complete)
{
    ctx_t *ctx = (ctx_t *)flux_aux_get (h, "simsrv");
    int max_fname_len = 100, i = 0;
    char filename[max_fname_len];

    if (!ctx) {
        if (save_path) {
            snprintf (filename, max_fname_len, "%s/sim_state_save.json", save_path);
            for (i = 0; access( filename, F_OK ) != -1; i++) { //file already exists
                snprintf (filename, max_fname_len, "%s/sim_state_save.%d.json", save_path, i);
            }
        } else {
            snprintf (filename, max_fname_len, "/tmp/sim_state_save.json");
            flux_log (h, LOG_ERR, "save_path not provided. using %s", filename);
        }

        ctx = xzmalloc (sizeof (*ctx));
        ctx->h = h;
		ctx->sim_state = new_simstate ();
        if ((ctx->output_file = fopen (filename, "w")) == NULL) {
            flux_log (h, LOG_ERR, "failed to open output_file: %s", strerror (errno));
        }
        ctx->rdl_string = NULL;
        ctx->rdl_changed = false;
        ctx->exit_on_complete = exit_on_complete;
        flux_aux_set (h, "simsrv", ctx, freectx);
    }

    return ctx;
}

//builds the trigger request and sends it to "mod_name"
//converts sim_state to JSON, formats request tag based on "mod_name"
static int send_trigger (flux_t h, char *mod_name, sim_state_t *sim_state)
{
	JSON o = sim_state_to_json (sim_state);
	char *topic = xasprintf ("%s.trigger", mod_name);
	if (flux_json_request (h, FLUX_NODEID_ANY,
                                  FLUX_MATCHTAG_NONE, topic, o) < 0) {
		flux_log (h, LOG_ERR, "failed to send trigger to %s", mod_name);
        Jput(o);
		return -1;
	}
	//flux_log (h, LOG_DEBUG, "sent a trigger to %s", mod_name);
	Jput(o);
	free (topic);
	return 0;
}

//Send out a call to all modules that the simulation is starting
//and that they should join
int send_start_event(flux_t h)
{
	JSON o = Jnew();
	zmsg_t *zmsg = NULL;
	Jadd_str (o, "mod_name", "sim");
	Jadd_int (o, "rank", flux_rank(h));
	Jadd_int (o, "sim_time", 0);
	if (!(zmsg = flux_event_encode ("sim.start", Jtostr (o)))
            || flux_sendmsg (h, &zmsg) < 0){
		Jput(o);
		zmsg_destroy (&zmsg);
		return -1;
	}
	Jput(o);
	zmsg_destroy (&zmsg);
	return 0;
}

//Looks at the current state and launches the next trigger
static int handle_next_event (ctx_t *ctx){
	zhash_t *timers;
	zlist_t *keys;
	sim_state_t *sim_state = ctx->sim_state;
	int rc = 0;

	//get the timer hashtable, make sure its full, and get a list of its keys
	timers = sim_state->timers;
	if (zhash_size (timers) < 1){
		flux_log (ctx->h, LOG_ERR, "timer hashtable has no elements");
		return -1;
	}
	keys = zhash_keys(timers);

	//Get the next occuring event time/module
	double *min_event_time = NULL, *curr_event_time = NULL;
	char *mod_name = NULL, *curr_name = NULL;

	while (min_event_time == NULL && zlist_size (keys) > 0){
		mod_name = zlist_pop (keys);
		min_event_time = (double *) zhash_lookup (timers, mod_name);
		if (*min_event_time < 0){
			min_event_time = NULL;
			free (mod_name);
		}
	}
	if (min_event_time == NULL){
		return -1;
	}
	while (zlist_size (keys) > 0){
		curr_name = zlist_pop (keys);
		curr_event_time = (double *) zhash_lookup (timers, curr_name);
		if ( *curr_event_time > 0 &&
			 ((*curr_event_time < *min_event_time) ||
			  (*curr_event_time == *min_event_time && !strcmp (curr_name, "sim_sched")))){
			free (mod_name);
			mod_name = curr_name;
			min_event_time = curr_event_time;
		}
	}

	//advance time then send the trigger to the module with the next event
	if (*min_event_time > sim_state->sim_time){
		//flux_log (ctx->h, LOG_DEBUG, "Time was advanced from %f to %f while triggering the next event for %s",
		//		  sim_state->sim_time, *min_event_time, mod_name);
		sim_state->sim_time = *min_event_time;
	}
	else {
		//flux_log (ctx->h, LOG_DEBUG, "Time was not advanced while triggering the next event for %s", mod_name);
	}
	flux_log (ctx->h, LOG_INFO, "Triggering %s.  Curr sim time: %f", mod_name, sim_state->sim_time);

	//usleep (1500);  //this sleep used to fix a race-condition, not sure if still necessary

	*min_event_time = -1;
	rc = send_trigger (ctx->h, mod_name, sim_state);

	//clean up
	free (mod_name);
	zlist_destroy (&keys);
	return rc;
}

//Recevied a request to join the simulation ("sim.join")
static int join_cb (flux_t h, int typemask, zmsg_t **zmsg, void *arg)
{
	JSON request = NULL;
	const char *mod_name;
	int mod_rank;
	double *next_event = (double *) malloc (sizeof (double));
	ctx_t *ctx = arg;
	sim_state_t *sim_state = ctx->sim_state;

	if (flux_json_request_decode (*zmsg, &request) < 0
		|| !Jget_str (request, "mod_name", &mod_name)
		|| !Jget_int (request, "rank", &mod_rank)
		|| !Jget_double (request, "next_event", next_event)) {
		flux_log (h, LOG_ERR, "%s: bad join message", __FUNCTION__);
		Jput(request);
		return 0;
	}
	if (mod_rank < 0 || mod_rank >= flux_size (h)) {
		Jput(request);
		flux_log (h, LOG_ERR, "%s: bad rank in join message", __FUNCTION__);
		return 0;
	}

	flux_log (h, LOG_DEBUG, "join rcvd from module %s on rank %d, next event at %f", mod_name, mod_rank, *next_event);

	zhash_t *timers = sim_state->timers;
	if (zhash_insert (timers, mod_name, next_event) < 0){ //key already exists
		flux_log (h, LOG_ERR, "duplicate join request from %s, module already exists in sim_state", mod_name);
		fprintf (stderr, "duplicate join request from %s, module already exists in sim_state\n", mod_name);
		return 0;
	}

	//TODO: this is horribly hackish, improve the handshake to avoid
	//this hardcoded # of modules. maybe use a timeout?  ZMQ provides
	//support for polling etc with timeouts, should try that
	static int num_modules = 3;
	num_modules--;
	if (num_modules <= 0){
		if (handle_next_event (ctx) < 0){
			flux_log (h, LOG_ERR, "failure while handling next event");
			return -1;
		}
	}

	Jput(request);
	zmsg_destroy(zmsg);
	return 0;
}

//Based on the simulation time, the previous timer value, and the reply timer value
//try and determine what the new timer value should be.  There are ~12 possible cases
//captured by the ~7 if/else statements below.
//The general idea is leave the timer alone if the reply = previous (echo'd back)
//and to use the reply time if the reply is > sim time but < prev
//There are many permutations of the three paramters which result in an invalid state
//hopefully all of those cases are checked for and logged.

//TODO: verify all of this logic is correct, bugs could easily creep up here
static int check_for_new_timers (const char *key, void *item, void *argument)
{
	ctx_t *ctx = (ctx_t *) argument;
	sim_state_t *curr_sim_state = ctx->sim_state;
	double sim_time = curr_sim_state->sim_time;
	double *reply_event_time = (double *) item;
	double *curr_event_time = (double *) zhash_lookup (curr_sim_state->timers, key);

	if (*curr_event_time < 0 && *reply_event_time < 0){
		//flux_log (ctx->h, LOG_DEBUG, "no timers found for %s, doing nothing", key);
		return 0;
	}
	else if (*curr_event_time < 0){
		if (*reply_event_time >= sim_time){
			*curr_event_time = *reply_event_time;
			//flux_log (ctx->h, LOG_DEBUG, "change in timer accepted for %s", key);
			return 0;
		}
		else {
			flux_log (ctx->h, LOG_ERR, "bad reply timer for %s", key);
			return -1;
		}
	}
	else if (*reply_event_time < 0){
		flux_log (ctx->h, LOG_ERR, "event timer deleted from %s", key);
		return -1;
	}
	else if (*reply_event_time < sim_time && *curr_event_time != *reply_event_time) {
		flux_log (ctx->h, LOG_ERR, "incoming modified time is before sim time for %s", key);
		return -1;
	}
	else if (*reply_event_time >= sim_time && *reply_event_time < *curr_event_time){
		*curr_event_time = *reply_event_time;
		//flux_log (ctx->h, LOG_DEBUG, "change in timer accepted for %s", key);
		return 0;
	}
	else {
		//flux_log (ctx->h, LOG_DEBUG, "no changes made to %s timer, curr_time: %f\t reply_time: %f", key, *curr_event_time, *reply_event_time);
		return 0;
	}
}

static void copy_new_state_data (ctx_t *ctx, sim_state_t *curr_sim_state, sim_state_t *reply_sim_state)
{
	if (reply_sim_state->sim_time > curr_sim_state->sim_time) {
		curr_sim_state->sim_time = reply_sim_state->sim_time;
    }

	zhash_foreach (reply_sim_state->timers, check_for_new_timers, ctx);
}

static int rdl_update_cb (flux_t h, int typemask, zmsg_t **zmsg, void *arg)
{
	JSON o = NULL;
	const char *tag = NULL;
    const char *json_string = NULL;
    const char *rdl_string = NULL;
	ctx_t *ctx = (ctx_t *) arg;

	if (flux_event_decode (*zmsg, &tag, &json_string) < 0 || json_string == NULL){
		flux_log (h, LOG_ERR, "%s: bad message", __FUNCTION__);
		Jput (o);
		return -1;
	}

    o = Jfromstr(json_string);
    Jget_str(o, "rdl_string", &rdl_string);
    if (rdl_string) {
        free (ctx->rdl_string);
        ctx->rdl_string = strdup (rdl_string);
        ctx->rdl_changed = true;
    }

    Jput (o);
    return 0;
}

//Recevied a reply to a trigger ("sim.reply")
static int reply_cb (flux_t h, int typemask, zmsg_t **zmsg, void *arg)
{
	JSON request = NULL;
	ctx_t *ctx = arg;
	sim_state_t *curr_sim_state = ctx->sim_state;
	sim_state_t *reply_sim_state;

	if (flux_json_request_decode (*zmsg, &request) < 0) {
		flux_log (h, LOG_ERR, "%s: bad reply message: %s", __FUNCTION__, Jtostr(request));
		Jput(request);
		return 0;
	}

	//De-serialize and get new info
	reply_sim_state = json_to_sim_state (request);
	copy_new_state_data (ctx, curr_sim_state, reply_sim_state);

	if (handle_next_event (ctx) < 0) {
        if (ctx->exit_on_complete) {
            msg_exit ("No events remaining");
        } else {
            flux_log (h, LOG_INFO, "No events remaining");
#if 0
            dump_kvs_dir(ctx->h, ".");
#endif
        }
    }

	free_simstate (reply_sim_state);
	Jput(request);
	zmsg_destroy(zmsg);
	return 0;
}

static int alive_cb (flux_t h, int typemask, zmsg_t **zmsg, void *arg)
{
	JSON request = NULL;
	const char *json_string;

	if (flux_json_request_decode (*zmsg, &request) < 0) {
		flux_log (h, LOG_ERR, "%s: bad reply message", __FUNCTION__);
		Jput(request);
		return 0;
	}

	json_string = Jtostr (request);
	flux_log (h, LOG_DEBUG, "received alive request - %s", json_string);

	if (send_start_event (h) < 0){
		flux_log (h, LOG_ERR, "sim failed to send start event");
		return -1;
	}
	flux_log (h, LOG_DEBUG, "sending start event again");

	Jput(request);
	zmsg_destroy(zmsg);
	return 0;
}

static msghandler_t htab[] = {
    { FLUX_MSGTYPE_REQUEST, "sim.join",       join_cb },
    { FLUX_MSGTYPE_REQUEST, "sim.reply",      reply_cb },
    { FLUX_MSGTYPE_REQUEST, "sim.alive",      alive_cb },
    { FLUX_MSGTYPE_EVENT,   "rdl.update",     rdl_update_cb },
};
const int htablen = sizeof (htab) / sizeof (htab[0]);

int mod_main(flux_t h, zhash_t *args)
{
	ctx_t *ctx;
    char *sim_state_save_path, *eoc_str;
    bool exit_on_complete;

	if (flux_rank (h) != 0) {
		flux_log(h, LOG_ERR, "sim module must only run on rank 0");
		return -1;
	}

	flux_log (h, LOG_INFO, "sim comms module starting");

    if (!(sim_state_save_path = zhash_lookup (args, "save-path"))) {
        flux_log (h, LOG_ERR, "save-path argument is not set, defaulting to ./");
        sim_state_save_path = "./";
    }
    if (!(eoc_str = zhash_lookup (args, "exit-on-complete"))) {
        flux_log (h, LOG_ERR, "exit-on-complete argument is not set, defaulting to false");
        exit_on_complete = false;
    } else {
        exit_on_complete = (!strcmp (eoc_str, "true") || !strcmp (eoc_str, "True"));
    }
    ctx = getctx(h, sim_state_save_path, exit_on_complete);

	if (flux_event_subscribe (h, "rdl.update") < 0){
        flux_log (h, LOG_ERR, "subscribing to event: %s", strerror (errno));
		return -1;
	}

	if (flux_msghandler_addvec (h, htab, htablen, ctx) < 0) {
		flux_log (h, LOG_ERR, "flux_msghandler_add: %s", strerror (errno));
		return -1;
	}

	if (send_start_event (h) < 0){
		flux_log (h, LOG_ERR, "sim failed to send start event");
		return -1;
	}
	flux_log (h, LOG_DEBUG, "sim sent start event");

	if (flux_reactor_start (h) < 0) {
		flux_log (h, LOG_ERR, "flux_reactor_start: %s", strerror (errno));
		return -1;
	}
	return 0;
}

MOD_NAME("sim");
