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

#ifndef SIMULATOR_H
#define SIMULATOR_H 1

#include <czmq.h>
#include <flux/core.h>
#include "src/common/libutil/shortjson.h"

#define SCHED_INTERVAL 30

typedef struct {
    double sim_time;
    zhash_t *timers;
} sim_state_t;

typedef struct {
    int id;
    char *user;
    char *jobname;
    char *account;
    double submit_time;
    double start_time;
    double execution_time;
    double io_time;
    double time_limit;
    int nnodes;
    int ncpus;
    int64_t io_rate;
    int ish;
    char *hfile;
    kvsdir_t *kvs_dir;
} job_t;

sim_state_t *new_simstate ();
void free_simstate (sim_state_t *sim_state);
JSON sim_state_to_json (sim_state_t *state);
sim_state_t *json_to_sim_state (JSON o);
int print_values (const char *key, void *item, void *argument);

int put_job_in_kvs (job_t *job);
job_t *pull_job_from_kvs (kvsdir_t *kvs_dir);
void free_job (job_t *job);
job_t *blank_job ();

int send_alive_request (flux_t h, flux_t remote_h, const char *module_name);
int send_reply_request (flux_t h, flux_t remote_h, const char *module_name, sim_state_t *sim_state);
int send_join_request (flux_t h, flux_t remote_h, const char *module_name, double next_event);
int send_leave_request (flux_t h, flux_t remote_h, const char *module_name);

zhash_t *zhash_fromargv (int argc, char **argv);

/*
struct rdl *get_rdl (flux_t h, char *path);
void close_rdl ();
*/
#endif /* SIMULATOR_H */
