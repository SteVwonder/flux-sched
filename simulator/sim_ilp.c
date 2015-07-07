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

static flux_t h = NULL;
static ctx_t *ctx = NULL;
static const char* NODETYPE = "node";
static const char* SWITCHTYPE = "switch";

bool allocate_bandwidth (flux_lwj_t *job, struct resource *r, zlist_t *ancestors)
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
			if (allocate_resources (rdl, uri, fr, *a, job, ancestors)) {
				flux_log (h, LOG_INFO, "scheduled job %ld without update", job->lwj_id);
                if (rc == 0)
                    rc = 1;
			} else {
                if (rdl_accumulator_is_empty(*a)) {
                    flux_log (h, LOG_DEBUG, "no resources found in accumulator");
                } else {
                    job->rdl = rdl_accumulator_copy (*a);
                    release_resources (ctx, rdl, uri, job);
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
        curr_lwj_job = find_lwj(ctx, curr_job_t->id);
        if (curr_lwj_job->alloc.ncores != curr_job_t->ncpus) {
            flux_log (h, LOG_ERR, "Job %d's ncpus don't match: %d (lwj_job) and %d (job_t)", curr_job_t->id, curr_lwj_job->alloc.ncores, curr_job_t->ncpus);
        }
        release_resources (ctx, shadow_rdl, uri, curr_lwj_job);

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
        curr_lwj_job = find_lwj (ctx, curr_job_t->id);
        if (curr_lwj_job->alloc.ncores != curr_job_t->ncpus) {
            flux_log (h, LOG_ERR, "Job %d's ncpus don't match: %d (lwj_job) and %d (job_t)", curr_job_t->id, curr_lwj_job->alloc.ncores, curr_job_t->ncpus);
        }
        release_resources (ctx, shadow_rdl, uri, curr_lwj_job);

        *shadow_free_cores += curr_job_t->ncpus;
        *shadow_time = curr_job_t->start_time + curr_job_t->time_limit;
        curr_job_t = zlist_next (running_jobs);
    }
    *shadow_free_cores -= reserved_job->req.ncores;
}

static bool job_backfill_eligible (struct rdl *rdl, struct rdl *shadow_rdl, const char *uri, flux_lwj_t *job, int64_t curr_free_cores, double shadow_time, double sim_time)
{
    struct rdl_accumulator *accum = NULL;
    struct rdl *rdl_to_test = NULL;
    bool terminates_before_shadow_time = ((sim_time + job->req.walltime) < shadow_time);

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
        release_resources(ctx, rdl_to_test, uri, job);
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
    struct resource* root_resource = rdl_resource_get (rdl, ctx->uri);

    if (!get_resource_of_type_helper (resource_type, root_resource, ancestors, res_list)) {
        rdl_resource_destroy(root_resource);
    }
    zlist_destroy (&ancestors);
    return res_list;
}

static void generate_lp_constraints (glp_prob *lp, zlist_t *eligible_jobs,
                                     flux_ilp_matrix* ilp_mat,
                                     const char* resource_type, zlist_t* resource_list,
                                     int shadow_free_cores,
                                     double shadow_time, double sim_time,
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
        flux_log (h, LOG_DEBUG, "Checking resource (%ld) %s\n", i, hierarchy);

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
            curr_job_t->start_time = sim_time;
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
    int num_free_resources;
    double upper_bound;
    int64_t i, j, col_num;
    flux_lwj_t *curr_job = NULL;
    int max_name_len = 100;
    char name[max_name_len];
    struct resource* curr_resource = NULL;

    num_free_resources = zlist_size (resource_list);

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
                                    int64_t shadow_free_cores,
                                    double shadow_time, double sim_time)
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
                            shadow_free_cores, shadow_time, sim_time,
                            jobs_on_starting_idx);

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
        asprintf (&uri, "%s:%s", ctx->uri, path);
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

    flux_log (h, LOG_DEBUG, "Num resources: %d, num jobs: %d\n",
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
    double col_val;
    flux_lwj_t *curr_job = NULL;
    zlist_t *ancestors = NULL, *scheduled_jobs = NULL, *scheduled_lwjs = NULL;
    flux_job_ilp *job_ilp = NULL;
    struct resource *job_rdl_root = NULL;
    struct rdl_accumulator* tmp_accum = NULL;

    num_free_resources = zlist_size (res_list);
    num_jobs = zlist_size (eligible_jobs);
    num_cols = glp_get_num_cols (lp);

    scheduled_jobs = zlist_new ();
    col_offset = num_cols - num_jobs;
    curr_job = zlist_first (eligible_jobs);
    for (i = 0; curr_job != NULL; i++) {
        col_idx = i + col_offset + 1;
        col_val = glp_mip_col_val (lp, col_idx);
        if (col_val == 1) {
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
        job_rdl_root = rdl_resource_get(curr_job->rdl, ctx->uri);
        ancestors = zlist_new();
        tmp_accum = rdl_accumulator_create(rdl);
        allocate_resources (rdl, ctx->uri, job_rdl_root, tmp_accum, curr_job, ancestors);
        curr_job->rdl = rdl_accumulator_copy (tmp_accum);
        rdl_accumulator_destroy(tmp_accum);
        zlist_destroy (&ancestors);
        if (update_job (ctx, curr_job)) {
            flux_log (h, LOG_ERR, "error updating job %ld's state", curr_job->lwj_id);
        }
        zlist_append(scheduled_lwjs, job_ilp->job);
        job_ilp = zlist_next (scheduled_jobs);
    }

    zlist_destroy (&scheduled_jobs);
    return scheduled_lwjs;
}

static int backfill_jobs (struct rdl *rdl, const char *uri, zlist_t *queued_jobs, zlist_t *running_jobs,
                           flux_lwj_t *reserved_job, double sim_time)
{
    int rc = 0;
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
            if (job_backfill_eligible (rdl, shadow_rdl, uri, curr_job, curr_free_cores, shadow_time, sim_time)) {
                flux_log(h, LOG_DEBUG, "Job %ld is eligible for backfilling, adding to ILP problem.", curr_job->lwj_id);
                zlist_append(eligible_jobs, curr_job);
            } else {
                flux_log(h, LOG_DEBUG, "Job %ld is not eligible for backfilling. Not added to ILP problem.", curr_job->lwj_id);
            }
        }

        if (zlist_size (eligible_jobs) >= 5) {
            resource_list = get_resources_of_type(free_rdl, resource_type);
            lp = build_ilp_problem (eligible_jobs, rdl, resource_type, resource_list,
                                    shadow_free_cores, shadow_time, sim_time);
            scheduled_jobs = schedule_jobs_from_ilp (rdl, free_rdl, lp, eligible_jobs,
                                                     resource_type, resource_list);
            rc += zlist_size (scheduled_jobs);
            glp_delete_prob (lp);
            resource_list = get_resources_of_type(free_rdl, resource_type);
            while (zlist_size (scheduled_jobs) > 0) {
                flux_lwj_t* lwj_job = zlist_pop (scheduled_jobs);
                kvs_get_dir (h,  &curr_kvs_dir, "lwj.%d", lwj_job->lwj_id);
                curr_job_t = pull_job_from_kvs (curr_kvs_dir);
                if (curr_job_t->start_time == 0)
                    curr_job_t->start_time = sim_time;
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
                                resource_list, shadow_free_cores,
                                shadow_time, sim_time);
        scheduled_jobs = schedule_jobs_from_ilp (rdl, free_rdl, lp, eligible_jobs,
                                resource_type, resource_list);
        rc += zlist_size (scheduled_jobs);
        glp_delete_prob (lp);
    } else if (zlist_size (eligible_jobs) == 1) {
        rc += schedule_job (ctx, rdl, free_rdl, uri, curr_free_cores, zlist_pop (eligible_jobs));
    }
    zlist_destroy (&eligible_jobs);

    rdl_destroy (free_rdl);
    rdl_destroy (shadow_rdl);

    return rc;
}

int schedule_jobs (ctx_t *ctx, double sim_time)
{
    flux_lwj_t *curr_job = NULL, *curr_lwj_job = NULL, *reserved_job = NULL;
    job_t *curr_job_t = NULL;
    kvsdir_t curr_kvs_dir;
    int rc = 0, job_scheduled = 1;
    zlist_t *jobs = ctx->p_queue, *queued_jobs = zlist_dup (jobs), *running_jobs = zlist_new ();
    char *uri = ctx->uri;
    int64_t curr_free_cores = -1;
    struct rdl *rdl = ctx->rdl, *free_rdl = NULL;

    free_rdl = get_free_subset (rdl, "core");
    if (free_rdl) {
        curr_free_cores = get_free_count (free_rdl, uri, "core");
    } else {
        flux_log (h, LOG_DEBUG, "get_free_subset returned nothing, setting curr_free_cores = 0");
        curr_free_cores = 0;
    }

    zlist_sort(queued_jobs, job_compare_id_fn);

    //Schedule all jobs at the front of the queue
    curr_job = zlist_first (queued_jobs);
    while (job_scheduled && curr_job) {
		if (curr_job->state == j_unsched) {
			job_scheduled = schedule_job (ctx, rdl, free_rdl, uri, curr_free_cores, curr_job);
            if (job_scheduled) {
                rc += 1;
                curr_free_cores -= curr_job->alloc.ncores;
                if (kvs_get_dir (h,  &curr_kvs_dir, "lwj.%ld", curr_job->lwj_id)) {
                    flux_log (h, LOG_ERR, "lwj.%ld kvsdir not found", curr_job->lwj_id);
                } else {
                    curr_job_t = pull_job_from_kvs (curr_kvs_dir);
                    if (curr_job_t->start_time == 0)
                        curr_job_t->start_time = sim_time;
                    zlist_append (running_jobs, curr_job_t);
                }
            } else {
                reserved_job = curr_job;
            }
		}
        curr_job = zlist_next (queued_jobs);
    }

    //reserved job is now set, start backfilling
    if (reserved_job) {
        curr_lwj_job = zlist_first (ctx->r_queue);
        while (curr_lwj_job != NULL) {
            if (kvs_get_dir (h,  &curr_kvs_dir, "lwj.%ld", curr_lwj_job->lwj_id)) {
                flux_log (h, LOG_ERR, "lwj.%ld kvsdir not found", curr_lwj_job->lwj_id);
            } else {
                curr_job_t = pull_job_from_kvs (curr_kvs_dir);
                if (curr_job_t->start_time == 0)
                    curr_job_t->start_time = sim_time;
                zlist_append (running_jobs, curr_job_t);
            }
            curr_lwj_job = zlist_next (ctx->r_queue);
        }
        rc += backfill_jobs(rdl, uri, queued_jobs, running_jobs, reserved_job, sim_time);
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


static msghandler_t htab[] = {
    { FLUX_MSGTYPE_EVENT,   "sim.start",     start_cb },
    { FLUX_MSGTYPE_REQUEST, "sim_sched.trigger", trigger_cb },
    //{ FLUX_MSGTYPE_EVENT,   "sim_sched.event",   event_cb },
    { FLUX_MSGTYPE_REQUEST, "sim_sched.lwj-watch",  newlwj_rpc },
};

int mod_main (flux_t p, zhash_t *args)
{
    int htablen = sizeof (htab) / sizeof (htab[0]);
    h = p;
    ctx = getctx (h);

    return init_and_start_scheduler(h, ctx, args, htab, htablen);
}

MOD_NAME ("sim_sched");

/*
 * vi:tabstop=4 shiftwidth=4 expandtab
 */
