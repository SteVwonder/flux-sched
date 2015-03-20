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

#include <glpk.h>
#include "rdl.h"
#include "flux_ilp.h"

flux_job_ilp* new_job_ilp (flux_lwj_t *job, struct rdl *rdl, int starting_idx) {
    flux_job_ilp* job_ilp;
    job_ilp = malloc (sizeof (flux_job_ilp));
    job_ilp->accum = rdl_accumulator_create (rdl);
    job_ilp->job = job;
    job_ilp->starting_idx = starting_idx;
    asprintf (&(job_ilp->lwjtag), "lwj.%ld", job->lwj_id);
    return job_ilp;
}

void free_job_ilp (flux_job_ilp *job_ilp) {
    rdl_accumulator_destroy (job_ilp->accum);
    free (job_ilp->lwjtag);
    free (job_ilp);
}

void freefn_job_ilp (void *job_ilp) {
    free_job_ilp ( (flux_job_ilp*) job_ilp);
}

flux_ilp_matrix* new_ilp_matrix(int starting_size) {
    flux_ilp_matrix* ilp_mat = malloc (sizeof (flux_ilp_matrix));
    ilp_mat->curr_max_size = starting_size;
    ilp_mat->non_zero_elements = 0;
    ilp_mat->rows = (int*) calloc (starting_size, sizeof (int));
    ilp_mat->cols = (int*) calloc (starting_size, sizeof (int));
    ilp_mat->vals = (double*) calloc (starting_size, sizeof (double));
    return ilp_mat;
}

void free_ilp_matrix(flux_ilp_matrix *ilp_mat) {
    free(ilp_mat->rows);
    free(ilp_mat->cols);
    free(ilp_mat->vals);
    free(ilp_mat);
}

static void increase_ilp_matrix (flux_ilp_matrix *ilp_mat) {
    int i, new_size = ilp_mat->curr_max_size * 1.5;
    ilp_mat->rows = (int*) realloc (ilp_mat->rows, new_size * sizeof (int));
    ilp_mat->cols = (int*) realloc (ilp_mat->cols, new_size * sizeof (int));
    ilp_mat->vals = (double*) realloc (ilp_mat->vals, new_size * sizeof (double));
    for (i = ilp_mat->curr_max_size; i < new_size; i++) {
        ilp_mat->rows[i] = 0;
        ilp_mat->cols[i] = 0;
        ilp_mat->vals[i] = 0;
    }
    ilp_mat->curr_max_size = new_size;
}

void insert_into_ilp_matrix(flux_ilp_matrix *ilp_mat, int row, int col, double val) {
    if (ilp_mat->non_zero_elements + 1 > ilp_mat->curr_max_size - 1) {
        increase_ilp_matrix (ilp_mat);
    }
    ilp_mat->rows[ilp_mat->non_zero_elements + 1] = row;
    ilp_mat->cols[ilp_mat->non_zero_elements + 1] = col;
    ilp_mat->vals[ilp_mat->non_zero_elements + 1] = val;
    ilp_mat->non_zero_elements++;
}

void load_matrix_into_ilp(flux_ilp_matrix *ilp_mat, glp_prob *lp) {
    glp_load_matrix (lp, ilp_mat->non_zero_elements,
                     ilp_mat->rows, ilp_mat->cols, ilp_mat->vals);
}
