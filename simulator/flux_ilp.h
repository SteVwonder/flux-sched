#ifndef FLUX_ILP_H
#define FLUX_ILP_H

#include <glpk.h>
#include "scheduler.h"
#include "rdl.h"

typedef struct {
    flux_lwj_t *job;
    struct rdl_accumulator *accum;
    int starting_idx;
    char *lwjtag;
} flux_job_ilp;

typedef struct {
    int *rows;
    int *cols;
    double *vals;
    int non_zero_elements;
    int curr_max_size;
} flux_ilp_matrix;

flux_job_ilp* new_job_ilp(flux_lwj_t *job, struct rdl *rdl, int starting_idx);
void free_job_ilp(flux_job_ilp *job_ilp);
void freefn_job_ilp (void *job_ilp);

flux_ilp_matrix* new_ilp_matrix(int starting_size);
void free_ilp_matrix(flux_ilp_matrix *ilp_mat);
void insert_into_ilp_matrix(flux_ilp_matrix *ilp_mat, int row, int col, double val);
void load_matrix_into_ilp(flux_ilp_matrix *ilp_mat, glp_prob *lp);

#endif
