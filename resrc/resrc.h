#ifndef FLUX_RESRC_H
#define FLUX_RESRC_H

/*
 *  C API interface to Flux Resources
 */

#include <uuid/uuid.h>
#include "src/common/libutil/shortjson.h"
#include "time.h"

#define TIME_MAX INT64_MAX

typedef struct resrc resrc_t;
typedef struct resrc_tree resrc_tree_t;
typedef struct resources resources_t;

typedef enum {
    RESOURCE_INVALID,
    RESOURCE_IDLE,
    RESOURCE_ALLOCATED,
    RESOURCE_RESERVED,
    RESOURCE_DOWN,
    RESOURCE_UNKNOWN,
    RESOURCE_END
} resource_state_t;


zhash_t *resrc_allocs (resrc_t *resrc);

/*
 * Return the type of the resouce
 */
char *resrc_type (resrc_t *resrc);

/*
 * Return the fully qualified name of the resouce
 */
char *resrc_path (resrc_t *resrc);

/*
 * Return the name of the resouce
 */
char *resrc_name (resrc_t *resrc);

/*
 * Return the id of the resouce
 */
int64_t resrc_id (resrc_t *resrc);

/*
 * Return the size of the resource
 */
size_t resrc_size (resrc_t *resrc);

zhash_t *resrc_twindow (resrc_t *resrc);

/*
 * Get a list of jobs with an allocation on this resource at time
 */
zlist_t *resrc_curr_job_ids (resrc_t *resrc, int64_t time);

/*
 * Return the amount of the resource available at the given time
 */
size_t resrc_available_at_time (resrc_t *resrc, int64_t time);

/*
 * Return the least amount of the resource available during the time
 * range
 */
size_t resrc_available_during_range (resrc_t *resrc,
                                     int64_t range_start_time,
                                     int64_t range_end_time);
/*
 * Return the resource state as a string
 */
char* resrc_state (resrc_t *resrc);

/*
 * Return the physical tree for the resouce
 */
resrc_tree_t *resrc_phys_tree (resrc_t *resrc);

/*
 * Create a new resource object
 */
resrc_t *resrc_new_resource (const char *type, const char *path,
                             const char *name, int64_t id, uuid_t uuid,
                             size_t size);

/*
 * Create a copy of a resource object
 */
resrc_t *resrc_copy_resource (resrc_t *resrc);

/*
 * Destroy a resource object
 */
void resrc_resource_destroy (void *object);

/*
 * Create a resrc_t object from a json object
 */
resrc_t *resrc_new_from_json (JSON o, resrc_t *parent, bool physical);

/*
 * Return the head of a resource tree of all resources described by a
 * configuration file
 */
resrc_t *resrc_generate_resources (const char *path, char*resource);

/*
 * Add the input resource to the json object
 */
int resrc_to_json (JSON o, resrc_t *resrc);

/*
 * Print details of a specific resource
 */
void resrc_print_resource (resrc_t *resrc, int64_t time_now);

/*
 * Determine whether a specific resource has the required characteristics
 * Inputs:  resrc     - the specific resource under evaluation
 *          sample    - sample resource with the required characteristics
 *          available - when true, consider only idle resources
 *                      otherwise find all possible resources matching type
 * Returns: true if the input resource has the required characteristics
 */
bool resrc_match_resource (resrc_t *resrc, resrc_t *sample, bool available);

/*
 * Stage size elements of a resource
 */
void resrc_stage_resrc(resrc_t *resrc, size_t size);

void resrc_unstage_resrc (resrc_t *resrc);

/*
 * Allocate a resource to a job
 */
int resrc_allocate_resource (resrc_t *resrc, int64_t job_id,
                             int64_t time_now, int64_t walltime);

int resrc_allocate_resource_unchecked (resrc_t *resrc, int64_t job_id,
                                       int64_t time_now, int64_t walltime);
/*
 * Reserve a resource for a job
 */
int resrc_reserve_resource (resrc_t *resrc, int64_t job_id,
                            int64_t time_now, int64_t walltime);

/*
 * Remove a job allocation from a resource
 */
int resrc_release_resource (resrc_t *resrc, int64_t rel_job);

int resrc_deallocate_resource (resrc_t *resrc, int64_t job_id, int64_t size);

/*
 * De-allocate the resources handle
 */
void resrc_destroy_resources (resources_t *resrcs);

/*
 * Get epoch time
 */
static inline int64_t epochtime ()
{
    return (int64_t) time (NULL);
}

/*
 * Find a resrc in a resource_t via the resrc's path
 */

resources_t *resrc_new_resources ();

void resrc_populate_resources_from_tree (resrc_tree_t *resrc_tree,
                                         resources_t *resrcs);

resources_t *resrc_new_resources_from_tree (resrc_tree_t *resrc_tree);

resrc_t *resrc_lookup (resources_t *resrcs, const char *resrc_path);

void resrc_destroy_resources (resources_t *resrcs);

#endif /* !FLUX_RESRC_H */
