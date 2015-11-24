#ifndef FLUX_RESRC_H
#define FLUX_RESRC_H

/*
 *  C API interface to Flux Resources
 */

#include <uuid/uuid.h>
#include "src/common/libutil/shortjson.h"

#define TIME_MAX INT64_MAX
#define SLACK_BUFFER_TIME 30
#define K_NEEDS 5
#include <flux/core.h>
#include "src/common/libutil/log.h"

typedef struct resrc resrc_t;
typedef struct resrc_tree resrc_tree_t;

typedef enum {
    RESOURCE_INVALID,
    RESOURCE_IDLE,
    RESOURCE_ALLOCATED,
    RESOURCE_RESERVED,
    RESOURCE_DOWN,
    RESOURCE_SLACKSUB,
    RESOURCE_UNKNOWN,
    RESOURCE_END
} resource_state_t;


void resrc_set_state (resrc_t *resrc, resource_state_t state);
int resrc_generate_as_child (resrc_t *resrc, JSON o);
int resrc_update_reservations (resrc_t *resrc, int64_t jobid, char *ustarttime_str);
int64_t resrc_find_ustart (resrc_t *resrc);

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
 * Retunr the uuid of resource
 */
void resrc_uuid (resrc_t *resrc, char *uuid);

/*
 * Return the size of the resource
 */
size_t resrc_size (resrc_t *resrc);

/*
 * Return the amount of the resource available at the given time
 */
size_t resrc_available_at_time (resrc_t *resrc, int64_t time);

/*
 * Return the least amount of the resource available during the time
 * range
 */
size_t resrc_available_during_range (resrc_t *resrc,
                                     int64_t range_starttime,
                                     int64_t range_endtime);
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
 * Return the head of a resource tree of all resources described by an
 * rdl-formatted configuration file
 */
resrc_t *resrc_generate_rdl_resources (const char *path, char*resource);

/*
 * Return the head of a resource tree of all resources described by an
 * xml serialization
 */
resrc_t *resrc_generate_xml_resources (resrc_t *host_resrc, const char *buf,
                                       size_t length);

/*
 * Add the input resource to the json object
 */
int resrc_to_json (JSON o, resrc_t *resrc, int64_t jobid);

/*
 * Print details of a specific resource
 */
void resrc_print_resource (resrc_t *resrc);

/*
 * Convenience function to create a specialized cluster resource
 */
resrc_t *resrc_create_cluster (char *cluster);

/*
 * Determine whether a specific resource has the required characteristics
 * Inputs:  resrc     - the specific resource under evaluation
 *          sample    - sample resource with the required characteristics
 *          available - when true, consider only idle resources
 *                      otherwise find all possible resources matching type
 *          starttime - start of period the resource needs to be available
 *                      When the starttime is 0, only the current resource
 *                      availability will be considered (now-based match).
 *          endtime   - end of period the resource needs to be available
 * Returns: true if the input resource has the required characteristics
 */
bool resrc_match_resource (resrc_t *resrc, resrc_t *sample, bool available,
                           int64_t starttime, int64_t endtime);

/*
 * Stage size elements of a resource
 */
void resrc_stage_resrc(resrc_t *resrc, size_t size);

/*
 * Allocate a resource to a job
 */
int resrc_allocate_resource (resrc_t *resrc, int64_t job_id,
                             int64_t starttime, int64_t endtime);

/*
 * Reserve a resource for a job
 */
int resrc_reserve_resource (resrc_t *resrc, int64_t job_id,
                            int64_t starttime, int64_t endtime);

/*
 * Remove a job allocation from a resource
 * Supports both now and time-based allocations.
 */
int resrc_release_allocation (resrc_t *resrc, int64_t rel_job);

/*
 * Remove all reservations of a resource
 * Supports both now and time-based reservations.
 */
int resrc_release_all_reservations (resrc_t *resrc);

/*
 * Get epoch time
 */
static inline int64_t epochtime ()
{
    return (int64_t)time (NULL);
}

int64_t resrc_owner (resrc_t *resrc);
int64_t resrc_leasee (resrc_t *resrc);

void resrc_set_owner (resrc_t *resrc, int64_t owner);
bool resrc_check_resource_destroy_ready (resrc_t *resrc);
int resrc_add_resources_from_json (resrc_t *resrc, zhash_t *hash_table, JSON o, int64_t owner);

bool resrc_check_slacksub_ready (resrc_t *resrc, int64_t *endtime);
bool resrc_check_return_ready (resrc_t *resrc, int64_t *jobid);
int resrc_collect_own_resources_unasked (zhash_t *hash_table, JSON ro_array);
int resrc_retrieve_lease_information (zhash_t *hash_table, JSON ro_array, JSON out);
int resrc_mark_resource_return_received (resrc_t *resrc, int64_t jobid);
int resrc_mark_resources_returned (zhash_t *hash_table, JSON ro_array);
int resrc_mark_resources_asked (zhash_t *hash_table, JSON ro_array);
int resrc_mark_resources_slacksub_or_returned (zhash_t *hash_table, JSON o);
int resrc_mark_resource_slack (resrc_t *resrc, int64_t jobid, int64_t endtime);
int resrc_mark_resources_to_be_returned (zhash_t *hash_table, JSON ro_array);

#endif /* !FLUX_RESRC_H */
