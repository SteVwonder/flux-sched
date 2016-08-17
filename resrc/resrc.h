#ifndef FLUX_RESRC_H
#define FLUX_RESRC_H

/*
 *  C API interface to Flux Resources
 */

#include <uuid/uuid.h>
#include <czmq.h>
#include <flux/core.h>

#define TIME_MAX INT64_MAX
#define SLACK_BUFFER_TIME 0
#define K_NEEDS 5

typedef struct hwloc_topology * TOPOLOGY;
typedef struct resrc resrc_t;
typedef struct resrc_reqst resrc_reqst_t;
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
int resrc_generate_as_child (resrc_t *resrc, json_object *o);
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
 * Return the basename of the resouce
 */
char *resrc_basename (resrc_t *resrc);

/*
 * Return the name of the resouce
 */
char *resrc_name (resrc_t *resrc);

 /*
 * Return the digest of resrc -- key to find corresponding
 * broker rank
 */
char *resrc_digest (resrc_t *resrc);

 /*
 * Set the digest field of resrc with 'digest'. This will
 * return the old digest.
 */
char *resrc_set_digest (resrc_t *resrc, char *digest);

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
size_t resrc_available_during_range (resrc_t *resrc, int64_t range_starttime,
                                     int64_t range_endtime, bool exclusive);

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
                             const char *basename, const char *name,
                             const char *sig, int64_t id, uuid_t uuid,
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
resrc_t *resrc_new_from_json (json_object *o, resrc_t *parent, bool physical);

/*
 * Return the head of a resource tree of all resources described by an
 * rdl-formatted configuration file
 */
resrc_t *resrc_generate_rdl_resources (const char *path, char*resource);

/*
 * Return the head of a resource tree of all resources described by a
 * hwloc topology or NULL if errors are encountered.
 * Note: If err_str is non-null and errors are encountered, err_str will
 *       contain reason why.  Caller must subsequently free err_str.
 */
resrc_t *resrc_generate_hwloc_resources (resrc_t *host_resrc, TOPOLOGY topo,
                                         const char *sig, char **err_str);

/*
 * Add the input resource to the json object
 */
int resrc_to_json (json_object *o, resrc_t *resrc, int64_t jobid);

/*
 * Print details of a specific resource to a string buffer
 * and return to the caller. The caller must free it.
 */
char *resrc_to_string (resrc_t *resrc);
/*
 * Print details of a specific resource to stdout
 */
void resrc_print_resource (resrc_t *resrc);

/*
 * Convenience function to create a specialized cluster resource
 */
resrc_t *resrc_create_cluster (char *cluster);

/*
 * Determine whether a specific resource has the required characteristics
 * Inputs:  resrc     - the specific resource under evaluation
 *          request   - resource request with the required characteristics
 *          available - when true, consider only idle resources
 *                      otherwise find all possible resources matching type
 * Returns: true if the input resource has the required characteristics
 */
bool resrc_match_resource (resrc_t *resrc, resrc_reqst_t *request,
                           bool available);

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
int64_t epochtime ();
int64_t resrc_epochtime ();

int64_t resrc_owner (resrc_t *resrc);
int64_t resrc_leasee (resrc_t *resrc);
void resrc_set_owner (resrc_t *resrc, int64_t owner);

void resrc_copy_lifetime (resrc_t *from, resrc_t *to);

bool resrc_check_resource_destroy_ready (resrc_t *resrc);
int resrc_add_resources_from_json (flux_t h, resrc_t *resrc, zhash_t *hash_table, json_object *o, int64_t owner);

bool resrc_check_slacksub_ready (flux_t h, resrc_t *resrc, int64_t *endtime);
bool resrc_check_return_ready (flux_t h, resrc_t *resrc, int64_t *jobid);
int resrc_collect_own_resources_unasked (flux_t h, zhash_t *hash_table, JSON ro_array);
int resrc_retrieve_lease_information (zhash_t *hash_table, json_object *ro_array, json_object *ut);
int resrc_mark_resource_return_received (resrc_t *resrc, int64_t jobid);
int resrc_mark_resources_returned (zhash_t *hash_table, json_object *ro_array);
int resrc_mark_resources_asked (zhash_t *hash_table, json_object *ro_array);
int resrc_mark_resources_slacksub_or_returned (zhash_t *hash_table, json_object *);
int resrc_mark_resource_slack (resrc_t *resrc, int64_t jobid, int64_t endtime);
int resrc_mark_resources_to_be_returned (zhash_t *hash_table, json_object *ro_array);
int resrc_allocate_resource_in_time_dynamic (resrc_t *resrc, int64_t job_id, int64_t starttime, int64_t endtime);

void resrc_set_sim_mode ();
void resrc_set_sim_time (int64_t t);

void resrc_flux_log (flux_t h, resrc_t *resrc);
void resrc_hash_flux_log (flux_t h, zhash_t *hash);


#endif /* !FLUX_RESRC_H */
