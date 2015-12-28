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
#include <stdbool.h>
#include <string.h>
#include <assert.h>
#include <czmq.h>

#include "rdl.h"
#include "resrc.h"
#include "resrc_tree.h"
#include "src/common/libutil/jsonutil.h"
#include "src/common/libutil/xzmalloc.h"

struct resrc {
    char *type;
    char *path;
    char *name;
    int64_t id;
    uuid_t uuid;
    size_t size;
    size_t staged;
    resource_state_t state;
    resrc_tree_t *phys_tree;
    zlist_t *graphs;
    zhash_t *properties;
    zhash_t *tags;
    zhash_t *allocs;
    zhash_t *reservtns;
    zhash_t *twindow;
};

struct resources {
    zhash_t *hash;
};

/***************************************************************************
 *  API
 ***************************************************************************/

char *resrc_type (resrc_t *resrc)
{
    if (resrc)
        return resrc->type;
    return NULL;
}

zhash_t *resrc_allocs (resrc_t *resrc)
{
    return resrc->allocs;
}

char *resrc_path (resrc_t *resrc)
{
    if (resrc)
        return resrc->path;
    return NULL;
}

char *resrc_name (resrc_t *resrc)
{
    if (resrc)
        return resrc->name;
    return NULL;
}

int64_t resrc_id (resrc_t *resrc)
{
    if (resrc)
        return resrc->id;
    return -1;
}

size_t resrc_size (resrc_t *resrc)
{
    if (resrc)
        return resrc->size;
    return 0;
}

// TODO: don't expose zhash_t in API
zhash_t *resrc_twindow (resrc_t *resrc)
{
    if (resrc)
        return resrc->twindow;
    return NULL;
}

// TODO: don't expose zlist_t in API
zlist_t *resrc_curr_job_ids (resrc_t *resrc, int64_t time)
{
    zlist_t *matching_jobs = zlist_new ();
    zlist_autofree (matching_jobs);

    // Iterate over all allocation windows in curr_resrc.  We iterate using
    // keys since we need the key to lookup the size in curr_resrc->allocs.
    JSON window_json = NULL;
    const char* window_json_str = NULL;
    int64_t start_time = 0, end_time = 0;
    zlist_t *window_keys = zhash_keys (resrc->twindow);
    char *window_key = NULL;
    for (window_key = zlist_next (window_keys);
         window_key;
         window_key = zlist_next (window_keys)) {

        if (!strncmp ("0", window_key, 2)) {
            // Skip the lifetime window
            continue;
        }
        window_json_str = (const char*) zhash_lookup (resrc->twindow, window_key);
        window_json = Jfromstr (window_json_str);
        if (window_json == NULL) {
            continue;
        }
        Jget_int64 (window_json, "starttime", &start_time);
        Jget_int64 (window_json, "endtime", &end_time);

        // Does time intersect with window?
        if (time >= start_time && time <= end_time) {
            zlist_append (matching_jobs, strdup (window_key));
        }

        Jput (window_json);
    }

    zlist_destroy (&window_keys);

    return matching_jobs;
}

size_t resrc_available_at_time (resrc_t *resrc, int64_t time)
{
    int64_t start_time = 0;
    int64_t end_time = 0;

    const char *curr_job_id = NULL;
    const char *window_json_str = NULL;
    JSON window_json = NULL;
    size_t *size_ptr = NULL;

    size_t available = resrc->size;

    if (time < 0) {
        time = epochtime();
    }

    // Check that the time is during the resource lifetime
    window_json_str = (const char*) zhash_lookup (resrc->twindow, "0");
    if (window_json_str) {
        window_json = Jfromstr (window_json_str);
        if (window_json == NULL) {
            return -1;
        }
        Jget_int64 (window_json, "starttime", &start_time);
        Jget_int64 (window_json, "endtime", &end_time);
        if (time < start_time || time > end_time) {
            return -1;
        }
    }

    // Iterate over all allocation windows in resrc.  We iterate using
    // keys since we need the key to lookup the size in resrc->allocs.
    zlist_t *curr_job_ids = resrc_curr_job_ids (resrc, time);
    for (curr_job_id = zlist_first (curr_job_ids);
         curr_job_id;
         curr_job_id = zlist_next (curr_job_ids)) {
        // Decrement available by allocation and/or reservation size
        size_ptr = (size_t*)zhash_lookup (resrc->allocs, curr_job_id);
        if (size_ptr) {
            available -= *size_ptr;
        }
        size_ptr = (size_t*)zhash_lookup (resrc->reservtns, curr_job_id);
        if (size_ptr) {
            available -= *size_ptr;
        }
    }

    zlist_destroy (&curr_job_ids);

    return available;
}


#if CZMQ_VERSION < CZMQ_MAKE_VERSION(3, 0, 1)
static bool compare_windows_starttime (void *item1, void *item2)
{
    int64_t starttime1 = 0, starttime2 = 0;
    JSON json1 = (JSON) item1;
    JSON json2 = (JSON) item2;

    Jget_int64 (json1, "starttime", &starttime1);
    Jget_int64 (json2, "starttime", &starttime2);

    return (starttime1 > starttime2);
}
#else
static int compare_windows_starttime (void *item1, void *item2)
{
    int64_t starttime1 = 0, starttime2 = 0;
    JSON json1 = (JSON) item1;
    JSON json2 = (JSON) item2;

    Jget_int64 (json1, "starttime", &starttime1);
    Jget_int64 (json2, "starttime", &starttime2);

    return (starttime1 - starttime2);
}
#endif


#if CZMQ_VERSION < CZMQ_MAKE_VERSION(3, 0, 1)
static bool compare_windows_endtime (void *item1, void *item2)
{
    int64_t endtime1 = 0, endtime2 = 0;
    JSON json1 = (JSON) item1;
    JSON json2 = (JSON) item2;

    Jget_int64 (json1, "endtime", &endtime1);
    Jget_int64 (json2, "endtime", &endtime2);

    return (endtime1 > endtime2);
}
#else
static int compare_windows_endtime (void *item1, void *item2)
{
    int64_t endtime1 = 0, endtime2 = 0;
    JSON json1 = (JSON) item1;
    JSON json2 = (JSON) item2;

    Jget_int64 (json1, "endtime", &endtime1);
    Jget_int64 (json2, "endtime", &endtime2);

    return (endtime1 - endtime2);
}
#endif

static __inline__ void
myJput (void* o)
{
    if (o)
        json_object_put ((JSON)o);
}

size_t resrc_available_during_range (resrc_t *resrc,
                                     int64_t range_start_time,
                                     int64_t range_end_time)
{
    if (range_start_time == range_end_time) {
        return resrc_available_at_time (resrc, range_start_time);
    }

    int64_t curr_start_time = 0;
    int64_t curr_end_time = 0;

    const char *window_key = NULL;
    const char *window_json_str = NULL;
    JSON window_json = NULL;
    size_t *alloc_ptr = NULL, *reservtn_ptr = NULL;
    zlist_t *window_keys = NULL;

    zlist_t *matching_windows = zlist_new ();


   // Check that the time is during the resource lifetime
    window_json_str = (const char*) zhash_lookup (resrc->twindow, "0");
    if (window_json_str) {
        window_json = Jfromstr (window_json_str);
        if (window_json == NULL) {
            return -1;
        }
        Jget_int64 (window_json, "starttime", &curr_start_time);
        Jget_int64 (window_json, "endtime", &curr_end_time);
        if ( (range_start_time < curr_start_time) ||
             (range_end_time > curr_end_time) ) {
            return -1;
        }
    }

    // Map allocation window strings to JSON objects.  Filter out
    // windows that don't overlap with the input range. Then add the
    // job id to the JSON obj and insert the JSON obj into the
    // "matching windows" list.
    window_keys = zhash_keys (resrc->twindow);
    window_key = (const char *) zlist_first (window_keys);
    while (window_key) {
        window_json_str = (const char*) zhash_lookup (resrc->twindow, window_key);
        window_json = Jfromstr (window_json_str);
        if (window_json == NULL) {
            return -1;
        }
        Jget_int64 (window_json, "starttime", &curr_start_time);
        Jget_int64 (window_json, "endtime", &curr_end_time);

        // Does input range intersect with window?
        if ( !((curr_start_time < range_start_time &&
                curr_end_time < range_start_time) ||
               (curr_start_time > range_end_time &&
                curr_end_time > range_end_time)) ) {

            alloc_ptr = (size_t*)zhash_lookup (resrc->allocs, window_key);
            reservtn_ptr = (size_t*)zhash_lookup (resrc->reservtns, window_key);
            if (alloc_ptr || reservtn_ptr) {
                // Add the window key and insert JSON obj into the
                // "matching windows" list
                Jadd_str (window_json, "key", window_key);
                zlist_append (matching_windows, window_json);
                zlist_freefn (matching_windows, window_json, myJput, true);
            }
        }

        window_key = zlist_next (window_keys);
    }

    // Duplicate the "matching windows" list and then sort the 2 lists
    // based on start and end times.  We will walk through these lists
    // in order to find the minimum available during the input range
    zlist_t *start_windows = matching_windows;
    zlist_t *end_windows = zlist_dup (matching_windows);
    zlist_sort (start_windows, compare_windows_starttime);
    zlist_sort (end_windows, compare_windows_endtime);

    JSON curr_start_window = zlist_first (start_windows);
    JSON curr_end_window = zlist_first (end_windows);

    Jget_int64 (curr_start_window, "starttime", &curr_start_time);
    Jget_int64 (curr_end_window, "endtime", &curr_end_time);

    size_t min_available = resrc->size;
    size_t curr_available = resrc->size;
    size_t *size_ptr = NULL;

    // Start iterating over the windows and calculating the min
    // available
    //
    // OPTIMIZE: stop iterating when curr_start_window == NULL Once we
    // run out of start windows, curr available cannot get any
    // smaller; we have hit our min.  Just need to test to verify that
    // this optimziation is correct/safe.
    while (curr_start_window) {
        if ((curr_start_window) &&
            (curr_start_time <= curr_end_time)) {
            // New range is starting, get its size and subtract it
            // from current available
            Jget_str (curr_start_window, "key", &window_key);
            size_ptr = (size_t*)zhash_lookup (resrc->allocs, window_key);
            if (size_ptr)
                curr_available -= *size_ptr;
            size_ptr = (size_t*)zhash_lookup (resrc->reservtns, window_key);
            if (size_ptr)
                curr_available -= *size_ptr;
            curr_start_window = zlist_next (start_windows);
            if (curr_start_window) {
                Jget_int64 (curr_start_window, "starttime", &curr_start_time);
            } else {
                curr_start_time = TIME_MAX;
            }
        } else if ((curr_end_window) &&
                   (curr_end_time < curr_start_time)) {
            // A range just ended, get its size and add it back into
            // current available
            Jget_str (curr_end_window, "key", &window_key);
            size_ptr = (size_t*)zhash_lookup (resrc->allocs, window_key);
            if (size_ptr)
                curr_available += *size_ptr;
            size_ptr = (size_t*)zhash_lookup (resrc->reservtns, window_key);
            if (size_ptr)
                curr_available += *size_ptr;
            curr_end_window = zlist_next (end_windows);
            if (curr_end_window) {
                Jget_int64 (curr_end_window, "endtime", &curr_end_time);
            } else {
                curr_end_time = TIME_MAX;
            }
        } else {
            fprintf (stderr,
                     "%s - ERR: Both start/end windows are empty for %s\n",
                     __FUNCTION__, resrc->path);
            return -1;
        }
        min_available = (curr_available < min_available) ? curr_available : min_available;
    }

    // Cleanup
    zlist_destroy (&window_keys);
    zlist_destroy (&end_windows);
    zlist_destroy (&start_windows);

    return min_available;
}

char* resrc_state (resrc_t *resrc)
{
    char* str = NULL;

    if (resrc) {
        switch (resrc->state) {
        case RESOURCE_INVALID:
            str = "invalid";    break;
        case RESOURCE_IDLE:
            str = "idle";       break;
        case RESOURCE_ALLOCATED:
            str = "allocated";  break;
        case RESOURCE_RESERVED:
            str = "reserved";   break;
        case RESOURCE_DOWN:
            str = "down";       break;
        case RESOURCE_UNKNOWN:
            str = "unknown";    break;
        case RESOURCE_END:
        default:
            str = "n/a";        break;
        }
    }
    return str;
}

resrc_tree_t *resrc_phys_tree (resrc_t *resrc)
{
    if (resrc)
        return resrc->phys_tree;
    return NULL;
}

resrc_t *resrc_new_resource (const char *type, const char *path,
                             const char *name, int64_t id, uuid_t uuid,
                             size_t size)
{
    resrc_t *resrc = xzmalloc (sizeof (resrc_t));
    if (resrc) {
        resrc->type = strdup (type);
        if (path)
            resrc->path = strdup (path);
        if (name)
            resrc->name = strdup (name);
        resrc->id = id;
        if (uuid)
            uuid_copy (resrc->uuid, uuid);
        resrc->size = size;
        resrc->staged = 0;
        resrc->state = RESOURCE_IDLE;
        resrc->phys_tree = NULL;
        resrc->graphs = NULL;
        resrc->allocs = zhash_new ();
        resrc->reservtns = zhash_new ();
        resrc->properties = zhash_new ();
        resrc->tags = zhash_new ();
        resrc->twindow = zhash_new ();
        zhash_autofree (resrc->twindow);
    }

    return resrc;
}

resrc_t *resrc_copy_resource (resrc_t *resrc)
{
    resrc_t *new_resrc = xzmalloc (sizeof (resrc_t));

    if (new_resrc) {
        new_resrc->type = strdup (resrc->type);
        new_resrc->path = strdup (resrc->path);
        new_resrc->name = strdup (resrc->name);
        new_resrc->id = resrc->id;
        uuid_copy (new_resrc->uuid, resrc->uuid);
        new_resrc->state = resrc->state;
        new_resrc->phys_tree = resrc_tree_copy (resrc->phys_tree);
        new_resrc->graphs = zlist_dup (resrc->graphs);
        new_resrc->allocs = zhash_dup (resrc->allocs);
        new_resrc->reservtns = zhash_dup (resrc->reservtns);
        new_resrc->properties = zhash_dup (resrc->properties);
        new_resrc->tags = zhash_dup (resrc->tags);
        new_resrc->twindow = zhash_dup (resrc->twindow);
    }

    return new_resrc;
}

void resrc_resource_destroy (void *object)
{
    resrc_t *resrc = (resrc_t *) object;

    if (resrc) {
        if (resrc->type)
            free (resrc->type);
        if (resrc->path)
            free (resrc->path);
        if (resrc->name)
            free (resrc->name);
        /* Don't worry about freeing resrc->phys_tree.  It will be
         * freed by resrc_tree_free()
         */
        if (resrc->graphs)
            zlist_destroy (&resrc->graphs);
        zhash_destroy (&resrc->allocs);
        zhash_destroy (&resrc->reservtns);
        zhash_destroy (&resrc->properties);
        zhash_destroy (&resrc->tags);
        zhash_destroy (&resrc->twindow);
        free (resrc);
    }
}

resrc_t *resrc_new_from_json (JSON o, resrc_t *parent, bool physical)
{
    JSON jhierarchyo = NULL; /* json hierarchy object */
    JSON jpropso = NULL; /* json properties object */
    JSON jtagso = NULL;  /* json tags object */
    const char *name = NULL;
    const char *path = NULL;
    const char *tmp = NULL;
    const char *type = NULL;
    int64_t id;
    int64_t ssize;
    json_object_iter iter;
    resrc_t *resrc = NULL;
    resrc_tree_t *parent_tree = NULL;
    size_t size = 1;
    uuid_t uuid;

    if (!Jget_str (o, "type", &type))
        goto ret;
    Jget_str (o, "name", &name);
    if (!(Jget_int64 (o, "id", &id)))
        id = 0;
    if (Jget_str (o, "uuid", &tmp))
        uuid_parse (tmp, uuid);
    else
        uuid_clear(uuid);
    if (Jget_int64 (o, "size", &ssize))
        size = (size_t) ssize;
    if (!Jget_str (o, "path", &path)) {
        if ((jhierarchyo = Jobj_get (o, "hierarchy")))
            Jget_str (jhierarchyo, "default", &path);
    }

    resrc = resrc_new_resource (type, path, name, id, uuid, size);
    if (resrc) {
        /*
         * Are we constructing the resource's physical tree?  If
         * false, this is just a resource that is part of a request.
         */
        if (physical) {
            if (parent)
                parent_tree = parent->phys_tree;
            resrc->phys_tree = resrc_tree_new (parent_tree, resrc);
        }

        jpropso = Jobj_get (o, "properties");
        if (jpropso) {
            JSON jpropo;        /* json property object */
            char *property;

            json_object_object_foreachC (jpropso, iter) {
                jpropo = Jget (iter.val);
                property = strdup (json_object_get_string (jpropo));
                zhash_insert (resrc->properties, iter.key, property);
                zhash_freefn (resrc->properties, iter.key, free);
                Jput (jpropo);
            }
        }

        jtagso = Jobj_get (o, "tags");
        if (jtagso) {
            JSON jtago;        /* json tag object */
            char *tag;

            json_object_object_foreachC (jtagso, iter) {
                jtago = Jget (iter.val);
                tag = strdup (json_object_get_string (jtago));
                zhash_insert (resrc->tags, iter.key, tag);
                zhash_freefn (resrc->tags, iter.key, free);
                Jput (jtago);
            }
        }

        /* add twindow */
        int64_t job_time;
        if (Jget_int64 (o, "walltime", &job_time)) {
            JSON w = Jnew ();
            Jadd_int64 (w, "walltime", job_time);
            char *json_str = strdup (Jtostr (w));
            zhash_insert (resrc->twindow, "0", (void *) json_str);
            Jput (w);
        } else if (Jget_int64 (o, "starttime", &job_time)) {
            JSON w = Jnew ();
            Jadd_int64 (w, "starttime", job_time);
            Jget_int64 (o, "endtime", &job_time);
            Jadd_int64 (w, "endtime", job_time);
            char *json_str = strdup (Jtostr (w));
            zhash_insert (resrc->twindow, "0", (void *)json_str);
            Jput (w);
        }
    }
ret:
    return resrc;
}

static resrc_t *resrc_add_resource (resrc_t *parent, struct resource *r)
{
    JSON o = NULL;
    resrc_t *resrc = NULL;
    struct resource *c;

    o = rdl_resource_json (r);
    resrc = resrc_new_from_json (o, parent, true);

    while ((c = rdl_resource_next_child (r))) {
        (void) resrc_add_resource (resrc, c);
        rdl_resource_destroy (c);
    }

    Jput (o);
    return resrc;
}

resrc_t *resrc_generate_resources (const char *path, char *resource)
{
    resrc_t *resrc = NULL;
    struct rdl *rdl = NULL;
    struct rdllib *l = NULL;
    struct resource *r = NULL;

    if (!(l = rdllib_open ()) || !(rdl = rdl_loadfile (l, path)))
        goto ret;

    if ((r = rdl_resource_get (rdl, resource)))
        resrc = resrc_add_resource (NULL, r);

    rdl_destroy (rdl);
    rdllib_close (l);
ret:
    return resrc;
}

int resrc_to_json (JSON o, resrc_t *resrc)
{
    char uuid[40];
    int rc = -1;

    if (resrc) {
        Jadd_str (o, "type", resrc_type (resrc));
        Jadd_str (o, "path", resrc_path (resrc));
        Jadd_str (o, "name", resrc_name (resrc));
        Jadd_int64 (o, "id", resrc_id (resrc));
        uuid_unparse (resrc->uuid, uuid);
        Jadd_str (o, "uuid", uuid);
        Jadd_int64 (o, "size", resrc_size (resrc));
        rc = 0;
    }
    return rc;
}

void resrc_print_resource (resrc_t *resrc, int64_t time_now)
{
    char uuid[40];
    char *property;
    char *tag;
    size_t *size_ptr;
    int64_t available = 0;

    if (time_now < 0) {
        time_now = epochtime ();
    }

    if (resrc) {
        available = resrc_available_at_time (resrc, time_now);
        uuid_unparse (resrc->uuid, uuid);
        printf ("resrc type: %s, path: %s, name: %s, id: %"PRId64", state: %s, "
                "uuid: %s, size: %"PRIu64", available: %"PRId64"",
                resrc->type, resrc->path, resrc->name, resrc->id,
                resrc_state (resrc), uuid, resrc->size, available);
        if (zhash_size (resrc->properties)) {
            printf (", properties:");
            property = zhash_first (resrc->properties);
            while (property) {
                printf (" %s: %s", (char *)zhash_cursor (resrc->properties),
                        property);
                property = zhash_next (resrc->properties);
            }
        }
        if (zhash_size (resrc->tags)) {
            printf (", tags:");
            tag = zhash_first (resrc->tags);
            while (tag) {
                printf (", %s", (char *)zhash_cursor (resrc->tags));
                tag = zhash_next (resrc->tags);
            }
        }
        if (zhash_size (resrc->allocs)) {
            printf (", allocs");
            size_ptr = zhash_first (resrc->allocs);
            while (size_ptr) {
                printf (", %s: %"PRIu64"",
                        (char *)zhash_cursor (resrc->allocs), *size_ptr);
                size_ptr = zhash_next (resrc->allocs);
            }
        }
        if (zhash_size (resrc->reservtns)) {
            printf (", reserved jobs");
            size_ptr = zhash_first (resrc->reservtns);
            while (size_ptr) {
                printf (", %s: %"PRIu64"",
                        (char *)zhash_cursor (resrc->reservtns), *size_ptr);
                size_ptr = zhash_next (resrc->reservtns);
            }
        }
        printf ("\n");
    }
}

/*
 * Finds if a resrc_t *sample matches with resrc_t *resrc in terms of walltime
 *
 * Note: this function is working on a resource that is already AVAILABLE.
 * Therefore it is sufficient if the walltime fits before the earliest starttime
 * of a reserved job.
 */
bool resrc_walltime_match (resrc_t *resrc, resrc_t *sample)
{
    bool rc = false;

    int64_t jstarttime = 0; // Job start time
    int64_t jendtime = 0; // Job end time
    int64_t jwalltime = 0; // Job walltime
    int64_t lendtime = 0; // Resource lifetime end time

    char *json_str_window = NULL;

    // retrieve first element of twindow from request sample
    json_str_window = zhash_first (sample->twindow);
    if (!json_str_window)
        return true;

    // retrieve the start & end time information from request sample
    JSON job_window = Jfromstr (json_str_window);
    if (!(Jget_int64 (job_window, "starttime", &jstarttime))) {
        Jput (job_window);
        return true;
    }
    if (!(Jget_int64 (job_window, "endtime", &jendtime))) {
        if ((Jget_int64 (job_window, "walltime", &jwalltime))) {
            jendtime = jstarttime + jwalltime;
        } else {
            Jput (job_window);
            return false;
        }
    }
    Jput (job_window);

    // If jendtime is greater than the lifetime of the resource, then false
    json_str_window = zhash_lookup (resrc->twindow, "0");
    if (json_str_window) {
        JSON lt = Jfromstr (json_str_window);
        Jget_int64 (lt, "endtime", &lendtime);
        Jput (lt);
        if (jendtime > (lendtime - 10)) {
            return false;
        }
    }

    // find if it sample fits in time range
    int64_t available = resrc_available_during_range(resrc, jstarttime, jendtime);
    rc = (available >= sample->size);

    return rc;
}


bool resrc_match_resource (resrc_t *resrc, resrc_t *sample, bool available)
{
    bool rc = false;
    char *sproperty = NULL;     /* sample property */
    char *stag = NULL;          /* sample tag */

    if (!strcmp (resrc->type, sample->type) && sample->size) {
        if (zhash_size (sample->properties)) {
            if (!zhash_size (resrc->properties)) {
                goto ret;
            }
            /* be sure the resource has all the requested properties */
            /* TODO: validate the value of each property */
            zhash_first (sample->properties);
            do {
                sproperty = (char *)zhash_cursor (sample->properties);
                if (!zhash_lookup (resrc->properties, sproperty))
                    goto ret;
            } while (zhash_next (sample->properties));
        }

        if (zhash_size (sample->tags)) {
            if (!zhash_size (resrc->tags)) {
                goto ret;
            }
            /* be sure the resource has all the requested tags */
            zhash_first (sample->tags);
            do {
                stag = (char *)zhash_cursor (sample->tags);
                if (!zhash_lookup (resrc->tags, stag))
                    goto ret;
            } while (zhash_next (sample->tags));
        }

        if (available && resrc->state != RESOURCE_IDLE) {
            goto ret;
        }

        // Moved this check to last, because it is the most expensive
        rc = resrc_walltime_match (resrc, sample);

    }
ret:
    return rc;
}

void resrc_stage_resrc (resrc_t *resrc, size_t size)
{
    if (resrc)
        resrc->staged = size;
}

void resrc_unstage_resrc (resrc_t *resrc)
{
    if (resrc)
        resrc->staged = 0;
}

/*
 * Allocate the staged size of a resource to the specified job_id and
 * change its state to allocated.
 */
int resrc_allocate_resource (resrc_t *resrc, int64_t job_id, int64_t time_now, int64_t walltime)
{
    char *id_ptr = NULL;
    char *json_str = NULL;
    size_t *size_ptr;
    size_t available;
    int64_t end_time;
    int rc = -1;
    JSON j;

    if (time_now < 0) {
        time_now = epochtime ();
    }

    if (walltime < 0) {
        end_time = TIME_MAX;
    } else {
        end_time = time_now + walltime;
    }

    if (resrc && job_id) {
        available = resrc_available_during_range (resrc, time_now, end_time);
        if (resrc->staged > available) {
            goto ret;
        }

        id_ptr = xasprintf("%"PRId64"", job_id);
        size_ptr = (size_t *) zhash_lookup (resrc->allocs, id_ptr);
        if (size_ptr) {
            *size_ptr += resrc->staged;
        } else {
            size_ptr = xzmalloc (sizeof (size_t));
            *size_ptr = resrc->staged;
            zhash_insert (resrc->allocs, id_ptr, size_ptr);
            zhash_freefn (resrc->allocs, id_ptr, free);

            /* add walltime */
            j = Jnew ();
            Jadd_int64 (j, "starttime", time_now);
            Jadd_int64 (j, "endtime", end_time);
            json_str = strdup (Jtostr (j));
            zhash_insert (resrc->twindow, id_ptr, (void *)json_str);
            Jput (j);
        }
        resrc->staged = 0;

        rc = 0;
        free (id_ptr);
    }
ret:
    return rc;
}

// TODO: flag instead?
int resrc_allocate_resource_unchecked (resrc_t *resrc, int64_t job_id,
                                       int64_t time_now, int64_t walltime)
{
    char *id_ptr = NULL;
    char *json_str = NULL;
    size_t *size_ptr;
    int64_t end_time;
    int rc = -1;
    JSON j;

    if (time_now < 0) {
        time_now = epochtime ();
    }

    if (walltime < 0) {
        end_time = TIME_MAX;
    } else {
        end_time = time_now + walltime;
    }

    if (resrc && job_id) {
        id_ptr = xasprintf("%"PRId64"", job_id);
        size_ptr = (size_t *) zhash_lookup (resrc->allocs, id_ptr);
        if (size_ptr) {
            *size_ptr += resrc->staged;
        } else {
            size_ptr = xzmalloc (sizeof (size_t));
            *size_ptr = resrc->staged;
            zhash_insert (resrc->allocs, id_ptr, size_ptr);
            zhash_freefn (resrc->allocs, id_ptr, free);

            /* add walltime */
            j = Jnew ();
            Jadd_int64 (j, "starttime", time_now);
            Jadd_int64 (j, "endtime", end_time);
            json_str = strdup (Jtostr (j));
            zhash_insert (resrc->twindow, id_ptr, (void *)json_str);
            Jput (j);
        }
        resrc->staged = 0;

        rc = 0;
        free (id_ptr);
    }

    return rc;
}

// TODO: use release_resource & stage instead?
int resrc_deallocate_resource (resrc_t *resrc, int64_t job_id, int64_t size)
{
    int rc = 0;
    char *id_ptr = xasprintf ("%"PRId64"", job_id);
    size_t *size_ptr = (size_t *) zhash_lookup (resrc->allocs, id_ptr);

    if (size_ptr) {
        *size_ptr -= size;
        if (*size_ptr <= 0) {
            resrc_release_resource (resrc, job_id);
        }
    } else {
        rc = -1;
    }

    free (id_ptr);

    return rc;
}


/*
 * Just like resrc_allocate_resource() above, but for a reservation
 */
int resrc_reserve_resource (resrc_t *resrc, int64_t job_id,
                            int64_t time_now, int64_t walltime)
{
    char *id_ptr = NULL, *json_str = NULL;
    size_t *size_ptr;
    int rc = -1;
    size_t available;
    JSON j;
    int64_t end_time = time_now + walltime;

    if (resrc && job_id) {
        available = resrc_available_during_range (resrc, time_now, end_time);
        if (resrc->staged > available)
            goto ret;

        id_ptr = xasprintf("%"PRId64"", job_id);
        size_ptr = (size_t *) zhash_lookup (resrc->reservtns, id_ptr);
        if (size_ptr) {
            *size_ptr += resrc->staged;
        } else {
            size_ptr = xzmalloc (sizeof (size_t));
            *size_ptr = resrc->staged;
            zhash_insert (resrc->reservtns, id_ptr, size_ptr);
            zhash_freefn (resrc->reservtns, id_ptr, free);

            /* add walltime */
            j = Jnew ();
            Jadd_int64 (j, "starttime", time_now);
            Jadd_int64 (j, "endtime", end_time);
            json_str = strdup (Jtostr (j));
            zhash_insert (resrc->twindow, id_ptr, (void *)json_str);
            Jput (j);
        }
        resrc->staged = 0;

        rc = 0;
        free (id_ptr);
    }
ret:
    return rc;
}

int resrc_release_resource (resrc_t *resrc, int64_t rel_job)
{
    char *id_ptr = NULL;
    size_t *alloc_ptr = NULL, *reservtn_ptr = NULL;
    int rc = 0;

    if (!resrc || !rel_job) {
        rc = -1;
        goto ret;
    }

    id_ptr = xasprintf ("%"PRId64"", rel_job);

    alloc_ptr = zhash_lookup (resrc->allocs, id_ptr);
    if (alloc_ptr) {
        zhash_delete (resrc->allocs, id_ptr);
    }

    reservtn_ptr = zhash_lookup (resrc->reservtns, id_ptr);
    if (reservtn_ptr) {
        zhash_delete (resrc->reservtns, id_ptr);
    }

    if (alloc_ptr || reservtn_ptr) {
        zhash_delete (resrc->twindow, id_ptr);
    }

    free (id_ptr);
ret:
    return rc;
}


/******
 * Resources
 ******/

resources_t *resrc_new_resources ()
{
    resources_t *resrcs = NULL;

    resrcs = (resources_t *) xzmalloc (sizeof (resources_t));
    if (resrcs) {
        resrcs->hash = zhash_new();
    }

    return resrcs;
}

void resrc_populate_resources_from_tree (resrc_tree_t *resrc_tree,
                                         resources_t *resrcs)
{
    resrc_t *resrc = resrc_tree_resrc (resrc_tree);
    char* resrc_path_str = resrc_path (resrc);
    zhash_insert (resrcs->hash, resrc_path_str, resrc);

    resrc_tree_list_t *child_list = resrc_tree_children (resrc_tree);
    resrc_tree_t *curr_child;
    for (curr_child = resrc_tree_list_first (child_list);
         curr_child;
         curr_child = resrc_tree_list_next (child_list)) {
        resrc_populate_resources_from_tree (curr_child, resrcs);
    }
}

resources_t *resrc_new_resources_from_tree (resrc_tree_t *resrc_tree)
{
    resources_t *output = resrc_new_resources ();

    resrc_t *resrc = resrc_tree_resrc (resrc_tree);
    zhash_insert (output->hash, "head", resrc);
    resrc_populate_resources_from_tree (resrc_tree, output);

    return output;
}

resrc_t *resrc_lookup (resources_t *resrcs, const char *resrc_path)
{
    if (resrcs && resrcs->hash)
        return (zhash_lookup (resrcs->hash, resrc_path));
    return NULL;
}

void resrc_destroy_resources (resources_t *resrcs)
{
    if (resrcs && resrcs->hash) {
        zhash_destroy (&(resrcs->hash));
        free (resrcs);
    }
}

/*
 * vi: ts=4 sw=4 expandtab
 */

