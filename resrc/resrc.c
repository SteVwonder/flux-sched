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

#if HAVE_CONFIG_H
# include <config.h>
#endif
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <assert.h>
#include <czmq.h>
#include <hwloc.h>

#include "src/common/libutil/shortjson.h"
#include "rdl.h"
#include "resrc.h"
#include "resrc_tree.h"
#include "resrc_reqst.h"
#include "src/common/libutil/xzmalloc.h"

#include <flux/core.h>
#include "src/common/libutil/log.h"
#define printf(...)
//#define flux_log(...)

typedef struct resrc_slack_info {
    int64_t owner;             /* 0 = self, -1 = parent, other = children */
    int64_t leasee;            /* 0 = self, -1 = parent, other = children */
    int      asked;             /* 1 = yes, 0 = no */
    int      to_be_returned;    /* 1 = yes, 0 = no */
    int      returned;          /* 1 = yes, 0 = no */
} resrc_slinfo_t;

struct resrc {
    char *type;
    char *path;
    char *basename;
    char *name;
    char *digest;
    int64_t id;
    uuid_t uuid;
    size_t size;
    size_t available;
    size_t staged;
    resource_state_t state;
    resrc_tree_t *phys_tree;
    zlist_t *graphs;
    zhash_t *properties;
    zhash_t *tags;
    zhash_t *allocs;
    zhash_t *reservtns;
    zhash_t *twindow;
    resrc_slinfo_t slinfo;
};

static int sim_mode = 0;
static int64_t sim_time;

// TODO: remove this
void resrc_set_sim_mode () {
    sim_mode = 1;
}

// TODO: remove this
void resrc_set_sim_time (int64_t st) {
    sim_time = st;
}

// TODO: remove this
int64_t epochtime ()
{
    if (sim_mode) {
        return sim_time;
    }
    return (int64_t)time (NULL);
}

// TODO: remove this
int64_t resrc_epochtime ()
{
    return epochtime ();
}

/***************************************************************************
 *  API
 ***************************************************************************/
char *resrc_type (resrc_t *resrc)
{
    if (resrc)
        return resrc->type;
    return NULL;
}

char *resrc_path (resrc_t *resrc)
{
    if (resrc)
        return resrc->path;
    return NULL;
}

char *resrc_basename (resrc_t *resrc)
{
    if (resrc)
        return resrc->basename;
    return NULL;
}

char *resrc_name (resrc_t *resrc)
{
    if (resrc)
        return resrc->name;
    return NULL;
}

char *resrc_digest (resrc_t *resrc)
{
    if (resrc)
        return resrc->digest;
    return NULL;
}

char *resrc_set_digest (resrc_t *resrc, char *digest)
{
    char *old = NULL;
    if (resrc) {
        old = resrc->digest;
        resrc->digest = digest;
    }
    return old;
}

int64_t resrc_id (resrc_t *resrc)
{
    if (resrc)
        return resrc->id;
    return -1;
}

void resrc_uuid (resrc_t *resrc, char *uuid)
{
    if (resrc)
        uuid_unparse (resrc->uuid, uuid);
    else
        uuid = NULL;
}

size_t resrc_size (resrc_t *resrc)
{
    if (resrc)
        return resrc->size;
    return 0;
}

size_t resrc_available_at_time (resrc_t *resrc, int64_t time)
{
    int64_t starttime;
    int64_t endtime;

    const char *id_ptr = NULL;
    const char *window_json_str = NULL;
    JSON window_json = NULL;
    zlist_t *window_keys = NULL;
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
            return 0;
        }
        Jget_int64 (window_json, "starttime", &starttime);
        Jget_int64 (window_json, "endtime", &endtime);
        if (time < starttime || time > endtime) {
            return 0;
        }
    }

    // Iterate over all allocation windows in resrc.  We iterate using
    // keys since we need the key to lookup the size in resrc->allocs.
    window_keys = zhash_keys (resrc->twindow);
    id_ptr = zlist_next (window_keys);
    while (id_ptr) {
        window_json_str = (const char*) zhash_lookup (resrc->twindow, id_ptr);
        window_json = Jfromstr (window_json_str);
        if (window_json == NULL) {
            return 0;
        }
        Jget_int64 (window_json, "starttime", &starttime);
        Jget_int64 (window_json, "endtime", &endtime);

        // Does time intersect with window?
        if (time >= starttime && time <= endtime) {
            // Decrement available by allocation and/or reservation size
            size_ptr = (size_t*)zhash_lookup (resrc->allocs, id_ptr);
            if (size_ptr) {
                available -= *size_ptr;
            }
            size_ptr = (size_t*)zhash_lookup (resrc->reservtns, id_ptr);
            if (size_ptr) {
                available -= *size_ptr;
            }
            if (!(strcmp (id_ptr, "slack"))) // hack
                available += 1;
        }

        Jput (window_json);
        id_ptr = zlist_next (window_keys);
    }

    zlist_destroy (&window_keys);

    return available;
}


#if CZMQ_VERSION < CZMQ_MAKE_VERSION(3, 0, 1)
static bool compare_windows_starttime (void *item1, void *item2)
{
    int64_t starttime1, starttime2;
    JSON json1 = (JSON) item1;
    JSON json2 = (JSON) item2;

    Jget_int64 (json1, "starttime", &starttime1);
    Jget_int64 (json2, "starttime", &starttime2);

    return (starttime1 > starttime2);
}
#else
static int compare_windows_starttime (void *item1, void *item2)
{
    int64_t starttime1, starttime2;
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
    int64_t endtime1, endtime2;
    JSON json1 = (JSON) item1;
    JSON json2 = (JSON) item2;

    Jget_int64 (json1, "endtime", &endtime1);
    Jget_int64 (json2, "endtime", &endtime2);

    return (endtime1 > endtime2);
}
#else
static int compare_windows_endtime (void *item1, void *item2)
{
    int64_t endtime1, endtime2;
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

size_t resrc_available_during_range (resrc_t *resrc, int64_t range_starttime,
                                     int64_t range_endtime, bool exclusive)
{
    JSON window_json = NULL;
    const char *id_ptr = NULL;
    const char *window_json_str = NULL;
    int64_t  curr_endtime;
    int64_t  curr_starttime;
    size_t   curr_available;
    size_t   min_available = 0;
    size_t  *alloc_ptr = NULL;
    size_t  *reservtn_ptr = NULL;
    size_t  *size_ptr = NULL;
    zlist_t *matching_windows = NULL;
    zlist_t *window_keys = NULL;

    if (range_starttime == range_endtime) {
        return resrc_available_at_time (resrc, range_starttime);
    }

    matching_windows = zlist_new ();

    // Check that the time is during the resource lifetime
    window_json_str = (const char*) zhash_lookup (resrc->twindow, "0");
    if (window_json_str) {
        window_json = Jfromstr (window_json_str);
        if (window_json == NULL) {
            return 0;
        }
        Jget_int64 (window_json, "starttime", &curr_starttime);
        Jget_int64 (window_json, "endtime", &curr_endtime);
        if ( (range_starttime < curr_starttime) ||
             (range_endtime > curr_endtime) ) {
            Jput (window_json);
            return 0;
        }
        Jput (window_json);
    }

    // Map allocation window strings to JSON objects.  Filter out
    // windows that don't overlap with the input range. Then add the
    // job id to the JSON obj and insert the JSON obj into the
    // "matching windows" list.
    window_keys = zhash_keys (resrc->twindow);
    for (id_ptr = (const char *) zlist_next (window_keys);
         id_ptr;
         id_ptr = zlist_next (window_keys)) {
        if (!strncmp (id_ptr, "0", 2)) {
            printf ("Skipping lifetime window in intersection check, id_ptr = %s\n", id_ptr);
            continue;
        }
        printf ("Considering id_ptr = %s\n", id_ptr);
        window_json_str = (const char*) zhash_lookup (resrc->twindow, id_ptr);
        window_json = Jfromstr (window_json_str);
        if (window_json == NULL) {
            return 0;
        }
        Jget_int64 (window_json, "starttime", &curr_starttime);
        Jget_int64 (window_json, "endtime", &curr_endtime);

        printf ("curr_starttime = %ld, curr_endtime = %ld, range_starttime = %ld, range_endtime = %ld\n", curr_starttime, curr_endtime, range_starttime, range_endtime);
        // Does input range intersect with window?
        if ( !((curr_starttime < range_starttime &&
                curr_endtime < range_starttime) ||
               (curr_starttime > range_endtime &&
                curr_endtime > range_endtime)) ) {

            alloc_ptr = (size_t*)zhash_lookup (resrc->allocs, id_ptr);
            reservtn_ptr = (size_t*)zhash_lookup (resrc->reservtns, id_ptr);
            if (alloc_ptr || reservtn_ptr || !(strcmp (id_ptr, "slack")) || !(strcmp (id_ptr, "slacksub"))) {
                // Add the window key and insert JSON obj into the
                // "matching windows" list
                Jadd_str (window_json, "job_id", id_ptr);
                printf ("Adding id_ptr = %s to matching windows\n", id_ptr);
                zlist_append (matching_windows, window_json);
                zlist_freefn (matching_windows, window_json, myJput, true);
            } else
                Jput (window_json);
        } else
            Jput (window_json);
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

    Jget_int64 (curr_start_window, "starttime", &curr_starttime);
    Jget_int64 (curr_end_window, "endtime", &curr_endtime);

    min_available = resrc->size;
    curr_available = resrc->size;

    printf ("Min available starting = %zu\n", min_available);
    // Start iterating over the windows and calculating the min
    // available
    //
    // OPTIMIZE: stop iterating when curr_start_window == NULL Once we
    // run out of start windows, curr available cannot get any
    // smaller; we have hit our min.  Just need to test to verify that
    // this optimziation is correct/safe.
    while (curr_start_window) {
        if ((curr_start_window) &&
            (curr_starttime <= curr_endtime)) {
            // New range is starting, get its size and subtract it
            // from current available
            Jget_str (curr_start_window, "job_id", &id_ptr);
            printf ("Coming here with id_ptr = %s\n", id_ptr);
            size_ptr = (size_t*)zhash_lookup (resrc->allocs, id_ptr);
            if (size_ptr) {
                curr_available -= *size_ptr;
                printf ("Subtracting sizeptr = %zu\n", *size_ptr);
            }
            size_ptr = (size_t*)zhash_lookup (resrc->reservtns, id_ptr);
            if (size_ptr)
                curr_available -= *size_ptr;
            if (!(strcmp (id_ptr, "slack"))) {   // crazy hack
                printf ("slack id_ptr found, adding 1\n");
                curr_available += 1;
                min_available += 1;
            }
            if (!(strcmp (id_ptr, "slacksub"))) { //hack
                printf ("slacksub id_ptr found, subtracting 1\n");
                curr_available -= 1;
            }

            curr_start_window = zlist_next (start_windows);
            if (curr_start_window) {
                Jget_int64 (curr_start_window, "starttime", &curr_starttime);
            } else {
                curr_starttime = TIME_MAX;
            }
        } else if ((curr_end_window) &&
                   (curr_endtime < curr_starttime)) {
            // A range just ended, get its size and add it back into
            // current available
            printf ("going here\n");
            Jget_str (curr_end_window, "job_id", &id_ptr);
            size_ptr = (size_t*)zhash_lookup (resrc->allocs, id_ptr);
            if (size_ptr)
                curr_available += *size_ptr;
            size_ptr = (size_t*)zhash_lookup (resrc->reservtns, id_ptr);
            if (size_ptr)
                curr_available += *size_ptr;
            if (!(strcmp (id_ptr, "slack"))) {// hack
                curr_available -= 1;
                //min_available -= 1;
            }
            if (!(strcmp (id_ptr, "slacksub"))) { //hack
                curr_available += 1;
            }
            curr_end_window = zlist_next (end_windows);
            if (curr_end_window) {
                Jget_int64 (curr_end_window, "endtime", &curr_endtime);
            } else {
                curr_endtime = TIME_MAX;
            }
        } else {
            fprintf (stderr,
                     "%s - ERR: Both start/end windows are empty\n",
                     __FUNCTION__);
        }
        // TODO: curr_available underflows (becomes MAX - 1)
        min_available = (curr_available < min_available) ? curr_available :
            min_available;
        printf ("curr available now = %zu and min available now = %zu\n", curr_available, min_available);
        if (exclusive && min_available < resrc->size) {
            // Is not exclusively allocated, so return 0
            min_available = 0;
        }
    }

    zlist_destroy (&end_windows);
ret:
    zlist_destroy (&window_keys);
    zlist_destroy (&matching_windows);

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
        case RESOURCE_SLACKSUB:
            str = "slacksub";   break;
        case RESOURCE_END:
        default:
            str = "n/a";        break;
        }
    }
    return str;
}

int64_t resrc_owner (resrc_t *resrc)
{
    if (resrc)
        return resrc->slinfo.owner;
    return -1;
}

int64_t resrc_leasee (resrc_t *resrc)
{
    if (resrc)
        return resrc->slinfo.leasee;
    return -1;
}

void resrc_set_state (resrc_t *resrc, resource_state_t state)
{
    resrc->state = state;
    return;
}

resrc_tree_t *resrc_phys_tree (resrc_t *resrc)
{
    if (resrc)
        return resrc->phys_tree;
    return NULL;
}

void resrc_set_owner (resrc_t *resrc, int64_t owner)
{
    if (resrc) {
        resrc->slinfo.owner = owner;
    }
    return;
}

void resrc_copy_lifetime (resrc_t *from, resrc_t *to)
{
    if (!from || !to) {
        return;
    }

    char *from_lifetime = zhash_lookup (from->twindow, "0");
    if (from_lifetime == NULL) {
        fprintf (stderr,
                 "%s: twindow 0 does not exist in added resource, keeping old twindow",
                 __FUNCTION__);
        return;
    }
    char *to_lifetime = zhash_lookup (to->twindow, "0");
    if (to_lifetime) {
        zhash_update (to->twindow, "0", xstrdup (from_lifetime));
    } else {
        zhash_insert (to->twindow, "0", xstrdup (from_lifetime));
    }


}

resrc_t *resrc_new_resource (const char *type, const char *path,
                             const char *basename, const char *name,
                             const char *sig, int64_t id, uuid_t uuid,
                             size_t size)
{
    resrc_t *resrc = xzmalloc (sizeof (resrc_t));
    if (resrc) {
        resrc->type = xstrdup (type);
        if (path)
            resrc->path = xstrdup (path);
        if (basename)
            resrc->basename = xstrdup (basename);
        else
            resrc->basename = xstrdup (type);
        resrc->id = id;
        if (name)
            resrc->name = xstrdup (name);
        else {
            if (id < 0)
                resrc->name = xstrdup (resrc->basename);
            else
                resrc->name = xasprintf ("%s%"PRId64"", resrc->basename, id);
        }
        resrc->digest = NULL;
        if (sig)
            resrc->digest = xstrdup (sig);
        if (uuid)
            uuid_copy (resrc->uuid, uuid);
        else
            uuid_clear (resrc->uuid);
        resrc->size = size;
        resrc->available = size;
        resrc->staged = 0;
        resrc->state = RESOURCE_INVALID;
        resrc->phys_tree = NULL;
        resrc->graphs = NULL;
        resrc->allocs = zhash_new ();
        resrc->reservtns = zhash_new ();
        resrc->properties = zhash_new ();
        resrc->tags = zhash_new ();
        resrc->twindow = zhash_new ();
        resrc->slinfo.owner = 0;
        resrc->slinfo.leasee = 0;
        resrc->slinfo.to_be_returned = 0;
        resrc->slinfo.asked = 0;
        resrc->slinfo.returned = 0;

    }

    return resrc;
}

resrc_t *resrc_copy_resource (resrc_t *resrc)
{
    resrc_t *new_resrc = xzmalloc (sizeof (resrc_t));

    if (new_resrc) {
        new_resrc->type = xstrdup (resrc->type);
        new_resrc->path = xstrdup (resrc->path);
        new_resrc->basename = xstrdup (resrc->basename);
        new_resrc->name = xstrdup (resrc->name);
        if (resrc->digest)
            new_resrc->digest = xstrdup (resrc->digest);
        new_resrc->id = resrc->id;
        uuid_copy (new_resrc->uuid, resrc->uuid);
        new_resrc->state = resrc->state;
        new_resrc->phys_tree = resrc_tree_copy (resrc->phys_tree);
        new_resrc->graphs = zlist_dup (resrc->graphs);
        new_resrc->allocs = zhash_dup (resrc->allocs);
        new_resrc->reservtns = zhash_dup (resrc->reservtns);
        new_resrc->properties = zhash_dup (resrc->properties);
        new_resrc->tags = zhash_dup (resrc->tags);
        if (resrc->twindow)
            new_resrc->twindow = zhash_dup (resrc->twindow);
        else
            new_resrc->twindow = NULL;
        new_resrc->slinfo = resrc->slinfo;
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
        if (resrc->basename)
            free (resrc->basename);
        if (resrc->name)
            free (resrc->name);
        if (resrc->digest)
            free (resrc->digest);
        if (resrc->phys_tree)
            resrc_tree_destroy (resrc->phys_tree, false);
        if (resrc->graphs)
            zlist_destroy (&resrc->graphs);
        zhash_destroy (&resrc->allocs);
        zhash_destroy (&resrc->reservtns);
        zhash_destroy (&resrc->properties);
        zhash_destroy (&resrc->tags);
        if (resrc->twindow)
            zhash_destroy (&resrc->twindow);
        free (resrc);
    }
}

void resrc_resource_destroy_special (void *object, zhash_t *hash_table)
{
    resrc_t *resrc = (resrc_t *) object;
    zhash_delete (hash_table, (const char *)resrc->uuid);
    resrc_resource_destroy (resrc);
}

resrc_t *resrc_new_from_json (JSON o, resrc_t *parent, bool physical)
{
    JSON jhierarchyo = NULL; /* json hierarchy object */
    JSON jpropso = NULL; /* json properties object */
    JSON jtagso = NULL;  /* json tags object */
    const char *basename = NULL;
    const char *name = NULL;
    const char *path = NULL;
    const char *tmp = NULL;
    const char *type = NULL;
    int64_t jduration;
    const char *slack_str = NULL;
    int64_t id;
    int64_t ssize;
    json_object_iter iter;
    resrc_t *resrc = NULL;
    resrc_tree_t *parent_tree = NULL;
    size_t size = 1;
    uuid_t uuid;

    if (!Jget_str (o, "type", &type))
        goto ret;
    Jget_str (o, "basename", &basename);
    Jget_str (o, "name", &name);
    if (!(Jget_int64 (o, "id", &id)))
        id = -1;
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

    resrc = resrc_new_resource (type, path, basename, name, NULL, id, uuid,
                                size);
    if (resrc) {
        /*
         * Are we constructing the resource's physical tree?  If
         * false, this is just a resource that is part of a request.
         */
        if (physical) {
            if (parent)
                parent_tree = parent->phys_tree;
            resrc->phys_tree = resrc_tree_new (parent_tree, resrc);

            /* add time window if we are given a start time */
            int64_t starttime;
            if (Jget_int64 (o, "starttime", &starttime)) {
                JSON    w = Jnew ();
                char    *json_str;
                int64_t endtime;
                int64_t wall_time;

                Jadd_int64 (w, "starttime", starttime);

                if (!Jget_int64 (o, "endtime", &endtime)) {
                    if (Jget_int64 (o, "walltime", &wall_time))
                        endtime = starttime + wall_time;
                    else
                        endtime = TIME_MAX;
                }
                Jadd_int64 (w, "endtime", endtime);

                json_str = xstrdup (Jtostr (w));
                zhash_insert (resrc->twindow, "0", (void *) json_str);
                zhash_freefn (resrc->twindow, "0", free);
                Jput (w);
            } else {
                JSON w = Jnew ();
                Jadd_int64 (w, "starttime", epochtime());
                Jadd_int64 (w, "endtime", TIME_MAX);
                char *json_str = xstrdup (Jtostr (w));
                zhash_insert (resrc->twindow, "0", (void *) json_str);
                zhash_freefn (resrc->twindow, "0", free);
                Jput (w);
            }
        }

        jpropso = Jobj_get (o, "properties");
        if (jpropso) {
            JSON jpropo;        /* json property object */
            char *property;

            json_object_object_foreachC (jpropso, iter) {
                jpropo = Jget (iter.val);
                property = xstrdup (json_object_get_string (jpropo));
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
                tag = xstrdup (json_object_get_string (jtago));
                zhash_insert (resrc->tags, iter.key, tag);
                zhash_freefn (resrc->tags, iter.key, free);
                Jput (jtago);
            }
        }

        if (Jget_str (o, "slack", &slack_str)) {
            zhash_update (resrc->twindow, "0", (void *)slack_str);
        }
    }
ret:
    return resrc;
}

static resrc_t *resrc_add_rdl_resource (resrc_t *parent, struct resource *r)
{
    JSON o = NULL;
    resrc_t *resrc = NULL;
    struct resource *c;

    o = rdl_resource_json (r);
    resrc = resrc_new_from_json (o, parent, true);

    while ((c = rdl_resource_next_child (r))) {
        (void) resrc_add_rdl_resource (resrc, c);
        rdl_resource_destroy (c);
    }

    Jput (o);
    return resrc;
}

resrc_t *resrc_generate_rdl_resources (const char *path, char *resource)
{
    resrc_t *resrc = NULL;
    struct rdl *rdl = NULL;
    struct rdllib *l = NULL;
    struct resource *r = NULL;

    if (!(l = rdllib_open ()) || !(rdl = rdl_loadfile (l, path)))
        goto ret;

    if ((r = rdl_resource_get (rdl, resource)))
        resrc = resrc_add_rdl_resource (NULL, r);

    rdl_destroy (rdl);
    rdllib_close (l);
ret:
    return resrc;
}

static char *lowercase (char *str)
{
    if (str) {
        int i = 0;
        while (str[i]) {
            str[i] = tolower((int) str[i]);
            i++;
        }
    }
    return str;
}

static resrc_t *resrc_new_from_hwloc_obj (hwloc_obj_t obj, resrc_t *parent,
                                          const char *sig)
{
    const char *hwloc_name = NULL;
    char *basename = NULL;
    char *name = NULL;
    char *path = NULL;
    char *signature = NULL;
    char *type = NULL;
    int64_t id;
    resrc_t *resrc = NULL;
    resrc_tree_t *parent_tree = NULL;
    size_t size = 1;
    uuid_t uuid;

    id = obj->logical_index;
    if (!hwloc_compare_types (obj->type, HWLOC_OBJ_MACHINE)) {
        type = xstrdup ("node");
        signature = sig? xstrdup (sig) : NULL;
        hwloc_name = hwloc_obj_get_info_by_name (obj, "HostName");
        if (!hwloc_name)
            goto ret;
        name = xstrdup (hwloc_name);
#if HWLOC_API_VERSION < 0x00010b00
    } else if (!hwloc_compare_types (obj->type, HWLOC_OBJ_NODE)) {
#else
    } else if (!hwloc_compare_types (obj->type, HWLOC_OBJ_NUMANODE)) {
#endif
        type = xstrdup ("numanode");
        name = xasprintf ("%s%"PRId64"", type, id);
#if HWLOC_API_VERSION < 0x00010b00
    } else if (!hwloc_compare_types (obj->type, HWLOC_OBJ_SOCKET)) {
#else
    } else if (!hwloc_compare_types (obj->type, HWLOC_OBJ_PACKAGE)) {
#endif
        type = xstrdup ("socket");
        name = xasprintf ("%s%"PRId64"", type, id);
#if HWLOC_API_VERSION < 0x00020000
    } else if (!hwloc_compare_types (obj->type, HWLOC_OBJ_CACHE)) {
        type = xstrdup ("cache");
        name = xasprintf ("L%"PRIu32"cache%"PRId64"", obj->attr->cache.depth,
                          id);
        size = obj->attr->cache.size / 1024;
#else
    } else if (!hwloc_compare_types (obj->type, HWLOC_OBJ_L1CACHE)) {
        type = xstrdup ("cache");
        name = xasprintf ("L1cache%"PRId64"", obj->attr->cache.depth, id);
        size = obj->attr->cache.size / 1024;
    } else if (!hwloc_compare_types (obj->type, HWLOC_OBJ_L2CACHE)) {
        type = xstrdup ("cache");
        name = xasprintf ("L2cache%"PRId64"", obj->attr->cache.depth, id);
        size = obj->attr->cache.size / 1024;
    } else if (!hwloc_compare_types (obj->type, HWLOC_OBJ_L3CACHE)) {
        type = xstrdup ("cache");
        name = xasprintf ("L3cache%"PRId64"", obj->attr->cache.depth, id);
        size = obj->attr->cache.size / 1024;
#endif
    } else if (!hwloc_compare_types (obj->type, HWLOC_OBJ_CORE)) {
        type = xstrdup ("core");
        name = xasprintf ("%s%"PRId64"", type, id);
    } else if (!hwloc_compare_types (obj->type, HWLOC_OBJ_PU)) {
        type = xstrdup ("pu");
        name = xasprintf ("%s%"PRId64"", type, id);
    } else {
        /* that's all we're supporting for now... */
        goto ret;
    }

    uuid_generate (uuid);
    if (parent)
        path = xasprintf ("%s/%s", parent->path, name);
    else
        path = xasprintf ("/%s", name);

    resrc = resrc_new_resource (type, path, basename, name, signature, id, uuid,
                                size);
    if (resrc) {
        if (parent)
            parent_tree = parent->phys_tree;
        resrc->phys_tree = resrc_tree_new (parent_tree, resrc);

        if (obj->memory.local_memory) {
            char *mempath = xasprintf ("%s/memory", path);
            resrc_t *mem_resrc = NULL;
            /*
             * We have to elevate the meager memory attribute of a
             * NUMANode to a full-fledged Flux resrouce
             */
            size = obj->memory.local_memory / 1024;
            uuid_generate (uuid);
            mem_resrc = resrc_new_resource ("memory", mempath, "memory",
                                            "memory0", signature, 0, uuid, size);
            mem_resrc->phys_tree = resrc_tree_new (resrc->phys_tree, mem_resrc);
            free (mempath);
        }

        /* add twindow */
        if ((!strncmp (type, "node", 5)) || (!strncmp (type, "core", 5))) {
            JSON w = Jnew ();
            Jadd_int64 (w, "starttime", epochtime ());
            Jadd_int64 (w, "endtime", TIME_MAX);
            char *json_str = xstrdup (Jtostr (w));
            zhash_insert (resrc->twindow, "0", (void *) json_str);
            zhash_freefn (resrc->twindow, "0", free);
            Jput (w);
        }
    }
ret:
    free (basename);
    free (name);
    free (path);
    free (signature);
    free (type);

    return resrc;
}

resrc_t *resrc_generate_hwloc_resources (resrc_t *cluster_resrc,
                                         hwloc_topology_t topo, const char *sig,
                                         char **err_str)
{
    char *obj_ptr = NULL;
    char *str = NULL;
    hwloc_obj_t obj;
    resrc_t *parent = NULL;
    resrc_t *resrc = NULL;
    uint32_t depth;
    uint32_t hwloc_version;
    uint32_t level_size;
    uint32_t size;
    uint32_t topodepth;
    zhash_t *resrc_objs = zhash_new ();

    if (!cluster_resrc) {
        str = xasprintf ("%s: cluster_resrc is null", __FUNCTION__);
        goto ret;
    }

    hwloc_version = hwloc_get_api_version();
    if ((hwloc_version >> 16) != (HWLOC_API_VERSION >> 16)) {
        str = xasprintf ("%s: Compiled for hwloc API 0x%x but running on library"
                         " API 0x%x", __FUNCTION__, HWLOC_API_VERSION,
                         hwloc_version);
        goto ret;
    }

    topodepth = hwloc_topology_get_depth (topo);
    parent = cluster_resrc;
    level_size = hwloc_get_nbobjs_by_depth (topo, 0);
    for (size = 0; size < level_size; size++) {
        obj = hwloc_get_obj_by_depth (topo, 0, size);
        if (!obj) {
            str = xasprintf ("%s: Failed to get hwloc obj at depth 0",
                             __FUNCTION__);
            goto ret;
        }
        resrc = resrc_new_from_hwloc_obj (obj, parent, sig);
        if (resrc) {
            obj_ptr = xasprintf ("%p", obj);
            zhash_insert (resrc_objs, obj_ptr, (void *) resrc);
            /* do not call the zhash_freefn() for the *resrc */
            free (obj_ptr);
        } else {
            str = xasprintf ("%s: Failed to create resrc from hwloc depth 0",
                             __FUNCTION__);
            goto ret;
        }
    }
    for (depth = 1; depth < topodepth; depth++) {
        level_size = hwloc_get_nbobjs_by_depth (topo, depth);
        for (size = 0; size < level_size; size++) {
            obj = hwloc_get_obj_by_depth (topo, depth, size);
            if (!obj) {
                str = xasprintf ("%s: Failed to get hwloc obj at depth %u",
                                 __FUNCTION__, depth);
                goto ret;
            }
            obj_ptr = xasprintf ("%p", obj->parent);
            parent = zhash_lookup (resrc_objs, obj_ptr);
            free (obj_ptr);
            if (!parent) {
                str = xasprintf ("%s: Failed to find parent of obj depth %u",
                                 __FUNCTION__, depth);
                goto ret;
            }
            resrc = resrc_new_from_hwloc_obj (obj, parent, sig);
            if (resrc) {
                obj_ptr = xasprintf ("%p", obj);
                zhash_insert (resrc_objs, obj_ptr, (void *) resrc);
                /* do not call the zhash_freefn() for the *resrc */
                free (obj_ptr);
            } else {
                str = xasprintf ("%s: Failed to create resrc from hwloc depth "
                                 "%u", __FUNCTION__, depth);
                goto ret;
            }
        }
    }
    resrc = cluster_resrc;
ret:
    zhash_destroy (&resrc_objs);
    if (str) {
        if (err_str)
            *err_str = str;
        else {
            fprintf (stderr, "%s\n", str);
            free (str);
        }
    }

    return resrc;
}

int resrc_to_json (json_object *o, resrc_t *resrc, int64_t jobid)
{
    char uuid[40];
    int rc = -1;
    if (resrc) {
        Jadd_str (o, "type", resrc_type (resrc));
        Jadd_str (o, "path", resrc_path (resrc));
        Jadd_str (o, "basename", resrc_basename (resrc));
        Jadd_str (o, "name", resrc_name (resrc));
        Jadd_int64 (o, "id", resrc_id (resrc));
        uuid_unparse (resrc->uuid, uuid);
        Jadd_str (o, "uuid", uuid);
        Jadd_int64 (o, "size", resrc_size (resrc));
        if (jobid > 0) {
            char *jobid_str;
            asprintf (&jobid_str, "%ld", jobid);
            char *entry = zhash_lookup (resrc->twindow, jobid_str);
            if (entry) {
                JSON j = Jfromstr (entry);
                int64_t starttime, endtime;
                Jget_int64 (j, "starttime", &starttime);
                Jget_int64 (j, "endtime", &endtime);
                Jadd_int64 (o, "starttime", starttime);
                Jadd_int64 (o, "endtime", endtime);
                Jput (j);
            }
            free (jobid_str);
        }

        rc = 0;
    }
    return rc;
}

char *resrc_to_string (resrc_t *resrc)
{
    char *buf;
    char uuid[40];
    char *property;
    char *tag;
    char *window_ptr;
    size_t *size_ptr;
    size_t len;
    FILE *ss;

    if (!resrc)
        return NULL;
    if (!(ss = open_memstream (&buf, &len)))
        return NULL;

    uuid_unparse (resrc->uuid, uuid);
    fprintf (ss, "resrc type: %s, path: %s, basename: %s, name: %s, digest: %s, "
             "id: %"PRId64", state: %s, "
             "uuid: %s, size: %"PRIu64", avail: %"PRIu64"",
             resrc->type, resrc->path, resrc->basename, resrc->name,
             resrc->digest, resrc->id, resrc_state (resrc),
             uuid, resrc->size, resrc->available);
    fprintf (ss, ", owner: %"PRId64", leasee: %"PRId64"%s%s%s",
             resrc->slinfo.owner, resrc->slinfo.leasee,
             (resrc->slinfo.asked) ? ", asked" : "",
             (resrc->slinfo.to_be_returned) ? ", to_be_returned" : "",
             (resrc->slinfo.returned) ? ", returned" : "");
    if (zhash_size (resrc->properties)) {
        fprintf (ss, ", properties:");
        property = zhash_first (resrc->properties);
        while (property) {
            fprintf (ss, " %s: %s", (char *)zhash_cursor (resrc->properties),
                    property);
            property = zhash_next (resrc->properties);
        }
    }
#if 0
    if (zhash_size (resrc->tags)) {
        fprintf (ss, ", tags:");
        tag = zhash_first (resrc->tags);
        while (tag) {
            fprintf (ss, ", %s", (char *)zhash_cursor (resrc->tags));
            tag = zhash_next (resrc->tags);
        }
    }
#endif
    if (zhash_size (resrc->twindow)) {
        fprintf (ss, ", twindows");
        window_ptr = zhash_first (resrc->twindow);
        while (window_ptr) {
            fprintf (ss, ", %s: %s",
                    (char *)zhash_cursor (resrc->twindow), window_ptr);
            window_ptr = zhash_next (resrc->twindow);
        }
    }
    if (zhash_size (resrc->allocs)) {
        fprintf (ss, ", resrc->allocs");
        size_ptr = zhash_first (resrc->allocs);
        while (size_ptr) {
            fprintf (ss, ", %s: %"PRIu64"",
                    (char *)zhash_cursor (resrc->allocs), *size_ptr);
            size_ptr = zhash_next (resrc->allocs);
        }
    }
    if (zhash_size (resrc->reservtns)) {
        fprintf (ss, ", reserved jobs");
        size_ptr = zhash_first (resrc->reservtns);
        while (size_ptr) {
            fprintf (ss, ", %s: %"PRIu64"",
                    (char *)zhash_cursor (resrc->reservtns), *size_ptr);
            size_ptr = zhash_next (resrc->reservtns);
        }
    }
    fclose (ss);
    return buf;
}

void resrc_print_resource (resrc_t *resrc)
{
    if (resrc) {
        char *buffer = resrc_to_string (resrc);
        printf ("%s\n", buffer);
        free (buffer);
    }
}

resrc_t *resrc_create_cluster (char *cluster)
{
    resrc_t *resrc = NULL;
    uuid_t uuid;
    char *path = xasprintf ("/%s", cluster);

    uuid_generate (uuid);
    resrc = resrc_new_resource ("cluster", path, cluster, cluster, NULL, -1,
                                uuid, 1);
    resrc->phys_tree = resrc_tree_new (NULL, resrc);
    free (path);
    return resrc;
}

/*
 * Finds if a resource request matches the specified resource over a period
 * defined by the start and end times.
 */
static bool resrc_walltime_match (resrc_t *resrc, resrc_reqst_t *request)
{
    bool rc = false;
    char *json_str_window = NULL;
    int64_t endtime = resrc_reqst_endtime (request);
    int64_t lendtime; // Resource lifetime end time
    int64_t starttime = resrc_reqst_starttime (request);
    size_t available = 0;

    printf ("inside walltime match for resource type : %s, starttime = %ld, endtime = %ld\n", resrc->type, starttime, endtime);

    json_str_window = zhash_lookup (resrc->twindow, "slack");
    if (!json_str_window) {
        printf ("no slack reservation present, fetching lifetime as 0\n");
        json_str_window = zhash_lookup (resrc->twindow, "0");
    } else {
        printf ("resource has slack reservation and taken as lifetime\n");
    }

    if (json_str_window) {
        JSON lt = Jfromstr (json_str_window);
        Jget_int64 (lt, "endtime", &lendtime);
        Jput (lt);
        printf ("Retrieved lendtime = %ld\n", lendtime);
        if (endtime > (lendtime - SLACK_BUFFER_TIME)) {
            printf ("entime > (lendtime - SLACK_BUFFER_TIME), return false (no match)\n");
            return false;
        }
    }

    /* find the minimum available resources during the requested time
     * range */
    available = resrc_available_during_range (resrc, starttime, endtime,
                                              resrc_reqst_exclusive (request));

    rc = (available >= resrc_reqst_reqrd_size (request));
    printf ("available (%zu) >= resrc_requested_size (%"PRId64") evals to %d\n",
             available, resrc_reqst_reqrd_size (request), rc);

    return rc;
}

bool resrc_match_resource (resrc_t *resrc, resrc_reqst_t *request,
                           bool available)
{
    bool rc = false;
    char *rproperty = NULL;     /* request property */
    char *rtag = NULL;          /* request tag */

    if (!strcmp (resrc->type, resrc_reqst_resrc (request)->type)) {
        if (zhash_size (resrc_reqst_resrc (request)->properties)) {
            if (!zhash_size (resrc->properties)) {
                goto ret;
            }
            /* be sure the resource has all the requested properties */
            /* TODO: validate the value of each property */
            zhash_first (resrc_reqst_resrc (request)->properties);
            do {
                rproperty = (char *)zhash_cursor (
                    resrc_reqst_resrc (request)->properties);
                if (!zhash_lookup (resrc->properties, rproperty))
                    goto ret;
            } while (zhash_next (resrc_reqst_resrc (request)->properties));
        }

        if (zhash_size (resrc_reqst_resrc (request)->tags)) {
            if (!zhash_size (resrc->tags)) {
                goto ret;
            }
            /* be sure the resource has all the requested tags */
            zhash_first (resrc_reqst_resrc (request)->tags);
            do {
                rtag = (char *)zhash_cursor (resrc_reqst_resrc (request)->tags);
                if (!zhash_lookup (resrc->tags, rtag))
                    goto ret;
            } while (zhash_next (resrc_reqst_resrc (request)->tags));
        }

        if (available) {
            /*
             * We use the request's start time to determine whether to
             * search for available resources now or in the future.
             * We save this for last because the time search will be
             * expensive.
             */
             if (resrc_reqst_starttime (request) >= 0) {
                if ((resrc->slinfo.to_be_returned != 1) && (resrc->slinfo.returned != 1)) {
                    rc = resrc_walltime_match (resrc, request);
                } else {
                    printf ("slinfo.to_be_returned (%d) == 1 or slinfo.returned (%d) == 1\n",
                             resrc->slinfo.to_be_returned, resrc->slinfo.returned);
                    rc = false;
                }
             } else {
                rc = (resrc_reqst_reqrd_size (request) <= resrc->available);
                if (rc && resrc_reqst_exclusive (request)) {
                    rc = !zhash_size (resrc->allocs) &&
                        !zhash_size (resrc->reservtns);
                }
            }
        } else {
            //printf ("resrc_match_resource: not checking for availability\n");
            rc = true;
        }
    }

ret:
    fflush(0);
    return rc;
}

void resrc_stage_resrc (resrc_t *resrc, size_t size)
{
    if (resrc)
        resrc->staged = size;
}

/*
 * Allocate the staged size of a resource to the specified job_id and
 * change its state to allocated.
 */
static int resrc_allocate_resource_now (resrc_t *resrc, int64_t job_id)
{
    char *id_ptr = NULL;
    size_t *size_ptr;
    int rc = -1;

    if (resrc && job_id) {
        printf ("Entering %s: allocating %"PRId64"\n", __FUNCTION__, job_id);
        if (resrc->staged > resrc->available) {
            fprintf (stderr, "%s: ERR - staged too many resources, skipping allocation", __FUNCTION__);
            goto ret;
        }

        id_ptr = xasprintf ("%"PRId64"", job_id);
        size_ptr = xzmalloc (sizeof (size_t));
        *size_ptr = resrc->staged;
        zhash_insert (resrc->allocs, id_ptr, size_ptr);
        zhash_freefn (resrc->allocs, id_ptr, free);
        resrc->available -= resrc->staged;
        resrc->staged = 0;
        resrc->state = RESOURCE_ALLOCATED;

       if (!(strcmp (resrc->type, "core"))) {
            if (resrc->slinfo.owner == 0)
                resrc->slinfo.owner = job_id;
            resrc->slinfo.leasee = job_id;
        }

        rc = 0;
        free (id_ptr);
    }
ret:
    return rc;
}


/*
 * Allocate the staged size of a resource to the specified job_id and
 * change its state to allocated.
 */
static int resrc_allocate_resource_in_time (resrc_t *resrc, int64_t job_id,
                                            int64_t starttime, int64_t endtime)
{
    JSON j;
    char *id_ptr = NULL;
    char *json_str = NULL;
    int rc = -1;
    size_t *size_ptr;
    size_t available;

    if (resrc && job_id) {
        printf ("Entering %s: allocating %"PRId64" from %"PRId64" to %"PRId64"\n", __FUNCTION__, job_id, starttime, endtime);
        /* Don't bother going through the exclusivity checks.  We will
         * save cycles and assume the selected resources are
         * exclusively available if that was the criteria of the
         * search. */
        available = resrc_available_during_range (resrc, starttime, endtime,
                                                  false);
        if (resrc->staged > available) {
            fprintf (stderr, "%s: ERR - staged too many resources, skipping allocation", __FUNCTION__);
            goto ret;
        }

        id_ptr = xasprintf ("%"PRId64"", job_id);
        size_ptr = xzmalloc (sizeof (size_t));
        *size_ptr = resrc->staged;
        zhash_insert (resrc->allocs, id_ptr, size_ptr);
        zhash_freefn (resrc->allocs, id_ptr, free);
        resrc->staged = 0;
        //resrc->available -= resrc->staged;
        resrc->state = RESOURCE_ALLOCATED;

        /* add walltime */
        j = Jnew ();
        Jadd_int64 (j, "starttime", starttime);
        Jadd_int64 (j, "endtime", endtime);
        json_str = xstrdup (Jtostr (j));
        if (zhash_lookup (resrc->twindow, id_ptr) != NULL) {
            fprintf (stderr,
                     "%s: ERR - entry in alloc table for job %"PRId64" already exists, "
                     "will update rather than insert, but this should never happen",
                     __FUNCTION__, job_id);
            zhash_update (resrc->twindow, id_ptr, (void *) json_str);
        } else {
            zhash_insert (resrc->twindow, id_ptr, (void *) json_str);
            zhash_freefn (resrc->twindow, id_ptr, free);
        }
        Jput (j);

        if (!(strcmp (resrc->type, "core"))) {
            if (resrc->slinfo.owner == 0)
                resrc->slinfo.owner = job_id;
            resrc->slinfo.leasee = job_id;
        }

        rc = 0;
        free (id_ptr);
    }
ret:
    return rc;
}

int resrc_allocate_resource_in_time_dynamic (resrc_t *resrc, int64_t job_id,
                                             int64_t starttime, int64_t endtime)
{
    JSON j;
    int rc = -1;
    char *id_ptr = NULL;
    char *json_str = NULL;
    size_t *size_ptr;
    size_t available;
    int64_t endtime_modified = 0;

    if (resrc && job_id) {
        printf ("Entering %s: allocating %"PRId64" from %"PRId64" to %"PRId64"\n", __FUNCTION__, job_id, starttime, endtime);
        json_str = zhash_lookup (resrc->twindow, "job_id");
        if (json_str) {
            // Entry already exists for this job. That means a slack
            // entry should be present. This happens when a parent is
            // currently leasing a resource from a child and is now
            // returning it to expand the child job
            // TODO: assert that the owner is the child and the leasee is the current instance
            // OPTIMIZE: will be faster to just check who the owner is
            char *json_str = zhash_lookup (resrc->twindow, "slack");
            if (!json_str) {
                fprintf (stderr, "FATAL ERROR: job id exists without a slack entry\n");
                goto ret;
            }
            zhash_delete (resrc->twindow, "slack");
        } else {
            // entry does not exist. Add entry
            id_ptr = xasprintf ("%"PRId64"", job_id);
            size_ptr = xzmalloc (sizeof (size_t));
            *size_ptr = resrc->staged;
            zhash_insert (resrc->allocs, id_ptr, size_ptr);
            zhash_freefn (resrc->allocs, id_ptr, free);

            json_str = zhash_lookup (resrc->twindow, "slack");
            if (json_str) {
                JSON e = Jfromstr (json_str);
                Jget_int64 (e, "endtime", &endtime_modified);
                /*
                 * TODO: what is this for?  is it to make sure the
                 * child returns the slack resource in time to return
                 * to the parent?
                 */
                endtime_modified -= 10;
                Jput (e);
            } else {
                endtime_modified = endtime;
            }

            j = Jnew ();
            Jadd_int64 (j, "starttime", starttime);
            Jadd_int64 (j, "endtime", endtime_modified);
            json_str = xstrdup (Jtostr (j));
            zhash_insert (resrc->twindow, id_ptr, (void *) json_str);
            zhash_freefn (resrc->twindow, id_ptr, free);
            Jput (j);
            free(id_ptr);

            // TODO: this only valid is the current instance is the
            // owner (see above TODO)
            if (!(strcmp (resrc->type, "core"))) {
                resrc->slinfo.leasee = job_id;
            }
        }


        resrc->staged = 0;
        //resrc->available -= resrc->staged;
        resrc->state = RESOURCE_ALLOCATED;
        rc = 0;
    }

ret:
    return rc;
}


int resrc_allocate_resource (resrc_t *resrc, int64_t job_id, int64_t starttime,
                             int64_t endtime)
{
    int rc;

    if (starttime < 0)
        rc = resrc_allocate_resource_now (resrc, job_id);
    else
        rc = resrc_allocate_resource_in_time (resrc, job_id, starttime, endtime);

    return rc;
}


/*
 * Just like resrc_allocate_resource_now() above, but for a reservation
 */
static int resrc_reserve_resource_now (resrc_t *resrc, int64_t job_id)
{
    char *id_ptr = NULL;
    size_t *size_ptr;
    int rc = -1;

    if (resrc && job_id) {
        if (resrc->staged > resrc->available) {
            printf ("%s: ERR - staged too many resources, skipping reservation", __FUNCTION__);
            goto ret;
        }

        id_ptr = xasprintf ("%"PRId64"", job_id);
        size_ptr = xzmalloc (sizeof (size_t));
        *size_ptr = resrc->staged;
        zhash_insert (resrc->reservtns, id_ptr, size_ptr);
        zhash_freefn (resrc->reservtns, id_ptr, free);
        resrc->available -= resrc->staged;
        resrc->staged = 0;
        if (resrc->state != RESOURCE_ALLOCATED)
            resrc->state = RESOURCE_RESERVED;
        rc = 0;
        free (id_ptr);
    }
ret:
    return rc;
}

/*
 * Just like resrc_allocate_resource_in_time () above, but for a reservation
 */
static int resrc_reserve_resource_in_time (resrc_t *resrc, int64_t job_id,
                                           int64_t starttime, int64_t endtime)
{
    JSON j;
    char *id_ptr = NULL;
    char *json_str = NULL;
    int rc = -1;
    size_t *size_ptr;
    size_t available;

    if (resrc && job_id) {
        /* Don't bother going through the exclusivity checks.  We will
         * save cycles and assume the selected resources are
         * exclusively available if that was the criteria of the
         * search. */
        available = resrc_available_during_range (resrc, starttime, endtime,
                                                  false);
        if (resrc->staged > available) {
            fprintf (stderr, "%s: ERR - staged too many resources, skipping reservation", __FUNCTION__);
            goto ret;
        }

        id_ptr = xasprintf ("%"PRId64"", job_id);
        size_ptr = xzmalloc (sizeof (size_t));
        *size_ptr = resrc->staged;
        zhash_insert (resrc->reservtns, id_ptr, size_ptr);
        zhash_freefn (resrc->reservtns, id_ptr, free);
        resrc->staged = 0;

        /* add walltime */
        j = Jnew ();
        Jadd_int64 (j, "starttime", starttime);
        Jadd_int64 (j, "endtime", endtime);
        json_str = xstrdup (Jtostr (j));
        zhash_insert (resrc->twindow, id_ptr, (void *) json_str);
        zhash_freefn (resrc->twindow, id_ptr, free);
        Jput (j);

        rc = 0;
        free (id_ptr);
    }
ret:
    return rc;
}

int resrc_reserve_resource (resrc_t *resrc, int64_t job_id, int64_t starttime,
                            int64_t endtime)
{
    int rc;

    if (starttime < 0)
        rc =resrc_reserve_resource_now (resrc, job_id);
    else
        rc =resrc_reserve_resource_in_time (resrc, job_id, starttime, endtime);

    return rc;
}

/*
 * Remove a job allocation from a resource.  Supports both now and
 * time-based allocations.  We use a valid resrc->state value to
 * determine whether the allocation is now-based.  I.e, time-based
 * allocations will never be reflected in the resource state
 * value.
 */
int resrc_release_allocation (resrc_t *resrc, int64_t rel_job)
{
    char *id_ptr = NULL;
    size_t *size_ptr = NULL;
    int rc = 0;

    if (!resrc || !rel_job) {
        rc = -1;
        goto ret;
    }

    id_ptr = xasprintf ("%"PRId64"", rel_job);
    size_ptr = zhash_lookup (resrc->allocs, id_ptr);
    if (size_ptr) {
        if (resrc->state == RESOURCE_ALLOCATED) {
            //resrc->available += *size_ptr;
        }
        zhash_delete (resrc->twindow, id_ptr);
        zhash_delete (resrc->allocs, id_ptr);
        if ((resrc->state != RESOURCE_INVALID) && !zhash_size (resrc->allocs)) {
            if (zhash_size (resrc->reservtns))
                resrc->state = RESOURCE_RESERVED;
            else
                resrc->state = RESOURCE_IDLE;
        }
    }

    if (! (strcmp (resrc->type, "core"))) {
        // printf ("============================== SETTING THIS RESOURCE BACK TO IDLE =================================, path = %s\n", resrc->path);
        if (resrc->slinfo.owner == rel_job) {
            resrc->slinfo.owner = 0;
            // TODO: this should have already been happening elsewhere
            resrc->slinfo.to_be_returned = 0;
            resrc->slinfo.returned = 0;
        }
        resrc->slinfo.leasee = 0;

        char *slackstr = zhash_lookup (resrc->twindow, "slack");
        if (slackstr) {
            JSON js = Jfromstr (slackstr);
            int64_t jobid = 0;
            if (!Jget_int64 (js, "jobid", &jobid)) {
                fprintf (stderr, "FATAL ERROR: slack reservation present, but couldn't get jobid");
                goto ret;
            }
            if (jobid == rel_job) {
                zhash_delete (resrc->twindow, "slack");
                free (slackstr);
            }
        }

        char *slacksubstr = zhash_lookup (resrc->twindow, "slacksub");
        if (slacksubstr) {
            resrc->state = RESOURCE_SLACKSUB;
        }
    }

    free (id_ptr);
ret:
    return rc;
}

// TODO: add flux logging to this function
int resrc_add_resources_from_json (flux_t h, resrc_t *resrc, zhash_t *hash_table, JSON o, int64_t owner)
{
    int rc = 0;

    if (!resrc)
        return rc;

    resrc_tree_list_t *deserialized_tree_list = resrc_tree_list_deserialize (o);

    resrc_tree_t *resrc_tree = NULL;
    for (resrc_tree = resrc_tree_list_first (deserialized_tree_list);
         resrc_tree;
         resrc_tree = resrc_tree_list_next (deserialized_tree_list))
        {
            rc = resrc_tree_add_child_special (h, resrc_phys_tree (resrc), resrc_tree, hash_table, owner);
            if (rc < 0) {
                return rc;
            }
        }

    return rc;
}


int resrc_mark_resource_slack (resrc_t *resrc, int64_t jobid, int64_t endtime)
{
    int rc = -1;
    int64_t time_now = epochtime ();

    if (!resrc) {
        return rc;
    }
    // printf ("Setting resource as SLACK\n");

    JSON j = Jnew ();
    Jadd_int64 (j, "starttime", time_now);
    Jadd_int64 (j, "endtime", endtime);
    Jadd_int64 (j, "jobid", jobid);
    char *json_str = xstrdup (Jtostr (j));
    zhash_insert (resrc->twindow, "slack", (void *) json_str);
    Jput (j);

    resrc->state = RESOURCE_IDLE;
    resrc->slinfo.leasee = 0;

    rc = 0;
    return rc;
}

int64_t resrc_find_slack_endtime (flux_t h, resrc_t *resrc)
{
    int64_t endtime = -1;
    int64_t tmp_endtime = 0;
    int64_t tmp_starttime = 0;
    int64_t time_now = epochtime ();
    JSON j = NULL;

    if (!resrc)
        return -1;

    char *jstr = zhash_lookup (resrc->twindow, "slack");
    if (jstr) {
        j = Jfromstr (jstr);
        if (!(Jget_int64 (j, "endtime", &tmp_endtime))) {
            flux_log (h, LOG_ERR, "%s: endtime key not found in slack twindow, slack-twindow-json: %s", __FUNCTION__, jstr);
            goto ret;
        }
        endtime = tmp_endtime;
        flux_log (h, LOG_DEBUG, "%s: endtime key was found in slack twindow: %"PRId64"", __FUNCTION__, endtime);
    } else {
        jstr = zhash_lookup (resrc->twindow, "0");
        if (!jstr) {
            printf ("resrc lifetime (aka key = 0) not found in twindow\n");
            goto ret;
        }
        j = Jfromstr (jstr);
        if (!(Jget_int64 (j, "endtime", &tmp_endtime))) {
            flux_log (h, LOG_ERR, "%s: endtime key not found in 0 twindow, 0-twindow-json: %s", __FUNCTION__, jstr);
            goto ret;
        }
        Jput (j);
        flux_log (h, LOG_DEBUG, "%s: endtime key was found in 0 twindow: %"PRId64"", __FUNCTION__, tmp_endtime);
        jstr = zhash_first (resrc->twindow);
        while (jstr) {
            j = Jfromstr (jstr);
            if (!(Jget_int64 (j, "starttime", &tmp_starttime))) {
                flux_log (h, LOG_ERR, "%s: starttime key not found in a twindow, twindow-json: %s", __FUNCTION__, jstr);
                goto next;
            }
            if ((tmp_starttime > time_now) && (tmp_starttime < tmp_endtime)) {
                tmp_endtime = tmp_starttime;
                flux_log (h, LOG_DEBUG, "%s: starttime key was found in 0 twindow with time < endtime: %"PRId64"", __FUNCTION__, tmp_endtime);
            }
next:
            Jput (j);
            jstr = zhash_next (resrc->twindow);
        }
        endtime = tmp_endtime;
    }

ret:
    if (j)
        Jput (j);
    return endtime;
}

/*
 * Remove all reservations of a resource.  Supports both now and
 * time-based reservations.  We use a valid resrc->state value to
 * determine whether the reservation is now-based.  I.e, time-based
 * reservations will never be reflected in the resource state
 * value.
 */
int resrc_release_all_reservations (resrc_t *resrc)
{
    char *id_ptr = NULL;
    size_t *size_ptr = NULL;
    int rc = 0;

    if (!resrc) {
        rc = -1;
        goto ret;
    }

    if (zhash_size (resrc->reservtns)) {
        size_ptr = zhash_first (resrc->reservtns);
        while (size_ptr) {
            if ((resrc->state == RESOURCE_ALLOCATED) ||
                (resrc->state == RESOURCE_RESERVED)) {
                //resrc->available += *size_ptr;
            }
            id_ptr = (char *)zhash_cursor (resrc->reservtns);
            zhash_delete (resrc->twindow, id_ptr);
            size_ptr = zhash_next (resrc->reservtns);
        }
        zhash_destroy (&resrc->reservtns);
        resrc->reservtns = zhash_new ();
    }

    if (resrc->state != RESOURCE_INVALID) {
        if (zhash_size (resrc->allocs))
            resrc->state = RESOURCE_ALLOCATED;
        else
            resrc->state = RESOURCE_IDLE;
    }
    // TODO: verify that slack state stuff doesn't need to occur here

ret:
    return rc;
}

int resrc_mark_resources_slacksub_or_returned (zhash_t *hash_table, JSON o)
{
    int rc = -1;
    json_object_iter iter;

    printf ("----------------- Setting resources as slacksub or returned for json = %s\n", Jtostr (o));

    json_object_object_foreachC (o, iter) {
        resrc_t *resrc = zhash_lookup (hash_table, iter.key);
        if (!resrc) {
            printf ("------------- resource not found\n");
            continue;
        }
        if (strcmp(resrc->type, "core")) {
            printf ("------------- resource not core\n");
            continue;
        }
        if (resrc->slinfo.owner == -1) {
            printf ("------------- resource owner is parent\n");
            resrc->state = RESOURCE_IDLE;
            if (resrc->slinfo.to_be_returned)
                resrc->slinfo.to_be_returned = 0;
            resrc->slinfo.returned = 1;
        } else {
            printf ("------------- resource owner is self\n");
            JSON entry = Jnew ();
            Jadd_int64 (entry, "starttime", epochtime());
            const char *endtime_str = json_object_get_string (iter.val);
            int64_t endtime = strtol (endtime_str, NULL, 10);
            Jadd_int64 (entry, "endttime", endtime);
            char *x = xstrdup (Jtostr (entry));
            zhash_insert (resrc->twindow, "slacksub", x);
            Jput (entry);
            resrc->state = RESOURCE_SLACKSUB; //irrelevant
            resrc->slinfo.leasee = -1;
        }
    }

    fflush(0);
    rc = 0;
    return rc;
}

int resrc_mark_resources_to_be_returned (zhash_t *hash_table, JSON ro_array)
{
    int rc = -1;
    int len = 0;
    int i;

    if (!(Jget_ar_len (ro_array, &len)) || (len <= 0)) {
        return rc;
    }

    for (i = 0; i < len; i++) {
        const char *uuid;
        Jget_ar_str (ro_array, i, &uuid);
        resrc_t *resrc = zhash_lookup (hash_table, uuid);
        if (resrc)
            resrc->slinfo.to_be_returned = 1;
    }

    rc = 0;
    return rc;
}

int resrc_mark_resources_asked (zhash_t *hash_table, JSON ro_array)
{
    int rc = -1;
    int len = 0;
    int i;

    if (!(Jget_ar_len (ro_array, &len)) || (len <= 0)) {
        return rc;
    }

    for (i = 0; i < len; i++) {
        const char *uuid;
        Jget_ar_str (ro_array, i, &uuid);
        resrc_t *resrc = zhash_lookup (hash_table, uuid);
        if (resrc)
            resrc->slinfo.asked = 1;

    }

    rc = 0;
    return rc;
}

int resrc_mark_resources_returned (zhash_t *hash_table, JSON ro_array)
{
    int rc = -1;
    int len = 0;
    int i;

    if (!(Jget_ar_len (ro_array, &len)) || (len <= 0)) {
        return rc;
    }

    for (i = 0; i < len; i++) {
        const char *uuid;
        Jget_ar_str (ro_array, i, &uuid);
        resrc_t *resrc = zhash_lookup (hash_table, uuid);
        if (!resrc)
            continue;
        if (resrc->slinfo.to_be_returned)
            resrc->slinfo.to_be_returned = 0;
        if (resrc->slinfo.asked)
            resrc->slinfo.asked = 0;

        if (resrc->slinfo.owner > 0) {
            // Owned by a child job
            // remove slack entry in twindow
            zhash_delete (resrc->twindow, "slack");
            zhash_delete (resrc->twindow, "slacksub");
            resrc->state = RESOURCE_ALLOCATED;
        } else if (resrc->slinfo.owner < 0) {
            // Owned by parent
            // remove resrc from tree
            resrc->slinfo.returned = 1;
            resrc->state = RESOURCE_IDLE;
        } else {
            // Owned by me
            // should not reach
            zhash_delete (resrc->twindow, "slack");
            zhash_delete (resrc->twindow, "slacksub");
            resrc->state = RESOURCE_IDLE;
        }
    }

    rc = 0;
    return rc;
}

int resrc_mark_resource_return_received (resrc_t *resrc, int64_t jobid)
{
    int rc = -1;

    if (!resrc)
        goto ret;
    // If not a core, ignore
    if (strcmp (resrc->type, "core"))
        goto ret;

    if (jobid != -1) {
        char *jobid_str;
        asprintf (&jobid_str, "%ld", jobid);
        zhash_delete (resrc->twindow, jobid_str);
        zhash_delete (resrc->allocs, jobid_str);
    }

    if (resrc->slinfo.asked)
        resrc->slinfo.asked = 0;
    resrc->slinfo.leasee = 0;

    zhash_delete (resrc->twindow, "slacksub");
    resrc->state = RESOURCE_IDLE;
    rc = 0;
ret:
    return rc;
}

int resrc_retrieve_lease_information (zhash_t *hash_table, JSON ro_array, JSON out)
{
    int rc = -1;
    int len = 0;
    int i;

//    printf ("11111111111111111 Retrieving lease information for json :%s\n", Jtostr (ro_array));

    if (!ro_array) {
        return rc;
    }
    if (!(Jget_ar_len (ro_array, &len)) || (len <= 0)) {
        return rc;
    }

    for (i = 0; i < len; i++) {
        /* find leasee */
        const char *uuid;
        Jget_ar_str (ro_array, i, &uuid);
        resrc_t *resrc = zhash_lookup (hash_table, uuid);
        if (!resrc) {
            fprintf (stderr, "FATAL ERROR: Coulnd't find resource\n");
            return rc;
        }
        char *jobid_str;
        asprintf (&jobid_str, "%ld", resrc->slinfo.leasee);

        JSON oar = NULL;
        if (!(Jget_obj (out, jobid_str, &oar))) {
            /* new entry needed */
            JSON new_ar = Jnew_ar ();
            Jadd_ar_str (new_ar, uuid);
            Jadd_obj (out, jobid_str, new_ar);
            Jput (new_ar);
        } else {
            /* modify existing entry */
            Jadd_ar_str (oar, uuid);
        }
    }

    // printf ("1111111111111111 JSON out = %s\n", Jtostr (out));

    rc = 0;
    return rc;
}

int resrc_collect_own_resources_unasked (flux_t h, zhash_t *hash_table, JSON ro_array)
{
    int rc = -1;

    resrc_t *resrc = zhash_first (hash_table);
    while (resrc) {
        //flux_log (h, LOG_DEBUG, "%s: Considering resource %s of type %s and state %s", __FUNCTION__, resrc->path, resrc->type, resrc_state (resrc));
        if (!(!(strcmp (resrc->type, "core")))) {
            //flux_log (h, LOG_DEBUG, "%s: Skipping because it is not a core", __FUNCTION__);
            goto next;
        }
        if ((resrc->state != RESOURCE_ALLOCATED) && (resrc->state != RESOURCE_SLACKSUB)) {
            //flux_log (h, LOG_DEBUG, "%s: Skipping because it is neither allocated nor slacksubbed", __FUNCTION__);
            goto next;
        }
        if (resrc->slinfo.owner != 0) {
            //flux_log (h, LOG_DEBUG, "%s: Skipping because I am not the owner", __FUNCTION__);
            goto next;
        }
        if (resrc->slinfo.leasee == 0) {
            //flux_log (h, LOG_DEBUG, "%s: Skipping because I am the leasee", __FUNCTION__);
            goto next;
        }
        if (resrc->slinfo.asked != 0) {
            //flux_log (h, LOG_DEBUG, "%s: Skipping because it is already asked", __FUNCTION__);
            goto next;
        }

        //flux_log (h, LOG_DEBUG, "%s: adding resource to array", __FUNCTION__);
        char uuid[40] = {0};
        resrc_uuid (resrc, uuid);
        Jadd_ar_str (ro_array, uuid);
next:
        resrc = zhash_next (hash_table);
    }


    flux_log (h, LOG_DEBUG, "%s: final entry = %s", __FUNCTION__, Jtostr (ro_array));

    rc = 0;
    return rc;
}

bool resrc_check_return_ready (flux_t h, resrc_t *resrc, int64_t *jobid)
{
    int64_t endtime = 0;
    char *json_str_window = NULL;
    int64_t time_now = epochtime();
    JSON j = NULL;


    if (!resrc)
        return false;
    if (!(!(strcmp(resrc->type, "core")))) {
        //flux_log (h, LOG_DEBUG, "%s: because resource is not a core", __FUNCTION__);
        return false;
    }
    if (resrc->slinfo.owner == 0) {
        //flux_log (h, LOG_DEBUG, "%s: because I am the owner", __FUNCTION__);
        return false;
    }
    if (resrc->state != RESOURCE_IDLE) {
        //flux_log (h, LOG_DEBUG, "%s: because resource is not idle, path = %s and state = %s", __FUNCTION__, resrc->path, resrc_state (resrc));
        return false;
    }
    if (resrc->slinfo.returned == 1) {
        flux_log (h, LOG_DEBUG, "%s: because it has already been returned", __FUNCTION__);
        return false;
    }
    if (resrc->slinfo.leasee != 0) {
        flux_log (h, LOG_DEBUG, "%s: because I am not the leasee", __FUNCTION__);
        return false;
    }
    if (resrc->slinfo.to_be_returned == 1) {
        flux_log (h, LOG_DEBUG, "%s: Resource to be returned. Not checking endtime and returning now", __FUNCTION__);
        *jobid = resrc->slinfo.owner;
        return true;
    }

    if (resrc->slinfo.owner > 0) {
        flux_log (h, LOG_DEBUG, "%s: looking up slack entry since owner is %"PRId64"", __FUNCTION__, resrc->slinfo.owner);
        json_str_window = zhash_lookup (resrc->twindow, "slack");
    } else {
        flux_log (h, LOG_DEBUG, "%s: looking up 0 entry", __FUNCTION__);
        json_str_window = zhash_lookup (resrc->twindow, "0");
    }

    if (!json_str_window) {
        flux_log (h, LOG_DEBUG, "%s: not ready because slack or 0 entry could not be found", __FUNCTION__);
        return false;
    }

    j = Jfromstr (json_str_window);
    if (!(Jget_int64 (j, "endtime", &endtime))) {
        Jput (j);
        flux_log (h, LOG_DEBUG, "%s: not ready because an endtime could not be found", __FUNCTION__);
        return false;
    }

    //flux_log (h, LOG_DEBUG, "%s: timenow = %ld is not less than endtime - buffer : %ld - %ld = %ld", __FUNCTION__, time_now, endtime, (int64_t) SLACK_BUFFER_TIME, endtime - SLACK_BUFFER_TIME);

    if (time_now < (endtime - SLACK_BUFFER_TIME)) {
        flux_log (h, LOG_DEBUG, "%s: because timenow = %ld is not less than endtime - buffer : %ld - %ld = %ld", __FUNCTION__, time_now, endtime, (int64_t) SLACK_BUFFER_TIME, endtime - SLACK_BUFFER_TIME);
        Jput (j);
        return false;
    }
    Jput (j);

    *jobid = resrc->slinfo.owner;
    return true;
}

bool resrc_check_slacksub_ready (flux_t h, resrc_t *resrc, int64_t *endtime)
{
    if (!resrc) {
        flux_log (h, LOG_DEBUG, "%s: Resource not in slack because there is no resource", __FUNCTION__);
        return false;
    }
    if (!(!(strcmp(resrc_type (resrc), "core")))) {
        flux_log (h, LOG_DEBUG, "%s: Resource not in slack because its not a core", __FUNCTION__);
        return false;
    }
    if (resrc->state != RESOURCE_IDLE) {
        flux_log (h, LOG_DEBUG, "%s: Resource not in slack because its not IDLE", __FUNCTION__);
        return false;
    }
    if (resrc->slinfo.returned == 1) {
        flux_log (h, LOG_DEBUG, "%s: Resource not in slack because it has been returned", __FUNCTION__);
        return false;
    }
    if ((resrc->slinfo.to_be_returned == 1) && (resrc->slinfo.owner != -1)) {
        flux_log (h, LOG_DEBUG, "%s: Resource not in slack because it is to be returned and the owner is not parent", __FUNCTION__);
        return false;
    }

    if (resrc->slinfo.owner == -1) {
        *endtime = 0;
    } else {
        *endtime = resrc_find_slack_endtime (h, resrc);
        flux_log (h, LOG_DEBUG, "%s: slacksub: slack endtime computed = %ld", __FUNCTION__, *endtime);

        if (*endtime == -1)
            return false;
        if (epochtime() > *endtime - 2*SLACK_BUFFER_TIME)
            return false;

        *endtime = *endtime - SLACK_BUFFER_TIME;
    }

    flux_log (h, LOG_DEBUG, "%s: resource is slacksub_ready with endtime = %"PRId64":", __FUNCTION__, *endtime);
    resrc_flux_log (h, resrc);

    return true;
}

bool resrc_check_resource_destroy_ready (resrc_t *resrc)
{
    if (!resrc)
        return false;
    if (resrc->slinfo.owner == 0)
        return false;
   if (resrc->slinfo.returned != 1)
        return false;
    return true;
}


void resrc_flux_log (flux_t h, resrc_t *resrc)
{
    char *str = NULL;
    str = resrc_to_string (resrc);
    flux_log (h, LOG_DEBUG, "%s", str);
    free (str);

    // TODO: move this verification to a separate verification function
    if (zhash_size (resrc->allocs) > 1) {
        size_t *curr_alloc = NULL;
        size_t total_allocs = 0;
        for (curr_alloc = zhash_first (resrc->allocs);
             curr_alloc;
             curr_alloc = zhash_next (resrc->allocs)) {

            total_allocs += *curr_alloc;
        }
        if (zhash_lookup (resrc->twindow, "slack") != NULL) {
            total_allocs -= 1;
        }
        if (total_allocs > resrc->size) {
            flux_log (h, LOG_ERR,
                      "Simple allocation calculation (%zu) larger than "
                      "resources size (%zu)",
                      total_allocs, resrc->size);
        }
        size_t curr_avail = resrc_available_at_time(resrc, resrc_epochtime());
        size_t actual_allocs = resrc->size - curr_avail;
        if (total_allocs != actual_allocs) {
            flux_log (h, LOG_ERR,
                      "Simple allocation calculation (%zu) differs from "
                      "available_at_time (%"PRId64") calculation (%zu)",
                      total_allocs, resrc_epochtime(), actual_allocs);
        }
    }
}

void resrc_hash_flux_log (flux_t h, zhash_t *hash)
{
    resrc_t *curr_resrc = NULL;
    for (curr_resrc = zhash_first (hash);
         curr_resrc;
         curr_resrc = zhash_next (hash))
        {
            resrc_flux_log (h, curr_resrc);
        }
}

void resrc_idle_resource (resrc_t *resrc)
{
    resrc_set_state (resrc, RESOURCE_IDLE);
    zhash_destroy (&resrc->twindow);
    resrc->twindow = zhash_new ();

    if ((!strncmp (resrc->type, "node", 5)) || (!strncmp (resrc->type, "core", 5))) {
        JSON w = Jnew ();
        Jadd_int64 (w, "starttime", epochtime ());
        Jadd_int64 (w, "endtime", TIME_MAX);
        char *json_str = xstrdup (Jtostr (w));
        zhash_insert (resrc->twindow, "0", (void *) json_str);
        zhash_freefn (resrc->twindow, "0", free);
        Jput (w);
    }
}

/*
 * vi: ts=4 sw=4 expandtab
 */

