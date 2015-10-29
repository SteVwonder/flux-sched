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
#include <libxml/parser.h>
#include <libxml/tree.h>

#include "rdl.h"
#include "resrc.h"
#include "resrc_tree.h"
#include "src/common/libutil/jsonutil.h"
#include "src/common/libutil/xzmalloc.h"


#include <flux/core.h>
#include "src/common/libutil/log.h"

typedef struct resrc_slack_info {
    uint64_t owner;             /* 0 = self, -1 = parent, other = children */ 
    uint64_t leasee;            /* 0 = self, -1 = parent, other = children */
    int      asked;             /* 1 = yes, 0 = no */ 
    int      to_be_returned;    /* 1 = yes, 0 = no */
    int      returned;          /* 1 = yes, 0 = no */
} resrc_slinfo_t;

struct resrc {
    char *type;
    char *path;
    char *name;
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

size_t resrc_available_during_range (resrc_t *resrc,
                                     int64_t range_starttime,
                                     int64_t range_endtime)
{
    if (range_starttime == range_endtime) {
        return resrc_available_at_time (resrc, range_starttime);
    }

    int64_t curr_starttime;
    int64_t curr_endtime;

    const char *id_ptr = NULL;
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
    id_ptr = (const char *) zlist_next (window_keys);
    while (id_ptr) {
        window_json_str = (const char*) zhash_lookup (resrc->twindow, id_ptr);
        window_json = Jfromstr (window_json_str);
        if (window_json == NULL) {
            return 0;
        }
        Jget_int64 (window_json, "starttime", &curr_starttime);
        Jget_int64 (window_json, "endtime", &curr_endtime);

        // Does input range intersect with window?
        if ( !((curr_starttime < range_starttime &&
                curr_endtime < range_starttime) ||
               (curr_starttime > range_endtime &&
                curr_endtime > range_endtime)) ) {

            alloc_ptr = (size_t*)zhash_lookup (resrc->allocs, id_ptr);
            reservtn_ptr = (size_t*)zhash_lookup (resrc->reservtns, id_ptr);
            if (alloc_ptr || reservtn_ptr) {
                // Add the window key and insert JSON obj into the
                // "matching windows" list
                Jadd_str (window_json, "job_id", id_ptr);
                zlist_append (matching_windows, window_json);
                zlist_freefn (matching_windows, window_json, myJput, true);
            } else
                Jput (window_json);
        } else
            Jput (window_json);

        id_ptr = zlist_next (window_keys);
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
            (curr_starttime < curr_endtime)) {
            // New range is starting, get its size and subtract it
            // from current available
            Jget_str (curr_start_window, "job_id", &id_ptr);
            size_ptr = (size_t*)zhash_lookup (resrc->allocs, id_ptr);
            if (size_ptr)
                curr_available -= *size_ptr;
            size_ptr = (size_t*)zhash_lookup (resrc->reservtns, id_ptr);
            if (size_ptr)
                curr_available -= *size_ptr;
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
            Jget_str (curr_end_window, "job_id", &id_ptr);
            size_ptr = (size_t*)zhash_lookup (resrc->allocs, id_ptr);
            if (size_ptr)
                curr_available += *size_ptr;
            size_ptr = (size_t*)zhash_lookup (resrc->reservtns, id_ptr);
            if (size_ptr)
                curr_available += *size_ptr;
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
        min_available = (curr_available < min_available) ? curr_available :
            min_available;
    }

    // Cleanup
    zlist_destroy (&window_keys);
    zlist_destroy (&end_windows);
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

resrc_t *resrc_new_resource (const char *type, const char *path,
                             const char *name, int64_t id, uuid_t uuid,
                             size_t size)
{
    resrc_t *resrc = xzmalloc (sizeof (resrc_t));
    if (resrc) {
        resrc->type = xstrdup (type);
        if (path)
            resrc->path = xstrdup (path);
        if (name)
            resrc->name = xstrdup (name);
        resrc->id = id;
        if (uuid)
            uuid_copy (resrc->uuid, uuid);
        resrc->size = size;
        resrc->available = size;
        resrc->staged = 0;
        resrc->state = RESOURCE_IDLE;
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
        new_resrc->name = xstrdup (resrc->name);
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
        if (resrc->twindow)
            zhash_destroy (&resrc->twindow);
        free (resrc);
    }
}

void resrc_resource_destroy_special (void *object, zhash_t *hash_table)
{
    resrc_t *resrc = (resrc_t *) object;
    zhash_delete (hash_table, resrc->uuid);
    resrc_resource_destroy (resrc);    
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
       
        // TODO: Review this 
        /* add twindow */
        if (Jget_int64 (o, "walltime", &jduration)) {
            JSON w = Jnew ();
            Jadd_int64 (w, "walltime", jduration);
            zhash_insert (resrc->twindow, "0", (void *)Jtostr (w));
            Jput (w);
        } else if ((!strncmp (type, "node", 5)) ||
                   (!strncmp (type, "core", 5))) {
            JSON j = Jnew ();
            Jadd_int64 (j, "start", epochtime ());
            Jadd_int64 (j, "end", TIME_MAX);
            zhash_insert (resrc->twindow, "0", (void *)Jtostr (j));
            Jput (j);
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

static resrc_t *resrc_new_from_xml (xmlNodePtr nodePtr, resrc_t *parent)
{
    char *name = NULL;
    char *path = NULL;
    char *type = NULL;
    int64_t id = 0;
    resrc_t *resrc = NULL;
    resrc_tree_t *parent_tree = NULL;
    size_t size = 1;
    uuid_t uuid;
    xmlChar *prop;
    xmlNodePtr cur;

    prop = xmlGetProp (nodePtr, (const xmlChar*) "type");
    if (prop) {
        type = xstrdup ((char *) prop);
        xmlFree (prop);
        lowercase (type);
    }
    if (!strcmp (type, "machine")) {
        char *c;
        free (type);
        type = xstrdup ("node");

        cur = nodePtr->xmlChildrenNode;
        while (cur != NULL) {
            prop = xmlGetProp (cur, (const xmlChar *) "name");
            if (prop && !xmlStrcmp (prop, (const xmlChar*) "HostName")) {
                xmlFree (prop);
                prop = xmlGetProp (cur, (const xmlChar *) "value");
                if (prop) {
                    name = xstrdup ((char *) prop);
                    xmlFree (prop);
                }
                break;
            }
            xmlFree (prop);
            cur = cur->next;
        }
        if (name) {
            /*
             * Break apart the hostname to fit the Flux resource model:
             * generic name + id
             */
            for (c = name; *c; c++) {
                if (isdigit(*c)) {
                    id = strtol (c, NULL, 10);
                    *c = '\0';
                    break;
                }
            }
        } else {
            goto ret;
        }
    } else if (!strcmp (type, "numanode")) {
        if (strcmp (parent->type, "numanode")) {
            name = xstrdup (type);
            prop = xmlGetProp (nodePtr, (const xmlChar *) "os_index");
            if (prop) {
                id = strtol ((char *) prop, NULL, 10);
                xmlFree (prop);
            }
        } else {
            /*
             * We have to elevate the meager memory attribute of a
             * NUMANode to a full-fledged Flux resrouce
             */
            free (type);
            name = xstrdup ("memory");
            type = xstrdup ("memory");
            prop = xmlGetProp (nodePtr, (const xmlChar *) "local_memory");
            if (prop) {
                size = strtol ((char *) prop, NULL, 10);
                size /= 1024;
                xmlFree (prop);
            }
        }
    } else if (!strcmp (type, "socket") || !strcmp (type, "core") ||
               !strcmp (type, "pu")) {
        name = xstrdup (type);
        prop =  xmlGetProp (nodePtr, (const xmlChar *) "os_index");
        if (prop) {
            id = strtol ((char *) prop, NULL, 10);
            xmlFree (prop);
        }
    } else {
        /* that's all we're supporting for now... */
        goto ret;
    }

    uuid_generate (uuid);
    if (parent)
        path = xasprintf ("%s/%s%"PRIu64"", parent->path, name, id);
    else
        path = xasprintf ("/%s%"PRIu64"", name, id);

    resrc = resrc_new_resource (type, path, name, id, uuid, size);
    if (resrc) {
        if (parent)
            parent_tree = parent->phys_tree;
        resrc->phys_tree = resrc_tree_new (parent_tree, resrc);

        if (!strcmp (type, "numanode") && strcmp (parent->type, "numanode")) {
            /*
             * create the memory resource so that it is never a parent
             * of the NUMANode's children
             */
            resrc_new_from_xml (nodePtr, resrc);
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
    free (name);
    free (path);
    free (type);

    return resrc;
}

static resrc_t *resrc_add_xml_resource (resrc_t *parent, xmlNodePtr nodePtr)
{
    resrc_t *resrc = NULL;
    resrc_t *retres = NULL;
    resrc_t *surrogate = NULL;
    xmlNode *nodeItr = NULL;

    for (nodeItr = nodePtr; nodeItr; nodeItr = nodeItr->next) {
        if (nodeItr->type == XML_ELEMENT_NODE &&
            !xmlStrcmp (nodeItr->name, (const xmlChar *)"object")) {
            resrc = resrc_new_from_xml (nodeItr, parent);
            if (!retres && resrc)
                retres = resrc;
        }
        if (nodeItr->xmlChildrenNode) {
            if (resrc)
                surrogate = resrc;
            else
                surrogate = parent;
            resrc = resrc_add_xml_resource (surrogate, nodeItr->xmlChildrenNode);
            if (!retres && resrc)
                retres = resrc;
        }
    }

    return retres;
}

resrc_t *resrc_generate_xml_resources (resrc_t *cluster_resrc, const char *buf,
                                       size_t length)
{
    resrc_t *resrc = NULL;
    xmlDocPtr doc; /* the resulting document tree */
    xmlNodePtr rootElem;

    if (!cluster_resrc)
        goto ret;
    doc = xmlReadMemory (buf, length, "noname.xml", NULL, 0);
    if (!doc)
        goto ret;

    rootElem = xmlDocGetRootElement (doc);

    if (rootElem)
        resrc = resrc_add_xml_resource (cluster_resrc, rootElem);

    xmlFreeDoc (doc);
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
        //TODO: Review this
        char *value = (char *) zhash_lookup (resrc->twindow, "slack");
        if (value)
            Jadd_str (o, "slack", value);

        rc = 0;
    }
    return rc;
}

void resrc_print_resource (resrc_t *resrc)
{
    char uuid[40];
    char *property;
    char *tag;
    size_t *size_ptr;

    if (resrc) {
        uuid_unparse (resrc->uuid, uuid);
        printf ("resrc type: %s, path: %s, name: %s, id: %"PRId64", state: %s, "
                "uuid: %s, size: %"PRIu64", avail: %"PRIu64"",
                resrc->type, resrc->path, resrc->name, resrc->id,
                resrc_state (resrc), uuid, resrc->size, resrc->available);
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

resrc_t *resrc_create_cluster (char *cluster)
{
    resrc_t *resrc = NULL;
    uuid_t uuid;
    char *path = xasprintf ("/%s", cluster);

    uuid_generate (uuid);
    resrc = resrc_new_resource ("cluster", path, cluster, 0, uuid, 1);
    resrc->phys_tree = resrc_tree_new (NULL, resrc);
    free (path);
    return resrc;
}

/*
 * Finds if a resrc_t *sample matches with resrc_t *resrc over a period
 * defined by the start and end times.
 */
static bool resrc_walltime_match (resrc_t *resrc, resrc_t *sample,
                                  int64_t starttime, int64_t endtime)
{
    bool rc = false;
    char *json_str_window = NULL;
    int64_t lendtime; // Resource lifetime end time

    /* If request endtime is greater than the lifetime of the
       resource, then return false */
    json_str_window = zhash_lookup (resrc->twindow, "0");
    if (json_str_window) {
        JSON lt = Jfromstr (json_str_window);
        Jget_int64 (lt, "endtime", &lendtime);
        Jput (lt);
        if (endtime > (lendtime - 10)) {
            return false;
        }

    /* find if it sample fits in time range */
    size_t available = resrc_available_during_range (resrc, starttime,
                                                     endtime);
    rc = (available >= sample->size);

    return rc;
}

/*
 * Ensure all the slack state information matches
 */
bool resrc_scons_match (resrc_t *resrc)
{
    if (resrc->slinfo.to_be_returned)
        return false;
    if (resrc->slinfo.returned)
        return false;
    
    return true;   
}

bool resrc_match_resource (resrc_t *resrc, resrc_t *sample, bool available,
                           int64_t starttime, int64_t endtime)
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

        if (available) {
            /*
             * We use the request's start time to determine whether to
             * search for available resources now or in the future.
             * We save this for last because the time search will be
             * expensive.
             */
            if (starttime)
                rc = resrc_walltime_match (resrc, sample, starttime, endtime);
            else
                rc = (sample->size <= resrc->available);
        } else {
            rc = true;
        }
    }
ret:
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
        if (resrc->staged > resrc->available)
            goto ret;

        id_ptr = xasprintf("%"PRId64"", job_id);
        size_ptr = xzmalloc (sizeof (size_t));
        *size_ptr = resrc->staged;
        zhash_insert (resrc->allocs, id_ptr, size_ptr);
        zhash_freefn (resrc->allocs, id_ptr, free);
        resrc->available -= resrc->staged;
        resrc->staged = 0;
        resrc->state = RESOURCE_ALLOCATED;
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
        available = resrc_available_during_range (resrc, starttime, endtime);
        if (resrc->staged > available)
            goto ret;

        id_ptr = xasprintf("%"PRId64"", job_id);
        size_ptr = xzmalloc (sizeof (size_t));
        *size_ptr = resrc->staged;
        zhash_insert (resrc->allocs, id_ptr, size_ptr);
        zhash_freefn (resrc->allocs, id_ptr, free);
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

int resrc_allocate_resource (resrc_t *resrc, int64_t job_id, int64_t starttime,
                             int64_t endtime)
{
    int rc;

    if (starttime)
        rc = resrc_allocate_resource_in_time (resrc, job_id, starttime, endtime);
    else
        rc = resrc_allocate_resource_now (resrc, job_id);

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
        if (resrc->staged > resrc->available)
            goto ret;

        id_ptr = xasprintf("%"PRId64"", job_id);
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
        available = resrc_available_during_range (resrc, starttime, endtime);
        if (resrc->staged > available)
            goto ret;

        id_ptr = xasprintf("%"PRId64"", job_id);
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

    if (starttime)
        rc =resrc_reserve_resource_in_time (resrc, job_id, starttime, endtime);
    else
        rc =resrc_reserve_resource_now (resrc, job_id);

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
        if (resrc->state == RESOURCE_ALLOCATED)
            resrc->available += *size_ptr;
        else
            zhash_delete (resrc->twindow, id_ptr);

        zhash_delete (resrc->allocs, id_ptr);
        if (!zhash_size (resrc->allocs)) {
            if (zhash_size (resrc->reservtns))
                resrc->state = RESOURCE_RESERVED;
            else
                resrc->state = RESOURCE_IDLE;
        }
    }

    free (id_ptr);
ret:
    return rc;
}
    
int resrc_add_resources_from_json (resrc_t *resrc, zhash_t *hash_table, JSON o, bool new, int64_t owner)
{
    int rc = -1;
    resrc_tree_list_t *deserialized_tree_list = resrc_tree_list_deserialize (o);
    
    if (new)
        resrc->phys_tree = resrc_tree_new (NULL, resrc);

    resrc_tree_t *resrc_tree = resrc_tree_list_first (deserialized_tree_list);

    while (resrc_tree) {
        rc = resrc_tree_add_child_special (resrc_phys_tree (resrc), resrc_tree, hash_table, owner);
        if (rc < 0) {
            return rc;
        }
        resrc_tree = resrc_tree_list_next (deserialized_tree_list);
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

    JSON j = Jnew ();
    Jadd_int64 (j, "starttime", time_now);
    Jadd_int64 (j, "endtime", endtime);
    zhash_insert (resrc->twindow, "slack", (void *)Jtostr (j));
    Jput (j);

    resrc->state = RESOURCE_IDLE;

    rc = 0;
    return rc;
}

int64_t resrc_find_slack_endtime (resrc_t *resrc)
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
        if (!(Jget_int64 (j, "endtime", &tmp_endtime))) 
            goto ret; 
        if (time_now > (tmp_endtime - 2*SLACK_BUFFER_TIME))
            goto ret;
        endtime = tmp_endtime; 
    } else {
        jstr = zhash_lookup (resrc->twindow, "0");
        if (!jstr)
            goto ret;
        j = Jfromstr (jstr);
        if (!(Jget_int64 (j, "endtime", &tmp_endtime)))
            goto ret;
        jstr = zhash_first (resrc->twindow);
        while (jstr) {
            if (!(Jget_int64 (j, "starttime", &tmp_starttime)))
                continue;
            if ((tmp_starttime > time_now) && (tmp_starttime < endtime))
                tmp_endtime = tmp_starttime;
            zhash_next (resrc->twindow);
        } 
        if (time_now > (tmp_endtime - 2*SLACK_BUFFER_TIME))
            goto ret;
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
                (resrc->state == RESOURCE_RESERVED))
                resrc->available += *size_ptr;
            else {
                id_ptr = (char *)zhash_cursor (resrc->reservtns);
                zhash_delete (resrc->twindow, id_ptr);
            }
            size_ptr = zhash_next (resrc->reservtns);
        }
        zhash_destroy (&resrc->reservtns);
        resrc->reservtns = zhash_new ();
    }

    if (zhash_size (resrc->allocs))
        resrc->state = RESOURCE_ALLOCATED;
    else
        resrc->state = RESOURCE_IDLE;
ret:
    return rc;
}

int resrc_mark_resources_slacksub_or_returned (zhash_t *hash_table, JSON o)
{
    int rc = -1;
    json_object_iter iter;
    
    json_object_object_foreachC (o, iter) {    
        resrc_t *resrc = zhash_lookup (hash_table, iter.key);
        if (!resrc)
            continue;
        if (!(!(strncmp(resrc->type, "cores", 5)))) 
            continue;
        if (resrc->slinfo.owner == -1) {
            resrc->state = RESOURCE_IDLE;
            if (resrc->slinfo.to_be_returned)
                resrc->slinfo.to_be_returned = 0;
            resrc->slinfo.returned = 1;   
        } else {
            resrc->state = RESOURCE_SLACKSUB;
            resrc->slinfo.leasee = -1;
        }     
    } 

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
        if (!resrc) {
            return rc;
        }
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
        if (!resrc) {
            return rc;
        }
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
            // remove slack entry in twindow
            zhash_delete (resrc->twindow, "slack");
            resrc->state = RESOURCE_ALLOCATED;
        } else if (resrc->slinfo.owner < 0) {
            // remove resrc from tree
            resrc->state = RESOURCE_IDLE;
            resrc->slinfo.returned = 1;
        } else {
            // should not reach
            zhash_delete (resrc->twindow, "slack");
            resrc->state = RESOURCE_IDLE;
        }
    }

    rc = 0;
    return rc;
}

int resrc_mark_resource_return_received (resrc_t *resrc, int64_t jobid)
{
    // TODO: allocs and stuff
    int rc = -1;
    if (!resrc)
        goto ret;
    if (!(!(strncmp (resrc->type, "cores", 5))))
        goto ret;

    char *jobid_str;
    asprintf (&jobid_str, "%ld", jobid);
    zhash_delete (resrc->twindow, jobid_str);
    if (resrc->slinfo.asked)
        resrc->slinfo.asked = 0;
    resrc->slinfo.leasee = 0;
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
    
    if (!(Jget_ar_len (ro_array, &len)) || (len <= 0)) {
        return rc;
    }

    for (i = 0; i < len; i++) {
        /* find leasee */
        const char *uuid;
        Jget_ar_str (ro_array, i, &uuid);
        resrc_t *resrc = zhash_lookup (hash_table, uuid);
        if (!resrc) {
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

    rc = 0;
    return rc;
}

int resrc_collect_own_resources_unasked (zhash_t *hash_table, JSON ro_array)
{
    int rc = -1;

    resrc_t *resrc = zhash_first (hash_table);
    while (resrc) {
        if ((resrc->state != RESOURCE_ALLOCATED) || (resrc->state != RESOURCE_SLACKSUB))
            goto next;
        if (resrc->slinfo.owner != 0)
            goto next;
        if (resrc->slinfo.leasee == 0)
            goto next;
        if (resrc->slinfo.asked != 0)
            goto next;
        Jadd_ar_str (ro_array, (const char*) resrc->uuid);
next:
        resrc = zhash_next (hash_table);
    } 

    rc = 0;
    return rc;
}

bool resrc_check_return_ready (resrc_t *resrc, int64_t *jobid)
{
    int64_t endtime = 0;
    char *json_str_window = NULL;
    int64_t time_now = epochtime();
    JSON j = NULL;
    
    if (!resrc)
        return false;
    if (!(!(strncmp(resrc->type, "cores", 5))));
        return false;
    if (resrc->slinfo.owner == 0)
        return false;
    if (resrc->state != RESOURCE_IDLE)
        return false;
    if (resrc->slinfo.returned == 1)
        return false;
    if (resrc->slinfo.leasee != 0)
        return false;
    json_str_window = zhash_lookup (resrc->twindow, "slack");    
    if (!json_str_window)
        return false;
    j = Jfromstr (json_str_window);
    if (!(Jget_int64 (j, "endtime", &endtime))) {
        Jput (j);
        return false;
    }
    if (time_now < (endtime - SLACK_BUFFER_TIME)) {
        Jput (j);
        return false;
    }
    *jobid = resrc->slinfo.owner;
    Jput (j);
    return true;
}
/* TODO: include any previously added resource by parent as well 
 */
bool resrc_check_slacksub_ready (resrc_t *resrc, int64_t *endtime)
{
    if (!resrc)
        return false;
    if (!(!(strncmp(resrc_type (resrc), "cores", 5))))
        return false;
    if (resrc->state != RESOURCE_IDLE)
        return false;
    if (resrc->slinfo.returned == 1)
        return false;
    if (resrc->slinfo.to_be_returned == 1)
        if (resrc->slinfo.owner != -1) 
            return false;
    *endtime = resrc_find_slack_endtime(resrc);
    if (*endtime == -1)
        return false;
    
    return true;
}

bool resrc_check_resource_destroy_ready (resrc_t *resrc)
{
    if (!resrc)
        return false;
    if (resrc->slinfo.owner == 0)
        return false;
    if (resrc->state != RESOURCE_IDLE)
        return false;
    if (resrc->slinfo.returned != 1)
        return false;
    return true;
}
/*
 * vi: ts=4 sw=4 expandtab
 */

