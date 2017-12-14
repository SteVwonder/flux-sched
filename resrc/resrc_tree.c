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

#include "src/common/libutil/shortjansson.h"
#include "rdl.h"
#include "resrc_tree.h"
#include "resrc_api_internal.h"
#include "src/common/libutil/xzmalloc.h"

struct resrc_tree_list {
    zlistx_t *list;
};

struct resrc_tree {
    resrc_tree_t *parent;
    resrc_t *resrc;
    resrc_tree_list_t *children;
};

/***********************************************************************
 * Resource tree
 ***********************************************************************/

resrc_t *resrc_tree_resrc (resrc_tree_t *resrc_tree)
{
    if (resrc_tree)
        return resrc_tree->resrc;
    return NULL;
}

resrc_tree_t *resrc_tree_root (resrc_api_ctx_t *ctx)
{
    if (!ctx)
        return NULL;
    return (ctx->tree_root)? ctx->tree_root : NULL;
}

const char *resrc_tree_name (resrc_api_ctx_t *ctx)
{
    if (!ctx)
        return NULL;
    return (ctx->tree_name)? ctx->tree_name : NULL;
}

size_t resrc_tree_num_children (resrc_tree_t *resrc_tree)
{
    if (resrc_tree)
        return resrc_tree_list_size (resrc_tree->children);
    return 0;
}

resrc_tree_list_t *resrc_tree_children (resrc_tree_t *resrc_tree)
{
    if (resrc_tree)
        return resrc_tree->children;
    return NULL;
}

int resrc_tree_add_child (resrc_tree_t *parent, resrc_tree_t *child)
{
    int rc = -1;
    if (parent) {
        child->parent = parent;
        rc = resrc_tree_list_append (parent->children, child);
    }

    return rc;
}

resrc_tree_t *resrc_tree_new (resrc_tree_t *parent, resrc_t *resrc)
{
    resrc_tree_t *resrc_tree = xzmalloc (sizeof (resrc_tree_t));
    if (resrc_tree) {
        resrc_tree->parent = parent;
        resrc_tree->resrc = resrc;
        resrc_tree->children = resrc_tree_list_new ();
        if (parent)
            (void) resrc_tree_add_child (parent, resrc_tree);
    }

    return resrc_tree;
}

resrc_tree_t *resrc_tree_copy (resrc_tree_t *resrc_tree)
{
    resrc_tree_t *new_resrc_tree = xzmalloc (sizeof (resrc_tree_t));

    if (new_resrc_tree) {
        new_resrc_tree->parent = resrc_tree->parent;
        new_resrc_tree->resrc = resrc_tree->resrc;
        new_resrc_tree->children = resrc_tree_list_new ();
        new_resrc_tree->children->list = zlistx_dup (resrc_tree->children->list);
    }

    return new_resrc_tree;
}

void resrc_tree_destroy (resrc_api_ctx_t *ctx, resrc_tree_t *resrc_tree,
                         bool is_root, bool destroy_resrc)
{
    if (resrc_tree) {
        if (resrc_tree->parent)
            resrc_tree_list_remove (resrc_tree->parent->children, resrc_tree);
        if (resrc_tree->children) {
            resrc_tree_list_destroy (ctx, resrc_tree->children, destroy_resrc);
            resrc_tree->children = NULL;
        }

        if (destroy_resrc)
            resrc_resource_destroy (ctx, resrc_tree->resrc);
        if (is_root) {
            if (ctx->tree_name) {
                free (ctx->tree_name);
                ctx->tree_name = NULL;
            }
            if (resrc_tree == ctx->tree_root)
                ctx->tree_root = NULL;
        }
        free (resrc_tree);
    }
}

void resrc_tree_print (resrc_tree_t *resrc_tree)
{
    if (resrc_tree) {
        resrc_print_resource (resrc_tree->resrc);
        if (resrc_tree_num_children (resrc_tree)) {
            resrc_tree_t *child = resrc_tree_list_first (resrc_tree->children);
            while (child) {
                resrc_tree_print (child);
                child = resrc_tree_list_next (resrc_tree->children);
            }
        }
    }
}

static void _resrc_tree_to_string (resrc_tree_t *rt, FILE *ss, int indent_level)
{
    int i;
    if (rt) {
        for (i=0; i < indent_level; i++) {
            fputs("\t", ss);
        }
        fprintf(ss, "%s\n", resrc_to_string(resrc_tree_resrc(rt)));
        resrc_tree_t *child;
        for (child = resrc_tree_list_first (rt->children);
             child;
             child = resrc_tree_list_next (rt->children))
            {
                _resrc_tree_to_string(child, ss, indent_level+1);
            }
    }
}

char *resrc_tree_to_string (resrc_tree_t *rt)
{
    size_t len;
    char *buf;
    FILE *ss;

    if (!rt) {
        return NULL;
    }
    if (!(ss = open_memstream (&buf, &len))) {
        return NULL;
    }
    _resrc_tree_to_string(rt, ss, 0);
    fclose(ss);
    return buf;
}

int resrc_tree_serialize (json_t *o, resrc_tree_t *resrc_tree)
{
    int rc = -1;

    if (o && resrc_tree) {
        rc = resrc_to_json (o, resrc_tree->resrc);
        if (!rc && resrc_tree_num_children (resrc_tree)) {
            json_t *ja = Jnew_ar ();

            if (!(rc = resrc_tree_list_serialize (ja, resrc_tree->children)))
                json_object_set_new (o, "children", ja);
        }
    }
    return rc;
}

resrc_tree_t *resrc_tree_deserialize (resrc_api_ctx_t *ctx, json_t *o,
                                      resrc_tree_t *parent)
{
    json_t *ca = NULL;     /* array of child json objects */
    json_t *co = NULL;     /* child json object */
    resrc_t *resrc = NULL;
    resrc_tree_t *resrc_tree = NULL;

    resrc = resrc_new_from_json (ctx, o, NULL, false);
    if (resrc) {
        resrc_tree = resrc_tree_new (parent, resrc);

        if ((ca = Jobj_get (o, "children"))) {
            int i, nchildren = 0;

            if (Jget_ar_len (ca, &nchildren)) {
                for (i=0; i < nchildren; i++) {
                    Jget_ar_obj (ca, i, &co);
                    (void) resrc_tree_deserialize (ctx, co, resrc_tree);
                }
            }
        }
    }

    return resrc_tree;
}

int resrc_tree_allocate (resrc_tree_t *resrc_tree, int64_t job_id,
                         int64_t starttime, int64_t endtime)
{
    int rc = -1;
    if (resrc_tree) {
        rc = resrc_allocate_resource (resrc_tree->resrc, job_id,
                                      starttime, endtime);
        if (resrc_tree_num_children (resrc_tree))
            rc = resrc_tree_list_allocate (resrc_tree->children, job_id,
                                           starttime, endtime);
    }
    return rc;
}

int resrc_tree_reserve (resrc_tree_t *resrc_tree, int64_t job_id,
                        int64_t starttime, int64_t endtime)
{
    int rc = -1;
    if (resrc_tree) {
        rc = resrc_reserve_resource (resrc_tree->resrc, job_id,
                                     starttime, endtime);
        if (resrc_tree_num_children (resrc_tree))
            rc = resrc_tree_list_reserve (resrc_tree->children, job_id,
                                          starttime, endtime);
    }
    return rc;
}

int resrc_tree_release (resrc_tree_t *resrc_tree, int64_t job_id)
{
    int rc = -1;
    if (resrc_tree) {
        rc = resrc_release_allocation (resrc_tree->resrc, job_id);
        if (resrc_tree_num_children (resrc_tree))
            rc = resrc_tree_list_release (resrc_tree->children, job_id);
    }
    return rc;
}

int resrc_tree_release_all_reservations (resrc_tree_t *resrc_tree)
{
    int rc = -1;
    if (resrc_tree) {
        rc = resrc_release_all_reservations (resrc_tree->resrc);
        if (resrc_tree_num_children (resrc_tree))
            rc = resrc_tree_list_release_all_reservations (resrc_tree->children);
    }
    return rc;
}

void resrc_tree_unstage_resources (resrc_tree_t *resrc_tree)
{
    if (resrc_tree) {
        resrc_unstage_resrc (resrc_tree->resrc);
        if (resrc_tree_num_children (resrc_tree))
            resrc_tree_list_unstage_resources (resrc_tree->children);
    }
}


/***********************************************************************
 * Resource tree list
 ***********************************************************************/

resrc_tree_list_t *resrc_tree_list_new ()
{
    resrc_tree_list_t *new_list = xzmalloc (sizeof (resrc_tree_list_t));
    new_list->list = zlistx_new ();
    return new_list;
}

int resrc_tree_list_append (resrc_tree_list_t *rtl, resrc_tree_t *rt)
{
    if (rtl && rtl->list && rt) {
        if (zlistx_add_end (rtl->list, (void *) rt) == NULL) {
            return -1;
        } else {
            return 0;
        }
    }
    return -1;
}

resrc_tree_t *resrc_tree_list_first (resrc_tree_list_t *rtl)
{
    if (rtl && rtl->list)
        return zlistx_first (rtl->list);
    return NULL;
}

resrc_tree_t *resrc_tree_list_next (resrc_tree_list_t *rtl)
{
    if (rtl && rtl->list)
        return zlistx_next (rtl->list);
    return NULL;
}

resrc_tree_t *resrc_tree_list_last (resrc_tree_list_t *rtl)
{
    if (rtl && rtl->list)
        return zlistx_last (rtl->list);
    return NULL;
}

resrc_tree_t *resrc_tree_list_prev (resrc_tree_list_t *rtl)
{
    if (rtl && rtl->list)
        return zlistx_prev (rtl->list);
    return NULL;
}

size_t resrc_tree_list_size (resrc_tree_list_t *rtl)
{
    if (rtl && rtl->list)
        return zlistx_size (rtl->list);
    return 0;
}

void resrc_tree_list_remove (resrc_tree_list_t *rtl, resrc_tree_t *rt)
{
    void *handle = zlistx_find(rtl->list, rt);
    zlistx_delete (rtl->list, handle);
}

void resrc_tree_list_destroy (resrc_api_ctx_t *ctx,
                              resrc_tree_list_t *resrc_tree_list,
                              bool destroy_resrc)
{
    if (resrc_tree_list) {
        if (resrc_tree_list_size (resrc_tree_list)) {
            resrc_tree_t *child = resrc_tree_list_first (resrc_tree_list);
            while (child) {
                resrc_tree_destroy (ctx, child, false, destroy_resrc);
                child = resrc_tree_list_next (resrc_tree_list);
            }
        }
        if (resrc_tree_list->list) {
            zlistx_destroy (&(resrc_tree_list->list));
        }
        free (resrc_tree_list);
    }
}

int resrc_tree_list_serialize (json_t *o, resrc_tree_list_t *rtl)
{
    resrc_tree_t *rt;
    int rc = -1;

    if (o && rtl && rtl->list) {
        rc = 0;
        rt = resrc_tree_list_first (rtl);
        while (rt) {
            json_t *co = Jnew ();

            if ((rc = resrc_tree_serialize (co, rt)))
                break;
            json_array_append_new (o, co);
            rt = resrc_tree_list_next (rtl);
        }
    }

    return rc;
}

resrc_tree_list_t *resrc_tree_list_deserialize (resrc_api_ctx_t *ctx, json_t *o)
{
    json_t *ca = NULL;     /* array of child json objects */
    int i, nchildren = 0;
    resrc_tree_t *rt = NULL;
    resrc_tree_list_t *rtl = resrc_tree_list_new ();

    if (o && rtl) {
        if (Jget_ar_len (o, &nchildren)) {
            for (i=0; i < nchildren; i++) {
                Jget_ar_obj (o, i, &ca);
                rt = resrc_tree_deserialize (ctx, ca, NULL);
                if (!rt || resrc_tree_list_append (rtl, rt))
                    break;
            }
        }
    }

    return rtl;
}

int resrc_tree_list_allocate (resrc_tree_list_t *rtl, int64_t job_id,
                              int64_t starttime, int64_t endtime)
{
    resrc_tree_t *rt;
    int rc = -1;

    if (rtl) {
        rc = 0;
        rt = resrc_tree_list_first (rtl);
        while (!rc && rt) {
            rc = resrc_tree_allocate (rt, job_id, starttime, endtime);
            rt = resrc_tree_list_next (rtl);
        }
    }

    return rc;
}

int resrc_tree_list_reserve (resrc_tree_list_t *rtl, int64_t job_id,
                             int64_t starttime, int64_t endtime)
{
    resrc_tree_t *rt;
    int rc = -1;

    if (rtl) {
        rc = 0;
        rt = resrc_tree_list_first (rtl);
        while (!rc && rt) {
            rc = resrc_tree_reserve (rt, job_id, starttime, endtime);
            rt = resrc_tree_list_next (rtl);
        }
    }

    return rc;
}

int resrc_tree_list_release (resrc_tree_list_t *rtl, int64_t job_id)
{
    resrc_tree_t *rt;
    int rc = -1;

    if (rtl) {
        rc = 0;
        rt = resrc_tree_list_first (rtl);
        while (!rc && rt) {
            rc = resrc_tree_release (rt, job_id);
            rt = resrc_tree_list_next (rtl);
        }
    }

    return rc;
}

int resrc_tree_list_release_all_reservations (resrc_tree_list_t *rtl)
{
    resrc_tree_t *rt;
    int rc = -1;

    if (rtl) {
        rc = 0;
        rt = resrc_tree_list_first (rtl);
        while (!rc && rt) {
            rc = resrc_tree_release_all_reservations (rt);
            rt = resrc_tree_list_next (rtl);
        }
    }

    return rc;
}

void resrc_tree_list_unstage_resources (resrc_tree_list_t *rtl)
{
    resrc_tree_t *rt;

    if (rtl) {
        rt = resrc_tree_list_first (rtl);
        while (rt) {
            resrc_tree_unstage_resources (rt);
            rt = resrc_tree_list_next (rtl);
        }
    }
}

/*
 * vi: ts=4 sw=4 expandtab
 */

