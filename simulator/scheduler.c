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
#include <errno.h>
#include <json/json.h>
#include <time.h>
#include <inttypes.h>

#include "src/common/libutil/jsonutil.h"
#include "src/common/libutil/log.h"
#include "src/common/libutil/shortjson.h"
#include "rdl.h"
#include "scheduler.h"

const char* IDLETAG = "idle";

int send_rdl_update (flux_t h, struct rdl* rdl)
{
    JSON o = Jnew();

    Jadd_int64(o, "rdl_int", (int64_t) rdl);

	if (flux_event_send (h, o, "%s", "rdl.update") < 0){
		Jput(o);
		return -1;
	}

    Jput (o);
    return 0;
}

struct rdl *get_free_subset (struct rdl *rdl, const char *type)
{
	JSON tags = Jnew();
	Jadd_bool (tags, IDLETAG, true);
	JSON args = Jnew ();
	Jadd_obj (args, "tags", tags);
	Jadd_str (args, "type", type);
    struct rdl *frdl = rdl_find (rdl, args);
	Jput (args);
	Jput (tags);
	return frdl;
}

static int64_t count_free (struct resource *r, const char *type)
{
    int64_t curr_count = 0;
    JSON o = NULL;
	const char *curr_type = NULL;
    struct resource *child = NULL;

    if (r) {
        rdl_resource_iterator_reset(r);
        while ((child = rdl_resource_next_child (r))) {
            curr_count += count_free (child, type);
            rdl_resource_destroy (child);
        }
        rdl_resource_iterator_reset(r);

        o = rdl_resource_json (r);
        Jget_str (o, "type", &curr_type);
        if (strcmp (type, curr_type)) {
            curr_count++;
        }
        Jput (o);
    }

    return curr_count;
}

int64_t get_free_count (struct rdl *rdl, const char *uri, const char *type)
{
	struct resource *fr = NULL;
    int64_t count = 0;

	if ((fr = rdl_resource_get (rdl, uri)) == NULL) {
		return -1;
	}
    count = count_free (fr, type);
    rdl_resource_destroy(fr);

    return count;
}
