/* kvssrv.c - yet another key value store */

#define _GNU_SOURCE
#include <stdio.h>
#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <getopt.h>
#include <libgen.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/param.h>
#include <stdbool.h>
#include <sys/un.h>
#include <sys/socket.h>
#include <ctype.h>
#include <sys/time.h>
#include <zmq.h>
#include <czmq.h>
#include <json/json.h>

#include "zmsg.h"
#include "route.h"
#include "cmbd.h"
#include "plugin.h"
#include "util.h"
#include "log.h"

typedef struct {
    json_object *o;
    zlist_t *reqs;
} hobj_t;

typedef enum { OP_NAME, OP_STORE } optype_t;
typedef struct {
    optype_t optype;
    char *key;
    char *ref;
} op_t;

typedef struct {
    zhash_t *store;
    href_t rootdir;
    zlist_t *writeback;
} ctx_t;


static void kvs_load (plugin_ctx_t *p, zmsg_t **zmsg);
static void kvs_get (plugin_ctx_t *p, zmsg_t **zmsg);

static op_t *op_create (optype_t optype, char *key, char *ref)
{
    op_t *op = xzmalloc (sizeof (*op));

    op->optype = optype;
    op->key = key;
    op->ref = ref;

    return op;
}

static void op_destroy (op_t *op)
{
    if (op->key)
        free (op->key);
    if (op->ref)
        free (op->ref);
    free (op);
}

static bool op_match (op_t *op1, op_t *op2)
{
    bool match = false;

    if (op1->optype == op2->optype) {
        switch (op1->optype) {
            case OP_STORE:
                if (!strcmp (op1->ref, op2->ref))
                    match = true;
                break;
            case OP_NAME:
                if (!strcmp (op1->key, op2->key))
                    match = true;
                break;
        }
    }
    return match;
}

static void writeback_add (plugin_ctx_t *p, optype_t optype,
                           char *key, char *ref)
{
    ctx_t *ctx = p->ctx;
    op_t *op = op_create (OP_STORE, NULL, xstrdup (ref));

    if (zlist_append (ctx->writeback, op) < 0)
        oom ();
}


static void writeback_del (plugin_ctx_t *p, optype_t optype,
                           char *key, char *ref)
{
    ctx_t *ctx = p->ctx;
    op_t mop = { .optype = optype, .key = key, .ref = ref };
    op_t *op = zlist_first (ctx->writeback);

    while (op) {
        if (op_match (op, &mop))
            break;
        op = zlist_next (ctx->writeback);
    }
    if (op) {
        zlist_remove (ctx->writeback, op);
        op_destroy (op);
    }
}

static hobj_t *hobj_create (json_object *o)
{
    hobj_t *hp = xzmalloc (sizeof (*hp));

    hp->o = o;
    if (!(hp->reqs = zlist_new ()))
        oom ();

    return hp;
}

static void hobj_destroy (hobj_t *hp)
{
    if (hp->o)
        json_object_put (hp->o);
    assert (zlist_size (hp->reqs) == 0);
    zlist_destroy (&hp->reqs);
    free (hp);
}

static void load_request_send (plugin_ctx_t *p, const href_t ref)
{
    json_object *o = util_json_object_new_object ();

    json_object_object_add (o, ref, NULL);
    plugin_send_request (p, o, "kvs.load");
    json_object_put (o);
}

static bool load (plugin_ctx_t *p, const href_t ref, zmsg_t **zmsg,
                  json_object **op)
{
    ctx_t *ctx = p->ctx;
    hobj_t *hp = zhash_lookup (ctx->store, ref);
    bool done = true;

    if (plugin_treeroot (p)) {
        if (!hp)
            plugin_panic (p, "dangling ref %s", ref);
    } else {
        if (!hp) {
            hp = hobj_create (NULL);
            zhash_insert (ctx->store, ref, hp);
            zhash_freefn (ctx->store, ref, (zhash_free_fn *)hobj_destroy);
            load_request_send (p, ref);
        }
        if (!hp->o) {
            assert (zmsg != NULL);
            if (zlist_append (hp->reqs, *zmsg) < 0)
                oom ();
            *zmsg = NULL;
            done = false;
        }
    }
    if (done) {
        assert (hp != NULL);
        assert (hp->o != NULL);
        *op = hp->o;
    }
    return done;
}

static void store_request_send (plugin_ctx_t *p, const href_t ref,
                                json_object *val)
{
    json_object *o = util_json_object_new_object ();

    json_object_get (val);
    json_object_object_add (o, ref, val);
    plugin_send_request (p, o, "kvs.store");
    json_object_put (o);
}

static void store (plugin_ctx_t *p, json_object *o, bool writeback, href_t ref)
{
    ctx_t *ctx = p->ctx;
    hobj_t *hp; 
    zmsg_t *zmsg;

    compute_json_href (o, ref);

    if ((hp = zhash_lookup (ctx->store, ref))) {
        if (hp->o) {
            json_object_put (o);
        } else {
            hp->o = o;
            while ((zmsg = zlist_pop (hp->reqs))) {
                if (cmb_msg_match (zmsg, "kvs.load"))
                    kvs_load (p, &zmsg);
                else if (cmb_msg_match (zmsg, "kvs.get"))
                    kvs_get (p, &zmsg);
                if (zmsg)
                    zmsg_destroy (&zmsg);
            }
        }
    } else {
        hp = hobj_create (o);
        zhash_insert (ctx->store, ref, hp);
        zhash_freefn (ctx->store, ref, (zhash_free_fn *)hobj_destroy);
        if (writeback) {
            assert (!plugin_treeroot (p));
            writeback_add (p, OP_STORE, NULL, xstrdup (ref));
            store_request_send (p, ref, o);
        }
    }
}

static void name_request_send (plugin_ctx_t *p, char *key, const href_t ref)
{
    json_object *o = util_json_object_new_object ();

    util_json_object_add_string (o, key, ref);
    plugin_send_request (p, o, "kvs.name");
    json_object_put (o);
}

static void name (plugin_ctx_t *p, char *key, const href_t ref, bool writeback)
{
    writeback_add (p, OP_NAME, xstrdup (key), ref ? xstrdup (ref) : NULL);
    if (writeback)
        name_request_send (p, key, ref);
}

static void kvs_load (plugin_ctx_t *p, zmsg_t **zmsg)
{
    json_object *val, *cpy = NULL, *o = NULL;
    json_object_iter iter;

    if (cmb_msg_decode (*zmsg, NULL, &o) < 0 || o == NULL) {
        plugin_log (p, LOG_ERR, "%s: bad message", __FUNCTION__);
        goto done;
    }
    cpy = util_json_object_dup (o);
    json_object_object_foreachC (o, iter) {
        if (!load (p, iter.key, zmsg, &val))
            goto done; /* stall */
        json_object_get (val);
        json_object_object_add (cpy, iter.key, val);
    }
    plugin_send_response (p, zmsg, cpy);
done:
    if (o)
        json_object_put (o);
    if (cpy)
        json_object_put (cpy);
    if (*zmsg)
        zmsg_destroy (zmsg);
}

static void kvs_load_response (plugin_ctx_t *p, zmsg_t **zmsg)
{
    json_object *o = NULL;
    json_object_iter iter;
    href_t href;
    
    if (cmb_msg_decode (*zmsg, NULL, &o) < 0 || o == NULL) {
        plugin_log (p, LOG_ERR, "%s: bad message", __FUNCTION__);
        goto done;
    }
    json_object_object_foreachC (o, iter) {
        json_object_get (iter.val);
        store (p, iter.val, false, href);
        if (strcmp (href, iter.key) != 0)
            plugin_log (p, LOG_ERR, "%s: bad href %s", __FUNCTION__, iter.key);
    }
done:
    if (o)
        json_object_put (o);
    if (*zmsg)
        zmsg_destroy (zmsg);
}

static void kvs_store (plugin_ctx_t *p, zmsg_t **zmsg)
{
    json_object *cpy = NULL, *o = NULL;
    json_object_iter iter;
    bool writeback = !plugin_treeroot (p);
    href_t href;

    if (cmb_msg_decode (*zmsg, NULL, &o) < 0 || o == NULL) {
        plugin_log (p, LOG_ERR, "%s: bad message", __FUNCTION__);
        goto done;
    }
    cpy = util_json_object_dup (o);
    json_object_object_foreachC (o, iter) {
        json_object_get (iter.val);
        store (p, iter.val, writeback, href);
        if (strcmp (href, iter.key) != 0)
            plugin_log (p, LOG_ERR, "%s: bad href %s", __FUNCTION__, iter.key);
        json_object_object_add (cpy, iter.key, NULL);
    }
    plugin_send_response (p, zmsg, cpy);
done:
    if (o)
        json_object_put (o);
    if (cpy)
        json_object_put (cpy);
    if (*zmsg)
        zmsg_destroy (zmsg);
}

static void kvs_store_response (plugin_ctx_t *p, zmsg_t **zmsg)
{
    json_object *o = NULL;
    json_object_iter iter;
    
    if (cmb_msg_decode (*zmsg, NULL, &o) < 0 || o == NULL) {
        plugin_log (p, LOG_ERR, "%s: bad message", __FUNCTION__);
        goto done;
    }
    json_object_object_foreachC (o, iter) {
        writeback_del (p, OP_STORE, NULL, iter.key);
    }
done:
    if (o)
        json_object_put (o);
    if (*zmsg)
        zmsg_destroy (zmsg);
}

static void kvs_name (plugin_ctx_t *p, zmsg_t **zmsg)
{
    json_object *cpy = NULL, *o = NULL;
    json_object_iter iter;
    bool writeback = !plugin_treeroot (p);

    if (cmb_msg_decode (*zmsg, NULL, &o) < 0 || o == NULL) {
        plugin_log (p, LOG_ERR, "%s: bad message", __FUNCTION__);
        goto done;
    }
    cpy = util_json_object_dup (o);
    json_object_object_foreachC (o, iter) {
        name (p, iter.key, json_object_get_string (iter.val), writeback);
        json_object_object_add (cpy, iter.key, NULL);
    }
    plugin_send_response (p, zmsg, cpy);
done:
    if (o)
        json_object_put (o);
    if (cpy)
        json_object_put (cpy);
    if (*zmsg)
        zmsg_destroy (zmsg);
}

static void kvs_name_response (plugin_ctx_t *p, zmsg_t **zmsg)
{
    json_object *o = NULL;
    json_object_iter iter;
    
    if (cmb_msg_decode (*zmsg, NULL, &o) < 0 || o == NULL) {
        plugin_log (p, LOG_ERR, "%s: bad message", __FUNCTION__);
        goto done;
    }
    json_object_object_foreachC (o, iter) {
        writeback_del (p, OP_NAME, iter.key, NULL);
    }
done:
    if (o)
        json_object_put (o);
    if (*zmsg)
        zmsg_destroy (zmsg);
}

static void kvs_get (plugin_ctx_t *p, zmsg_t **zmsg)
{
    ctx_t *ctx = p->ctx;
    json_object *dir, *val, *cpy = NULL, *o = NULL;
    json_object_iter iter;
    const char *ref;

    if (cmb_msg_decode (*zmsg, NULL, &o) < 0 || o == NULL) {
        plugin_log (p, LOG_ERR, "%s: bad message", __FUNCTION__);
        goto done;
    }
    if (!load (p, ctx->rootdir, zmsg, &dir))
        goto done; /* stall */
    cpy = util_json_object_dup (o);
    json_object_object_foreachC (o, iter) {
        if (util_json_object_get_string (dir, iter.key, &ref) < 0)
            continue;
        if (!load (p, ref, zmsg, &val))
            goto done; /* stall */
        json_object_get (val);
        json_object_object_add (cpy, iter.key, val); 
    }
    plugin_send_response (p, zmsg, cpy);
done:
    if (o)
        json_object_put (o);
    if (cpy)
        json_object_put (cpy);
    if (*zmsg)
        zmsg_destroy (zmsg);
}

static void kvs_disconnect (plugin_ctx_t *p, zmsg_t **zmsg)
{
    /* FIXME */
    zmsg_destroy (zmsg);
}

static void kvs_put (plugin_ctx_t *p, zmsg_t **zmsg)
{
    json_object *o = NULL;
    json_object_iter iter;
    href_t ref;

    assert (plugin_treeroot (p));

    if (cmb_msg_decode (*zmsg, NULL, &o) < 0 || o == NULL) {
        plugin_log (p, LOG_ERR, "%s: bad message", __FUNCTION__);
        goto done;
    }
    json_object_object_foreachC (o, iter) {
        if (json_object_get_type (iter.val) == json_type_null) {
            writeback_add (p, OP_NAME, xstrdup (iter.key), NULL);
        } else { 
            json_object_get (iter.val);
            store (p, iter.val, false, ref);
            writeback_add (p, OP_NAME, xstrdup (iter.key), xstrdup (ref)); 
        }
    }
    plugin_send_response_errnum (p, zmsg, 0); /* success */
done:
    if (o)
        json_object_put (o);
    if (*zmsg)
        zmsg_destroy (zmsg);
}

static void kvs_commit (plugin_ctx_t *p, zmsg_t **zmsg)
{
    ctx_t *ctx = p->ctx;
    op_t *op;
    json_object *o, *cpy;

    assert (plugin_treeroot (p));

    if (zlist_size (ctx->writeback) > 0) {
        (void)load (p, ctx->rootdir, NULL, &o);
        assert (o != NULL);
        cpy = util_json_object_dup (o);
        while ((op = zlist_pop (ctx->writeback))) {
            switch (op->optype) {
                case OP_NAME:
                    if (op->ref)
                        util_json_object_add_string (cpy, op->key, op->ref);
                    else
                        json_object_object_del (cpy, op->key);
                    break;
                case OP_STORE:
                    break;
            }
            op_destroy (op);
        }
        store (p, cpy, false, ctx->rootdir);
        plugin_send_event (p, "event.kvs.setroot.%s", ctx->rootdir);
    }
    plugin_send_response_errnum (p, zmsg, 0); /* success */
}

static void event_kvs_setroot (plugin_ctx_t *p, char *arg, zmsg_t **zmsg)
{
    ctx_t *ctx = p->ctx;

    assert (!plugin_treeroot (p));

    if (strlen (arg) + 1 == sizeof (href_t))
        memcpy (ctx->rootdir, arg, sizeof (href_t));
    else
        plugin_log (p, LOG_ERR, "%s: bad href %s", __FUNCTION__, arg);
}

static void kvs_getroot (plugin_ctx_t *p, zmsg_t **zmsg)
{
    ctx_t *ctx = p->ctx;
    json_object *o = json_object_new_string (ctx->rootdir);

    if (!o)
        oom ();
    plugin_send_response (p, zmsg, o);
}

static void kvs_recv (plugin_ctx_t *p, zmsg_t **zmsg, zmsg_type_t type)
{
    char *arg = NULL;

    if (cmb_msg_match (*zmsg, "kvs.getroot")) {
        kvs_getroot (p, zmsg);
    } else if (cmb_msg_match_substr (*zmsg, "event.kvs.setroot.", &arg)) {
        event_kvs_setroot (p, arg, zmsg);
    } else if (cmb_msg_match (*zmsg, "kvs.disconnect")) {
        kvs_disconnect (p, zmsg);
    } else if (cmb_msg_match (*zmsg, "kvs.get")) {
        kvs_get (p, zmsg);
    } else if (cmb_msg_match (*zmsg, "kvs.load")) {
        if (type == ZMSG_REQUEST)
            kvs_load (p, zmsg);
        else
            kvs_load_response (p, zmsg);
    } else if (cmb_msg_match (*zmsg, "kvs.store")) {
        if (type == ZMSG_REQUEST)
            kvs_store (p, zmsg);
        else
            kvs_store_response (p, zmsg);
    } else if (cmb_msg_match (*zmsg, "kvs.name")) {
        if (type == ZMSG_REQUEST)
            kvs_name (p, zmsg);
        else
            kvs_name_response (p, zmsg);
    } else if (cmb_msg_match (*zmsg, "kvs.put")) {
        if (type == ZMSG_REQUEST) {
            if (plugin_treeroot (p))
                kvs_put (p, zmsg);
            else
                plugin_send_request_raw (p, zmsg); /* fwd to root */
        } else
            plugin_send_response_raw (p, zmsg); /* fwd to requestor */
    } else if (cmb_msg_match (*zmsg, "kvs.commit")) {
        if (type == ZMSG_REQUEST) {
            if (plugin_treeroot (p))
                kvs_commit (p, zmsg);
            else
                plugin_send_request_raw (p, zmsg); /* fwd to root */
        } else
            plugin_send_response_raw (p, zmsg); /* fwd to requestor */
    }

    if (arg)
        free (arg);
    if (*zmsg)
        zmsg_destroy (zmsg);
}

static void kvs_init (plugin_ctx_t *p)
{
    ctx_t *ctx;

    ctx = p->ctx = xzmalloc (sizeof (ctx_t));
    if (!plugin_treeroot (p))
        zsocket_set_subscribe (p->zs_evin, "event.kvs.");
    if (!(ctx->store = zhash_new ()))
        oom ();
    if (!(ctx->writeback = zlist_new ()))
        oom ();

    if (plugin_treeroot (p)) {
        json_object *rootdir = util_json_object_new_object ();

        store (p, rootdir, false, ctx->rootdir);
    } else {
        json_object *rep = plugin_request (p, NULL, "kvs.getroot");
        const char *ref = json_object_get_string (rep);

        if (ref && strlen (ref) + 1 == sizeof (href_t))
            memcpy (ctx->rootdir, ref, sizeof (href_t));
        else
            plugin_panic (p, "malformed kvs.getroot reply");
    }
}

static void kvs_fini (plugin_ctx_t *p)
{
    ctx_t *ctx = p->ctx;

    zhash_destroy (&ctx->store);
    zlist_destroy (&ctx->writeback);
    free (ctx);
}

struct plugin_struct kvssrv = {
    .name      = "kvs",
    .initFn    = kvs_init,
    .finiFn    = kvs_fini,
    .recvFn    = kvs_recv,
};

/*
 * vi:tabstop=4 shiftwidth=4 expandtab
 */
