/*****************************************************************************\
 *  Copyright (c) 2020 Lawrence Livermore National Security, LLC.  Produced at
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

#include <vector>
#include <iostream>
#include <cerrno>
#include <cstdlib>
#include <sstream>
#include <algorithm>
#include <stdexcept>
#include "resource/readers/resource_namespace_remapper.hpp"

extern "C" {
#if HAVE_CONFIG_H
#include "config.h"
#endif
}

using namespace Flux::resource_model;


/********************************************************************************
 *                                                                              *
 *                   Private Resource Namespace Remapper API                    *
 *                                                                              *
 ********************************************************************************/

resource_namespace_remapper_t
    ::distinct_range_t::distinct_range_t (uint64_t point)
{
    m_low = point;
    m_high = point;
}

resource_namespace_remapper_t
    ::distinct_range_t::distinct_range_t (uint64_t lo, uint64_t hi)
{
    m_low = lo;
    m_high = hi;
}

uint64_t resource_namespace_remapper_t
             ::distinct_range_t::get_low () const
{
    return m_low;
}

uint64_t resource_namespace_remapper_t
             ::distinct_range_t::get_high () const
{
    return m_high;
}

bool resource_namespace_remapper_t
         ::distinct_range_t::is_point () const
{
    return m_low == m_high;
}

bool resource_namespace_remapper_t
         ::distinct_range_t::operator< (const distinct_range_t &o) const {
    // this class is used as key for std::map, which relies on the following
    // x and y are equivalent if !(x < y) && !(y < x)
    return m_high < o.m_low;
}

bool resource_namespace_remapper_t
         ::distinct_range_t::operator== (const distinct_range_t &o) const {
    // x and y are equal if and only if both high and low
    // of the operands are equal
    return m_high == o.m_high && m_low == o.m_low;
}

bool resource_namespace_remapper_t
         ::distinct_range_t::operator!= (const distinct_range_t &o) const {
    // x and y are not equal if either high or low is not equal
    return m_high != o.m_high || m_low != o.m_low;
}


/********************************************************************************
 *                                                                              *
 *                    Public Resource Namespace Remapper API                    *
 *                                                                              *
 ********************************************************************************/

int resource_namespace_remapper_t::add (const uint64_t low, const uint64_t high,
                                        const std::string &name_type,
                                        uint64_t ref_id, uint64_t remapped_id)
{
    if (low > high)
        goto inval;
    try {
        const distinct_range_t exec_target_range{low, high};
        auto m_remap_iter = m_remap.find (exec_target_range);

        if (m_remap_iter == m_remap.end ()) {
            m_remap.emplace (exec_target_range,
                             std::map<const std::string,
                                      std::map<uint64_t, uint64_t>> ());
        } else if (exec_target_range != m_remap_iter->first)
            goto inval; // key must be exact

        if (m_remap[exec_target_range].find (name_type)
            == m_remap[exec_target_range].end ())
            m_remap[exec_target_range][name_type] = std::map<uint64_t,
                                                             uint64_t> ();
        if (m_remap[exec_target_range][name_type].find (ref_id)
            != m_remap[exec_target_range][name_type].end ()) {
            errno = EEXIST;
            goto error;
        }
        m_remap[exec_target_range][name_type][ref_id] = remapped_id;
    } catch (std::bad_alloc &) {
        errno = ENOMEM;
        goto error;
    }
    return 0;
inval:
    errno = EINVAL;
error:
    return -1;
}

int resource_namespace_remapper_t::add (const std::string &exec_target_range,
                                        const std::string &name_type,
                                        uint64_t ref_id, uint64_t remapped_id)
{
    try {
        long int n;
        size_t ndash;
        std::string exec_target;
        std::istringstream istr{exec_target_range};
        std::vector<uint64_t> targets;

        if ( (ndash = std::count (exec_target_range.begin (),
                                  exec_target_range.end (), '-')) > 1)
            goto inval;
        while (std::getline (istr, exec_target, '-')) {
            if ( (n = std::stol (exec_target)) < 0)
                goto inval;
            targets.push_back (static_cast<uint64_t> (n));
        }
        return (targets.size () == 2) ? add (targets[0], targets[1],
                                             name_type, ref_id, remapped_id)
                                      : add (targets[0], targets[0],
                                             name_type, ref_id, remapped_id);
    } catch (std::invalid_argument &) {
        goto inval;
    } catch (std::out_of_range &) {
        errno = ERANGE;
        goto error;
    } catch (std::bad_alloc &) {
        errno = ENOMEM;
        goto error;
    }
    return 0;
inval:
    errno = EINVAL;
error:
    return -1;
}

int resource_namespace_remapper_t::query (const uint64_t exec_target,
                                          const std::string &name_type,
                                          uint64_t ref_id,
                                          uint64_t &remapped_id_out) const
{
    try {
        remapped_id_out = m_remap.at (distinct_range_t{exec_target})
                                         .at (name_type). at (ref_id);
        return 0;
    } catch (std::out_of_range &) {
        errno = ENOENT;
        return -1;
    }
}

bool resource_namespace_remapper_t::is_remapped () const
{
    return !m_remap.empty ();
}

/*
 * vi:tabstop=4 shiftwidth=4 expandtab
 */
