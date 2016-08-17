--/***************************************************************************\
--  Copyright (c) 2014 Lawrence Livermore National Security, LLC.  Produced at
--  the Lawrence Livermore National Laboratory (cf, AUTHORS, DISCLAIMER.LLNS).
--  LLNL-CODE-658032 All rights reserved.
--
--  This file is part of the Flux resource manager framework.
--  For details, see https://github.com/flux-framework.
--
--  This program is free software; you can redistribute it and/or modify it
--  under the terms of the GNU General Public License as published by the Free
--  Software Foundation; either version 2 of the license, or (at your option)
--  any later version.
--
--  Flux is distributed in the hope that it will be useful, but WITHOUT
--  ANY WARRANTY; without even the IMPLIED WARRANTY OF MERCHANTABILITY or
--  FITNESS FOR A PARTICULAR PURPOSE.  See the terms and conditions of the
--  GNU General Public License for more details.
--
--  You should have received a copy of the GNU General Public License along
--  with this program; if not, write to the Free Software Foundation, Inc.,
--  59 Temple Place, Suite 330, Boston, MA 02111-1307 USA.
--  See also:  http://www.gnu.org/licenses/
--\***************************************************************************/

SimpleNode = Resource:subclass ('SimpleNode')

function SimpleNode:initialize (arg)
    local basename = arg.basename
    assert (basename, "Required SimpleNode arg `basename' missing")

    local id = arg.id
    assert (arg.ncores, "Required SimpleNode arg `ncores' missing")

    local name = arg.name or string.format ("%s%s", basename, id or "")

    Resource.initialize (self,
        { "node",
          basename = basename,
          id = id,
          name = name,
          properties = arg.properties or {},
          tags = arg.tags or {}
        }
    )

    -- Add core0..core(n-1)
    for i = 0, arg.ncores - 1 do
        self:add_child (
            Resource{ "core",
                id = i,
                properties = {},
                tags = {}
            }
        )
    end

    if arg.memory and tonumber (arg.memory) then
        self:add_child (
            Resource{ "memory", size = arg.memory }
        )
    end

end

-- vi: ts=4 sw=4 expandtab
return SimpleNode
