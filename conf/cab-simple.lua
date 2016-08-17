
uses "Node"

Hierarchy "default" {
    Resource{ "cluster", name = "cab",
    children = { ListOf{ Node,
                  ids = "1-1232",
                  args = { basename = "cab", num_cores=16 }
                 },
               }
    }
}
