
uses "Node"

Hierarchy "default" {
  Resource{ "cluster", name = "clus",
    children = {
       ListOf { Node, ids = "0-3",
                args = { basename = "clus", num_cores=8}
              }
    }
  }
}
