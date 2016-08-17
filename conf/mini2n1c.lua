
uses "Node"

Hierarchy "default" {
  Resource{ "cluster", name = "mini",
    children = {
       ListOf { Node, ids = "0-1",
                args = { name = "mini", ncores = 1, memory = 16384 }
              }
    }
  }
}
