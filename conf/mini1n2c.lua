
uses "Node"

Hierarchy "default" {
  Resource{ "cluster", name = "mini",
    children = {
       ListOf { Node, ids = "0",
                args = { name = "mini", ncores = 2, memory = 16384 }
              }
    }
  }
}
