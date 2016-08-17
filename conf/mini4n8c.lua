
uses "Node"

Hierarchy "default" {
  Resource{ "cluster", name = "mini",
    children = {
       ListOf { Node, ids = "0-3",
                args = { name = "mini", ncores = 8, memory = 16384 }
              }
    }
  }
}
