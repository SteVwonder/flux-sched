
uses "Node"

Hierarchy "default" {
  Resource{ "cluster", name = "mini",
    children = {
       ListOf { Node, ids = "0-8",
                args = { name = "mini", ncores = 16, memory = 16384 }
              }
    }
  }
}
