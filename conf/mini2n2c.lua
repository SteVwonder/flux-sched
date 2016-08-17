
uses "SimpleNode"

Hierarchy "default" {
  Resource{ "cluster", name = "mini",
    children = {
       ListOf { SimpleNode, ids = "0-1",
                args = { basename = "mini", ncores = 2, memory = 16384 }
              }
    }
  }
}
