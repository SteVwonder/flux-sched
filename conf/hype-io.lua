
uses "Node"

Hierarchy "default"
{
   Resource{ "cluster", name = "hype", io=10000,
             children = { ListOf{ Node,
                                  ids = "201-354",
                                  args = {
                                     name = "hype",
                                     sockets = {"0-7", "8-15"},
                                     memory_per_socket = 15000,
                                     io = 1440
                                  }
                                }
             }
   }
}
