uses "Node"
Hierarchy "default"
{
   Resource{ "filesystem", name="pfs", io = 90000, children = {
      Resource {"gateway", name = "gateway_node_pool", io = 90000, children = {
         Resource {"core_switch", name = "core_switch_pool", io = 144000, children = {
            Resource {"edge_switch", name = "edge_switch1", io = 72000, children = {
               ListOf{ Node, ids = "1-18", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch2", io = 72000, children = {
               ListOf{ Node, ids = "19-36", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch3", io = 72000, children = {
               ListOf{ Node, ids = "37-54", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch4", io = 72000, children = {
               ListOf{ Node, ids = "55-72", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch5", io = 72000, children = {
               ListOf{ Node, ids = "73-90", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch6", io = 72000, children = {
               ListOf{ Node, ids = "91-108", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch7", io = 72000, children = {
               ListOf{ Node, ids = "109-126", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch8", io = 72000, children = {
               ListOf{ Node, ids = "127-144", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch9", io = 72000, children = {
               ListOf{ Node, ids = "145-160", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch10", io = 72000, children = {
               ListOf{ Node, ids = "163-180", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch11", io = 72000, children = {
               ListOf{ Node, ids = "181-198", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch12", io = 72000, children = {
               ListOf{ Node, ids = "199-216", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch13", io = 72000, children = {
               ListOf{ Node, ids = "217-234", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch14", io = 72000, children = {
               ListOf{ Node, ids = "235-252", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch15", io = 72000, children = {
               ListOf{ Node, ids = "253-270", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch16", io = 72000, children = {
               ListOf{ Node, ids = "271-288", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch17", io = 72000, children = {
               ListOf{ Node, ids = "289-306", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch18", io = 72000, children = {
               ListOf{ Node, ids = "307-322", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch19", io = 72000, children = {
               ListOf{ Node, ids = "325-342", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch20", io = 72000, children = {
               ListOf{ Node, ids = "343-360", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch21", io = 72000, children = {
               ListOf{ Node, ids = "361-378", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch22", io = 72000, children = {
               ListOf{ Node, ids = "379-396", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch23", io = 72000, children = {
               ListOf{ Node, ids = "397-414", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch24", io = 72000, children = {
               ListOf{ Node, ids = "415-432", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch25", io = 72000, children = {
               ListOf{ Node, ids = "433-450", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch26", io = 72000, children = {
               ListOf{ Node, ids = "451-468", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch27", io = 72000, children = {
               ListOf{ Node, ids = "469-484", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch28", io = 72000, children = {
               ListOf{ Node, ids = "487-504", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch29", io = 72000, children = {
               ListOf{ Node, ids = "505-522", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch30", io = 72000, children = {
               ListOf{ Node, ids = "523-540", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch31", io = 72000, children = {
               ListOf{ Node, ids = "541-558", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch32", io = 72000, children = {
               ListOf{ Node, ids = "559-576", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch33", io = 72000, children = {
               ListOf{ Node, ids = "577-594", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch34", io = 72000, children = {
               ListOf{ Node, ids = "595-612", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch35", io = 72000, children = {
               ListOf{ Node, ids = "613-630", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch36", io = 72000, children = {
               ListOf{ Node, ids = "631-646", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch37", io = 72000, children = {
               ListOf{ Node, ids = "649-666", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch38", io = 72000, children = {
               ListOf{ Node, ids = "667-684", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch39", io = 72000, children = {
               ListOf{ Node, ids = "685-702", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch40", io = 72000, children = {
               ListOf{ Node, ids = "703-720", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch41", io = 72000, children = {
               ListOf{ Node, ids = "721-738", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch42", io = 72000, children = {
               ListOf{ Node, ids = "739-756", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch43", io = 72000, children = {
               ListOf{ Node, ids = "757-774", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch44", io = 72000, children = {
               ListOf{ Node, ids = "775-792", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch45", io = 72000, children = {
               ListOf{ Node, ids = "793-808", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch46", io = 72000, children = {
               ListOf{ Node, ids = "811-828", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch47", io = 72000, children = {
               ListOf{ Node, ids = "829-846", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch48", io = 72000, children = {
               ListOf{ Node, ids = "847-864", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch49", io = 72000, children = {
               ListOf{ Node, ids = "865-882", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch50", io = 72000, children = {
               ListOf{ Node, ids = "883-900", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch51", io = 72000, children = {
               ListOf{ Node, ids = "901-918", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch52", io = 72000, children = {
               ListOf{ Node, ids = "919-936", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch53", io = 72000, children = {
               ListOf{ Node, ids = "937-954", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch54", io = 72000, children = {
               ListOf{ Node, ids = "955-970", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch55", io = 72000, children = {
               ListOf{ Node, ids = "973-990", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch56", io = 72000, children = {
               ListOf{ Node, ids = "991-1008", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch57", io = 72000, children = {
               ListOf{ Node, ids = "1009-1026", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch58", io = 72000, children = {
               ListOf{ Node, ids = "1027-1044", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch59", io = 72000, children = {
               ListOf{ Node, ids = "1045-1062", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch60", io = 72000, children = {
               ListOf{ Node, ids = "1063-1080", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch61", io = 72000, children = {
               ListOf{ Node, ids = "1081-1098", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch62", io = 72000, children = {
               ListOf{ Node, ids = "1099-1116", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch63", io = 72000, children = {
               ListOf{ Node, ids = "1117-1132", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch64", io = 72000, children = {
               ListOf{ Node, ids = "1135-1152", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch65", io = 72000, children = {
               ListOf{ Node, ids = "1153-1170", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch66", io = 72000, children = {
               ListOf{ Node, ids = "1171-1188", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch67", io = 72000, children = {
               ListOf{ Node, ids = "1189-1206", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch68", io = 72000, children = {
               ListOf{ Node, ids = "1207-1224", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch69", io = 72000, children = {
               ListOf{ Node, ids = "1225-1242", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch70", io = 72000, children = {
               ListOf{ Node, ids = "1243-1260", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch71", io = 72000, children = {
               ListOf{ Node, ids = "1261-1278", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch72", io = 72000, children = {
               ListOf{ Node, ids = "1279-1294", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch73", io = 72000, children = {
               ListOf{ Node, ids = "1297-1314", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch74", io = 72000, children = {
               ListOf{ Node, ids = "1315-1332", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch75", io = 72000, children = {
               ListOf{ Node, ids = "1333-1350", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch76", io = 72000, children = {
               ListOf{ Node, ids = "1351-1368", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch77", io = 72000, children = {
               ListOf{ Node, ids = "1369-1386", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch78", io = 72000, children = {
               ListOf{ Node, ids = "1387-1404", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch79", io = 72000, children = {
               ListOf{ Node, ids = "1405-1422", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch80", io = 72000, children = {
               ListOf{ Node, ids = "1423-1440", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch81", io = 72000, children = {
               ListOf{ Node, ids = "1441-1456", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch82", io = 72000, children = {
               ListOf{ Node, ids = "1459-1476", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch83", io = 72000, children = {
               ListOf{ Node, ids = "1477-1494", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch84", io = 72000, children = {
               ListOf{ Node, ids = "1495-1512", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch85", io = 72000, children = {
               ListOf{ Node, ids = "1513-1530", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch86", io = 72000, children = {
               ListOf{ Node, ids = "1531-1548", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch87", io = 72000, children = {
               ListOf{ Node, ids = "1549-1566", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch88", io = 72000, children = {
               ListOf{ Node, ids = "1567-1584", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch89", io = 72000, children = {
               ListOf{ Node, ids = "1585-1602", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch90", io = 72000, children = {
               ListOf{ Node, ids = "1603-1618", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch91", io = 72000, children = {
               ListOf{ Node, ids = "1621-1638", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch92", io = 72000, children = {
               ListOf{ Node, ids = "1639-1656", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch93", io = 72000, children = {
               ListOf{ Node, ids = "1657-1674", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch94", io = 72000, children = {
               ListOf{ Node, ids = "1675-1692", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch95", io = 72000, children = {
               ListOf{ Node, ids = "1693-1710", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch96", io = 72000, children = {
               ListOf{ Node, ids = "1711-1728", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch97", io = 72000, children = {
               ListOf{ Node, ids = "1729-1746", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch98", io = 72000, children = {
               ListOf{ Node, ids = "1747-1764", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch99", io = 72000, children = {
               ListOf{ Node, ids = "1765-1780", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch100", io = 72000, children = {
               ListOf{ Node, ids = "1783-1800", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch101", io = 72000, children = {
               ListOf{ Node, ids = "1801-1818", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch102", io = 72000, children = {
               ListOf{ Node, ids = "1819-1836", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch103", io = 72000, children = {
               ListOf{ Node, ids = "1837-1854", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch104", io = 72000, children = {
               ListOf{ Node, ids = "1855-1872", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch105", io = 72000, children = {
               ListOf{ Node, ids = "1873-1890", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch106", io = 72000, children = {
               ListOf{ Node, ids = "1891-1908", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch107", io = 72000, children = {
               ListOf{ Node, ids = "1909-1926", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch108", io = 72000, children = {
               ListOf{ Node, ids = "1927-1942", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch109", io = 72000, children = {
               ListOf{ Node, ids = "1945-1962", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch110", io = 72000, children = {
               ListOf{ Node, ids = "1963-1980", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch111", io = 72000, children = {
               ListOf{ Node, ids = "1981-1998", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch112", io = 72000, children = {
               ListOf{ Node, ids = "1999-2016", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch113", io = 72000, children = {
               ListOf{ Node, ids = "2017-2034", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch114", io = 72000, children = {
               ListOf{ Node, ids = "2035-2052", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch115", io = 72000, children = {
               ListOf{ Node, ids = "2053-2070", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch116", io = 72000, children = {
               ListOf{ Node, ids = "2071-2088", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch117", io = 72000, children = {
               ListOf{ Node, ids = "2089-2104", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch118", io = 72000, children = {
               ListOf{ Node, ids = "2107-2124", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch119", io = 72000, children = {
               ListOf{ Node, ids = "2125-2142", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch120", io = 72000, children = {
               ListOf{ Node, ids = "2143-2160", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch121", io = 72000, children = {
               ListOf{ Node, ids = "2161-2178", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch122", io = 72000, children = {
               ListOf{ Node, ids = "2179-2196", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch123", io = 72000, children = {
               ListOf{ Node, ids = "2197-2214", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch124", io = 72000, children = {
               ListOf{ Node, ids = "2215-2232", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch125", io = 72000, children = {
               ListOf{ Node, ids = "2233-2250", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch126", io = 72000, children = {
               ListOf{ Node, ids = "2251-2266", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch127", io = 72000, children = {
               ListOf{ Node, ids = "2269-2286", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch128", io = 72000, children = {
               ListOf{ Node, ids = "2287-2304", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch129", io = 72000, children = {
               ListOf{ Node, ids = "2305-2322", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch130", io = 72000, children = {
               ListOf{ Node, ids = "2323-2340", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch131", io = 72000, children = {
               ListOf{ Node, ids = "2341-2358", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch132", io = 72000, children = {
               ListOf{ Node, ids = "2359-2376", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch133", io = 72000, children = {
               ListOf{ Node, ids = "2377-2394", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch134", io = 72000, children = {
               ListOf{ Node, ids = "2395-2412", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch135", io = 72000, children = {
               ListOf{ Node, ids = "2413-2428", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch136", io = 72000, children = {
               ListOf{ Node, ids = "2431-2448", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch137", io = 72000, children = {
               ListOf{ Node, ids = "2449-2466", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch138", io = 72000, children = {
               ListOf{ Node, ids = "2467-2484", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch139", io = 72000, children = {
               ListOf{ Node, ids = "2485-2502", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch140", io = 72000, children = {
               ListOf{ Node, ids = "2503-2520", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch141", io = 72000, children = {
               ListOf{ Node, ids = "2521-2538", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch142", io = 72000, children = {
               ListOf{ Node, ids = "2539-2556", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch143", io = 72000, children = {
               ListOf{ Node, ids = "2557-2574", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch144", io = 72000, children = {
               ListOf{ Node, ids = "2575-2590", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch145", io = 72000, children = {
               ListOf{ Node, ids = "2593-2610", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch146", io = 72000, children = {
               ListOf{ Node, ids = "2611-2628", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch147", io = 72000, children = {
               ListOf{ Node, ids = "2629-2646", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch148", io = 72000, children = {
               ListOf{ Node, ids = "2647-2664", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch149", io = 72000, children = {
               ListOf{ Node, ids = "2665-2682", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch150", io = 72000, children = {
               ListOf{ Node, ids = "2683-2700", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch151", io = 72000, children = {
               ListOf{ Node, ids = "2701-2718", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch152", io = 72000, children = {
               ListOf{ Node, ids = "2719-2736", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch153", io = 72000, children = {
               ListOf{ Node, ids = "2737-2752", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch154", io = 72000, children = {
               ListOf{ Node, ids = "2755-2772", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch155", io = 72000, children = {
               ListOf{ Node, ids = "2773-2790", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch156", io = 72000, children = {
               ListOf{ Node, ids = "2791-2808", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch157", io = 72000, children = {
               ListOf{ Node, ids = "2809-2826", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch158", io = 72000, children = {
               ListOf{ Node, ids = "2827-2844", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch159", io = 72000, children = {
               ListOf{ Node, ids = "2845-2862", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch160", io = 72000, children = {
               ListOf{ Node, ids = "2863-2880", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch161", io = 72000, children = {
               ListOf{ Node, ids = "2881-2898", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch162", io = 72000, children = {
               ListOf{ Node, ids = "2899-2914", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch163", io = 72000, children = {
               ListOf{ Node, ids = "2917-2934", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch164", io = 72000, children = {
               ListOf{ Node, ids = "2935-2952", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch165", io = 72000, children = {
               ListOf{ Node, ids = "2953-2970", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch166", io = 72000, children = {
               ListOf{ Node, ids = "2971-2988", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch167", io = 72000, children = {
               ListOf{ Node, ids = "2989-3006", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch168", io = 72000, children = {
               ListOf{ Node, ids = "3007-3024", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch169", io = 72000, children = {
               ListOf{ Node, ids = "3025-3042", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch170", io = 72000, children = {
               ListOf{ Node, ids = "3043-3060", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch171", io = 72000, children = {
               ListOf{ Node, ids = "3061-3076", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch172", io = 72000, children = {
               ListOf{ Node, ids = "3079-3096", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch173", io = 72000, children = {
               ListOf{ Node, ids = "3097-3114", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch174", io = 72000, children = {
               ListOf{ Node, ids = "3115-3132", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch175", io = 72000, children = {
               ListOf{ Node, ids = "3133-3150", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch176", io = 72000, children = {
               ListOf{ Node, ids = "3151-3168", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch177", io = 72000, children = {
               ListOf{ Node, ids = "3169-3186", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch178", io = 72000, children = {
               ListOf{ Node, ids = "3187-3204", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch179", io = 72000, children = {
               ListOf{ Node, ids = "3205-3222", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch180", io = 72000, children = {
               ListOf{ Node, ids = "3223-3238", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch181", io = 72000, children = {
               ListOf{ Node, ids = "3241-3258", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch182", io = 72000, children = {
               ListOf{ Node, ids = "3259-3276", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch183", io = 72000, children = {
               ListOf{ Node, ids = "3277-3294", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch184", io = 72000, children = {
               ListOf{ Node, ids = "3295-3312", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch185", io = 72000, children = {
               ListOf{ Node, ids = "3313-3330", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch186", io = 72000, children = {
               ListOf{ Node, ids = "3331-3348", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch187", io = 72000, children = {
               ListOf{ Node, ids = "3349-3366", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch188", io = 72000, children = {
               ListOf{ Node, ids = "3367-3384", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch189", io = 72000, children = {
               ListOf{ Node, ids = "3385-3400", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch190", io = 72000, children = {
               ListOf{ Node, ids = "3403-3420", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch191", io = 72000, children = {
               ListOf{ Node, ids = "3421-3438", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch192", io = 72000, children = {
               ListOf{ Node, ids = "3439-3456", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch193", io = 72000, children = {
               ListOf{ Node, ids = "3457-3474", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch194", io = 72000, children = {
               ListOf{ Node, ids = "3475-3492", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch195", io = 72000, children = {
               ListOf{ Node, ids = "3493-3510", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch196", io = 72000, children = {
               ListOf{ Node, ids = "3511-3528", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch197", io = 72000, children = {
               ListOf{ Node, ids = "3529-3546", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch198", io = 72000, children = {
               ListOf{ Node, ids = "3547-3562", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch199", io = 72000, children = {
               ListOf{ Node, ids = "3565-3582", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch200", io = 72000, children = {
               ListOf{ Node, ids = "3583-3600", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch201", io = 72000, children = {
               ListOf{ Node, ids = "3601-3618", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch202", io = 72000, children = {
               ListOf{ Node, ids = "3619-3636", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch203", io = 72000, children = {
               ListOf{ Node, ids = "3637-3654", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch204", io = 72000, children = {
               ListOf{ Node, ids = "3655-3672", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch205", io = 72000, children = {
               ListOf{ Node, ids = "3673-3690", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch206", io = 72000, children = {
               ListOf{ Node, ids = "3691-3708", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch207", io = 72000, children = {
               ListOf{ Node, ids = "3709-3724", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch208", io = 72000, children = {
               ListOf{ Node, ids = "3727-3744", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch209", io = 72000, children = {
               ListOf{ Node, ids = "3745-3762", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch210", io = 72000, children = {
               ListOf{ Node, ids = "3763-3780", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch211", io = 72000, children = {
               ListOf{ Node, ids = "3781-3798", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch212", io = 72000, children = {
               ListOf{ Node, ids = "3799-3816", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch213", io = 72000, children = {
               ListOf{ Node, ids = "3817-3834", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch214", io = 72000, children = {
               ListOf{ Node, ids = "3835-3852", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch215", io = 72000, children = {
               ListOf{ Node, ids = "3853-3870", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }},
            Resource {"edge_switch", name = "edge_switch216", io = 72000, children = {
               ListOf{ Node, ids = "3871-3886", args = { name = "cts-1", sockets = {"0-7", "8-15"}, io = 4000}}
            }}
         }}
      }}
   }}
}
