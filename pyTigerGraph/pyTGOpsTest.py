import json

from pyTigerGraphOps import TigerGraphOps as tg
from pyTigerGraphException import TigerGraphException

sep = "-" * 60


def pr(fn, res):
    print(sep)
    print("Function: " + res + "\n")
    res = eval(res)
    # print("==> " + str(type(res)))
    if isinstance(res, set):
        print("Response: " + res)
    else:
        print("Response: " + json.dumps(res, indent=4))


def pr2(fn, res):
    try:
        pr(fn, res)
    except TigerGraphException as tge:
        print("ERROR: " + tge.message)


print("Start\n" + ("-" * 100))

conn = tg(host="http://127.0.0.1", restppPort="30900", gsPort="30240", graphname="FraudGraph", debug=False)

# Graphs ===================================================================
pr2("DROP GRAPH", 'conn.dropGraph("g1")')
pr2("DROP GRAPH", 'conn.dropGraph("g2")')
pr2("DROP GRAPH", 'conn.dropGraph("g3")')

pr2("CREATE GRAPH", 'conn.createGraph("g1", "*")')
pr2("CREATE GRAPH", 'conn.createGraph("g2", ["v1", "v2"], "e1")')
pr2("CREATE GRAPH", 'conn.createGraph("g3", ["v1", "v2"])')
try:
    pr2("CREATE GRAPH", 'conn.createGraph("g3", ["v1", "v2"])')
except TigerGraphException as tga:
    print("ERROR: " + tga.message)
pr2("CREATE GRAPH", 'conn.createGraph("g4", ["v1", "v2"], ["e1", "e2"])')
pr2("CREATE GRAPH", 'conn.createGraph("g5", [])')

pr2("USE GRAPH", 'conn.useGraph("g1")')
pr2("USE GRAPH", 'conn.useGraph("g9")')

# pr("USE GRAPH", conn.useGraph("g1"))

print("\n" + ("-" * 100) + "\nEnd")
