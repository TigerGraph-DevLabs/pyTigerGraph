'''
Created on 7 Apr 2020

@author: szilardbarany
'''
import traceback

if __name__ == '__main__':
    pass

import json
import pandas as pd
from pyTigerGraphBase import TigerGraphBase as tgB

sep = "-" * 60


def pr(msg, function, *args, **kwargs):
    """

    :param msg:
    :param function:
    :param args:
    :param kwargs:
    """
    print(sep)
    print("Function:    " + msg)
    try:
        res = function(*args, **kwargs)
        print("Return type: " + str(type(res)))
        print()
        if isinstance(res, set):
            print(res)
        elif isinstance(res, pd.DataFrame):
            print(pd)
        else:
            print(json.dumps(res, indent=4))
    except Exception:
        print()
        print(traceback.format_exc())


def prx(msg, res):
    print(sep)
    print("Function:    " + msg)
    print()
    print(json.dumps(res, indent=4))


print("Start\n" + ("-" * 100))

# conn = tgB(host="http://127.0.0.1", username="tigergraph", password="tigergraph1", restppPort="31900", gsPort="31240", graphname="MyGraph",
#     secret="ljhrnahjgk5c8hoji67n1pnp4i65sp77", debug=True)
conn = tgB(host="http://127.0.0.1", username="tigergraph", password="tigergraph", restppPort="31900", gsPort="31240", graphname="MyGraph",
    debug=True)
# conn = tg.TigerGraphBase(host="https://szbtg-test.i.tgcloud.io", graphname="test", useCert=True)

# token = conn.getToken("7dcavgjrsouqinebukfek4cu15a1e3ac")
# pr("getCurrentTokens()", conn.getCurrentTokens)
# pr("getCurrentGraph()", conn.getCurrentGraph)

# print(token)
# conn.getToken("2g7scij43oqfdc5oespbdg3p9guei3l5", "geo_pogo")
# pr("getCurrentTokens()", conn.getCurrentTokens)
# pr("getCurrentGraph()", conn.getCurrentGraph)

# conn.getToken("dfl156tq588hqb5d6i3pdb55mgfbbjch", "abc")


# Graph related functions ======================================================

# pr("getGraphs()", conn.getGraphs)


# pr("getGraph()", conn.getGraph)
# conn.useGlobal()
# pr("getGraph()", conn.getGraph)
# pr("getGraph()", conn.getGraph, ("GLOBAL"))
# pr("getGraph()", conn.getGraph, ("geo_pogo"))
# grs = conn.getGraphs()
# if grs:
#     pr("getGraph()", conn.getGraph, (grs[0]))

# pr("getCurrentGraph()", conn.getCurrentGraph)
# pr("useGraph(abc)", conn.useGraph, ("abc"))
# pr("getCurrentGraph()", conn.getCurrentGraph)
# pr("useGlobal()", conn.useGlobal)
# pr("getCurrentGraph()", conn.getCurrentGraph)
# pr("useGraph(MyGraph)", conn.useGraph, ("MyGraph"))
# pr("getCurrentGraph()", conn.getCurrentGraph)
# pr("useGraph(GLOBAL)", conn.useGraph, ("GLOBAL"))
# pr("getCurrentGraph()", conn.getCurrentGraph)
# pr("useGraph(MyGraph)", conn.useGraph, ("MyGraph"))
# pr("getCurrentGraph()", conn.getCurrentGraph)
# pr("useGraph(invalid)", conn.useGraph, ("invalid"))
# pr("getCurrentGraph()", conn.getCurrentGraph)

# Schema related functions =====================================================

# Get schema metadata
# pr("getSchema()", conn.getSchema)

pr("useGraph(g1)", conn.useGraph, ("g1"))
# pr("getSchema()", conn.getSchema)
pr("useGraph(global)", conn.useGraph, ("global"))
# pr("getSchema()", conn.getSchema)
pr("useGraph(global)", conn.useGraph, ("hiwe"))
# pr("getSchema()", conn.getSchema)

# Get schema version
# pr("getSchemaVersion()", conn.getSchemaVersion)

# Vertex related functions =====================================================

# Get vertex types
# conn.getGraph()
# conn.useGlobal()
# pr("getVertexTypes", conn.getVertexTypes)
# pr("getVertexTypes", conn.getVertexTypes, (True))

# vts = conn.getVertexTypes()
# if vts:
# Get the description of a specific vertex type
# pr("getVertexType()", conn.getVertexType, (vts[0]))
# pr("getVertexType(\"<invalid>\")", conn.getVertexType, ("<invalid>"))

# Get vertex count [with filtering]
# pr("getVertexCount()", conn.getVertexCount, (vts[0]))

# pr("getVertexCount(*)", conn.getVertexCount, ("*"))
# prx("getVertexCount", conn.getVertexCount("Patient", "birth_year>2000"))

# pr("getVertexCount(*)", conn.getVertexCount, ("*"))
# conn.useGraph("abc")
# pr("getVertexCount(*)", conn.getVertexCount, ("*"))
# conn.useGraph("MyGraph")
# pr("getVertexCount(*)", conn.getVertexCount, ("*"))

# Get vertex stats
# if vts:
#     pr("getVertexStats()", conn.getVertexStats, (vts[0]))
# pr("getVertexStats()", conn.getVertexStats, ("*"))
# conn.useGraph("abc")
# pr("getVertexStats()", conn.getVertexStats, ("*"))
# conn.useGraph("MyGraph")
# pr("getVertexStats()", conn.getVertexStats, ("*"))
# conn.useGlobal()
# pr("getVertexStats()", conn.getVertexStats, ("*"))
# prx("getVertexStats()", conn.getVertexStats("*", skipNA=True))

# TODO: isTaggable()

# Index related functions ======================================================

# pr("getIndices()", conn.getIndices)
ixs = conn.getIndices()
if ixs:
    pr("getIndex()", conn.getIndex, (ixs[0]))

# Edge related functions =======================================================

# Get edge types (type names only)
# pr("getEdgeTypes", conn.getEdgeTypes())
# pr("getEdgeTypes", conn.getEdgeTypes(True))

# Get the description of a specific edge type
# pr("getEdgeType", conn.getEdgeType("CASE_IN_CITY"))
# pr("getEdgeType", conn.getEdgeType("WEATHER_STAT"))
# pr("getEdgeType", conn.getEdgeType("HopTo"))
# pr("getEdgeType", conn.getEdgeType("SessionEndPoint"))

# Get edge details
# pr("getEdgeSourceVertexType", conn2.getEdgeSourceVertexType("customer2transaction"))
# pr("getEdgeSourceVertexType", conn3.getEdgeSourceVertexType("edge_type_1"))
# pr("getEdgeSourceVertexType", conn2.getEdgeTargetVertexType("customer2transaction"))
# pr("getEdgeSourceVertexType", conn3.getEdgeTargetVertexType("edge_type_1"))
# pr("getEdgeTargetVertexType", conn2.getEdgeTargetVertexType("customer2transaction"))
# pr("getEdgeTargetVertexType", conn3.getEdgeTargetVertexType("customer2transaction"))
# pr("isDirected", conn.isDirected("CASE_IN_CITY"))
# pr("isDirected", conn.isDirected("INFECTED_BY"))
# pr("getReverseEdge", conn.getReverseEdge("CASE_IN_CITY"))
# pr("getReverseEdge", conn.getReverseEdge("INFECTED_BY"))

# Get edge count
# pr("getEdgeCount", conn.getEdgeCount("*"))

# pr("getEdgeCount", conn.getEdgeCount("BELONGS_TO_CASE"))
# pr("getEdgeCount", conn.getEdgeCount("TRAVEL_EVENT_IN", "City"))
# pr("getEdgeCount", conn.getEdgeCount("TRAVEL_EVENT_IN", "City", "Travel_Event"))
# pr("getEdgeCount", conn.getEdgeCountFrom("City", None, "TRAVEL_EVENT_IN"))
# pr("getEdgeCount", conn.getEdgeCountFrom("City", "Seongnam-si", "TRAVEL_EVENT_IN"))
# pr("getEdgeCount", conn.getEdgeCountFrom("InfectionCase", None, "CASE_IN_CITY"))
# pr("getEdgeCount", conn.getEdgeCountFrom("InfectionCase", "River of Grace Community Church", "CASE_IN_CITY"))
# pr("getEdgeCount", conn.getEdgeCountFrom("Patient", "2000000003", "PATEINT_TRAVELED"))
# pr("getEdgeCount", conn.getEdgeCountFrom("Patient", "2000000003"))
# pr("getEdgeCount", conn.getEdgeCount(edgeType="Alert_User", sourceVertexType="Alert"))
# pr("getEdgeCount", conn.getEdgeCount(edgeType="e1", sourceVertexType="v1", targetVertexType="v2"))
# pr("getEdgeCount", conn.getEdgeCount(sourceVertexType="v1", sourceVertexId="48"))
# pr("getEdgeCount", conn.getEdgeCount(sourceVertexType="User", sourceVertexId="fp3", edgeType="Alert_User"))

# Upsert edge
# pr("upsertEdge()", conn.upsertEdge("v1", "iub_g611", "e2", "v2", "srs_d_3", {"prop1": "boing"}))
# pr("upsertEdge()", conn.upsertEdge("City", "Yeoncheon-gun", "SOME_EDGE","InfectionCase", "Hansarang Convalescent Hospital"))
# pr("upsertEdge()", conn.upsertEdge("Patient", "6020000020", "SOME_EDGE","Travel_Event", "2020-02-1936.8011509127.150744"))
# pr("upsertEdge()", conn.upsertEdge("v1", "iub_g611", "e2", "v2", "srs_d_3", {"prop1": (" bumm", "+")}))
# pr("upsertEdges()", conn.upsertEdges("v1", "e2", "v2", [
#    ("iub_g3", "srs_d_2", {"prop1": "alpha", "prop2": "2019-01-01"}),
#    ("iub_g31", "srs_d_2", {"prop1": "beta", "prop2": "2019-01-02"}),
#    ("iub_g3", "srs_d_3", {"prop1": "gamma", "prop2": "2019-01-03"}),
#    ("iub_g111", "srs_d_2", {"prop1": "delta", "prop2": "2019-01-04"})
#    ]))

# Get edges
# pr("getEdges", conn.getEdges("Patient", "2000000003"))
# pr("getEdges", conn.getEdges("City", "Gyeongsan-si", "CASE_IN_CITY"))
# pr("getEdges", conn.getEdges("InfectionCase", "gym facility in Cheonan", "CASE_IN_CITY"))

# Get edge by type
# pr("getEdgesByType", conn.getEdgesByType("PROVINCE_IN_COUNTRY"))

# Get edge stats
# pr("getEdgeStats()", conn.getEdgeStats(["HopTo","SessionHopStat"]))
# pr("getEdgeStats()", conn.getEdgeStats(["HopTo","SessionHopStat"]))

# Delete edges
# pr("delEdges", conn.delEdges("v1", "26"))
# pr("delEdges", conn.delEdges("v1", "6", "e1"))
# pr("delEdges", conn.delEdges("v1", "1", "e1", "v2"))
# pr("delEdges", conn.delEdges("v1", "1", "e1", "v1", where="prop1<1437770833"))
# pr("delEdges", conn.delEdges("v1", "17", "e1", limit=2, sort="prop1"))
# pr("delEdges", conn.delEdges("v1", "17", "e1"))
# pr("delEdges", conn.delEdges("v1", "9", "e1", "v1", 15))
# pr("delEdges", conn.delEdges("v2", "20d"))

# UDT related functions ========================================================

# Get Used Defined Types
pr("getUDTs", conn.getUDTs)

# Get the description of a specific UDT
udts = conn.getUDTs()
if udts:
    pr("getUDT", conn.getUDT, (udts[0]))

# Query related functions =======================================================

# vs = conn.runInstalledQuery("get_all_trans_tabular_100", "days_history=1&max_trans=10", sizeLimit=300*1024*1024)
# pr("runInstalledQuery", conn.vertexSetToDataframe(vs[])
# pr("runInstalledQuery", conn.vertexSetToDataframe(vs[0]["T"]))
# pr("runInstalledQuery", conn.runInstalledQuery("CommonHops", "ep=109.6.13.1&fromTime=2020-04-15&toTime=2020-04-17&maxDepth=2"))
# pr("runInstalledQuery", conn.runInstalledQuery("a01_journey_atlas", {"sample_size": "100"}))

# some_variable = conn.runInstalledQuery("a01_journey_atlas", {"sample_size": "100"})

#
# gsql="INTERPRET QUERY (INT a) FOR GRAPH $graphname {\
#    PRINT a;\
# }"
# pr("runInterpretedQuery", conn.runInterpretedQuery(gsql, "a=5", timeout=50000, sizeLimit=50000))

# Token related functions =======================================================

# print(conn.apiToken)
# print(conn.authHeader)
# pr("getToken()", conn.getToken("6ngp7dohaitrsacco0rd58ptj56l58h4"))
# print(conn.apiToken)
# print(conn.authHeader)

# pr("refreshToken()", conn.refreshToken("stihe64ehp714aebabq38i9tuqrqdj3l"))
# pr("refreshToken()", conn.refreshToken("stihe64ehp714aebabq38i9tuqrqdj3l", lifetime=10*24*60*60))
# pr("refreshToken()", conn.refreshToken("stihe64ehp714aebabq38i9tuqrqdj3l","hoy"))
# pr("refreshToken()", conn.refreshToken("stihe64ehp714aebabq38i9tuqrqdj3l","5euujdcqer6lc83ehfs7erhcvn5kg2la", 3*24*60*60))
# pr("deleteToken()", conn.deleteToken("stihe64ehp714aebabq38i9tuqrqdj3l"))
# pr("deleteToken()", conn.deleteToken("stihe64ehp714aebabq38i9tuqrqdj3l","hs9n314k7dtm3tv5sovlhq27c7epbq6b"))

# Other functions ===============================================================

# echo -- Diagnostics
# https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#get-echo-and-post-echo
# Expected response: {'error': False, 'message': 'Hello GSQL'}
# pr("/echo", conn.echo())

# endpoints - List of the installed endpoints and their parameters
# https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#get-endpoints
# pr("/endpoints", conn.getEndpoints())
# res = conn.getEndpoints(builtin=True)
# for q in res:
#    print(q)
# pr("/endpoints", conn.getEndpoints(create builtin=True, dynamic=True, static=True))
# pr("/endpoints", conn.getEndpoints(dynamic=True))

# /statistics
# https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#get-statistics
# Returns real-time query performance statistics over the given time period
# pr("/statistics", conn.getStatistics())

# /version
# https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#get-version
# Returns the git versions of all components of the system
# pr("/version", conn.getVersion())
# f = open("/Users/szilardbarany/Temp/stats.txt", "w")
# f.close()
# f.write(conn.getVersion(True))

# Get version for a specific component; short or long form
# pr("getVer", conn.getVer())
# pr("getVer", conn.getVer(full=True))

# pr("getEdition()", conn.getEdition())

# pr("getLicenseInfo()", conn.getLicenseInfo())

# print(conn.gsql("ls"))
# conn.initGsql(version="2.5.2")

# pr("/api/info/config", conn.xg("/api/info/config"))
# pr("/api/schema", conn.xg("/api/schema"))
# pr("/api/info/job/style", conn.xg("/api/info/job/style"))
# pr("/gsql/querylist", conn.xg("/gsql/querylist"))
# pr("/api/query", conn.xg("/api/query"))
# pr("/gsql/datasources?graph=MyGRaph", conn.xg("/gsql/datasources?graph=MyGRaph"))
# pr("/gsql/datasources?graph=MyGRaph", conn.xg("/gsql/datasources?graph=MyGRaph"))

# pr("parseQueryOutput", conn.parseQueryOutput(conn.runInstalledQuery("getData"), False))
# pr("parseQueryOutput", conn.parseQueryOutput(conn.runInstalledQuery("test1"), False))

# srcv = [("v", "15")]
# srcv = conn.getVertices("Country")
# trgv = ("v", "17")
# vfs = []
# efs = [{"type": "e", "condition": "a > to_datetime(\"2020-01-01\")"}]
# efs = [{"type": "e", "condition": "a > now()"}]
# pr("shortestPath", conn.shortestPath(srcv, trgv, 4, vfs, efs, True))
# pr("allPaths", conn.allPaths(srcv, trgv, 4, vfs, efs))

# print(conn.gsql("create vertex v2(primary_id id int, a1 string)"))
# print(conn.gsql("drop vertex v2"))
# print(json.dumps(conn.gsql("SELECT * FROM test3 LIMIT 10"), indent=4))
# print(conn.gsql("CREATE USER"))
# print(conn.gsql("SHOW USER"))

# print(conn.gsql("CREATE QUERY test_py(/* Parameters here */) FOR GRAPH cja { \
#  /* Write query logic here */ \
#  PRINT \"test works!\"; \
# }"))
# print(conn.gsql("INSTALL QUERY test_py"))

# print(conn.gsql("use global\ncreate vertex v2(primary_id id int, a1 string)\nshow vertex v2"))

# Loading jobs ==================================================================
# pr("getDataSources()", conn.getDataSources("s3"))

# Loading jobs ==================================================================
# pr("getLoadingJobs()", conn.getLoadingJobs())

# pr("startLoadingJob()", conn.startLoadingJob("fslj1", ("f", "file", "smalldata.csv")))

# res = conn.startLoadingJob("s3lj1")
# res = conn.startLoadingJob("fslj1", ("f", "file", "newdata.csv"))
# pr("startLoadingJob()", res)
#
# time.sleep(1)
# pr("getLoadingJobStatus()", conn.getLoadingJobStatus(res["jobId"]))
#
# time.sleep(1)
# pr("pauseLoadingJob()", conn.pauseLoadingJob(res["jobId"]))
# pr("getLoadingJobStatus()", conn.getLoadingJobStatus(res["jobId"]))
#
# time.sleep(60)
# pr("resumeLoadingJob()", conn.resumeLoadingJob(res["jobId"]))
# pr("getLoadingJobStatus()", conn.getLoadingJobStatus(res["jobId"]))
#
# time.sleep(1)
# # pr("stopLoadingJob()", conn.stopLoadingJob(res["jobId"]))
# pr("getLoadingJobStatus()", conn.getLoadingJobStatus(res["jobId"]))

# pr("clearGraphStore()", conn.clearGraphStore())
# pr("getVertexTypes", conn.getVertexTypes())

# pr("dropAll()", conn.dropAll())


print("\n" + ("-" * 100) + "\nEnd")
