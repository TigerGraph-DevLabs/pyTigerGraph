"""
Base for higher level pyTigerGraph functionality:
 - Generic communication with pyTigerDriver
 - Metadata collection
 - Token management
 - Miscellaneous functions
"""

import json
import logging
import re
import sys
import time
import urllib.parse
from datetime import datetime
from typing import Union

import pyTigerDriver as pyTD
import requests

from pyTigerGraphException import TigerGraphException

GLOBAL = "GLOBAL"

SCH = "Schema"
VTS = "VertexTypes"
IXS = "Indexes"
ETS = "EdgeTypes"
QUS = "Queries"
DSS = "DataSources"
LJS = "LoadingJobs"
SCJS = "SchemaChangeJobs"
TAGS = "Tags"
UDTS = "UserDefinedTypes"
SEC = "Secret"
TOK = "ApiToken"

GSQL_PREFIX = "/gsqlserver/gsql/"


def nvl(val) -> str:
    """
    Returns stringified value or 'None' if value is None.

    :param val:
        A value of any type.
    :return:
        Stringified value or 'None' if value is None.
    """
    if not val:
        return "None"
    else:
        if isinstance(val, str):
            return "'" + val + "'"
        return str(val)


class TigerGraphBase(object):
    """Base Python wrapper for TigerGraph's REST++ and GSQL APIs."""

    def __init__(
            self, host: str = "http://localhost", graphname: str = "MyGraph", username: str = "tigergraph", password: str = "tigergraph",
            restppPort: Union[str, int] = "9000", gsPort: Union[str, int] = "14240", secret: str = "", apiToken: str = "", gsqlVersion: str = "",
            certPath: str = "", debug: bool = False):
        """
        Initiate a connection object.

        :param host:
            The IP address or hostname of the TigerGraph server, including the scheme (`http` or `https`).
        :param graphname:
            The name of the graph.
            TODO: Change default to GLOBAL? (tokens are linked to secrets linked to graphs!)
        :param username:
            The username in the TigerGraph database. Used for GSQL Server authentication.
        :param password:
            The password for the user in the TigerGraph database. Used for GSQL Server authentication.
        :param restppPort:
            The port for REST++ API requests.
        :param gsPort:
            The port for GSQL Server API requests.
        :param secret:
            A secret to request an REST++ API token. If not provided, `apiToken` will be used, if that provided.
            If neither `apiToken` nor `secret` is provided, it is assumed that the REST++ API authentication is not enabled.
            If REST++ API authentication is enabled, then it is preferred to provide secret only and let pyTigerGraph handle the token generation.
        :param apiToken:
            A REST++ API token to be used for authentication in case the REST++ API authentication is enabled.
            If neither `apiToken` nor `secret` is provided, it is assumed that the REST++ API authentication is not enabled.
        :param gsqlVersion:
            The version of GSQL client to be used. Defaults to database version.
            pyTigerGraph can detect the version from the database, but in rare cases (when the changes/fixes do not impact the GSQL functionality) no new GSQL
                version is released at the time when a new version of the database is shipped. In these cases an appropriate GSQL client version needs to be
                manually specified (typically the latest available version lesser than the database version).
            You can check the list of available GSQL clients at https://bintray.com/tigergraphecosys/tgjars/gsql_client
        :param certPath:
            The folder/directory _and_ the name of the SSL certification file where the certification should be stored.
            If certificate is not available, the underlying driver will attempt to download it from the server.
        :param debug:
            Enables debug output.
        """
        # Set up logging
        self.log = logging.getLogger("pytigergraph.base")
        logh = logging.StreamHandler(sys.stdout)
        logh.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s", "%H:%M:%S"))
        self.log.addHandler(logh)
        if debug:
            self.log.setLevel(logging.DEBUG)

        self.url = urllib.parse.urlparse(host)
        self.restPpPort = restppPort

        # Set up (first) graph
        self.graphName = graphname  # Current graph
        self.graphs = {GLOBAL: {}, self.graphName: {}}  # Details of graphs that have been accessed in the session (secret, token, schema)

        # Set up REST++ API authentication
        if secret:  # If `secret` has been provided, use it to get apiToken (preferred approach)
            self.graphs[self.graphName][SEC] = secret
            self.graphs[self.graphName][TOK] = apiToken = self.getToken(secret)
        elif apiToken:  # If `apiToken` has been provided, use it
            self.graphs[self.graphName][SEC] = ""
            self.graphs[self.graphName][TOK] = apiToken
        else:  # Neither `secret`, nor `apiToken` were provided, assume REST++ API authentication is not enabled
            self.graphs[self.graphName][SEC] = ""
            self.graphs[self.graphName][TOK] = ""

        # Set up connection
        self.log.debug(
            "Connecting (" + nvl(self.url.netloc) + ", " + nvl(username) + ", " + nvl(certPath) + ", " + nvl(gsqlVersion) + ", " + nvl(restppPort) +
            ", " + nvl(gsPort) + ", " + nvl(self.url.scheme) + ", " + nvl(graphname) + ", " + nvl(apiToken) + ")")
        self.conn = pyTD.Client(self.url.netloc, username, password, False, certPath, gsqlVersion, "", restppPort, gsPort, self.url.scheme, graphname, apiToken)

        # Collections for global object types
        self.udts = []  # The details of all UDTs (global types)
        self.dataSources = []  # The details of data sources (global objects)
        self.users = []  # The details of users (visibility depends on role)
        self.groups = []  # The details of proxy groups (visibility depends on role)

        self.debug = debug
        self._gsql_prefix_len = len(GSQL_PREFIX)

    # REST++ and GSQL Server API access ============================

    def get(self, path: str, params: dict = None, headers: dict = None, resKey: str = "results", skipCheck: bool = False) -> dict:
        """
        Generic GET method.

        TODO: parameters
        """
        self.log.debug("get(" + nvl(path) + ", " + nvl(params) + ", " + nvl(headers) + ", " + nvl(resKey) + ", " + nvl(skipCheck) + ")")

        if path.startswith(GSQL_PREFIX):
            self.log.debug("  Calling GSQL get()")
            return self.conn.Gsql.get(path[self._gsql_prefix_len:], params, headers, resKey, skipCheck)

        self.log.debug("  Calling REST++ get()")
        return self.conn.Rest.get(path, params, headers, resKey, skipCheck)

    def post(
            self, path: str, params: dict = None, data=None, headers: dict = None, resKey: str = "results", skipCheck: bool = False) -> dict:
        """
        Generic POST method.

        TODO: parameters
        """
        self.log.debug("post(" + nvl(path) + ", " + nvl(params) + ", " + nvl(headers) + ", " + nvl(resKey) + ", " + nvl(skipCheck) + ")")

        if data:
            self.log.debug("  get(): " + json.dumps(data))
        else:
            self.log.debug("  get(): no payload")

        if path.startswith(GSQL_PREFIX):
            self.log.debug("  Calling GSQL post()")
            return self.conn.Gsql.post(path[self._gsql_prefix_len:], params, data, headers, resKey, skipCheck)

        self.log.debug("  Calling REST++ post()")
        return self.conn.Rest.post(path, params, data, headers, resKey, skipCheck)

    def delete(self, path: str) -> dict:
        """
        Generic DELETE method.

        TODO: parameters
        """
        self.log.debug("post(" + nvl(path) + ")")

        if path.startswith(GSQL_PREFIX):
            self.log.debug("  Calling GSQL delete()")
            return self.conn.Gsql.delete(path[self._gsql_prefix_len:])

        self.log.debug("  Calling REST++ delete()")
        return self.conn.Rest.delete(path)

    def execute(self, query: str) -> str:
        """
        Executes an arbitrary GSQL statement.

        ⚠️ Some GSQL statements are interactive, i.e. they expect user keyboard input (and thus will make your app hung); some display a progress indicator that
            is refreshed multiple times.
        Make sure that you are executing only such GSQL statements that are not interactive and only produce real results.
        Preferably do not use this function if there is an equivalent pyTigerGraph function for it.

        :param query:
        :return:
        """
        self.log.debug("execute(" + nvl(query) + ")")

        return self.conn.Gsql.execute(query)

    # Metadata collection ======================================================

    def _getGlobalObj(self, objType: str, objName: str) -> [dict, None]:
        """
        Returns the metadata of a global object of the specified type and by the specified name.

        :param objType:
            Type of the object.
        :param objName:
            The name of the object.
        :return:
            The metadata of the object (if found; None otherwise).
        """
        self.log.debug("_getGlobal(" + nvl(objType) + ", " + nvl(objName) + ")")

        os = self.graphs[GLOBAL][SCH][objType]
        for o in os:
            if o["Name"] == objName:
                return o
        return None

    def _ls(self) -> dict:
        """
        Collects metadata for various graph objects and object types from the output of the `LS` GSQL command.

        :returns:
            (Some of the) metadata of the objects in the graph or global scope.
        """
        self.log.debug("_ls()")

        qpatt = re.compile(r"[\s\S\n]+CREATE", re.MULTILINE)

        vts = []  # Vertex types
        ixs = []  # Indices
        ets = []  # Edge types
        qus = []  # Queries
        dss = []  # Data sources
        ljs = []  # Loading jobs
        scjs = []  # Schema change jobs
        tags = []  # Tags (vertex level access control)
        udts = []  # User Defined Types

        res = self.execute('LS')

        i = 0
        while i < len(res):
            line = res[i]
            # Processing vertex types
            if "- VERTEX" in line:
                vtn = line[line.find("- VERTEX ") + 9:line.find("(")]
                vt = {"Name": vtn, "Statement": line[line.find("- ") + 2:]}
                vts.append(vt)

            # Processing indices (or indexes)
            elif line.startswith("Indexes:"):
                i += 1
                line = res[i]
                while line.lstrip().startswith("- "):
                    tmp1 = line[line.find("- ") + 2:].split(":")
                    tmp2 = tmp1[1]
                    ixn = tmp1[0]
                    vtn = tmp1[1][0:tmp2.find("(")]
                    att = tmp2[tmp2.find("(") + 1:tmp2.find(")")]
                    stmt = ""  # TODO
                    ix = {"Name": ixn, "Statement": stmt, "Vertex": vtn, "Attribute": att}
                    ixs.append(ix)
                    i = i + 1
                    line = res[i]

            # Processing edge types
            elif "- DIRECTED EDGE" in line or "- UNDIRECTED EDGE" in line:
                etn = line[line.find("EDGE") + 5:line.find("(")]
                et = {"Name": etn, "Statement": line[line.find("- ") + 2:]}
                ets.append(et)

            # Processing loading jobs
            elif "- CREATE LOADING JOB" in res[i]:
                txt = ""
                stmt = line[line.find("- ") + 2:]
                txt += stmt + "\n"
                jobName = stmt.split(" ")[3]
                i += 1
                line = res[i]
                while not ("- CREATE" in line or line.startswith("Queries")):
                    txt += line + "\n"
                    i += 1
                    line = res[i]
                txt = txt.rstrip(" \n")

                fds = []  # Filename definitions
                fd = re.findall(r"DEFINE\s+FILENAME\s+.+?;", txt.replace("\n", " "), re.IGNORECASE)
                for f in fd:
                    stmt = re.sub(r"DEFINE\s+FILENAME\s+", "", f, 0, re.IGNORECASE).rstrip(";")
                    stmt = re.split(r"\s+=\s+", stmt)
                    if len(stmt) == 2:
                        stmt = (stmt[0], stmt[1])
                    else:
                        stmt = (stmt[0], "")
                    fds.append(stmt)

                i -= 1
                ljs.append({"Name": jobName, "Statement": txt, "FilenameDefinitions": fds})

            # Processing schema change jobs
            elif "- CREATE SCHEMA_CHANGE JOB" in res[i]:
                txt = ""
                stmt = line[line.find("- ") + 2:]
                txt += stmt + "\n"
                jobName = stmt.split(" ")[3]
                i += 1
                line = res[i]
                while not ("- CREATE" in line or line.startswith("Queries")):
                    txt += line + "\n"
                    i += 1
                    line = res[i]
                txt = txt.rstrip(" \n")

                i -= 1
                scjs.append({"Name": jobName, "Statement": txt})

            # Processing queries
            elif line.startswith("Queries:"):
                i += 1
                line = res[i]
                while line.lstrip().startswith("- "):
                    qName = line[line.find("- ") + 2:line.find("(")]
                    dep = line.endswith('(deprecated)')
                    txt = self.execute("SHOW QUERY " + qName)
                    txt = re.sub(qpatt, "CREATE", "\n".join(txt))
                    qus.append({"Name": qName, "Statement": txt, "Deprecated": dep})
                    i = i + 1
                    line = res[i]

            # Processing UDTs
            #
            elif line.startswith("User defined tuples:"):
                i += 1
                line = res[i]
                while line.lstrip().startswith("- "):
                    udtName = line[line.find("- ") + 2:line.find("(")].rstrip()
                    udts.append({"Name": udtName, "Statement": "TYPEDEF TUPLE <" + line[line.find("(") + 1:-1] + "> " + udtName})
                    i = i + 1
                    line = res[i]

            # Processing data sources
            elif line.startswith("Data Sources:"):
                i += 1
                line = res[i]
                while line.lstrip().startswith("- "):
                    dsDetails = line[line.find("- ") + 2:].split()
                    dss.append({
                        "Name": dsDetails[1],
                        "Type": dsDetails[0], "Details": dsDetails[2],
                        "Statement": "CREATE DATA_SOURCE " + dsDetails[0].upper() + " " + dsDetails[1] + ' = "' + dsDetails[2].lstrip("(").rstrip(
                            ")").replace('"', "'") + '"'})
                    i = i + 1
                    line = res[i]

            # Ignoring the rest (graphs, labels, comments, empty lines, etc.)
            else:
                pass
            i += 1

        if self.graphName == GLOBAL:
            ret = {VTS: vts, IXS: ixs, ETS: ets, DSS: dss, SCJS: scjs, UDTS: udts}
        else:
            ret = {VTS: vts, IXS: ixs, ETS: ets, QUS: qus, DSS: dss, LJS: ljs, SCJS: scjs, TAGS: tags}
        return ret

    # TODO: GET /gsqlserver/gsql/queryinfo
    #       https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#get-gsqlserver-gsql-queryinfo-get-query-metadata

    def _getQueries(self):
        """
        Collects query metadata from REST++ endpoint.

        It will not retrieve data for queries that are saved but not (yet) installed.
        """
        self.log.debug("_getQueries()")

        return self.getEndpoints(dynamic=True)

    def _getTags(self) -> dict:
        """
        Returns a list of tag names defined in the graph.

        :return:
        """
        self.log.debug("_getTags()")

        ret = {}
        return ret

    def _getUDTs(self):
        """
        Collects User Defined Types (UDTs) metadata.

        UDTs are global object types (i.e. not defined in a graph).

        Endpoint:      GET /gsqlserver/gsql/udtlist
        Documentation: Not yet documented publicly.
        """
        self.log.debug("_getUDTs()")

        if GLOBAL not in self.graphs:
            self._getSchema(True)
        self.udts = self.graphs[GLOBAL][SCH][UDTS]

        res = self.get("/gsqlserver/gsql/udtlist", {"graph=": self.graphName})
        for u1 in res:
            found = False
            for u2 in self.udts:
                if u2["Name"] == u1["name"]:
                    found = True
                    u2["Fields"] = u1["fields"]
            if not found:  # This should not happen (all UDTs should already be enumerated by LS earlier)
                stmt = ""  # TODO: Generate statement
                self.udts.append({"Name": u1["name"], "Fields": u1["fields"], "Statement": stmt})

    def _getUsers(self):
        """
        Collects user metadata.
        """
        self.log.debug("_getUsers()")

        us = []
        res = self.execute("SHOW USER")
        i = 0
        while i < len(res):
            line = res[i]
            if "- Name:" in line:
                uName = line[line.find("- Name: ") + 8:]
                grs = []
                lrs = []
                ses = {}
                i += 1
                line = res[i]
                while line != "":
                    if "- Roles: " in line:
                        grs = line[line.find(": ") + 2:].split(", ")
                    elif "- GraphName: " in line:
                        gn = line[line.find(": ") + 2:]
                        i += 1
                        line = res[i]
                        lrs.append({gn: line[line.find(":") + 2:].split(", ")})
                    elif "- Secret: " in line:
                        se = line[line.find("- Secret: ") + 10:]
                        sd = {"Graph": "", "Alias": "", "Tokens": []}
                        i += 1
                        line = res[i]
                        while i < len(res) and line and "- Secret: " not in line:
                            if "- GraphName: " in line:
                                sd["Graph"] = line[line.find(": ") + 2:]
                            elif "- Alias: " in line:
                                sd["Alias"] = line[line.find(": ") + 2:]
                            elif "- Token: " in line:
                                token = line[line.find("- Token: ") + 9:line.find(" expire at: ")]
                                exp = line[line.find(" expire at: ") + 12:]
                                sd["Tokens"].append((token, exp))
                            i += 1
                            line = res[i]
                        ses[se] = sd
                        # if "- Secret: " in line:
                        i -= 1
                    i += 1
                    line = res[i]
                us.append({"Name": uName, "GlobalRoles": grs, "LocalRoles": lrs, "Secrets": ses})
            i += 1
        self.users = us

    def _getGroups(self):
        """Collects proxy group metadata."""
        gs = []
        res = self.execute("SHOW GROUP")
        i = 0
        while i < len(res):
            line = res[i]
            if "- Name:" in line:
                gName = line[line.find(": ") + 2:]
                grs = []
                lrs = []
                rule = ""
                i += 1
                line = res[i]
                while line != "":
                    if "- Roles: " in line:
                        grs = line[line.find(": ") + 2:].split(", ")
                    elif "- GraphName: " in line:
                        gn = line[line.find(": ") + 2:]
                        i += 1
                        line = res[i]
                        lrs.append({gn: line[line.find(":") + 2:].split(", ")})
                    elif "- Rule: " in line:
                        rule = line[line.find(":") + 2:]
                    i += 1
                    line = res[i]
                gs.append({"Name": gName, "Rule": rule, "GlobalRoles": grs, "LocalRoles": lrs})
            i += 1
        self.groups = gs

    def _getSchema(self, global_: bool = False):
        """
        Retrieves schema metadata from various sources.

        :param global_:
        :return:
        """
        self.log.debug("_getSchema(" + nvl(global_) + ")")

        if global_:
            graphName = self.graphName
            self.useGlobal()

            ret = self._ls()

            self.graphs[GLOBAL][SCH] = ret

            self.useGraph(graphName)
        else:
            ret = self._ls()

            # Additional details of vertex and edge types from REST++
            ret2 = self.get("/gsqlserver/gsql/schema", {"graph": self.graphName})
            # Update vertex types
            vts2 = ret2[VTS]
            for vt2 in vts2:
                for vt in ret[VTS]:
                    if vt["Name"] == vt2["Name"]:
                        vt.update(vt2)
                        # Update global vertex type details
                        for gvt in self.graphs[GLOBAL][SCH][VTS]:
                            if gvt["Name"] == vt2["Name"]:
                                gvt.update(vt2)
                        break
            # Update edge types
            ets2 = ret2[ETS]
            for et2 in ets2:
                for et in ret[ETS]:
                    if et["Name"] == et2["Name"]:
                        et.update(et2)
                        # Updating global edge type details
                        for get in self.graphs[GLOBAL][SCH][ETS]:
                            if get["Name"] == et2["Name"]:
                                get.update(et2)
                        break

            # Additional details of queries from REST++
            qus = ret[QUS]
            eps = self._getQueries()
            for ep in eps:
                epData = eps[ep]
                params = epData["parameters"]
                qName = params["query"]["default"]
                query = {}
                found = False
                # Do we have this query already on our list?
                for q in qus:
                    if q["Name"] == qName:
                        query = q
                        found = True
                        break
                if not found:  # Most likely the query is created but not installed; add to our list
                    query = {"Name": qName}
                    qus.append(query)
                params.pop("query")
                query["Parameters"] = params
                query["Endpoint"] = ep.split(" ")[1]
                query["Method"] = ep.split(" ")[0]

            # Schema version
            ret["Version"] = self.get("/graph/" + self.graphName + "/vertices/dummy", resKey="", skipCheck=True)["version"]["schema"]

            self.graphs[self.graphName][SCH] = ret

    # Graphs ===================================================================

    def getGraphs(self) -> list:
        """
        Returns the names of all graphs in the database.

        :returns:
            List of graph names ("GLOBAL" not included).

        ⚠️ `SHOW GRAPH *` currently lists all graphs, even if user does not have any role/privilege over them. This is expected to change in the future to
            show only those graphs that user has any relevant role/privilege over.
        """
        self.log.debug("getGraphs()")

        ret = self.execute("SHOW GRAPH *")
        gs = []
        for g in ret:
            if "- Graph " in g:
                gn = g[g.find("- Graph ") + 8:g.find("(")]
                gs.append(gn)
        return gs

    def getGraph(self, force: bool = False):
        """
        Returns the schema of the current graph (can be global graph).

        :param force:
            If `True`, retrieves the schema details again, otherwise returns a cached copy of the schema details (if they were already fetched previously).

        Alias for `getSchema()`.
        """
        self.log.debug("getGraph(" + nvl(force) + ")")

        return self.getSchema(force)

    def useGraph(self, graphName: str, secret: str = "", apiToken: str = ""):
        """
        Selects a graph to be used as working graph.

        :param graphName:
            The name of the graph.
        :param secret:
            A secret to request an REST++ API token. If not provided, `apiToken` will be used, if that is provided.
            Don't specify it if graph already has been used in the session; previously generated/provided token will be reused (unless you want to force the
                generation of a new token).
            If neither `apiToken` nor `secret` is provided, it is assumed that the REST++ API authentication is not enabled.
            If REST++ API authentication is enabled, then it is preferred to provide secret only and let pyTigerGraph handle the token generation.
        :param apiToken:
            A REST++ API token to be used for authentication in case the REST++ API authentication is enabled.
            Don't specify it if graph already has been used in the session; previously generated/provided token will be reused (unless you want to force the
                use of a new token).
            If neither `apiToken` nor `secret` is provided, it is assumed that the REST++ API authentication is not enabled.

        Documentation: https://docs.tigergraph.com/dev/gsql-ref/ddl-and-loading/defining-a-graph-schema#use-graph
        """
        self.log.debug("useGraph(" + nvl(graphName) + "," + nvl(secret) + "," + nvl(apiToken) + ")")

        if graphName.upper() == GLOBAL:
            self.useGlobal()
            return
        ret = self.execute("USE GRAPH " + graphName)
        if "Using graph " not in ret[0]:
            raise TigerGraphException(ret[0])
        self.graphName = self.conn.Gsql.graph
        if self.graphName not in self.graphs:
            self.graphs[self.graphName] = {}

        # Setup REST++ API authentication for newly selected graph
        if secret:  # If `secret` has been provided, use it to get apiToken (preferred approach)
            self.graphs[self.graphName][SEC] = secret
            self.graphs[self.graphName][TOK] = self.getToken(secret)
        elif apiToken:  # If `apiToken` has been provided, use it
            self.graphs[self.graphName][SEC] = ""
            self.graphs[self.graphName][TOK] = apiToken
        else:  # Neither `secret`, nor `apiToken` were provided, check if token was generated previously
            if TOK in self.graphs[self.graphName]:
                pass  # All ok, use existing token
            else:
                self.graphs[self.graphName][SEC] = ""
                self.graphs[self.graphName][TOK] = ""

        self.conn.Rest.setToken(self.graphs[self.graphName][TOK])

    def useGlobal(self):
        """
        Selects the global scope to be used as working graph.

        Documentation: https://docs.tigergraph.com/dev/gsql-ref/ddl-and-loading/defining-a-graph-schema#use-graph
        """
        self.log.debug("useGlobal()")

        self.execute("USE GLOBAL")
        self.graphName = self.conn.Gsql.graph  # Should be GLOBAL
        if not self.graphName:
            self.graphName = GLOBAL

    def getCurrentGraph(self) -> str:
        """
        Returns the name of the current graph.

        :return:
            The name of the graph currently in use (could be GLOBAL)
        """
        self.log.debug("getCurrentGraph()")

        return self.graphName

    # Schema ===================================================================

    def getSchema(self, force: bool = False) -> dict:
        """
        Returns the schema of the current graph (can be GLOBAL graph).

        :param force:
            If `True`, retrieves the schema details again, otherwise returns a cached copy of the schema details (if they were already fetched previously).

        Endpoint:      GET /gsqlserver/gsql/schema
        Documentation: https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#get-the-graph-schema-get-gsql-schema

        TODO: Investigate "/schema" that returns YAML
        """
        self.log.debug("getSchema(" + nvl(force) + ")")

        # Always get details global object types first
        if SCH not in self.graphs[GLOBAL] or force:
            self._getSchema(True)

        # Get schema details
        if self.graphName != GLOBAL:
            if SCH not in self.graphs[self.graphName] or force:
                self._getSchema()

        return self.graphs[self.graphName][SCH]

    def getSchemaVersion(self) -> [int, None]:
        """
        Retrieves the schema version.

        :return:
            The current version of the schema of the graph. None for GLOBAL.
        """
        self.log.debug("getSchemaVersion()")

        if self.graphName == GLOBAL:
            return None  # TODO Should raise exception instead?

        return int(self.graphs[self.graphName][SCH]["Version"])

    # Vertex types =============================================================

    def getVertexTypes(self, force: bool = False) -> list:
        """
        Returns the list of vertex type names of the current graph (can be global graph).

        :param force:
            If `True`, forces the retrieval the schema details again, otherwise returns a cached copy of vertex type details (if they were already fetched
                previously).
        """
        self.log.debug("getVertexTypes(" + nvl(force) + ")")

        ret = []
        for vt in self.getSchema(force=force)[VTS]:
            ret.append(vt["Name"])
        return ret

    def getVertexType(self, vertexType: str, force: bool = False) -> dict:
        """
        Returns the details of the specified vertex type.
        Works within current graph (can be global graph).

        :param vertexType:
            The name of the vertex type.
        :param force:
            If `True`, forces the retrieval the schema details again, otherwise returns a cached copy of vertex type details (if they were already fetched
                previously).
        :return:
            The details of the specified vertex type.
        """
        self.log.debug("getVertexType(" + nvl(vertexType) + ", " + nvl(force) + ")")

        for vt in self.getSchema(force=force)[VTS]:
            if vt["Name"] == vertexType:
                return vt
        return {}  # Vertex type was not found # TODO Should we raise an exception instead?

    def getVertexCount(self, vertexType: str, where: str = "") -> dict:
        """
        Returns the number of vertices.
        Works within current graph (can be global graph).

        :param vertexType:
            The name of the vertex type.
        :param where:
            Filter condition.
        :return:
            A dictionary of <vertex_type>: <vertex_count> pairs.

        Uses:
        - If `vertexType` is "*": vertex count of all vertex types (`where` cannot be specified in this case)
        - If `vertexType` is specified only: vertex count of the given type
        - If `vertexType` and `where` are specified: vertex count of the given type after filtered by `where` condition(s)

        For valid values of `where` condition, see https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#filter

        Endpoint:      GET /graph/{graph_name}/vertices
        Documentation: https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#get-graph-graph_name-vertices
        Endpoint:      POST /builtins
        Documentation: https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#stat_vertex_number
        """
        self.log.debug("getVertexCount(" + nvl(vertexType) + ", " + nvl(where) + ")")

        if self.graphName == GLOBAL:
            raise TigerGraphException("Vertex count is not available in global context")

        # If WHERE condition is not specified, use /builtins else user /vertices
        if where:
            if vertexType == "*":
                raise TigerGraphException("VertexType cannot be \"*\" if where condition is specified.", "")
            res = self.get("/graph/" + self.graphName + "/vertices/" + vertexType, {"count_only": True, "filter": where})
        else:
            data = '{"function":"stat_vertex_number","type":"' + vertexType + '"}'
            res = self.post("/builtins/" + self.graphName, data=data)
        if len(res) == 1 and res[0]["v_type"] == vertexType:
            return res[0]["count"]
        ret = {}
        for r in res:
            ret[r["v_type"]] = r["count"]
        return ret

    def getVertexStats(self, vertexTypes: str, skipNA: bool = False) -> dict:
        """
        Returns vertex attribute statistics.
        Works within current graph (can be global graph).

        :param vertexTypes:
            A single vertex type name or a list of vertex types names or '*' for all vertex types.
        :param skipNA:
            Skip those non-applicable vertices that do not have attributes or none of their attributes have statistics gathered.

        Endpoint:      POST /builtins
        Documentation: https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#stat_vertex_attr
        """
        self.log.debug("getVertexStats(" + nvl(vertexTypes) + ", " + nvl(skipNA) + ")")

        if self.graphName == GLOBAL:
            raise TigerGraphException("Vertex statistics are not available in global context")

        if vertexTypes == "*":
            vts = self.getVertexTypes()
        elif isinstance(vertexTypes, str):
            vts = [vertexTypes]
        elif isinstance(vertexTypes, list):
            vts = vertexTypes
        else:
            return {}
        ret = {}
        for vt in vts:
            data = '{"function":"stat_vertex_attr","type":"' + vt + '"}'
            res = self.post("/builtins/" + self.graphName, data=data, resKey="", skipCheck=True)
            if res["error"]:
                if "stat_vertex_attr is skip" in res["message"]:
                    if not skipNA:
                        ret[vt] = {}
                else:
                    raise TigerGraphException(res["message"], (res["code"] if "code" in res else None))
            else:
                res = res["results"]
                for r in res:
                    ret[r["v_type"]] = r["attributes"]
        return ret

    def isTaggable(self, vertexType: str):
        """Is the vertex type marked as taggable?

        :param vertexType:
            The name of the vertex type.
        """
        pass

    # Indices ==================================================================

    def getIndices(self, force: bool = False) -> list:
        """Returns the list of all index names.

        :return:
        """
        self.log.debug("getIndices(" + nvl(force) + ")")

        ret = []
        for ix in self.getSchema(force=force)[IXS]:
            ret.append(ix["Name"])
        return ret

    def getIndexes(self, force: bool = False) -> list:
        """
        Alias for getIndices()
        """
        return self.getIndices(force)

    def getVertexTypeIndices(self, vertexType: str) -> dict:
        """Returns the list of index names defined on a specific vertex type.

        :param vertexType:
            The name of the vertex type.
        :return:
        """
        pass

    def getIndex(self, indexName: str, force: bool = False) -> dict:
        """

        :param indexName:
        :param force:
        :return:
        """
        self.log.debug("getIndex(" + nvl(indexName) + ", " + nvl(force) + ")")

        for ix in self.getSchema(force=force)[IXS]:
            if ix["Name"] == indexName:
                return ix
        return {}  # Index type was not found # TODO Should we raise an exception instead?

    # Edge types ===============================================================

    def getEdgeTypes(self, force: bool = False) -> list:
        """Returns the list of edge type names of the graph.

        :param force:
            If `True`, forces the retrieval the schema details again, otherwise returns a cached copy of edge type details (if they were already fetched
                previously).
        """
        ret = []
        for et in self.getSchema(force=force)["EdgeTypes"]:
            ret.append(et["Name"])
        return ret

    def getEdgeType(self, edgeType: str, force: bool = False) -> dict:
        """Returns the details of vertex type.

        :param edgeType:
            The name of the edge type.
        :param force:
            If `True`, forces the retrieval the schema details again, otherwise returns a cached copy of edge type details (if they were already fetched
                previously).
        """
        for et in self.getSchema(force=force)["EdgeTypes"]:
            if et["Name"] == edgeType:
                return et
        return {}

    def getEdgeSourceVertexType(self, edgeType: str) -> list:
        """Returns the type(s) of the edge type's source vertex.

        :param edgeType:
            The name of the edge type.

        Returns a list of:
        - A single source vertex type name string if the edge has a single source vertex type.
        - "*" if the edge can originate from any vertex type (notation used in 2.6.1 and earlier versions).
            See https://docs.tigergraph.com/v/2.6/dev/gsql-ref/ddl-and-loading/defining-a-graph-schema#creating-an-edge-from-or-to-any-vertex-type
        - Vertex type name strings (unique values) if the edge has multiple source vertex types (notation used in 3.0 and later versions).
            Note: Even if the source vertex types were defined as "*", the REST API will list them as pairs (i.e. not as "*" in 2.6.1 and earlier versions),
                  just like as if there were defined one by one (e.g. `FROM v1, TO v2 | FROM v3, TO v4 | …`).
            Note: The returned set contains all source vertex types, but does not certainly mean that the edge is defined between all source and all target
                  vertex types. You need to look at the individual source/target pairs to find out which combinations are valid/defined.
        """
        edgeTypeDetails = self.getEdgeType(edgeType)

        # Edge type with a single source vertex type
        if edgeTypeDetails["FromVertexTypeName"] != "*":
            return [edgeTypeDetails["FromVertexTypeName"]]

        # Edge type with multiple source vertex types
        if "EdgePairs" in edgeTypeDetails:
            # v3.0 and later notation
            vts = set()
            for ep in edgeTypeDetails["EdgePairs"]:
                vts.add(ep["From"])
            return list(vts)
        else:
            # 2.6.1 and earlier notation
            return ["*"]

    def getEdgeTargetVertexType(self, edgeType) -> list:
        """Returns the type(s) of the edge type's target vertex.

        :param edgeType:
            The name of the edge type.

        Returns a list of:
        - A single target vertex type name string if the edge has a single target vertex type.
        - "*" if the edge can end in any vertex type (notation used in 2.6.1 and earlier versions).
            See https://docs.tigergraph.com/v/2.6/dev/gsql-ref/ddl-and-loading/defining-a-graph-schema#creating-an-edge-from-or-to-any-vertex-type
        - Vertex type name strings (unique values) if the edge has multiple target vertex types (notation used in 3.0 and later versions).
            Note: Even if the target vertex types were defined as "*", the REST API will list them as pairs (i.e. not as "*" in 2.6.1 and earlier versions),
                  just like as if there were defined one by one (e.g. `FROM v1, TO v2 | FROM v3, TO v4 | …`).
            Note: The returned set contains all target vertex types, but does not certainly mean that the edge is defined between all source and all target
                  vertex types. You need to look at the individual source/target pairs to find out which combinations are valid/defined..
        """
        edgeTypeDetails = self.getEdgeType(edgeType)

        # Edge type with a single target vertex type
        if edgeTypeDetails["ToVertexTypeName"] != "*":
            return [edgeTypeDetails["ToVertexTypeName"]]

        # Edge type with multiple target vertex types
        if "EdgePairs" in edgeTypeDetails:
            # v3.0 and later notation
            vts = set()
            for ep in edgeTypeDetails["EdgePairs"]:
                vts.add(ep["To"])
            return list(vts)
        else:
            # 2.6.1 and earlier notation
            return ["*"]

    def isDirected(self, edgeType) -> bool:
        """Is the specified edge type directed?

        :param edgeType:
            The name of the edge type.
        """
        return self.getEdgeType(edgeType)["IsDirected"]

    def getReverseEdge(self, edgeType) -> str:
        """Returns the name of the reverse edge of the specified edge type, if applicable.

        :param edgeType:
            The name of the edge type.
        """
        if not self.isDirected(edgeType):
            return ""
        config = self.getEdgeType(edgeType)["Config"]
        if "REVERSE_EDGE" in config:
            return config["REVERSE_EDGE"]
        return ""

    def getEdgeCountFrom(self, sourceVertexType=None, sourceVertexId=None, edgeType=None, targetVertexType=None, targetVertexId=None, where="") -> dict:
        """Returns the number of edges from a specific vertex.

        :param sourceVertexType:
            The type of the source vertex.
        :param sourceVertexId:
            The ID of the source vertex.
        :param edgeType:
            The name of the edge type.
        :param targetVertexType:
            The type of the target vertex.
        :param targetVertexId:
            The ID of the target vertex.
        :param where:
            Comma separated list of conditions that are all applied on each edge's attributes.
            The conditions are in logical conjunction (i.e. they are "AND'ed" together).
            See https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#filter

        Uses:
        - If `edgeType` = "*": edge count of all edge types (no other arguments can be specified in this case).
        - If `edgeType` is specified only: edge count of the given edge type.
        - If `sourceVertexType`, `edgeType`, `targetVertexType` are specified: edge count of the given edge type between source and target vertex types.
        - If `sourceVertexType`, `sourceVertexId` are specified: edge count of all edge types from the given vertex instance.
        - If `sourceVertexType`, `sourceVertexId`, `edgeType` are specified: edge count of all edge types from the given vertex instance.
        - If `sourceVertexType`, `sourceVertexId`, `edgeType`, `where` are specified: the edge count of the given edge type after filtered by `where` condition.

        If `targetVertexId` is specified, then `targetVertexType` must also be specified.
        If `targetVertexType` is specified, then `edgeType` must also be specified.

        For valid values of `where` condition, see https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#filter

        Returns a dictionary of <edge_type>: <edge_count> pairs.

        Endpoint:      GET /graph/{graph_name}/edges
        Documentation: https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#get-graph-graph_name-edges
        Endpoint:      POST /builtins
        Documentation: https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#stat_edge_number
        """
        # If WHERE condition is not specified, use /builtins else user /vertices
        if where or (sourceVertexType and sourceVertexId):
            if not sourceVertexType or not sourceVertexId:
                raise TigerGraphException("If where condition is specified, then both sourceVertexType and sourceVertexId must be provided too.", "")
            url = "/graph/" + self.graphName + "/edges/" + sourceVertexType + "/" + str(sourceVertexId)
            if edgeType:
                url += "/" + edgeType
                if targetVertexType:
                    url += "/" + targetVertexType
                    if targetVertexId:
                        url += "/" + str(targetVertexId)
            params = {"count_only": True}
            if where:
                params["filter"] = where
            res = self.get(url, params)
        else:
            if not edgeType:  # TODO is this a valid check?
                raise TigerGraphException("A valid edge type or \"*\" must be specified for edge type.", "")
            data = '{"function":"stat_edge_number","type":"' + edgeType + '"' \
                   + (',"from_type":"' + sourceVertexType + '"' if sourceVertexType else '') \
                   + (',"to_type":"' + targetVertexType + '"' if targetVertexType else '') \
                   + '}'
            res = self.post("/builtins/" + self.graphName, data=data)
        if len(res) == 1 and res[0]["e_type"] == edgeType:
            return res[0]["count"]
        ret = {}
        for r in res:
            ret[r["e_type"]] = r["count"]
        return ret

    def getEdgeCount(self, edgeType="*", sourceVertexType="", targetVertexType="") -> dict:
        """Returns the number of edges of an edge type.

        :param edgeType:
            The name of the edge type.
        :param sourceVertexType:
            The type of the source vertex.
        :param targetVertexType:
            The type of the target vertex.

        This is a simplified version of `getEdgeCountFrom`, to be used when the total number of edges of a given type is needed, regardless which vertex
            instance they are originated from.
        See documentation of `getEdgeCountFrom` above for more details.
        """
        return self.getEdgeCountFrom(edgeType=edgeType, sourceVertexType=sourceVertexType, targetVertexType=targetVertexType)

    def getEdgeStats(self, edgeTypes, skipNA=False) -> dict:
        """Returns edge attribute statistics.

        :param str|list edgeTypes:
            A single edge type name or a list of edges types names or '*' for all edges types.
        :param skipNA:
            Skip those edges that do not have attributes or none of their attributes have statistics gathered.

        Endpoint:      POST /builtins
        Documentation: https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#stat_edge_attr
        """
        if edgeTypes == "*":
            ets = self.getEdgeTypes()
        elif isinstance(edgeTypes, str):
            ets = [edgeTypes]
        elif isinstance(edgeTypes, list):
            ets = edgeTypes
        else:
            return {}
        ret = {}
        for et in ets:
            data = '{"function":"stat_edge_attr","type":"' + et + '","from_type":"*","to_type":"*"}'
            res = self.post("/builtins/" + self.graphName, data=data, resKey="", skipCheck=True)
            if res["error"]:
                if "stat_edge_attr is skiped" in res["message"]:
                    if not skipNA:
                        ret[et] = {}
                else:
                    raise TigerGraphException(res["message"], (res["code"] if "code" in res else None))
            else:
                res = res["results"]
                for r in res:
                    ret[r["e_type"]] = r["attributes"]
        return ret

    # User defined types =======================================================

    def getUDTs(self, force: bool = True) -> list:
        """
        Returns the list of User Defined Types (names only).

        :param force:
            If `True`, retrieves the schema details again, otherwise returns a cached copy of the schema details (if they were already fetched previously).
        :returns:
            List of UDTs names.
        """
        if not self.udts or force:
            self._getUDTs()
        ret = []
        for udt in self.udts:
            ret.append(udt["Name"])
        return ret

    def getUDT(self, udtName: str, force: bool = True) -> dict:
        """
        Returns the field details of a specific User Defined Type.

        :param udtName:
            The name of the User Defined Type.
        :param force:
            If `True`, retrieves the schema details again, otherwise returns a cached copy of the schema details (if they were already fetched previously).
        :returns:
            Details of the specified UDT.
        """
        if not self.udts or force:
            self._getUDTs()
        for udt in self.udts:
            if udt["Name"] == udtName:
                return udt
        raise TigerGraphException("Invalid UDT name: " + udtName)

    # Queries ==================================================================

    def getInstalledQueries(self):
        """
        Returns a list of installed queries.
        """
        ret = self.getEndpoints(dynamic=True)
        return ret

    # TODO What about created but _not_ installed queries?

    def getQuery(self, queryName: str):
        """

        :param queryName:
        :return:
        """
        pass

    def getRunningQueries(self):
        """
        TODO: GET /showprocesslist/{graph_name}
              https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#get-running-queries-showprocesslist-graph_name
        """
        pass

    def getQueryStatus(self, queryName: str):
        """

        :param queryName:
        :return:
        """
        pass

    # Data sources =============================================================

    def getDataSources(self, dsType=None):
        """Returns the list of data source names, optionally filtered by type (AWS S3, Kafka)

        @param str dsType:
            The type of the data sources to be listed: "s3" or "kafka". If not specified, all types included.
        """
        res = self.getSchema()["DataSources"]
        ret = []
        for ds in res:
            if ds["Type"] == dsType or not dsType:
                ret.append((ds["Name"], ds["Type"]))
        return ret

    def getDataSource(self, dsName):
        """Retrieves the details of the specified data source."""
        res = self.getSchema()["DataSources"]
        for ds in res:
            if ds["Name"] == dsName:
                return ds
        return {}

    # Loading jobs =============================================================

    def getLoadingJobs(self) -> list:
        """Returns the list of loading job names of the graph"""
        res = self.getSchema()["LoadingJobs"]
        ret = []
        for lj in res:
            ret.append(lj["Name"])
        return ret

    def getLoadingJob(self, jobName: str) -> [dict, None]:
        """Returns the details of the specified loading job.

        :param jobName:
            The name of the loading job.

        TODO: We should probably extract details (such as file definitions and destination clauses). Maybe not here, but in `getSchema()`.
        """
        res = self.getSchema()["LoadingJobs"]
        for lj in res:
            if lj["Name"] == jobName:
                return lj
        return None

    # Schema change jobs =======================================================

    def getSchemaChangeJobs(self) -> list:
        """

        :return:
        """
        ret = []
        return ret

    def getSchemaChangeJob(self, jobName: str) -> dict:
        """

        :param jobName:
        :return:
        """
        pass

    # Users ====================================================================

    def getUsers(self) -> list:
        """

        :return:
        """
        pass

    def getUser(self, userName: str) -> dict:
        """

        :param userName:
        :return:
        """
        pass

    # Proxy groups =============================================================

    def getGroups(self) -> list:
        """

        :return:
        """
        pass

    def getGroup(self, groupName: str) -> list:
        """

        :param groupName:
        :return:
        """
        pass

    # Tags =====================================================================

    def getTags(self) -> list:
        """

        :return:
        """
        pass

    # REST++ endpoints =========================================================

    def getEndpoints(self, builtin: bool = False, dynamic: bool = False, static: bool = False) -> dict:
        """
        Lists the REST++ endpoints and their parameters.

        :param builtin:
            List TigerGraph provided REST++ endpoints.
        :param dynamic:
            List endpoints generated for user installed queries.
        :param static:
            List static endpoints.

        If none of the above arguments are specified, all endpoints are listed

        Endpoint:      GET /endpoints
        Documentation: https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#get-endpoints
        """
        self.log.debug("getEndpoints(" + nvl(builtin) + ", " + nvl(dynamic) + ", " + nvl(static) + ")")

        ret = {}
        if not (builtin or dynamic or static):
            bui = dyn = sta = True
        else:
            bui = builtin
            dyn = dynamic
            sta = static
        url = "/endpoints/" + self.graphName
        if bui:
            eps = {}
            res = self.get(url, {"builtin": True}, resKey="")
            for ep in res:
                if not re.search(r" /graph/", ep) or re.search(r" /graph/{graph_name}/", ep):
                    eps[ep] = res[ep]
            ret.update(eps)
        if dyn:
            eps = {}
            res = self.get(url, {"dynamic": True}, resKey="")
            for ep in res:
                if re.search(r"^GET /query/" + self.graphName, ep):
                    eps[ep] = res[ep]
            ret.update(eps)
        if sta:
            ret.update(self.get(url, {"static": True}, resKey=""))
        return ret

    # Authentication and security ==============================================

    def getSecrets(self) -> list:
        """
        ⚠️ Consider security implications
        """
        pass

    def getTokens(self, secret: str) -> list:
        """

        :param secret:
        :return:

        ⚠️ Consider security implications
        """
        pass

    def getCurrentTokens(self) -> dict:
        """

        :return:

        ⚠️ Consider security implications
        """
        self.log.debug("getCurrentTokens()")

        apiTokens = {}
        for graph in self.graphs.keys():
            if TOK in self.graphs[graph]:
                apiTokens[graph] = self.graphs[graph][TOK]
        return apiTokens

    def getToken(self, secret: str = "", lifetime: int = 2592000) -> tuple:
        """
        Requests an authorization token.

        :param secret:
            The secret (string) generated in GSQL using `CREATE SECRET`. If not specified, use current connection's secret.
            See https://docs.tigergraph.com/admin/admin-guide/user-access-management/user-privileges-and-authentication#managing-credentials
        :param lifetime:
            Duration of token validity (in secs, default 30 days = 2,592,000 secs).
        :return:
            A tuple of (<new_token>, <expiration_timestamp_unixtime>, <expiration_timestamp_ISO8601>).
            Return value can be ignored.

        This function returns a token only if REST++ authentication is enabled. If not, an exception will be raised.
        See: https://docs.tigergraph.com/admin/admin-guide/user-access-management/user-privileges-and-authentication#rest-authentication

        Note: expiration timestamp's time zone might be different from your computer's local time zone.

        Endpoint:      GET /requesttoken
        Documentation: https://docs.tigergraph.com/dev/restpp-api/restpp-requests#requesting-a-token-with-get-requesttoken
        """
        self.log.debug("getToken(" + nvl(secret[:4] + "****" + secret[-4:]) + ", " + nvl(lifetime) + ")")

        if not secret:
            secret = self.graphs[self.graphName]["secret"]
            if not secret:  # `secret` was not previously provided
                raise TigerGraphException("Secret is not available")
            self.log.debug("  getToken(): use current secret")

        res = json.loads(requests.request(
            "GET",
            self.url.scheme + "://" + self.url.netloc + ":" + self.restPpPort + "/requesttoken?secret=" + secret + (
                "&lifetime=" + str(lifetime) if lifetime else "")).text)

        if not res["error"]:
            return res["token"], res["expiration"], datetime.utcfromtimestamp(res["expiration"]).strftime('%Y-%m-%d %H:%M:%S')

        if "Endpoint is not found from url = /requesttoken" in res["message"]:
            raise TigerGraphException("REST++ authentication is not enabled, can't generate token.", "")

        raise TigerGraphException(res["message"], (res["code"] if "code" in res else None))

    def refreshToken(self, secret: str = "", token: str = "", lifetime: int = 2592000) -> tuple:
        """
        Extends a token's lifetime.

        :param secret:
            The secret (string) generated in GSQL using `CREATE SECRET`. If not specified, use current connection's secret.
            See https://docs.tigergraph.com/admin/admin-guide/user-access-management/user-privileges-and-authentication#managing-credentials
        :param token:
            The token requested earlier. If not specified, refreshes current connection's token.
        :param lifetime:
            Duration of token validity (in secs, default 30 days = 2,592,000 secs) from current system timestamp.
        :return:
            Returns a tuple of (<token>, <expiration_timestamp_unixtime>, <expiration_timestamp_ISO8601>).
            Return value can be ignored.
            Raises exception if specified token does not exists.

        This function works only if REST++ authentication is enabled. If not, an exception will be raised.
        See: https://docs.tigergraph.com/admin/admin-guide/user-access-management/user-privileges-and-authentication#rest-authentication

        Note:
        - New expiration timestamp will be `now + lifetime` seconds, _not_ `current expiration timestamp + lifetime` seconds.
        - Expiration timestamp's time zone might be different from your computer's local time zone.

        Endpoint:      PUT /requesttoken
        Documentation: https://docs.tigergraph.com/dev/restpp-api/restpp-requests#refreshing-tokens
        """
        self.log.debug("refreshToken(" + nvl(secret[:4] + "****" + secret[-4:]) + ", " + nvl(token[:4] + "****" + token[-4:]) + ", " +
                       nvl(lifetime) + ")")

        if not secret:
            secret = self.graphs[self.graphName]["secret"]
            if not secret:  # `secret` was not previously provided
                raise TigerGraphException("Secret is not available")
            self.log.debug("  refreshToken(): use current secret")

        if not token:
            token = self.graphs[self.graphName]["apiToken"]
            if not token:  # `apiToken` was not previously provided
                raise TigerGraphException("API token is not available")
            self.log.debug("  refreshToken(): use current token")

        res = json.loads(requests.request(
            "PUT",
            self.url.scheme + "://" + self.url.netloc + ":" + self.restPpPort + "/requesttoken?secret=" + secret + "&token=" + token + (
                "&lifetime=" + str(lifetime) if lifetime else "")).text)

        if not res["error"]:
            exp = time.time() + res["expiration"]
            return res["token"], int(exp), datetime.utcfromtimestamp(exp).strftime('%Y-%m-%d %H:%M:%S')

        if "Endpoint is not found from url = /requesttoken" in res["message"]:
            raise TigerGraphException("REST++ authentication is not enabled, can't refresh token.", "")

        raise TigerGraphException(res["message"], (res["code"] if "code" in res else None))

    def deleteToken(self, secret: str, token: str = None, skipNA: bool = True):
        """
        Deletes a token.

        :param secret:
            The secret (string) generated in GSQL using `CREATE SECRET`. If not specified, use current connection's secret.
            See https://docs.tigergraph.com/admin/admin-guide/user-access-management/user-privileges-and-authentication#managing-credentials
        :param token:
            The token requested earlier. If not specified, delete current connection's token, so be careful.
        :param skipNA:
            Don't raise exception if specified token does not exist.
        :return:
            `True` if deletion was successful or token did not exist but `skipNA` was `True`; raises exception otherwise.

        This function works only if REST++ authentication is enabled. If not, an exception will be raised.
        See: https://docs.tigergraph.com/admin/admin-guide/user-access-management/user-privileges-and-authentication#rest-authentication

        Endpoint:      DELETE /requesttoken
        Documentation: https://docs.tigergraph.com/dev/restpp-api/restpp-requests#deleting-tokens
        """
        self.log.debug("deleteToken(" + nvl(secret[:4] + "****" + secret[-4:]) + ", " ", " + nvl(token[:4] + "****" + token[-4:]) + ", " +
                       nvl(skipNA) + ")")

        if not secret:
            secret = self.graphs[self.graphName]["secret"]
            if not secret:  # `secret` was not previously provided
                raise TigerGraphException("Secret is not available")
            self.log.debug("  deleteToken(): use current secret")

        if not token:
            token = self.graphs[self.graphName]["apiToken"]
            if not token:  # `apiToken` was not previously provided
                raise TigerGraphException("API token is not available")
            self.log.debug("  deleteToken(): use current token")

        res = json.loads(requests.request(
            "DELETE",
            self.url.scheme + "://" + self.url.netloc + ":" + self.restPpPort + "/requesttoken?secret=" + secret + "&token=" + token).text)

        if not res["error"]:
            return

        if res["code"] == "REST-3300" and skipNA:
            return

        if "Endpoint is not found from url = /requesttoken" in res["message"]:
            raise TigerGraphException("REST++ authentication is not enabled, can't delete token.", "")

        raise TigerGraphException(res["message"], (res["code"] if "code" in res else None))

    # Other functions ==========================================================

    def echo(self):
        """
        Pings the database.

        Expected return value is "Hello GSQL"

        Endpoint:      GET /echo  and  POST /echo
        Documentation: https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#get-echo-and-post-echo
        """
        self.log.debug("echo()")

        return self.get("/echo/" + self.graphName, resKey="message")

    def getStatistics(self, seconds: int = 10, segments: int = 10) -> dict:
        """
        Retrieves real-time query performance statistics over the given time period.

        :param seconds:
            The duration of statistic collection period (the last n seconds before the function call).
        :param segments:
            The number of segments of the latency distribution (shown in results as LatencyPercentile).
                      By default, segments is 10, meaning the percentile range 0-100% will be divided into ten equal segments: 0%-10%, 11%-20%, etc.
                      Segments must be [1, 100].

        Endpoint:      GET /statistics
        Documentation: https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#get-statistics
        """
        self.log.debug("getStatistics(" + nvl(seconds) + ", " + nvl(segments) + ")")

        if not seconds or type(seconds) != "int":
            seconds = 10
        else:
            seconds = max(min(seconds, 0), 60)
        if not segments or type(segments) != "int":
            segment = 10
        else:
            segment = max(min(segments, 0), 100)
        return self.get("/statistics/" + self.graphName, {"seconds": seconds, "segment": segment}, resKey="")

    def getVersion(self) -> list:
        """
        Retrieves the Git versions of all components of the system.

        :returns:
            A list of components with their version, hash value and build datetime.

        Endpoint:      GET /version
        Documentation: https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#get-version
        """
        self.log.debug("getVersion()")

        res = self.get("/version/" + self.graphName, resKey="")

        res = res["message"].split("\n")
        components = []
        for i in range(len(res)):
            if 2 < i < len(res) - 1:
                m = res[i].split()
                component = {"Name": m[0], "Version": m[1], "Hash": m[2], "Datetime": m[3] + " " + m[4] + " " + m[5]}
                components.append(component)
        return components

    def getVer(self, component: str = "product", full: bool = False) -> str:
        """
        Gets the version information of specific component.

        :param component:
            One of TigerGraph's components (e.g. product, gpe, gse).
        :param full:
            If `True`, returns the full build version string, instead of just the X.Y.Z version number,
        :returns:

        Get the full list of components using `getVersion`.
        """
        self.log.debug("getVer(" + nvl(component) + ", " + nvl(full) + ")")

        ret = ""
        for v in self.getVersion():
            if v["name"] == component:
                ret = v["version"]
        if ret != "":
            if full:
                return ret
            ret = re.search("_.+_", ret)
            return ret.group().strip("_")
        else:
            raise TigerGraphException("\"" + component + "\" is not a valid component.", "")

    def getEdition(self) -> str:
        """
        Gets the database edition information
        """
        self.log.debug("getEdition()")

        ret = self.get("/graph/" + self.graphName + "/vertices/dummy", resKey="", skipCheck=True)
        return ret["version"]["edition"]

    def getLicenseInfo(self) -> dict:
        """
        Returns the expiration date and remaining days of the license.

        In case of evaluation/trial deployment, an information message and -1 remaining days are returned.
        TODO This endpoint is deprecated; what is the current endpoint?
        """
        self.log.debug("getLicenseInfo()")

        res = self.get("/showlicenseinfo", resKey="", skipCheck=True)
        ret = {}
        if not res["error"]:
            ret["message"] = res["message"]
            ret["expirationDate"] = res["results"][0]["Expiration date"]
            ret["daysRemaining"] = res["results"][0]["Days remaining"]
        elif "code" in res and res["code"] == "REST-5000":
            ret["message"] = "This instance does not have a valid enterprise license. Is this a trial version?"
            ret["daysRemaining"] = -1
        else:
            raise TigerGraphException(res["message"], res["code"])
        return ret

# EOF
