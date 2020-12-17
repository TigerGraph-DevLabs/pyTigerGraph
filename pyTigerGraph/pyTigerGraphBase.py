from datetime import datetime
import json
import os
import pandas as pd
import re
import requests
import time
import urllib.parse

from pyTigerGraph.pyTigerGraphException import TigerGraphException
from pyTigerGraph.pyTigerGraphGSQL import TigerGraphGSQL


class TigerGraphBase(object):
    """Base Python wrapper for TigerGraph's REST++ and GSQL APIs."""

    VERSION_MAPPING = {
        "2.5.1": "2.5.0",
        "2.6.3": "2.6.2"
    }

    def __init__(self, host="http://localhost", graphname="MyGraph", username="tigergraph", password="tigergraph", restppPort="9000", gsPort="14240", apiToken="", gsqlVersion="", useCert=False, certPath="", debug=False):
        # TODO: configuration file 
        """Initiate a connection object.

        :param str host:
            The IP address or hostname of the TigerGraph server, including the scheme (`http` or `https`).
        :param str graphname:
            The name of the graph.
        :param str username:
            The username on the TigerGraph server.
        :param str password:
            The password for that user.
        :param str restppPort:
            The port for REST++ queries.
        :param str gsPort:
            The port of all other queries (GSQL server).
        :param str apiToken:
            A token to use when making queries. Ignored if REST++ authentication is not enabled.
        :param str gsqlVersion:
            The version of GSQL client to be used. Default to database version.
            pyTigerGraph can detect the version from the database, but in rare cases (when the changes/fixes do not impact the GSQL functionality) no new GSQL version is released
            when a new version of the database is shipper. In these cases an appropriate GSQL client version needs to be manually specified (typically the latest available version
            lesser than the database version).
            You can check the list of available GSQL clients at https://bintray.com/tigergraphecosys/tgjars/gsql_client
        :param bool useCert:
            True if you need to use a certificate because the server is secure (such as on TigerGraph
                               Cloud). This needs to be False when connecting to an unsecure server such as a TigerGraph Developer instance.
                               When True the certificate would be downloaded when it is first needed.
        :param str certPath:
            The folder/directory _and_ the name of the SSL certification file where the certification should be stored.
        :param bool debug:
            Enables debug output.
        """

        _host = urllib.parse.urlparse(host)
        self.host = _host.scheme + "://" + _host.netloc

        self.graphname = graphname
        self.username = username
        self.password = password

        self.restppPort = str(restppPort)
        self.restppUrl = self.host + ":" + self.restppPort

        self.gsPort = str(gsPort)
        self.gsUrl = self.host + ":" + self.gsPort

        self.apiToken = apiToken
        self.authHeader = {'Authorization': "Bearer " + self.apiToken}

        self.debug = debug
        self.schema = None

        self.ttkGetEF = None  # TODO: this needs to be rethought, or at least renamed

        if gsqlVersion:
            self.gsqlVersion = gsqlVersion
            if self.gsqlVersion in self.VERSION_MAPPING:
                self.gsqlVersion = self.VERSION_MAPPING[self.gsqlVersion]
        else:
            self.gsqlVersion = self.getVer()

        self.tgDir = os.path.expanduser(os.path.join("~", ".tigergraph"))  # TODO This is where we should look for the config file

        self.certDownloaded = False
        self.useCert = useCert  # TODO: if self._host.scheme == 'https' and userCert == False, should we throw exception here or let it be thrown later when gsql is called?
        self.certPath = certPath
        if not self.certPath:
            self.certPath = os.path.join("~", ".tigergraph", _host.netloc.replace(".", "_") + "-" + self.graphname + "-cert.txt")
        self.certPath = os.path.expanduser(self.certPath)

        self.gsql = TigerGraphGSQL(self.host, self.graphname, self.username, self.password, self.gsqlVersion, self.useCert, self.certPath)

    # Private functions ========================================================

    def _errorCheck(self, res: dict) -> None:
        """Checks if the JSON document returned by an endpoint has contains error: true; if so, it raises an exception.

        :param res:
            The JSON document returned by an endpoint
        """
        if "error" in res and res["error"] and res["error"] != "false":  # Endpoint might return string "false" rather than Boolean false
            raise TigerGraphException(res["message"], (res["code"] if "code" in res else None))

    def _req(self, method: str, url: str, authMode: str = "token", headers: dict = None, data: str = None, resKey: str = "results", skipCheck: bool = False, params: [str, dict, list] = None) -> dict:
        """Generic REST++ API request

        :param method:
            HTTP method, currently one of GET, POST or DELETE.
        :param url:
            Complete REST++ API URL including path and parameters.
        :param authMode:
            Authentication mode, one of 'token' (default) or 'pwd'.
        :param dict headers:
            Standard HTTP request headers.
        :param data:
            Request payload, typically a JSON document.
        :param resKey:
            The key to the JSON subdocument to be returned, default is 'result'.
        :param skipCheck:
            Skip error checking? Some endpoints return error to indicate that the requested action is not applicable; a problem, but not really an error.
        :param params:
            Request URL parameters.
        """
        if self.debug:
            print(method + " " + url + ("\n" + json.dumps(data, indent=2) if data else ""))
        if authMode == "pwd":
            _auth = (self.username, self.password)
        else:
            _auth = None
        if authMode == "token":
            _headers = self.authHeader
        else:
            _headers = {}
        if headers:
            _headers.update(headers)
        if method == "POST":
            _data = data  # TODO: check content type and convert from JSON if necessary
        else:
            _data = None

        if False and self.useCert and self.certDownloaded:
            res = requests.request(method, url, auth=_auth, headers=_headers, data=_data, params=params, verify=self.certPath)
        else:
            res = requests.request(method, url, auth=_auth, headers=_headers, data=_data, params=params)

        if self.debug:
            print(res.url)
        if res.status_code != 200:
            res.raise_for_status()
        res = json.loads(res.text)
        if not skipCheck:
            self._errorCheck(res)
        if not resKey:
            if self.debug:
                print(res)
            return res
        if self.debug:
            print(res[resKey])
        return res[resKey]

    def _get(self, url, authMode="token", headers=None, resKey="results", skipCheck=False, params=None):
        """Generic GET method.

        :param str url:
            Complete REST++ API URL including path and parameters.
        :param str authMode:
            Authentication mode, one of 'token' (default) or 'pwd'.
        :param dict headers:
            Standard HTTP request headers.
        :param str resKey:
            The key to the JSON subdocument to be returned, default is 'result'.
        :param bool skipCheck:
            Skip error checking? Some endpoints return error to indicate that the requested action is not applicable; a problem, but not really an error.
        :param str|dict|list params:
            Request URL parameters.
        """
        return self._req("GET", url, authMode, headers, "", resKey, skipCheck, params)

    def _post(self, url, authMode="token", headers=None, data=None, resKey="results", skipCheck=False, params=None):
        """Generic POST method.

        :param str url:
            Complete REST++ API URL including path and parameters.
        :param str authMode:
            Authentication mode, one of 'token' (default) or 'pwd'.
        :param dict headers:
            Standard HTTP request headers.
        :param str data:
            Request payload, typically a JSON document.
        :param str resKey:
            The key to the JSON subdocument to be returned, default is 'result'.
        :param bool skipCheck:
            Skip error checking? Some endpoints return error to indicate that the requested action is not applicable; a problem, but not really an error.
        :param str|dict|list params:
            Request URL parameters.
        """
        return self._req("POST", url, authMode, headers, data, resKey, skipCheck, params)

    def _delete(self, url, authMode="token"):
        """Generic DELETE method.

        :param str url:
            Complete REST++ API URL including path and parameters.
        :param str authMode:
            Authentication mode, one of 'token' (default) or 'pwd'.
        """
        return self._req("DELETE", url, authMode)

    # Metadata related functions ===============================================

    def _getUDTs(self):
        """Collects User Defined Types (UDTs) metadata.

        Endpoint:      GET /gsqlserver/gsql/udtlist
        Documentation: Not documented publicly
        """
        res = self._get(self.gsUrl + "/gsqlserver/gsql/udtlist?graph=" + self.graphname, authMode="pwd")
        for u in res:
            u["Name"] = u["name"]
            u.pop("name")
            u["Fields"] = u["fields"]
            u.pop("fields")
        self.schema["UDTs"] = res

    def _getSchemaLs(self):
        """Collects metadata for various schema object types from the output of the `ls` gsql command."""

        res = self.gsql.execute('ls')

        for objType in ["VertexTypes", "EdgeTypes", "Indexes", "Queries", "LoadingJobs", "DataSources", "Graphs"]:
            if objType not in self.schema:
                self.schema[objType] = []

        qpatt = re.compile(r"[\s\S\n]+CREATE", re.MULTILINE)

        res = res.split("\n")
        i = 0
        while i < len(res):
            line = res[i]
            # Processing vertices
            if line.startswith("  - VERTEX"):
                vts = self.schema["VertexTypes"]
                vtName = line[11:line.find("(")]
                for vt in vts:
                    if vt["Name"] == vtName:
                        vt["Statement"] = "CREATE " + line[4:]
                        break

            # Processing edges
            elif line.startswith("  - DIRECTED") or line.startswith("  - UNDIRECTED"):
                ets = self.schema["EdgeTypes"]
                etName = line[line.find("EDGE") + 5:line.find("(")]
                for et in ets:
                    if et["Name"] == etName:
                        et["Statement"] = "CREATE " + line[4:]
                        break

            # Processing indices (or indexes)
            elif line.startswith("Indexes:"):
                i += 1
                line = res[i]
                idxs = self.schema["Indexes"]
                while line != "":
                    l2 = line[4:].split(":")
                    l21 = l2[1]
                    idx = {"Name": l2[0], "Vertex": l2[1][0:l21.find("(")], "Attribute": l21[l21.find("(") + 1:l21.find(")")]}
                    idxs.append(idx)
                    i = i + 1
                    line = res[i]

            # Processing loading jobs
            elif res[i].startswith("  - CREATE LOADING JOB"):
                txt = ""
                tmp = line[4:]
                txt += tmp + "\n"
                jobName = tmp.split(" ")[3]
                i += 1
                line = res[i]
                while not (line.startswith("  - CREATE") or line.startswith("Queries")):
                    txt += line[4:] + "\n"
                    i += 1
                    line = res[i]
                txt = txt.rstrip(" \n")

                fds = []
                fd = re.findall(r"define\s+filename\s+.+?;", txt.replace("\n", " "), re.IGNORECASE)
                for f in fd:
                    tmp = re.sub(r"define\s+filename\s+", "", f, 0, re.IGNORECASE).rstrip(";")
                    tmp = re.split(r"\s+=\s+", tmp)
                    if len(tmp) == 2:
                        tmp = (tmp[0], tmp[1])
                    else:
                        tmp = (tmp[0], "")
                    fds.append(tmp)

                i -= 1
                self.schema["LoadingJobs"].append({"Name": jobName, "FilenameDefinitions": fds, "Statement": txt})

            # Processing queries
            elif line.startswith("Queries:"):
                i += 1
                line = res[i]
                while line != "":
                    qName = line[4:line.find("(")]
                    dep = line.endswith('(deprecated)')
                    txt = self.gsql.execute("SHOW QUERY " + qName).rstrip(" \n")
                    txt = re.sub(qpatt, "CREATE", txt)
                    qs = self.schema["Queries"]
                    found = False
                    for q in qs:
                        if q["Name"] == qName:
                            q["Statement"] = txt
                            q["Deprecated"] = dep
                            found = True
                            break
                    if not found:  # Most likely the query is created but not installed
                        qs.append({"Name": qName, "Statement": txt, "Deprecated": dep})
                    i = i + 1
                    line = res[i]

            # Processing UDTs
            elif line.startswith("User defined tuples:"):
                i += 1
                line = res[i]
                while line != "":
                    udtName = line[4:line.find("(")].rstrip()
                    us = self.schema["UDTs"]
                    for u in us:
                        if u["Name"] == udtName:
                            u["Statement"] = "TYPEDEF TUPLE <" + line[line.find("(") + 1:-1] + "> " + udtName
                            break
                    i = i + 1
                    line = res[i]

            # Processing data sources
            elif line.startswith("Data Sources:"):
                i += 1
                line = res[i]
                while line != "":
                    dsDetails = line[4:].split()
                    ds = {"Name": dsDetails[1], "Type": dsDetails[0], "Details": dsDetails[2], "Statement": [
                        "CREATE DATA_SOURCE " + dsDetails[0].upper() + " " + dsDetails[1] + ' = "' + dsDetails[2].lstrip("(").rstrip(")").replace('"', "'") + '"',
                        "GRANT DATA_SOURCE " + dsDetails[1] + " TO GRAPH " + self.graphname
                    ]}
                    self.schema["DataSources"].append(ds)
                    i = i + 1
                    line = res[i]

            # Processing graphs (actually, only one graph should be listed)
            elif line.startswith("  - Graph"):
                gName = line[10:line.find("(")]
                self.schema["Graphs"].append({"Name": gName, "Statement": "CREATE GRAPH " + line[10:].replace(":v", "").replace(":e", ""), "Text": line[10:]})

            # Ignoring the rest (schema change jobs, labels, comments, empty lines, etc.)
            else:
                pass
            i += 1

    # TODO: GET /gsqlserver/gsql/queryinfo
    #       https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#get-gsqlserver-gsql-queryinfo-get-query-metadata

    def _getQueries(self):
        """Collects query metadata from REST++ endpoint.

        It will not return data for queries that are saved but not (yet) installed.
        """
        qs = self.schema["Queries"]
        eps = self.getEndpoints(dynamic=True)
        for ep in eps:
            e = eps[ep]
            params = e["parameters"]
            qName = params["query"]["default"]
            query = {}
            found = False
            # Do we have this query already on our list?
            for q in qs:
                if q["Name"] == qName:
                    query = q
                    found = True
                    break
            if not found:  # Most likely the query is created but not installed; add to our list
                query = {"Name": qName}
                qs.append(query)
            params.pop("query")
            query["Parameters"] = params
            query["Endpoint"] = ep.split(" ")[1]
            query["Method"] = ep.split(" ")[0]

    def _getUsers(self):
        """Collects user metedata."""
        us = []
        res = self.gsql.execute("SHOW USER")
        res = res.split("\n")
        i = 0
        while i < len(res):
            line = res[i]
            if "- Name:" in line:
                if "tigergraph" in line:
                    i += 1
                    line = res[i]
                    while line != "":
                        i += 1
                        line = res[i]
                else:
                    uName = line[10:]
                    roles = []
                    i += 1
                    line = res[i]
                    while line != "":
                        if "- GraphName: " + self.graphname in line:
                            i += 1
                            line = res[i]
                            roles = line[line.find(":") + 2:].split(", ")
                        i += 1
                        line = res[i]
                    us.append({"Name": uName, "Roles": roles})
            i += 1
        self.schema["Users"] = us

    def _getGroups(self):
        """Collects proxy group metadata."""
        gs = []
        res = self.gsql.execute("SHOW GROUP")
        res = res.split("\n")
        i = 0
        while i < len(res):
            line = res[i]
            if "- Name:" in line:
                gName = line[10:]
                roles = []
                rule = ""
                i += 1
                line = res[i]
                while line != "":
                    if "- GraphName: " + self.graphname in line:
                        i += 1
                        line = res[i]
                        roles = line[line.find(":") + 2:].split(", ")
                    elif "- Rule: " in line:
                        rule = line[line.find(":") + 2:]
                    i += 1
                    line = res[i]
                gs.append({"Name": gName, "Roles": roles, "Rule": rule})
            i += 1
        self.schema["Groups"] = gs

    def getSchema(self, full=True, force=False) -> dict:
        """Retrieves the schema of the graph.

        :param bool full:
            If `False`, only metadata of vertices and edges is retrieved, not for other object types, unless those have been fetched previously (with `full=True`).
        :param bool force:
            If `True`, retrieves the schema details again, otherwise returns a cached copy of the schema details (if they were already fetched previously).

        Endpoint:      GET /gsqlserver/gsql/schema
        Documentation: https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#get-the-graph-schema-get-gsql-schema
        """
        if not self.schema or force:
            self.schema = self._get(self.gsUrl + "/gsqlserver/gsql/schema?graph=" + self.graphname, authMode="pwd")
            ret = self._get(self.restppUrl + "/graph/" + self.graphname + "/vertices/dummy", resKey="", skipCheck=True)
            self.schema["Version"] = ret["version"]["schema"]
        if full:
            if "UDTs" not in self.schema or force:
                self._getUDTs()
            if "Queries" not in self.schema or force:
                self._getSchemaLs()
                self._getQueries()
            if "Users" not in self.schema or force:
                self._getUsers()
            if "Groups" not in self.schema or force:
                self._getGroups()
        return self.schema

    def getSchemaVersion(self, force=False) -> str:
        """Retrieves the schema version.

        :param bool force:
            If `True`, retrieves the schema details again, otherwise returns a cached copy of the schema details (if they were already fetched previously).
        """
        return self.getSchema(force=force)["Version"]

    def getUDTs(self) -> list:
        """Returns the list of User Defined Types (names only)."""
        if "UDTs" not in self.schema:
            self.getSchema()
        ret = []
        for udt in self.schema["UDTs"]:
            ret.append(udt["name"])
        return ret

    def getUDT(self, udtName) -> list:
        """Returns the field details of a specific User Defined Type.

        :param str udtName:
            The name of the User Defined Type.
        """
        if "UDTs" not in self.schema:
            self.getSchema()
        for udt in self.schema["UDTs"]:
            if udt["name"] == udtName:
                return udt["fields"]
        return []  # UDT was not found

    # Vertex related functions =================================================

    def getVertexTypes(self, force=False) -> list:
        """Returns the list of vertex type names of the graph.

        :param bool force:
            If `True`, forces the retrieval the schema details again, otherwise returns a cached copy of vertex type details (if they were already fetched previously).
        """
        ret = []
        for vt in self.getSchema(force=force)["VertexTypes"]:
            ret.append(vt["Name"])
        return ret

    def getVertexType(self, vertexType, force=False) -> dict:
        """Returns the details of the specified vertex type.

        :param str vertexType:
            The name of the vertex type.
        :param bool force:
            If `True`, forces the retrieval the schema details again, otherwise returns a cached copy of vertex type details (if they were already fetched previously).
        """
        for vt in self.getSchema(force=force)["VertexTypes"]:
            if vt["Name"] == vertexType:
                return vt
        return {}  # Vertex type was not found

    def getVertexCount(self, vertexType, where="") -> dict:
        """Returns the number of vertices.

        :param str vertexType:
            The name of the vertex type.
        :param str where:
            Filter condition.

        Uses:
        - If `vertexType` is "*": vertex count of all vertex types (`where` cannot be specified in this case)
        - If `vertexType` is specified only: vertex count of the given type
        - If `vertexType` and `where` are specified: vertex count of the given type after filtered by `where` condition(s)

        For valid values of `where` condition, see https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#filter

        Returns a dictionary of <vertex_type>: <vertex_count> pairs.

        Endpoint:      GET /graph/{graph_name}/vertices
        Documentation: https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#get-graph-graph_name-vertices
        Endpoint:      POST /builtins
        Documentation: https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#stat_vertex_number
        """
        # If WHERE condition is not specified, use /builtins else user /vertices
        if where:
            if vertexType == "*":
                raise TigerGraphException("VertexType cannot be \"*\" if where condition is specified.", None)
            res = self._get(self.restppUrl + "/graph/" + self.graphname + "/vertices/" + vertexType + "?count_only=true&filter=" + where)
        else:
            data = '{"function":"stat_vertex_number","type":"' + vertexType + '"}'
            res = self._post(self.restppUrl + "/builtins/" + self.graphname, data=data)
        if len(res) == 1 and res[0]["v_type"] == vertexType:
            return res[0]["count"]
        ret = {}
        for r in res:
            ret[r["v_type"]] = r["count"]
        return ret

    def getVertexStats(self, vertexTypes, skipNA=False) -> dict:
        """Returns vertex attribute statistics.

        :param str vertexTypes:
            A single vertex type name or a list of vertex types names or '*' for all vertex types.
        :param bool skipNA:
            Skip those non-applicable vertices that do not have attributes or none of their attributes have statistics gathered.

        Endpoint:      POST /builtins
        Documentation: https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#stat_vertex_attr
        """
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
            res = self._post(self.restppUrl + "/builtins/" + self.graphname, data=data, resKey="", skipCheck=True)
            if res["error"]:
                if "stat_vertex_attr is skipped" in res["message"]:
                    if not skipNA:
                        ret[vt] = {}
                else:
                    raise TigerGraphException(res["message"], (res["code"] if "code" in res else None))
            else:
                res = res["results"]
                for r in res:
                    ret[r["v_type"]] = r["attributes"]
        return ret

    # Edge related functions ===================================================

    def getEdgeTypes(self, force=False) -> list:
        """Returns the list of edge type names of the graph.

        :param bool force:
            If `True`, forces the retrieval the schema details again, otherwise returns a cached copy of edge type details (if they were already fetched previously).
        """
        ret = []
        for et in self.getSchema(force=force)["EdgeTypes"]:
            ret.append(et["Name"])
        return ret

    def getEdgeType(self, edgeType, force=False) -> dict:
        """Returns the details of vertex type.

        :param str edgeType:
            The name of the edge type.
        :param bool force:
            If `True`, forces the retrieval the schema details again, otherwise returns a cached copy of edge type details (if they were already fetched previously).
        """
        for et in self.getSchema(force=force)["EdgeTypes"]:
            if et["Name"] == edgeType:
                return et
        return {}

    def getEdgeSourceVertexType(self, edgeType) -> list:
        """Returns the type(s) of the edge type's source vertex.

        :param str edgeType:
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

        :param str edgeType:
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

        :param str edgeType:
            The name of the edge type.
        """
        return self.getEdgeType(edgeType)["IsDirected"]

    def getReverseEdge(self, edgeType) -> str:
        """Returns the name of the reverse edge of the specified edge type, if applicable.

        :param str edgeType:
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

        :param str sourceVertexType:
            The type of the source vertex.
        :param str sourceVertexId:
            The ID of the source vertex.
        :param str edgeType:
            The name of the edge type.
        :param str targetVertexType:
            The type of the target vertex.
        :param str targetVertexId:
            The ID of the target vertex.
        :param str where:
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
                raise TigerGraphException("If where condition is specified, then both sourceVertexType and sourceVertexId must be provided too.", None)
            url = self.restppUrl + "/graph/" + self.graphname + "/edges/" + sourceVertexType + "/" + str(sourceVertexId)
            if edgeType:
                url += "/" + edgeType
                if targetVertexType:
                    url += "/" + targetVertexType
                    if targetVertexId:
                        url += "/" + str(targetVertexId)
            url += "?count_only=true"
            if where:
                url += "&filter=" + where
            res = self._get(url)
        else:
            if not edgeType:  # TODO is this a valid check?
                raise TigerGraphException("A valid edge type or \"*\" must be specified for edge type.", None)
            data = '{"function":"stat_edge_number","type":"' + edgeType + '"' \
                   + (',"from_type":"' + sourceVertexType + '"' if sourceVertexType else '') \
                   + (',"to_type":"' + targetVertexType + '"' if targetVertexType else '') \
                   + '}'
            res = self._post(self.restppUrl + "/builtins/" + self.graphname, data=data)
        if len(res) == 1 and res[0]["e_type"] == edgeType:
            return res[0]["count"]
        ret = {}
        for r in res:
            ret[r["e_type"]] = r["count"]
        return ret

    def getEdgeCount(self, edgeType="*", sourceVertexType="", targetVertexType="") -> dict:
        """Returns the number of edges of an edge type.

        :param str edgeType:
            The name of the edge type.
        :param str sourceVertexType:
            The type of the source vertex.
        :param str targetVertexType:
            The type of the target vertex.

        This is a simplified version of `getEdgeCountFrom`, to be used when the total number of edges of a given type is needed, regardless which vertex instance they are originated from.
        See documentation of `getEdgeCountFrom` above for more details.
        """
        return self.getEdgeCountFrom(edgeType=edgeType, sourceVertexType=sourceVertexType, targetVertexType=targetVertexType)

    def getEdgeStats(self, edgeTypes, skipNA=False) -> dict:
        """Returns edge attribute statistics.

        :param str|list edgeTypes:
            A single edge type name or a list of edges types names or '*' for all edges types.
        :param bool skipNA:
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
            res = self._post(self.restppUrl + "/builtins/" + self.graphname, data=data, resKey="", skipCheck=True)
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

    # Query related functions ==================================================

    def getInstalledQueries(self, fmt="py"):
        """
        Returns a list of installed queries.
        
        :param str fmt:
            Format of the results:
            - "py":   Python objects (default)
            - "json": JSON document
            - "df":   Pandas DataFrame
        """
        ret = self.getEndpoints(dynamic=True)
        if fmt == "json":
            return json.dumps(ret)
        if fmt == "df":
            return pd.DataFrame(ret).T
        return ret

    # Data sources =================================================

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

    # Loading jobs =================================================

    def getLoadingJobs(self) -> list:
        """Returns the list of loading job names of the graph"""
        res = self.getSchema()["LoadingJobs"]
        ret = []
        for lj in res:
            ret.append(lj["Name"])
        return ret

    def getLoadingJob(self, jobName):
        """Returns the details of the specified loading job.

        :param str jobName:
            The name of the loading job.

        TODO: We should probably extract details (such as file definitions and destination clauses). Maybe not here, but in `getSchema()`.
        """
        res = self.getSchema()["LoadingJobs"]
        for lj in res:
            if lj["Name"] == jobName:
                return lj
        return None

    # Authentication and security ==============================================

    def getToken(self, secret, setToken=True, lifetime=None) -> tuple:
        """Requests an authorization token.

        :param str secret:
            The secret (string) generated in GSQL using `CREATE SECRET`.
            See https://docs.tigergraph.com/admin/admin-guide/user-access-management/user-privileges-and-authentication#managing-credentials
        :param bool setToken:
            Set the connection's API token to the new value (default: true).
        :param int lifetime:
            Duration of token validity (in secs, default 30 days = 2,592,000 secs).

        This function returns a token only if REST++ authentication is enabled. If not, an exception will be raised.
        See: https://docs.tigergraph.com/admin/admin-guide/user-access-management/user-privileges-and-authentication#rest-authentication

        Returns a tuple of (<new_token>, <exporation_timestamp_unixtime>, <expiration_timestamp_ISO8601>).
                 Return value can be ignored.

        Note: expiration timestamp's time zone might be different from your computer's local time zone.

        Endpoint:      GET /requesttoken
        Documentation: https://docs.tigergraph.com/dev/restpp-api/restpp-requests#requesting-a-token-with-get-requesttoken
        """
        res = json.loads(requests.request("GET", self.restppUrl + "/requesttoken?secret=" + secret + ("&lifetime=" + str(lifetime) if lifetime else "")).text)
        if not res["error"]:
            if setToken:
                self.apiToken = res["token"]
                self.authHeader = {'Authorization': "Bearer " + self.apiToken}
            return res["token"], res["expiration"], datetime.utcfromtimestamp(res["expiration"]).strftime('%Y-%m-%d %H:%M:%S')
        if "Endpoint is not found from url = /requesttoken" in res["message"]:
            raise TigerGraphException("REST++ authentication is not enabled, can't generate token.", None)
        raise TigerGraphException(res["message"], (res["code"] if "code" in res else None))

    def refreshToken(self, secret, token=None, lifetime=2592000) -> tuple:
        """Extends a token's lifetime.

        :param str secret:
            The secret (string) generated in GSQL using `CREATE SECRET`.
            See https://docs.tigergraph.com/admin/admin-guide/user-access-management/user-privileges-and-authentication#managing-credentials
        :param str token:
            The token requested earlier. If not specified, refreshes current connection's token.
        :param int lifetime:
            Duration of token validity (in secs, default 30 days = 2,592,000 secs) from current system timestamp.

        This function works only if REST++ authentication is enabled. If not, an exception will be raised.
        See: https://docs.tigergraph.com/admin/admin-guide/user-access-management/user-privileges-and-authentication#rest-authentication

        Returns a tuple of (<token>, <exporation_timestamp_unixtime>, <expiration_timestamp_ISO8601>).
                 Return value can be ignored.
                 Raises exception if specified token does not exists.

        Note:
        - New expiration timestamp will be `now + lifetime` seconds, _not_ `current expiration timestamp + lifetime` seconds.
        - Expiration timestamp's time zone might be different from your computer's local time zone.

        Endpoint:      PUT /requesttoken
        Documentation: https://docs.tigergraph.com/dev/restpp-api/restpp-requests#refreshing-tokens
        """
        if not token:
            token = self.apiToken
        res = json.loads(requests.request("PUT", self.restppUrl + "/requesttoken?secret=" + secret + "&token=" + token + ("&lifetime=" + str(lifetime) if lifetime else "")).text)
        if not res["error"]:
            exp = time.time() + res["expiration"]
            return res["token"], int(exp), datetime.utcfromtimestamp(exp).strftime('%Y-%m-%d %H:%M:%S')
        if "Endpoint is not found from url = /requesttoken" in res["message"]:
            raise TigerGraphException("REST++ authentication is not enabled, can't refresh token.", None)
        raise TigerGraphException(res["message"], (res["code"] if "code" in res else None))

    def deleteToken(self, secret, token=None, skipNA=True) -> bool:
        """Deletes a token.

        :param str secret:
            The secret (string) generated in GSQL using `CREATE SECRET`.
            See https://docs.tigergraph.com/admin/admin-guide/user-access-management/user-privileges-and-authentication#managing-credentials
        :param str token:
            The token requested earlier. If not specified, deletes current connection's token, so be careful.
        :param bool skipNA:
            Don't raise exception if specified token does not exist.

        This function works only if REST++ authentication is enabled. If not, an exception will be raised.
        See: https://docs.tigergraph.com/admin/admin-guide/user-access-management/user-privileges-and-authentication#rest-authentication

        Returns `True` if deletion was successful or token did not exist but `skipNA` was `True`; raises exception otherwise.

        Endpoint:      DELETE /requesttoken
        Documentation: https://docs.tigergraph.com/dev/restpp-api/restpp-requests#deleting-tokens
        """
        if not token:
            token = self.apiToken
        res = json.loads(requests.request("DELETE", self.restppUrl + "/requesttoken?secret=" + secret + "&token=" + token).text)
        if not res["error"]:
            return True
        if res["code"] == "REST-3300" and skipNA:
            return True
        if "Endpoint is not found from url = /requesttoken" in res["message"]:
            raise TigerGraphException("REST++ authentication is not enabled, can't delete token.", None)
        raise TigerGraphException(res["message"], (res["code"] if "code" in res else None))

    def createSecret(self, alias="") -> str:
        """Issues a `CREATE SECRET` GSQL statement and returns the secret generated by that statement.

        :param str alias:
            The alias for the secret.
        """
        response = self.gsql.execute("CREATE SECRET " + alias)
        try:
            secret = re.search(r'The secret: (\w*)', response.replace('\n', ''))[1]
            return secret
        except re.error:
            return None

    # TODO: showSecret()

    # Other functions ==========================================================

    def echo(self):
        """Pings the database.

        Expected return value is "Hello GSQL"

        Endpoint:      GET /echo  and  POST /echo
        Documentation: https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#get-echo-and-post-echo
        """
        return self._get(self.restppUrl + "/echo/" + self.graphname, resKey="message")

    def getEndpoints(self, builtin=False, dynamic=False, static=False) -> dict:
        """Lists the REST++ endpoints and their parameters.

        :param bool builtin:
            List TigerGraph provided REST++ endpoints.
        :param bool dymamic:
            List endpoints generated for user installed queries.
        :param bool static:
            List static endpoints.

        If none of the above arguments are specified, all endpoints are listed

        Endpoint:      GET /endpoints
        Documentation: https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#get-endpoints
        """
        ret = {}
        if not (builtin or dynamic or static):
            bui = dyn = sta = True
        else:
            bui = builtin
            dyn = dynamic
            sta = static
        url = self.restppUrl + "/endpoints/" + self.graphname + "?"
        if bui:
            eps = {}
            res = self._get(url + "builtin=true", resKey="")
            for ep in res:
                if not re.search(r" /graph/", ep) or re.search(r" /graph/{graph_name}/", ep):
                    eps[ep] = res[ep]
            ret.update(eps)
        if dyn:
            eps = {}
            res = self._get(url + "dynamic=true", resKey="")
            for ep in res:
                if re.search(r"^GET /query/" + self.graphname, ep):
                    eps[ep] = res[ep]
            ret.update(eps)
        if sta:
            ret.update(self._get(url + "static=true", resKey=""))
        return ret

    def getStatistics(self, seconds=10, segment=10) -> dict:
        """Retrieves real-time query performance statistics over the given time period.

        :param int seconds:
            The duration of statistic collection period (the last n seconds before the function call).
        :param int segments:
            The number of segments of the latency distribution (shown in results as LatencyPercentile).
                      By default, segments is 10, meaning the percentile range 0-100% will be divided into ten equal segments: 0%-10%, 11%-20%, etc.
                      Segments must be [1, 100].

        Endpoint:      GET /statistics
        Documentation: https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#get-statistics
        """
        if not seconds or type(seconds) != "int":
            seconds = 10
        else:
            seconds = max(min(seconds, 0), 60)
        if not segment or type(segment) != "int":
            segment = 10
        else:
            segment = max(min(segment, 0), 100)
        return self._get(self.restppUrl + "/statistics/" + self.graphname + "?seconds=" + str(seconds) + "&segment=" + str(segment), resKey="")

    def getVersion(self, raw=False) -> str | list:
        """Retrieves the git versions of all components of the system.

        :param bool raw:
            If `True`, returns the unprocssed (not quite JSON) response. Return nice list of components and their version number otherwise.

        Endpoint:      GET /version
        Documentation: https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#get-version
        """
        response = requests.request("GET", self.restppUrl + "/version/" + self.graphname, headers=self.authHeader)
        res = json.loads(response.text, strict=False)  # "strict=False" is why _get() was not used
        self._errorCheck(res)

        if raw:
            return response.text
        res = res["message"].split("\n")
        components = []
        for i in range(len(res)):
            if 2 < i < len(res) - 1:
                m = res[i].split()
                component = {"name": m[0], "version": m[1], "hash": m[2], "datetime": m[3] + " " + m[4] + " " + m[5]}
                components.append(component)
        return components

    def getVer(self, component="product", full=False) -> str:
        """Gets the version information of specific component.

        :param str component:
            One of TigerGraph's components (e.g. product, gpe, gse).
        :param bool full:
            If `True`, retuns the full build version string, instead of just the X.Y.Z version number,

        Get the full list of components using `getVersion`.
        """
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
            raise TigerGraphException("\"" + component + "\" is not a valid component.", None)

    def getEdition(self) -> str:
        """Gets the database edition information"""
        ret = self._get(self.restppUrl + "/graph/" + self.graphname + "/vertices/dummy", resKey="", skipCheck=True)
        return ret["version"]["edition"]

    def getLicenseInfo(self) -> dict:
        """Returns the expiration date and remaining days of the license.

        In case of evaluation/trial deployment, an information message and -1 remaining days are returned.
        """
        res = self._get(self.restppUrl + "/showlicenseinfo", resKey="", skipCheck=True)
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
