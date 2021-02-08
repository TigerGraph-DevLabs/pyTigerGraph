import json
import urllib.parse

import pandas as pd

from pyTigerGraph import TigerGraphBase, TigerGraphException


class TigerGraphConnection(TigerGraphBase):
    """Python wrapper for TigerGraph's REST++ and GSQL APIs, mostly for analytics/data science."""

    def __init__(self, host="http://localhost", graphname="MyGraph", username="tigergraph", password="tigergraph", restppPort="9000", gsPort="14240", apiToken="", gsqlVersion="", tgPath="", useCert=False, certPath="", debug=False):
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
        :param str tgPath:
            The directory where TigerGraph related configuration and certification files and downloaded executables are stored. Default is "~/.tigergraph"
        :param bool useCert:
            True if you need to use a certificate because the server is secure (such as on TigerGraph
                               Cloud). This needs to be False when connecting to an unsecure server such as a TigerGraph Developer instance.
                               When True the certificate would be downloaded when it is first needed.
        :param str certPath:
            The folder/directory _and_ the name of the SSL certification file where the certification should be stored.
        :param bool debug:
            Enables debug output.
        """

        super().__init__(host, graphname, username, password, restppPort, gsPort, apiToken, gsqlVersion, tgPath, useCert, certPath, debug)

    # Generic DML functions ====================================================

    def _upsertAttrs(self, attributes):
        """Transforms attributes (provided as a table) into a hierarchy as expect by the upsert functions."""
        if not isinstance(attributes, dict):
            return {}
        vals = {}
        for attr in attributes:
            val = attributes[attr]
            if isinstance(val, tuple):
                vals[attr] = {"value": val[0], "op": val[1]}
            else:
                vals[attr] = {"value": val}
        return vals

    def upsertData(self, data):
        """Upserts data (vertices and edges) from a JSON document or equivalent object structure.

        Endpoint:      POST /graph
        Documentation: https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#get-the-graph-schema-get-gsql-schema
        """
        if not isinstance(data, str):
            data = json.dumps(data)
        if self.debug:
            print(data)
        return self.conn.post("/graph/" + self.graphname, data=data)[0]

    # Vertex related functions =================================================

    def upsertVertex(self, vertexType, vertexId, attributes=None):
        """Upserts a vertex.

        Data is upserted:
        - If vertex is not yet present in graph, it will be created.
        - If it's already in the graph, its attributes are updated with the values specified in the request. An optional operator controls how the attributes are updated.

        The `attributes` argument is expected to be a dictionary in this format:
            {<attribute_name>: <attribute_value>|(<attribute_name>, <operator>), …}

        Example:
            {"name": "Thorin", points: (10, "+"), "bestScore": (67, "max")}

        For valid values of <operator> see: https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#post-graph-graph_name-upsert-the-given-data

        Returns a single number of accepted (successfully upserted) vertices (0 or 1).

        Endpoint:      POST /graph
        Documentation: https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#post-graph-graph_name-upsert-the-given-data
        """
        if not isinstance(attributes, dict):
            return None
        vals = self._upsertAttrs(attributes)
        data = json.dumps({"vertices": {vertexType: {vertexId: vals}}})
        return self.conn.post("/graph/" + self.graphname, data=data)[0]["accepted_vertices"]

    def upsertVertices(self, vertexType, vertices):
        """Upserts multiple vertices (of the same type).

        See the description of `upsertVertex` for generic information.

        The `vertices` argument is expected to be a list of tuples in this format:
            [
                (<vertex_id>, {<attribute_name>, <attribute_value>|(<attribute_name>, <operator>), …}),
                ⋮
            ]

        Example:
            [
               (2, {"name": "Balin", "points": (10, "+"), "bestScore": (67, "max")}),
               (3, {"name": "Dwalin", "points": (7, "+"), "bestScore": (35, "max")})
            ]

        For valid values of <operator> see: https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#post-graph-graph_name-upsert-the-given-data

        Returns a single number of accepted (successfully upserted) vertices (0 or positive integer).

        Endpoint:      POST /graph
        Documentation: https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#post-graph-graph_name-upsert-the-given-data
        """
        if not isinstance(vertices, list):
            return None
        data = {}
        for v in vertices:
            vals = self._upsertAttrs(v[1])
            data[v[0]] = vals
        data = json.dumps({"vertices": {vertexType: data}})
        return self.conn.post("/graph/" + self.graphname, data=data)[0]["accepted_vertices"]

    def getVertices(self, vertexType, select="", where="", limit="", sort="", fmt="py", withId=True, withType=False, timeout=0):
        """Retrieves vertices of the given vertex type.

        Arguments:
        - `select`:   Comma separated list of vertex attributes to be retrieved or omitted.
                      See https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#select
        - `where`:    Comma separated list of conditions that are all applied on each vertex' attributes.
                      The conditions are in logical conjunction (i.e. they are "AND'ed" together).
                      See https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#filter
        - `limit`:    Maximum number of vertex instances to be returned (after sorting).
                      See https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#limit
        - `sort`:     Comma separated list of attributes the results should be sorted by.
                      See https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#sort
        - `fmt`:      Format of the results:
                      "py":   Python objects (default)
                      "json": JSON document
                      "df":   Pandas DataFrame
        - `withId`:   (If the output format is "df") should the vertex ID be included in the dataframe?
        - `withType`: (If the output format is "df") should the vertex type be included in the dataframe?
        - `timeout`:  Time allowed for successful execution (0 = no limit, default).

        NOTE: The primary ID of a vertex instance is NOT an attribute, thus cannot be used in above arguments.
              Use `getVerticesById` if you need to retrieve by vertex ID.

        Endpoint:      GET /graph/{graph_name}/vertices
        Documentation: https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#get-graph-graph_name-vertices
        """
        url = "/graph/" + self.graphname + "/vertices/" + vertexType
        isFirst = True
        if select:
            url += "?select=" + select
            isFirst = False
        if where:
            url += ("?" if isFirst else "&") + "filter=" + where
            isFirst = False
        if limit:
            url += ("?" if isFirst else "&") + "limit=" + str(limit)
            isFirst = False
        if sort:
            url += ("?" if isFirst else "&") + "sort=" + sort
            isFirst = False
        if timeout and timeout > 0:
            url += ("?" if isFirst else "&") + "timeout=" + str(timeout)

        ret = self.conn.get(url)

        if fmt == "json":
            return json.dumps(ret)
        if fmt == "df":
            return self.vertexSetToDataFrame(ret, withId, withType)
        return ret

    def getVertexDataframe(self, vertexType, select="", where="", limit="", sort="", timeout=0):
        """Retrieves vertices of the given vertex type and returns them as Pandas DataFrame.

        For details on arguments see `getVertices` above.
        """
        return self.getVertices(vertexType, select=select, where=where, limit=limit, sort=sort, fmt="df", withId=True, withType=False, timeout=timeout)

    def getVerticesById(self, vertexType, vertexIds, fmt="py", withId=True, withType=False):
        """Retrieves vertices of the given vertex type, identified by their ID.

        Arguments
        - `vertexIds`: A single vertex ID or a list of vertex IDs.
        - `fmt`:      Format of the results:
                      "py":   Python objects (default)
                      "json": JSON document
                      "df":   Pandas DataFrame
        - `withId`:   (If the output format is "df") should the vertex ID be included in the dataframe?
        - `withType`: (If the output format is "df") should the vertex type be included in the dataframe?
        - `timeout`:  Time allowed for successful execution (0 = no limit, default).

        Endpoint:      GET /graph/{graph_name}/vertices
        Documentation: https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#get-graph-graph_name-vertices
        """
        if not vertexIds:
            raise TigerGraphException("No vertex ID was specified.", None)
        vids = []
        if isinstance(vertexIds, (int, str)):
            vids.append(vertexIds)
        elif not isinstance(vertexIds, list):
            return None  # TODO: a better return value?
        else:
            vids = vertexIds
        url = "/graph/" + self.graphname + "/vertices/" + vertexType + "/"

        ret = []
        for vid in vids:
            ret += self.conn.get(url + str(vid))

        if fmt == "json":
            return json.dumps(ret)
        if fmt == "df":
            return self.vertexSetToDataFrame(ret, withId, withType)
        return ret

    def getVertexDataframeById(self, vertexType, vertexIds):
        """Retrieves vertices of the given vertex type, identified by their ID.

        For details on arguments see `getVerticesById` above.
        """
        return self.getVerticesById(vertexType, vertexIds, fmt="df", withId=True, withType=False)

    def delVertices(self, vertexType, where="", limit="", sort="", permanent=False, timeout=0):
        """Deletes vertices from graph.

        Arguments:
        - `where`:     Comma separated list of conditions that are all applied on each vertex' attributes.
                       The conditions are in logical conjunction (i.e. they are "AND'ed" together).
                       See https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#filter
        - `limit`:     Maximum number of vertex instances to be returned (after sorting).
                       See https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#limit
                       Must be used with `sort`.
        - `sort`:      Comma separated list of attributes the results should be sorted by.
                       See https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#sort
                       Must be used with `limit`.
        - `permanent`: If true, the deleted vertex IDs can never be inserted back, unless the graph is dropped or the graph store is cleared.
        - `timeout`:   Time allowed for successful execution (0 = no limit, default).

        NOTE: The primary ID of a vertex instance is NOT an attribute, thus cannot be used in above arguments.
              Use `delVerticesById` if you need to delete by vertex ID.

        Returns a single number of vertices deleted.

        Endpoint:      DELETE /graph/{graph_name}/vertices
        Documentation: https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#delete-graph-graph_name-vertices
        """
        url = "/graph/" + self.graphname + "/vertices/" + vertexType
        isFirst = True
        if where:
            url += "?filter=" + where
            isFirst = False
        if limit and sort:  # These two must be provided together
            url += ("?" if isFirst else "&") + "limit=" + str(limit) + "&sort=" + sort
            isFirst = False
        if permanent:
            url += ("?" if isFirst else "&") + "permanent=true"
            isFirst = False
        if timeout and timeout > 0:
            url += ("?" if isFirst else "&") + "timeout=" + str(timeout)
        return self.conn.delete(url)["deleted_vertices"]

    def delVerticesById(self, vertexType, vertexIds, permanent=False, timeout=0):
        """Deletes vertices from graph identified by their ID.

        Arguments:
        - `vertexIds`: A single vertex ID or a list of vertex IDs.
        - `permanent`: If true, the deleted vertex IDs can never be inserted back, unless the graph is dropped or the graph store is cleared.
        - `timeout`:   Time allowed for successful execution (0 = no limit, default).

        Returns a single number of vertices deleted.

        Endpoint:      DELETE /graph/{graph_name}/vertices
        Documentation: https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#delete-graph-graph_name-vertices
        """
        if not vertexIds:
            raise TigerGraphException("No vertex ID was not specified.", None)
        vids = []
        if isinstance(vertexIds, (int, str)):
            vids.append(vertexIds)
        elif not isinstance(vertexIds, list):
            return None  # TODO: a better return value?
        else:
            vids = vertexIds
        url1 = "/graph/" + self.graphname + "/vertices/" + vertexType + "/"
        url2 = ""
        if permanent:
            url2 = "?permanent=true"
        if timeout and timeout > 0:
            url2 += ("&" if url2 else "?") + "timeout=" + str(timeout)
        ret = 0
        for vid in vids:
            ret += self.conn.delete(url1 + str(vid) + url2)["deleted_vertices"]
        return ret

    # Edge related functions ===================================================

    def upsertEdge(self, sourceVertexType, sourceVertexId, edgeType, targetVertexType, targetVertexId, attributes=None):
        """Upserts an edge.

        Data is upserted:
        - If edge is not yet present in graph, it will be created (see special case below).
        - If it's already in the graph, it is updated with the values specified in the request.

        The `attributes` argument is expected to be a dictionary in this format:
            {<attribute_name>, <attribute_value>|(<attribute_name>, <operator>), …}

        Example:
            {"visits": (1482, "+"), "max_duration": (371, "max")}

        For valid values of <operator> see: https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#post-graph-graph_name-upsert-the-given-data

        Returns a single number of accepted (successfully upserted) edges (0 or 1).

        Note: If operator is "vertex_must_exist" then edge will only be created if both vertex exists in graph.
              Otherwise missing vertices are created with the new edge.

        Endpoint:      POST /graph
        Documentation: https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#post-graph-graph_name-upsert-the-given-data
        """
        if attributes is None:
            attributes = {}
        if not isinstance(attributes, dict):
            return None
        vals = self._upsertAttrs(attributes)
        data = json.dumps({"edges": {sourceVertexType: {sourceVertexId: {edgeType: {targetVertexType: {targetVertexId: vals}}}}}})
        return self.conn.post("/graph/" + self.graphname, data=data)[0]["accepted_edges"]

    def upsertEdges(self, sourceVertexType, edgeType, targetVertexType, edges):
        """Upserts multiple edges (of the same type).

        See the description of `upsertEdge` for generic information.

        The `edges` argument is expected to be a list in of tuples in this format:
        [
          (<source_vertex_id>, <target_vertex_id>, {<attribute_name>: <attribute_value>|(<attribute_name>, <operator>), …})
          ⋮
        ]

        Example:
            [
              (17, "home_page", {"visits": (35, "+"), "max_duration": (93, "max")}),
              (42, "search", {"visits": (17, "+"), "max_duration": (41, "max")}),
            ]

        For valid values of <operator> see: https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#post-graph-graph_name-upsert-the-given-data

        Returns a single number of accepted (successfully upserted) edges (0 or positive integer).

        Endpoint:      POST /graph
        Documentation: https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#post-graph-graph_name-upsert-the-given-data
        """
        if not isinstance(edges, list):
            return None
        data = {sourceVertexType: {}}
        l1 = data[sourceVertexType]
        for e in edges:
            if len(e) > 2:
                vals = self._upsertAttrs(e[2])
            else:
                vals = {}
            # fromVertexId
            if e[0] not in l1:
                l1[e[0]] = {}
            l2 = l1[e[0]]
            # edgeType
            if edgeType not in l2:
                l2[edgeType] = {}
            l3 = l2[edgeType]
            # targetVertexType
            if targetVertexType not in l3:
                l3[targetVertexType] = {}
            l4 = l3[targetVertexType]
            # targetVertexId
            l4[e[1]] = vals
        data = json.dumps({"edges": data})
        return self.conn.post("/graph/" + self.graphname, data=data)[0]["accepted_edges"]

    def getEdges(self, sourceVertexType, sourceVertexId, edgeType=None, targetVertexType=None, targetVertexId=None, select="", where="", limit="", sort="", fmt="py", withId=True, withType=False, timeout=0):
        """Retrieves edges of the given edge type originating from a specific source vertex.

        Only `sourceVertexType` and `sourceVertexId` are required.
        If `targetVertexId` is specified, then `targetVertexType` must also be specified.
        If `targetVertexType` is specified, then `edgeType` must also be specified.

        Arguments:
        - `select`:   Comma separated list of edge attributes to be retrieved or omitted.
                      See https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#select
        - `where`:    Comma separated list of conditions that are all applied on each edge's attributes.
                      The conditions are in logical conjunction (i.e. they are "AND'ed" together).
                      See https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#filter
        - `limit`:    Maximum number of edge instances to be returned (after sorting).
                      See https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#limit
        - `sort`:     Comma separated list of attributes the results should be sorted by.
                      See https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#sort
        - `fmt`:      Format of the results:
                      "py":   Python objects (default)
                      "json": JSON document
                      "df":   Pandas DataFrame
        - `withId`:   (If the output format is "df") should the source and target vertex types and IDs be included in the dataframe?
        - `withType`: (If the output format is "df") should the edge type be included in the dataframe?
        - `timeout`:  Time allowed for successful execution (0 = no limit, default).

        Endpoint:      GET /graph/{graph_name}/vertices
        Documentation: https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#get-graph-graph_name-edges
        """
        # TODO: change sourceVertexId to sourceVertexIds and allow passing both number and list as parameter
        if not sourceVertexType or not sourceVertexId:
            raise TigerGraphException("Both source vertex type and source vertex ID must be provided.", None)
        url = "/graph/" + self.graphname + "/edges/" + sourceVertexType + "/" + str(sourceVertexId)
        if edgeType:
            url += "/" + edgeType
            if targetVertexType:
                url += "/" + targetVertexType
                if targetVertexId:
                    url += "/" + str(targetVertexId)
        isFirst = True
        if select:
            url += "?select=" + select
            isFirst = False
        if where:
            url += ("?" if isFirst else "&") + "filter=" + where
            isFirst = False
        if limit:
            url += ("?" if isFirst else "&") + "limit=" + str(limit)
            isFirst = False
        if sort:
            url += ("?" if isFirst else "&") + "sort=" + sort
            isFirst = False
        if timeout and timeout > 0:
            url += ("?" if isFirst else "&") + "timeout=" + str(timeout)
        ret = self.conn.get(url)

        if fmt == "json":
            return json.dumps(ret)
        if fmt == "df":
            return self.edgeSetToDataFrame(ret, withId, withType)
        return ret

    def getEdgesDataframe(self, sourceVertexType, sourceVertexId, edgeType=None, targetVertexType=None, targetVertexId=None, select="", where="", limit="", sort="", timeout=0):
        """Retrieves edges of the given edge type originating from a specific source vertex.

        For details on arguments see `getEdges` above.
        """
        return self.getEdges(sourceVertexType, sourceVertexId, edgeType, targetVertexType, targetVertexId, select, where, limit, sort, fmt="df", timeout=timeout)

    def getEdgesByType(self, edgeType, fmt="py", withId=True, withType=False):
        """Retrieves edges of the given edge type regardless the source vertex.

        Arguments:
        - `edgeType`: The name of the edge type.
        - `fmt`:      Format of the results:
                      "py":   Python objects (default)
                      "json": JSON document
                      "df":   Pandas DataFrame
        - `withId`:   (If the output format is "df") should the source and target vertex types and IDs be included in the dataframe?
        - `withType`: (If the output format is "df") should the edge type be included in the dataframe?
        - `timeout`:  Time allowed for successful execution (0 = no limit, default).

        TODO: add limit parameter
        """
        if not edgeType:
            return []

        # Check if ttk_getEdgesFrom query was installed
        # if self.ttkGetEF is None:
        #     self.ttkGetEF = False
        #     eps = self.getEndpoints(dynamic=True)
        #     for ep in eps:
        #         if ep.endswith("ttk_getEdgesFrom"):
        #             self.ttkGetEF = True

        sourceVertexType = self.getEdgeSourceVertexType(edgeType)
        if isinstance(sourceVertexType, set) or sourceVertexType == "*":  # TODO: support edges with multiple source vertex types
            raise TigerGraphException("Edges with multiple source vertex types are not currently supported.", None)

        # TODO: Rethink how it should be handled. Require query to be installed? 
        # Do we have to worry about 2.x at all?
        if False:  # self.ttkGetEF:  # If installed version is available, use it, as it can return edge attributes too.
            ret = self.runInstalledQuery("ttk_getEdgesFrom", {"edgeType": edgeType, "sourceVertexType": sourceVertexType})
        else:  # If installed version is not available, use interpreted version. Always available, but couldn't return attributes before v3.0.
            queryText = \
                'INTERPRET QUERY () FOR GRAPH $graph { \
                    SetAccum<EDGE> @@edges; \
                    start = {ANY}; \
                    res = \
                        SELECT s \
                        FROM   start:s-(:e)->ANY:t \
                        WHERE  e.type == "$edgeType" \
                           AND s.type == "$sourceEdgeType" \
                        ACCUM  @@edges += e; \
                    PRINT @@edges AS edges; \
             }'

            queryText = queryText.replace("$graph",          self.graphname) \
                                 .replace('$sourceEdgeType', sourceVertexType[0]) \
                                 .replace('$edgeType',       edgeType)
            ret = self.runInterpretedQuery(queryText)
        ret = ret[0]["edges"]

        if fmt == "json":
            return json.dumps(ret)
        if fmt == "df":
            return self.edgeSetToDataFrame(ret, withId, withType)
        return ret

    # TODO: getEdgesDataframeByType

    def delEdges(self, sourceVertexType, sourceVertexId, edgeType=None, targetVertexType=None, targetVertexId=None, where="", limit="", sort="", timeout=0):
        """Deletes edges from the graph.

        Only `sourceVertexType` and `sourceVertexId` are required.
        If `targetVertexId` is specified, then `targetVertexType` must also be specified.
        If `targetVertexType` is specified, then `edgeType` must also be specified.

        Arguments:
        - `where`:   Comma separated list of conditions that are all applied on each edge's attributes.
                     The conditions are in logical conjunction (i.e. they are "AND'ed" together).
                     See https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#filter
        - `limit`:   Maximum number of edge instances to be returned (after sorting).
                     See https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#limit
        - `sort`     Comma separated list of attributes the results should be sorted by.
                     See https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#sort
        - `timeout`: Time allowed for successful execution (0 = no limit, default).

        Returns a dictionary of <edge_type>: <deleted_edge_count> pairs.

        Endpoint:      DELETE /graph/{/graph_name}/edges
        Documentation: https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#delete-graph-graph_name-edges
        """
        if not sourceVertexType or not sourceVertexId:
            raise TigerGraphException("Both sourceVertexType and sourceVertexId must be provided.", None)
        url = "/graph/" + self.graphname + "/edges/" + sourceVertexType + "/" + str(sourceVertexId)
        if edgeType:
            url += "/" + edgeType
            if targetVertexType:
                url += "/" + targetVertexType
                if targetVertexId:
                    url += "/" + str(targetVertexId)
        isFirst = True
        if where:
            url += ("?" if isFirst else "&") + "filter=" + where
            isFirst = False
        if limit and sort:  # These two must be provided together
            url += ("?" if isFirst else "&") + "limit=" + str(limit) + "&sort=" + sort
            isFirst = False
        if timeout and timeout > 0:
            url += ("?" if isFirst else "&") + "timeout=" + str(timeout)
        res = self.conn.delete(url)
        ret = {}
        for r in res:
            ret[r["e_type"]] = r["deleted_edges"]
        return ret

    # Query related functions ==================================================

    def runInstalledQuery(self, queryName, params=None, timeout=None, sizeLimit=None):
        """Runs an installed query.

        The query must be already created and installed in the graph.
        Use `getEndpoints(dynamic=True)` or GraphStudio to find out the generated endpoint URL of the query, but only the query name needs to be specified here.

        Arguments:
        - `params`:    A string of param1=value1&param2=value2 format or a dictionary.
        - `timeout`:   Maximum duration for successful query execution (in milliseconds).
                       See https://docs.tigergraph.com/dev/restpp-api/restpp-requests#gsql-query-timeout
        - `sizeLimit`: Maximum size of response (in bytes).
                       See https://docs.tigergraph.com/dev/restpp-api/restpp-requests#request-body-size

        Endpoint:      POST /query/{graph_name}/<query_name>
        Documentation: https://docs.tigergraph.com/dev/gsql-ref/querying/query-operations#running-a-query
        """
        query1 = ""
        for param in params.keys():
            if " " in params[param]:
                params[param] = urllib.parse.quote(params[param])  # ' ' ==> %20 HTML Format
            query1 += param + "=" + params[param] + "&"
        if query1[-1] == "&":
            query1 = query1[:-1]

        headers = {}
        if timeout:
            headers["GSQL-TIMEOUT"] = str(timeout)
        if sizeLimit:
            headers["RESPONSE-LIMIT"] = str(sizeLimit)
        return self.conn.get("/query/" + self.graphname + "/" + queryName + "?" + query1, headers=headers)

    def runInterpretedQuery(self, queryText, params=None, timeout=None, sizeLimit=None):
        """Runs an interpreted query.

        You must provide the query text in this format:
            INTERPRET QUERY (<params>) FOR GRAPH <graph_name> {
               <statements>
            }

        Use `$graphname` in the `FOR GRAPH` clause to avoid hard-coding it; it will be replaced by the actual graph name. E.g.

            INTERPRET QUERY (INT a) FOR GRAPH $graphname {
                PRINT a;
            }

        Arguments:
        - `params`:    A string of param1=value1&param2=value2 format or a dictionary.
        - `timeout`:   Maximum duration for successful query execution (in milliseconds).
                       See https://docs.tigergraph.com/dev/restpp-api/restpp-requests#gsql-query-timeout
        - `sizeLimit`: Maximum size of response (in bytes).
                       See https://docs.tigergraph.com/dev/restpp-api/restpp-requests#request-body-size

        Endpoint:      POST /gsqlserver/interpreted_query
        Documentation: https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#post-gsqlserver-interpreted_query-run-an-interpreted-query
        """
        queryText = queryText.replace("$graphname", self.graphname)
        if self.debug:
            print(queryText)
        headers = {}
        if timeout:
            headers["GSQL-TIMEOUT"] = str(timeout)
        if sizeLimit:
            headers["RESPONSE-LIMIT"] = str(sizeLimit)
        return self.conn.post("/gsqlserver/interpreted_query", data=queryText, params=params, authMode="pwd", headers=headers)

    # TODO: GET /showprocesslist/{graph_name}
    #       https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#get-running-queries-showprocesslist-graph_name

    def parseQueryOutput(self, output, graphOnly=True):
        """Parses query output and separates vertex and edge data (and optionally other output) for easier use.

        The JSON output from a query can contain a mixture of results: vertex sets (the output of a SELECT statement),
            edge sets (e.g. collected in a global accumulator), printout of global and local variables and accumulators,
            including complex types (LIST, MAP, etc.). The type of the various output entries is not explicit, you need
            to inspect the content to find out what it is actually.
        This function "cleans" this output, separating and collecting vertices and edges in an easy to access way.
            It can also collect other output or ignore it.
        The output of this function can be used e.g. with the `vertexSetToDataFrame()` and `edgeSetToDataFrame()` functions or
            (after some transformation) to pass a subgraph to a visualisation component.

        Arguments:
        - `output`:    The data structure returned by `runInstalledQuery()` or `runInterpretedQuery()`
        - `graphOnly`: Should output be restricted to vertices and edges (True, default) or should any other output (e.g. values of
                       variables or accumulators, or plain text printed) be captured as well.

        Returns: A dictionary with two (or three) keys: "vertices", "edges" and optionally "output". First two refer to another dictionary
            containing keys for each vertex and edge types found, and the instances of those vertex and edge types. "output" is a list of
            dictionaries containing the key/value pairs of any other output.
        """

        def attCopy(src, trg):
            """Copies the attributes of a vertex or edge into another vertex or edge, respectively."""
            srca = src["attributes"]
            trga = trg["attributes"]
            for att in srca:
                trga[att] = srca[att]

        def addOccurrences(obj, src):
            """Counts and lists te occurrences of a vertex or edge.
            A given vertex or edge can appear multiple times (in different vertex or edge sets) in the output of a query.
            Each output has a label (either the variable name or an alias used in the PRINT statement), `x_sources` contains a list of these labels.
            """
            if "x_occurrences" in obj:
                obj["x_occurrences"] += 1
            else:
                obj["x_occurrences"] = 1
            if "x_sources" in obj:
                obj["x_sources"].append(src)
            else:
                obj["x_sources"] = [src]

        vs = {}
        es = {}
        ou = []

        # Outermost data type is a list
        for o1 in output:
            # Next level data type is dictionary that could be vertex sets, edge sets or generic output (of simple or complex data types)
            for o2 in o1:
                _o2 = o1[o2]
                if isinstance(_o2, list) and len(_o2) > 0 and isinstance(_o2[0], dict):  # Is it an array of dictionaries?
                    for o3 in _o2:  # Iterate through the array
                        if "v_type" in o3:  # It's a vertex!

                            # Handle vertex type first
                            vType = o3["v_type"]
                            vtm = {}
                            if vType in vs:  # Do we have this type of vertices in our list (which is a dictionary, really)?
                                vtm = vs[vType]
                            else:  # No, let's create a dictionary for them and add to the list
                                vtm = {}
                                vs[vType] = vtm

                            # Then handle the vertex itself
                            vId = o3["v_id"]
                            if vId in vtm:  # Do we have this specific vertex (identified by the ID) in our list?
                                tmp = vtm[vId]
                                attCopy(o3, tmp)
                                addOccurrences(tmp, o2)
                            else:  # No, add it
                                addOccurrences(o3, o2)
                                vtm[vId] = o3

                        elif "e_type" in o3:  # It's an edge!

                            # Handle edge type first
                            eType = o3["e_type"]
                            etm = {}
                            if eType in es:  # Do we have this type of edges in our list (which is a dictionary, really)?
                                etm = es[eType]
                            else:  # No, let's create a dictionary for them and add to the list
                                etm = {}
                                es[eType] = etm

                            # Then handle the edge itself
                            eId = o3["from_type"] + "(" + o3["from_id"] + ")->" + o3["to_type"] + "(" + o3["to_id"] + ")"
                            o3["e_id"] = eId

                            # Add reverse edge name, if applicable
                            if self.isDirected(eType):
                                rev = self.getReverseEdge(eType)
                                if rev:
                                    o3["reverse_edge"] = rev

                            if eId in etm:  # Do we have this specific edge (identified by the composite ID) in our list?
                                tmp = etm[eId]
                                attCopy(o3, tmp)
                                addOccurrences(tmp, o2)
                            else:  # No, add it
                                addOccurrences(o3, o2)
                                etm[eId] = o3

                        else:  # It's a ... something else
                            ou.append({"label": o2, "value": _o2})
                else:  # It's a ... something else
                    ou.append({"label": o2, "value": _o2})

        ret = {"vertices": vs, "edges": es}
        if not graphOnly:
            ret["output"] = ou
        return ret

    # Path-finding algorithms ==================================================

    def _preparePathParams(self, sourceVertices, targetVertices, maxLength=None, vertexFilters=None, edgeFilters=None, allShortestPaths=False):
        """Prepares the input parameters by transforming them to the format expected by the path algorithms.

        Arguments:
        - `sourceVertices`:   A vertex set (a list of vertices) or a list of (vertexType, vertexID) tuples; the source vertices of the shortest paths sought.
        - `targetVertices`:   A vertex set (a list of vertices) or a list of (vertexType, vertexID) tuples; the target vertices of the shortest paths sought.
        - `maxLength`:        The maximum length of a shortest path. Optional, default is 6.
        - `vertexFilters`:    An optional list of (vertexType, condition) tuples or {"type": <str>, "condition": <str>} dictionaries.
        - `edgeFilters`:      An optional list of (edgeType, condition) tuples or {"type": <str>, "condition": <str>} dictionaries.
        - `allShortestPaths`: If true, the endpoint will return all shortest paths between the source and target.
                              Default is false, meaning that the endpoint will return only one path.

        See https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#input-parameters-and-output-format-for-path-finding for information on filters.
        """

        def parseVertices(vertices):
            """Parses vertex input parameters and converts it to the format required by the path finding endpoints."""
            ret = []
            if not isinstance(vertices, list):
                vertices = [vertices]
            for v in vertices:
                if isinstance(v, tuple):
                    tmp = {"type": v[0], "id": v[1]}
                    ret.append(tmp)
                elif isinstance(v, dict) and "v_type" in v and "v_id" in v:
                    tmp = {"type": v["v_type"], "id": v["v_id"]}
                    ret.append(tmp)
                elif self.debug:
                    print("Invalid vertex type or value: " + str(v))
            return ret

        def parseFilters(filters):
            """Parses filter input parameters and converts it to the format required by the path finding endpoints."""
            ret = []
            if not isinstance(filters, list):
                filters = [filters]
            for f in filters:
                if isinstance(f, tuple):
                    tmp = {"type": f[0], "condition": f[1]}
                    ret.append(tmp)
                elif isinstance(f, dict) and "type" in f and "condition" in f:
                    tmp = {"type": f["type"], "condition": f["condition"]}
                    ret.append(tmp)
                elif self.debug:
                    print("Invalid filter type or value: " + str(f))
            return ret

        # Assembling the input payload
        if not sourceVertices or not targetVertices:
            return None  # Should allow TigerGraph to return error instead of handling missing parameters here?
        data = {"sources": parseVertices(sourceVertices), "targets": parseVertices(targetVertices)}
        if vertexFilters:
            data["vertexFilters"] = parseFilters(vertexFilters)
        if edgeFilters:
            data["edgeFilters"] = parseFilters(edgeFilters)
        if maxLength:
            data["maxLength"] = maxLength
        if allShortestPaths:
            data["allShortestPaths"] = True

        return json.dumps(data)

    def shortestPath(self, sourceVertices, targetVertices, maxLength=None, vertexFilters=None, edgeFilters=None, allShortestPaths=False):
        """Find the shortest path (or all shortest paths) between the source and target vertex sets.

        Arguments:
        - `sourceVertices`:   A vertex set (a list of vertices) or a list of (vertexType, vertexID) tuples; the source vertices of the shortest paths sought.
        - `targetVertices`:   A vertex set (a list of vertices) or a list of (vertexType, vertexID) tuples; the target vertices of the shortest paths sought.
        - `maxLength`:        The maximum length of a shortest path. Optional, default is 6.
        - `vertexFilters`:    An optional list of (vertexType, condition) tuples or {"type": <str>, "condition": <str>} dictionaries.
        - `edgeFilters`:      An optional list of (edgeType, condition) tuples or {"type": <str>, "condition": <str>} dictionaries.
        - `allShortestPaths`: If true, the endpoint will return all shortest paths between the source and target.
                              Default is false, meaning that the endpoint will return only one path.

        See https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#input-parameters-and-output-format-for-path-finding for information on filters.

        Endpoint:      POST /shortestpath/{graphName}
        Documentation: https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#post-shortestpath-graphname-shortest-path-search
        """
        data = self._preparePathParams(sourceVertices, targetVertices, maxLength, vertexFilters, edgeFilters, allShortestPaths)
        return self.conn.post("/shortestpath/" + self.graphname, data=data)

    def allPaths(self, sourceVertices, targetVertices, maxLength, vertexFilters=None, edgeFilters=None):
        """Find all possible paths up to a given maximum path length between the source and target vertex sets.

        Arguments:
        - `sourceVertices`:   A vertex set (a list of vertices) or a list of (vertexType, vertexID) tuples; the source vertices of the shortest paths sought.
        - `targetVertices`:   A vertex set (a list of vertices) or a list of (vertexType, vertexID) tuples; the target vertices of the shortest paths sought.
        - `maxLength`:        The maximum length of the paths.
        - `vertexFilters`:    An optional list of (vertexType, condition) tuples or {"type": <str>, "condition": <str>} dictionaries.
        - `edgeFilters`:      An optional list of (edgeType, condition) tuples or {"type": <str>, "condition": <str>} dictionaries.

        See https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#input-parameters-and-output-format-for-path-finding for information on filters.

        Endpoint:      POST /allpaths/{graphName}
        Documentation: https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#post-allpaths-graphname-all-paths-search
        """
        data = self._preparePathParams(sourceVertices, targetVertices, maxLength, vertexFilters, edgeFilters)
        return self.conn.post("/allpaths/" + self.graphname, data=data)

    # Pandas DataFrame support =================================================

    def vertexSetToDataFrame(self, vertexSet, withId=True, withType=False):
        """Converts a vertex set to Pandas DataFrame.

        Arguments:
        - `vertexSet`: A vertex set (a list of vertices of the same vertex type).
        - `withId`:    Add a column with vertex IDs to the DataFrame.
        - `withType`:  Add a column with vertex type to the DataFrame.

        Vertex sets are used for both the input and output of `SELECT` statements. They contain instances of vertices of the same type.
        For each vertex instance the vertex ID, the vertex type and the (optional) attributes are present (under `v_id`, `v_type` and `attributes` keys, respectively).
        See example in `edgeSetToDataFrame`.

        A vertex set has this structure:
        [
            {
                "v_id": <vertex_id>,
                "v_type": <vertex_type_name>,
                "attributes":
                    {
                        "attr1": <value1>,
                        "attr2": <value2>,
                         ⋮
                    }
            },
                ⋮
        ]

        See: https://docs.tigergraph.com/dev/gsql-ref/querying/declaration-and-assignment-statements#vertex-set-variable-declaration-and-assignment
        """
        df = pd.DataFrame(vertexSet)
        cols = []
        if withId:
            cols.append(df["v_id"])
        if withType:
            cols.append(df["v_type"])
        cols.append(pd.DataFrame(df["attributes"].tolist()))
        return pd.concat(cols, axis=1)

    def edgeSetToDataFrame(self, edgeSet, withId=True, withType=False):
        """Converts an edge set to Pandas DataFrame.

        Arguments:
        - `edgeSet`:  An edge set (a list of edges of the same edge type).
        - `withId`:   Add a column with edge IDs to the DataFrame.
                      Note: As edges do not have internal ID, this column will contain a generated composite ID, a combination of source and target vertex types
                            and IDs (specifically: [<source vertex type>, <source vertex ID>, <target vertex type>, <target vertex ID>]).
                            This is unique within the vertex type, but not guaranteed to be globally (i.e. within the whole graph) unique. To get a globally
                            unique edge id, the edge type needs to be added to the above combination (see `withType` below).
        - `withType`: Add a column with edge type to the DataFrame.
                      Note: The value of this column should be combined with the value of ID column to get a globally unique edge ID.

        Edge sets contain instances of the same edge type. Edge sets are not generated "naturally" like vertex sets, you need to collect edges in (global) accumulators,
            e.g. in case you want to visualise them in GraphStudio or by other tools.
        Example:

            SetAccum<EDGE> @@edges;
            start = {Country.*};
            result =
                SELECT t
                FROM   start:s -(PROVINCE_IN_COUNTRY:e)- Province:t
                ACCUM  @@edges += e;
            PRINT start, result, @@edges;

        The `@@edges` is an edge set.
        It contains for each edge instance the source and target vertex type and ID, the edge type, an directedness indicator and the (optional) attributes.
        Note: `start` and `result` are vertex sets.

        An edge set has this structure:
        [
            {
                "e_type": <edge_type_name>,
                "from_type": <source_vertex_type_name>,
                "from_id": <source_vertex_id>,
                "to_type": <target_vertex_type_name>,
                "to_id": <targe_vertex_id>,
                "directed": <true_or_false>,
                "attributes":
                    {
                        "attr1": <value1>,
                        "attr2": <value2>,
                         ⋮
                    }
            },
                ⋮
        ]
        """
        df = pd.DataFrame(edgeSet)
        cols = []
        if withId:
            cols.extend([df["from_type"], df["from_id"], df["to_type"], df["to_id"]])
        if withType:
            cols.append(df["e_type"])
        cols.append(pd.DataFrame(df["attributes"].tolist()))
        return pd.concat(cols, axis=1)

    def upsertVertexDataFrame(self, df, vertexType, v_id=None, attributes=None):
        """Upserts vertices from a Pandas DataFrame.

        Arguments:
        - `df`:          The DataFrame to upsert.
        - `vertexType`:  The type of vertex to upsert data to.
        - `v_id`:        The field name where the vertex primary id is given. If omitted the dataframe
                         index would be used instead.
        - `attributes`:  A dictionary in the form of {target: source} where source is the column name
                         in the dataframe and target is the attribute name in the graph vertex. When omitted
                         all columns would be upserted with their current names. In this case column names
                         must match the vertex's attribute names.

        Returns: The number of vertices upserted.
        """

        json_up = []

        for index in df.index:

            json_up.append(json.loads(df.loc[index].to_json()))
            json_up[-1] = (
                index if v_id is None else json_up[-1][v_id],
                json_up[-1] if attributes is None
                else {target: json_up[-1][source]
                      for target, source in attributes.items()}
            )

        return self.upsertVertices(vertexType=vertexType, vertices=json_up)

    def upsertEdgeDataFrame(self, df, sourceVertexType, edgeType, targetVertexType, from_id=None, to_id=None, attributes=None):
        """Upserts edges from a Pandas DataFrame.

        Arguments:
        - `df`:                The DataFrame to upsert.
        - `sourceVertexType`:  The type of source vertex for the edge.
        - `edgeType`:          The type of edge to upsert data to.
        - `targetVertexType`:  The type of target vertex for the edge.
        - `from_id`:     The field name where the source vertex primary id is given. If omitted the
                         dataframe index would be used instead.
        - `to_id`:       The field name where the target vertex primary id is given. If omitted the
                         dataframe index would be used instead.
        - `attributes`:  A dictionary in the form of {target: source} where source is the column name
                         in the dataframe and target is the attribute name in the graph vertex. When omitted
                         all columns would be upserted with their current names. In this case column names
                         must match the vertex's attribute names.

        Returns: The number of edges upserted.
        """

        json_up = []

        for index in df.index:

            json_up.append(json.loads(df.loc[index].to_json()))
            json_up[-1] = (
                index if from_id is None else json_up[-1][from_id],
                index if to_id is None else json_up[-1][to_id],
                json_up[-1] if attributes is None
                else {target: json_up[-1][source]
                      for target, source in attributes.items()}
            )

        return self.upsertEdges(sourceVertexType=sourceVertexType, edgeType=edgeType, targetVertexType=targetVertexType, edges=json_up)

# EOF
