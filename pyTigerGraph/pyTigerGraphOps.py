import json
import re

from pyTigerGraph import TigerGraphBase, TigerGraphException


class TigerGraphOps(TigerGraphBase):
    """Python wrapper for TigerGraph's REST++ and GSQL APIs, mostly for operations/data engineering."""

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

    # Graphs ===================================================================

    def createGraph(self, graphname, vertexTypes, edgeTypes=None, tags=None):
        """Creates a graph

        :param str graphname:
            The name of the graph.
        :param str|list vertexTypes:
            The name(s) of the vertex type(s) to be included in the graph.
            Is its value is "*", all vertices and edges will be included.
            Vertex type names can be one of "vertexType", "vertexType:tagName(s)", ("vertexType","tagName") or ("vertexType",["tagName", ...]).
        :param str|list edgeTypes:
            The name(s) of the edge type(s) to be included in the graph.
            Ignored if `vertices` is "*".
        :param str|list tags:
            The name(s) of the tags applicable to all vertex types in the new graph.
        """
        pass

    def dropGraph(self, graphname):
        """Creates a graph

        :param str graphname:
            The name of the graph.
        """
        pass

    def clearGraphStore(self):
        """Clears the graph store; removing all vertices and edges from all graphs.
        Graph schema is not affected.
        ⚠️ This operation cannot be undone. You will need to restore data from backup, if necessary and if you have a backup.
        ⚠️ This operation might take a long time, depending on the data volume in the graph store.
        Available only to superusers.

        Documentation: https://docs.tigergraph.com/dev/gsql-ref/ddl-and-loading/running-a-loading-job#clear-graph-store
        """
        res = self.conn.execute("CLEAR GRAPH STORE -HARD", False)
        if not ("Successfully cleared graph store" in res and "Successfully started GPE GSE RESTPP" in res):
            raise TigerGraphException("Error occurred while clearing graph store:\n" + res, None)

    def dropAll(self):
        """Clears the graph store and removes all definitions from the catalog.
        ⚠️ This operation cannot be undone. You will need to recreate graph(s) and restore data from backup, if necessary and if you have a backup.
        ⚠️ This operation might take a long time.
        Available only to superusers.
        Dropping all drops secrets and tokens too. If you need to continue the work on the database, you will need to create a new graph, generate a new secret in that graph and get a new token.

        Documentation: https://docs.tigergraph.com/dev/gsql-ref/ddl-and-loading/running-a-loading-job#drop-all
        """
        res = self.conn.execute("DROP ALL", False)
        if not ("Successfully cleared graph store" in res and "Everything is dropped." in res):
            raise TigerGraphException("Error occurred while dropping all:\n" + res, None)

    # Vertex types =============================================================

    def createVertexType(self):
        pass

    def alterVertexType(self):
        pass

    def dropVertexType(self):
        pass

    # Indices ==================================================================

    def createIndex(self, indexName, vertexType, attributeName):
        # CREATE [GLOBAL] SCHEMA_CHANGE JOB
        # RUN [GLOBAL] SCHEMA_CHANGE JOB
        # DROP [GLOBAL] SCHEMA_CHANGE JOB
        pass

    def dropIndex(self, indexName):
        # CREATE [GLOBAL] SCHEMA_CHANGE JOB
        # RUN [GLOBAL] SCHEMA_CHANGE JOB
        # DROP [GLOBAL] SCHEMA_CHANGE JOB
        pass

    # Edge types ===============================================================

    def createUnDirectedEdgeType(self):
        pass

    def createDirectedEdgeType(self):
        pass

    def alterEdgeType(self):
        pass

    def dropEgdeType(self):
        pass

    # User defined types =======================================================

    def createUDT(self):
        pass

    def dropUDT(self):
        pass

    # Queries ==================================================================

    def installQuery(self, queryName, force=False, optimize=False, distributed=False):
        """Installs query/queries

        :param str|list queryName:
            One of: a single query name, a list of query names, "*" for all, "?" for all uninstalled.
        """
        pass

    def optimizeQuery(self, queryName):
        """Optimizes query/queries

        :param str|list queryName:
            One of: a single query name, a list of query names, "*" for all.
        """
        pass

    def abortQuery(self):
        # GET /abortquery/{graph_name}
        pass

    # Data sources =============================================================

    def createS3DataSource(self, dsName, awsAccessKey, awsSecretKey):
        pass

    def createKafkaDataSource(self, dsName, broker, params):
        pass

    def grantDataSource(self, dsName, graphName):
        pass

    def revokeDataSource(self, dsName, graphName):
        pass

    def dropDataSource(self, dsName):
        pass

    # Loading jobs =============================================================

    # TODO: Loading job generation ;-)

    def _loadingJobControl(self, action, jobId):
        """Base function for most loading job related functions

        @param str action:
            One of the possible actions: pause, resume, stop, checkprogress
        @param str jobId:
            The ID of the (active) job
        """
        return self.conn.get("/gsqlserver/gsql/loadingjobs?graph=" + self.graphname + "&action=" + action + "&jobId=" + jobId, authMode="pwd")

    def startLoadingJob(self, name: str, files: list = None, streaming: bool = False):
        """Starts a loading job.

        @param str name:
            The name of the loading job.
        @param list files:
            A list of tuples describing the files to be used in this execution (i.e. the info specified in the `USING` clause of `RUN LOADING JOB`)
            Format: `(<filename_var>, <data_source_name>, <file>)`, where
              - <filename_far> is the filename variable from DEFINE FILENAME
              - <data_source_name> is the name of the datasource, one of: "file", an S3 datasource name, a Kafka data source name
              - <file> is filename or bucket/object or topic name
        @param bool streaming:
            Is this loading job a streaming one (i.e. using Kafka data source)?

        TODO: pre-check existence of S3 and Kafka data sources?
        """
        fs = []
        if isinstance(files, tuple):
            fs = [files]
        elif isinstance(files, list):
            fs = files
        else:
            return None

        ds = []
        for f in fs:
            ds.append({
                "filename": f[0],  # The filename variable from DEFINE FILENAME
                "name": f[1],  # The name of the datasource, one of: "file", an S3 datasource name, a Kafka data source name
                "path": f[2]  # Filename or bucket/object or topic name
            })

        data = {
            "jobs": [{
                "name": name,
                "streaming": streaming,
                "dataSources": ds
            }]
        }
        data = json.dumps(data)

        res = self.conn.post("/gsqlserver/gsql/loadingjobs?graph=" + self.graphname + "&action=start", data=data, authMode="pwd")[name]
        msg = res["message"]
        if "please check the GSQL log" in msg:
            log = ""
        else:
            msg = msg[msg.find("Loading log: '") + 14:]
            log = msg[:msg.find("'")]
        ret = {"jobId": res["results"], "log": log}
        return ret

    def pauseLoadingJob(self, jobId):
        return self._loadingJobControl("pause", jobId)

    def resumeLoadingJob(self, jobId):
        return self._loadingJobControl("resume", jobId)

    def stopLoadingJob(self, jobId):
        return self._loadingJobControl("stop", jobId)

    def getLoadingJobStatus(self, jobId):
        return self._loadingJobControl("checkprogress", jobId)

    # Schema change jobs =======================================================

    def runSchemaChangeJob(self, jobName):
        pass

    def dropSchemaChangeJob(self, jobName):
        pass

    # Users ====================================================================

    def createuser(self, userName, password):
        pass

    def alterUser(self, userName, password):
        pass

    def dropUser(self, userName):
        pass

    # roxy groups ==============================================================

    def createGroup(self, groupName, rule):
        """Creates a proxy group.

        ⚠️ Only users with the admin and superuser role can create a group.

        :param str groupName:
            The name of the new proxy group.
        :param str rule:
            A rule to match LDAP attributes  # TODO Can it be a complex rule like "role=engineer|role=admin"?
        """
        pass

    def dropGroup(self, groupName):
        """Drops proxy group(s).

        ⚠️ Only users with the admin and superuser role can create a group.

        :param str|list groupName:

        """
        if isinstance(groupName, str):
            gns = [groupName]
        elif isinstance(groupName, list):
            gns = groupName
        else:
            return

        for gn in gns:
            self.conn.execute("DROP GROUP " + gn)

    # Secrets and tokens =======================================================

    def createSecret(self, alias) -> str:
        """Issues a `CREATE SECRET` GSQL statement and returns the secret generated by that statement.

        :param str alias:
            The alias for the secret. Required, otherwise secret cannot be dropped.
        """
        response = self.conn.execute("CREATE SECRET " + alias)
        try:
            secret = re.search(r'The secret: (\w*)', response.replace('\n', ''))[1]
            return secret
        except re.error:
            return ""

    def dropSecret(self, alias):
        pass;

    # Tags =====================================================================

    def createTag(self, tagName, description):
        self.createTags((tagName, description))

    def createTags(self, tagList):
        """

        :param list|tuple tagList:
            List of (tagName, description) tuples.  # TODO Should it be instead dictionary?
        """
        # CREATE [GLOBAL] SCHEMA_CHANGE JOB
        # RUN [GLOBAL] SCHEMA_CHANGE JOB
        # DROP [GLOBAL] SCHEMA_CHANGE JOB
        pass

    def dropTag(self, tagName):
        """

        :param tagName:
            A single tag name or a list of tag names.
        """
        # CREATE [GLOBAL] SCHEMA_CHANGE JOB
        # RUN [GLOBAL] SCHEMA_CHANGE JOB
        # DROP [GLOBAL] SCHEMA_CHANGE JOB
        pass

    def tagVertex(self, vertexTypeName):
        """Marks vertex type(s) as taggable.

        :param vertexName:
            A single vertex type name or a list of vertex type names.
        """

    def untagVertex(self, vertexTypeName):
        """Marks vertex type(s) as not taggable.

        :param vertexName:
            A single vertex type name or a list of vertex type names.
        """
        # CREATE [GLOBAL] SCHEMA_CHANGE JOB
        # RUN [GLOBAL] SCHEMA_CHANGE JOB
        # DROP [GLOBAL] SCHEMA_CHANGE JOB
        pass

    # Other functions ==========================================================

    def sanityCheck(self):
        # dbsanitycheck
        pass

    def rebuild(self):
        # GET /rebuildnow/{graph_name}
        pass

    def restppLoader(self):
        # POST /restpploader/{graph_name}
        pass

# EOF
