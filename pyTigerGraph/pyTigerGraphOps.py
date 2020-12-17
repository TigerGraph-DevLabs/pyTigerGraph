import json
import urllib.parse

import pandas as pd

from pyTigerGraph.pyTigerGraphBase import TigerGraphBase, TigerGraphException


class TigerGraphOps(TigerGraphBase):
    """Python wrapper for TigerGraph's REST++ and GSQL APIs, mostly for operations/data engineering."""

    def __init__(self, host="http://localhost", graphname="MyGraph", username="tigergraph", password="tigergraph", restppPort="9000", gsPort="14240", apiToken="", gsqlVersion="", tgDir="", useCert=False, certPath=""):
        """Initiate a connection object.

        Arguments
        - `host`:              The IP address or hostname of the TigerGraph server, including the scheme (`http` or `https`).
        - `graphname`:         The default graph for running queries.
        - `username`:          The username on the TigerGraph server.
        - `password`:          The password for that user.
        - `restppPort`:        The post for REST++ queries.
        - `gsPort`:            The port of all other queries.
        - `apiToken`:          A token to use when making queries. Ignored if REST++ authentication is not enabled.
        - `gsqlVersion`:       The version of GSQL client to be used. Default to database version.
                               pyTigerGraph can detect the version from the database, but in rare cases (when the changes/fixes do not impact
                               the GSQL functionality) no new GSQL version is released
                               when a new version of the database is shipper. In these cases an appropriate GSQL client version needs to be
                               manually specified (typically the latest available version lesser than the database version).
                               You can check the list of available GSQL clients at https://bintray.com/tigergraphecosys/tgjars/gsql_client
        - `gsqlPath`:          The folder/directory where the GSQL client JAR(s) will be stored
        - `useCert`:           True if you need to use a certificate because the server is secure (such as on TigerGraph
                               Cloud). This needs to be False when connecting to an unsecure server such as a TigerGraph Developer instance.
                               When True the certificate would be downloaded when it is first needed.
        - `certPath`:          The folder/directory _and_ the name of the SSL certification file where the certification should be stored.
        """

        super().__init__(host, graphname, username, password, restppPort, gsPort, apiToken, gsqlVersion, tgDir, useCert, certPath)

    # Graph related functions ==================================================

    def createGraph(self):
        pass

    def dropGraph(self):
        pass

    def clearGraphStore(self):
        """Clears the graph store; removing all vertices and edges from all graphs.
        Graph schema is not affected.
        ⚠️ This operation cannot be undone. You will need to restore data from backup, if necessary and if you have a backup.
        ⚠️ This operation might take a long time, depending on the data volume in the graph store.
        Available only to superusers.

        Documentation: https://docs.tigergraph.com/dev/gsql-ref/ddl-and-loading/running-a-loading-job#clear-graph-store
        """
        res = self.gsql("CLEAR GRAPH STORE -HARD", options=[])
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
        res = self.gsql("DROP ALL", options=[])
        if not ("Successfully cleared graph store" in res and "Everything is dropped." in res):
            raise TigerGraphException("Error occurred while dropping all:\n" + res, None)

    # Vertex related functions =================================================

    def createVertexType(self):
        pass

    def createGlobalVertexType(self):
        pass

    def dropVertexType(self):
        pass

    def dropGlobalVertexType(self):
        pass

    # Edge related functions ===================================================

    def createEdgeType(self):
        pass

    def createUnDirectedEdgeType(self):
        pass

    def createDirectedEdgeType(self):
        pass

    # Query related functions ==================================================

    # TODO: GET /showprocesslist/{graph_name}
    #       https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#get-running-queries-showprocesslist-graph_name

    def runningQueries(self):
        pass

    def abortQuery(self):
        # GET /abortquery/{graph_name}
        pass

    # Data source related functions ============================================

    def createS3DataSource(self):
        pass

    def createKafkaDataSource(self):
        pass

    def dropDataSource(self):
        pass

    # Loading job related functions ============================================

    def _loadingJobControl(self, action, jobId):
        """Base function for most loading job related functions

        @param str action:
            One of the possible actions: pause, resume, stop, checkprogress
        @param str jobId:
            The ID of the (active) job
        """
        return self._get(self.gsUrl + "/gsqlserver/gsql/loadingjobs?graph=" + self.graphname + "&action=" + action + "&jobId=" + jobId, authMode="pwd")

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

        res = self._post(self.gsUrl + "/gsqlserver/gsql/loadingjobs?graph=" + self.graphname + "&action=start", data=data, authMode="pwd")[name]
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
