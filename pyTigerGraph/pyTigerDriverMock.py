"""
Mock TigerGraph Driver
"""

import json
import os
import re
import shutil
import subprocess
import time
import urllib.parse
from datetime import datetime

import requests

TG_PATH = "~/.tigergraph"
GSQL_PREFIX = "/gsql"


class TigerDriverException(Exception):
    """Generic TigerDriver specific exception.

    Where possible, error message and code returned by TigerGraph will be used.
    """

    def __init__(self, message, code=None):
        self.message = message
        self.code = code


class TigerDriver(object):
    """Python bridge for TigerGraph's REST++ and GSQL Server API connections"""

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

        self.url = urllib.parse.urlparse(host)
        self.host = self.url.scheme + "://" + self.url.netloc
        self.username = username
        self.password = password
        self.graphName = graphname
        self.restPpPort = str(restppPort)
        self.restPpUrl = self.host + ":" + self.restPpPort
        self.gsPort = str(gsPort)
        self.gsUrl = self.host + ":" + self.gsPort
        self.serverUrl = self.url.netloc + ":" + self.gsPort
        self.apiToken = apiToken
        self.authHeader = {'Authorization': "Bearer " + self.apiToken}
        self.debug = debug
        self.schema = None

        self.gsqlInitiated = False
        self.gsqlVersion = gsqlVersion
        self.tgPath = tgPath
        if not self.tgPath:
            self.tgPath = TG_PATH
        self.tgPath = os.path.expanduser(self.tgPath)
        self.jarName = ""

        self.certDownloaded = False
        self.useCert = useCert  # TODO: if self.tgLocation.scheme == 'https' and userCert == False, should we throw exception here or let it be thrown later when gsql is called?
        self.certPath = certPath
        if not self.certPath:
            self.certPath = os.path.join(TG_PATH, self.url.netloc.replace(".", "_") + "-" + self.graphName + "-cert.txt")
        self.certPath = os.path.expanduser(self.certPath)
        self._downloadCertificate()

    def _downloadCertificate(self):
        """In case of secure connection downloads an SSL certification (if not available already)"""
        if self.useCert:
            if os.path.exists(self.certPath):
                self.certDownloaded = True
            else:
                if self.debug:
                    print("Downloading SSL certificate")
                # TODO: Windows support

                # Check if OpenSSL is installed.
                if not shutil.which('openssl'):
                    raise TigerDriverException("Could not find OpenSSL. Please install it.", None)

                os.system("openssl s_client -connect " + self.serverUrl + " < /dev/null 2> /dev/null | openssl x509 -text > " + self.certPath)  # TODO: Python-native SSL?
                if os.stat(self.certPath).st_size == 0:
                    raise TigerDriverException("Certificate download failed. Please check that the server is online.", None)

                self.certDownloaded = True

    # REST++ and GSQL Server API access ============================

    def _errorCheck(self, res):
        """Checks if the JSON document returned by an endpoint has contains error: true; if so, it raises an exception.

        Arguments
        - `res`:  The JSON document returned by an endpoint
        """
        if "error" in res and res["error"] and res["error"] != "false":  # Endpoint might return string "false" rather than Boolean false
            raise TigerDriverException(res["message"], (res["code"] if "code" in res else None))

    def _request(self, method, path, headers=None, data=None, resKey="results", skipCheck=False, params=None):
        """Generic REST API request

        :param str method:
            HTTP method, currently one of GET, POST or DELETE.
        :param str path:
            URL path and parameters.
        :param dict headers:
            Standard HTTP request headers.
        :param str data:
            Request payload, typically a JSON document.
        :param str resKey:
            The key to the JSON subdocument to be returned, default is 'result'.
        :param bool skipCheck:
            Skip error checking? Some endpoints return error to indicate that the requested action is not applicable; a problem, but not really an error.
        :param dict params:
            Request URL parameters.
        """
        if self.debug:
            print(method + " " + path + ("\n" + json.dumps(data, indent=2) if data else ""))

        if path.startswith(GSQL_PREFIX):
            # Service: GSQL Server
            _auth = (self.username, self.password)
            _headers = {}
            _url = self.gsUrl
        else:
            # Service: REST++
            _auth = None
            _headers = self.authHeader
            _url = self.restPpUrl

        # Custom headers?
        if headers:
            _headers.update(headers)

        # If POST, add payload
        if method == "POST":
            _data = data  # TODO: check content type and convert from JSON if necessary
        else:
            _data = None

        # Use CA certificate?
        #  TODO: fix certificate handling
        if False and self.useCert and self.certDownloaded:
            res = requests.request(method, _url + path, auth=_auth, headers=_headers, data=_data, params=params, verify=self.certPath)
        else:
            res = requests.request(method, _url + path, auth=_auth, headers=_headers, data=_data, params=params)

        if self.debug:
            print(res.url)

        # Any HTTP error? Escalate it
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

    def get(self, path, headers=None, resKey="results", skipCheck=False, params=None):
        """Generic GET method.

        For argument details, see `request`.
        """
        return self._request("GET", path, headers, "", resKey, skipCheck, params)

    def post(self, path, headers=None, data=None, resKey="results", skipCheck=False, params=None):
        """Generic POST method.

        For argument details, see `request`.
        """
        return self._request("POST", path, headers, data, resKey, skipCheck, params)

    def delete(self, path):
        """Generic DELETE method.

        For argument details, see `request`.
        """
        return self._request("DELETE", path)

    # GSQL support =================================================

    def _cleanseResponse(self, res: str, asList: bool = False, sep: str = "\n"):
        """Cleanse the message returned by the execute() function by removing all details of establishing the connection.

        :param str res:
            The response received from the execute() function.
        :param bool asList:
            Return the cleansed message as list of lines or as a (re)concatenated string (default).
        :param str sep:
            Separator character to be used in (re)concatenated string.
        :return str|list:
            The cleansed message, containing only text relevant to the execution of teh GSQL statement.
        """
        _res = res.split("\n")
        ret = []
        for l in _res:
            if l.startswith("====") or l.startswith("Trying version:") or l.startswith("Connecting to") or l.startswith("If there is any relative path"):
                pass
            else:
                ret.append(l)
        if asList:
            return ret
        return sep.join(ret)

    def _initGsql(self):
        """Initialises the GSQL functionality: downloads the appropriate GSQL client JAR (if not available already)."""

        # Check if Java runtime is installed.
        if not shutil.which("java"):
            raise TigerDriverException("Could not find Java runtime. Please download and install from https://www.oracle.com/java/technologies/javase-downloads.html", None)

        # Create a directory for the JAR file if it does not exist.
        if not os.path.exists(self.tgPath):
            if self.debug:
                print("GSQL location was not found, creating")
            os.mkdir(self.tgPath)

        # Download the gsql_client.jar file if not yet available locally
        if self.gsqlVersion:
            if self.debug:
                print("Using version " + self.gsqlVersion + " instead of " + self._getVer())
        else:
            self.gsqlVersion = self._getVer()
        self.jarName = os.path.join(self.tgPath, 'gsql_client-' + self.gsqlVersion + ".jar")
        if not os.path.exists(self.jarName):
            if self.debug:
                print("Jar not found, downloading to " + self.jarName)
            jar_url = ('https://bintray.com/api/ui/download/tigergraphecosys/tgjars/com/tigergraph/client/gsql_client/' + self.gsqlVersion + '/gsql_client-' + self.gsqlVersion + '.jar')
            res = requests.get(jar_url)
            if res.status_code == 404:
                if self.debug:
                    print(jar_url)
                raise TigerDriverException("GSQL client v" + self.gsqlVersion + " could not be found. Check https://bintray.com/tigergraphecosys/tgjars/gsql_client for available versions.", res.status_code)
            if res.status_code != 200:  # The client JAR was not successfully downloaded for whatever other reasons
                res.raise_for_status()
            open(self.jarName, 'wb').write(res.content)

        self.gsqlInitiated = True

    def execute(self, query, local=False, options=None):
        """Runs a GSQL query and process the output.

        - `query`:      The text of the query to run as one string.
        - `local`:      Use local graph (adds `-g` to command line)
        - `options`:    A list of strings that will be passed as options the the gsql_client.
        """
        if not self.gsqlInitiated:
            self._initGsql()

        cmd = ['java',
               '-DGSQL_CLIENT_VERSION=v' + self.gsqlVersion.replace('.', '_'),
               '-jar', self.jarName,
               '-u', self.username,
               '-p', self.password,
               '-ip', self.serverUrl]

        if local:
            cmd += ["-g", self.graphName]

        if self.useCert:
            cmd += ['-cacert', self.certPath]

        if options:
            cmd += options

        comp = subprocess.run(cmd + [query],
                              stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE)

        stdout = comp.stdout.decode()
        stderr = comp.stderr.decode()  # TODO: this should be parsed or handled some way, not ignored
        if self.debug:
            print("-- stdout " + "-" * 70)
            print(stdout)
            print("-- stderr " + "-" * 70)
            print(stderr)
            print("-" * 80)

        if "Connection refused." in stdout:
            if self.url.scheme == "https" and not self.useCert:
                raise TigerDriverException("Connection to " + self.serverUrl + " was refused. Certificate was not used.", None)
            else:
                raise TigerDriverException("Connection to " + self.serverUrl + " was refused.", None)

        try:
            json_string = re.search(r"(\{|\[).*$", stdout.replace("\n", ""))[0]
            json_object = json.loads(json_string)
        except:
            return self._cleanseResponse(stdout)
        else:
            return json_object

    # Supporting functions =====================================================

    def _getVersion(self, raw=False):
        """Retrieves the git versions of all components of the system.

        Endpoint:      GET /version
        Documentation: https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#get-version
        """
        response = requests.request("GET", self.restPpUrl + "/version/" + self.graphName, headers=self.authHeader)
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

    def _getVer(self, component="product", full=False):
        """Gets the version information of specific component.

        Arguments:
        - `component`: One of TigerGraph's components (e.g. product, gpe, gse).

        Get the full list of components using `getVersion`.
        """
        ret = ""
        for v in self._getVersion():
            if v["name"] == component:
                ret = v["version"]
        if ret != "":
            if full:
                return ret
            ret = re.search("_.+_", ret)
            return ret.group().strip("_")
        else:
            raise TigerDriverException("\"" + component + "\" is not a valid component.", None)
