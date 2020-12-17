import json
import os
import platform
import re
import requests
import shutil
import subprocess
import urllib.parse

from pyTigerGraph.pyTigerGraphException import TigerGraphException


class TigerGraphGSQL(object):

    def __init__(self, host="http://localhost", graphname="MyGraph", username="tigergraph", password="tigergraph", gsqlVersion=None, useCert=False, certPath="", debug=False):
        _host = urllib.parse.urlparse(host)
        self.scheme = _host.scheme
        self.server = _host.netloc
        self.host = _host.scheme + "://" + _host.netloc

        self.graphname = graphname
        self.username = username
        self.password = password
        self.gsqlVersion = gsqlVersion

        self.tgDir = os.path.expanduser(os.path.join("~", ".tigergraph"))

        self.useCert = useCert  # TODO: if self.scheme == 'https' and userCert == False, should we throw exception here or let it be thrown later when gsql is called?
        self.certPath = certPath

        self.gsqlOk = False
        self.certOk = False

        self.debug = debug

    def _initGsql(self):
        """Initialises the GSQL functionality: downloads the appropriate GSQL client JAR (if not available already)."""

        if self.gsqlOk:
            return

        # Check if Java runtime is installed.
        if not shutil.which("java"):
            raise TigerGraphException("Could not find Java runtime. Please download and install from https://www.oracle.com/java/technologies/javase-downloads.html", None)

        # Create a directory for the JAR file if it does not exist.
        if not os.path.exists(self.tgDir):
            if self.debug:
                print("GSQL location was not found, creating")
            os.mkdir(self.tgDir)

        # Download the gsql_client.jar file if not yet available locally
        self.jarName = os.path.join(self.tgDir, 'gsql_client-' + self.gsqlVersion + ".jar")
        if not os.path.exists(self.jarName):
            if self.debug:
                print("GSQL client was not found, downloading to " + self.jarName)
            jar_url = ('https://bintray.com/api/ui/download/tigergraphecosys/tgjars/com/tigergraph/client/gsql_client/' + self.gsqlVersion + '/gsql_client-' + self.gsqlVersion + '.jar')
            res = requests.get(jar_url)
            if res.status_code == 404:
                if self.debug:
                    print(jar_url)
                raise TigerGraphException("GSQL client v" + self.gsqlVersion + " could not be found. Check https://bintray.com/tigergraphecosys/tgjars/gsql_client for available versions.", res.status_code)
            if res.status_code != 200:  # The client JAR was not successfully downloaded for whatever other reasons
                res.raise_for_status()
            open(self.jarName, 'wb').write(res.content)

        self.gsqlOk = True

    def _manageCertificate(self):
        """In case of secure connection, downloads an SSL certification (if not available already)"""
        # TODO: handle self-signed certs vs. real certs
        # TODO: Python-native SSL?
        # TODO: Windows support
        if self.certOk:
            return

        if self.useCert:
            if not self.certPath:
                self.certPath = os.path.join(self.tgDir, self.host.replace(".", "_") + "-" + self.graphname + "-cert.txt")
            self.certPath = os.path.expanduser(self.certPath)

            if os.path.exists(self.certPath):
                self.certOk = True
            else:
                if self.debug:
                    print("Downloading SSL certificate")

                # Check if OpenSSL is installed.
                if not shutil.which('openssl'):
                    raise TigerGraphException("Could not find OpenSSL. Please install it.", None)

                if platform.system() == "Windows":
                    raise TigerGraphException("Windows platform is not currently supported", None)
                else:
                    os.system("openssl s_client -connect " + self.host + " < /dev/null 2> /dev/null | openssl x509 -text > " + self.certPath)
                if os.stat(self.certPath).st_size == 0:
                    raise TigerGraphException("Certificate download failed. Please check that the server is online.", None)

                self.certOk = True
        else:
            self.certOk = True

    def execute(self, query, useGlobal=False):
        """Runs a GSQL query and process the output.

        :param str query:
            The text of the query to run as one string.
        :param bool useGlobal:
            Connect to global graph or the current local one (default)
        """
        self._initGsql()
        self._manageCertificate()

        cmd = ['java', '-DGSQL_CLIENT_VERSION=v' + self.gsqlVersion.replace('.', '_'), '-jar', self.jarName]

        if self.useCert:
            cmd += ['-cacert', self.certPath]

        cmd += [
            '-u', self.username,
            '-p', self.password,
            '-ip', self.server]

        if not useGlobal:
            cmd += ["-g", self.graphname]

        comp = subprocess.run(cmd + [query], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        stdout = comp.stdout.decode()
        stderr = comp.stderr.decode()  # TODO: this should be parsed or handled some way, not ignored

        if self.debug:
            print("-- stdout " + "-" * 70)
            print(stdout)
            print("-- stderr " + "-" * 70)
            print(stderr)
            print("-" * 80)

        if "Connection refused." in stdout:
            if self.scheme == "https" and not self.useCert:
                raise TigerGraphException("Connection to " + self.server + " was refused. Certificate was not used.", None)
            else:
                raise TigerGraphException("Connection to " + self.server + " was refused.", None)

        try:
            return json.loads(re.search('(\{|\[).*$', stdout.replace('\n', ''))[0])
        except:
            return stdout

# EOF
