import base64
import ssl
import urllib.parse


class TigerGraphGSQL(object):
    """Native Python TigerGraph GSQL client."""

    VERSION_MAPPING = {
        "2.5.1": "2.5.0",
        "2.6.3": "2.6.2"
    }

    COMMIT_HASH = {
        "2.4.0": "f6b4892ad3be8e805d49ffd05ee2bc7e7be10dff",
        "2.4.1": "47229e675f792374d4525afe6ea10898decc2e44",
        "2.5.0": "bc49e20553e9e68212652f6c565cb96c068fab9e",
        "2.5.2": "291680f0b003eb89da1267c967728a2d4022a89e",
        "2.6.0": "6fe2f50ab9dc8457c4405094080186208bd2edc4",
        "2.6.2": "47be618a7fa40a8f5c2f6b8914a8eb47d06b7995",
        "3.0.0": "c90ec746a7e77ef5b108554be2133dfd1e1ab1b2",
        "3.0.5": "a9f902e5c552780589a15ba458adb48984359165",
        "3.0.1": "e9d3c5d98e7229118309f6d4bbc9446bad7c4c3d"
    }

    def __init__(self, host="localhost", username="tigergraph", password="tigergraph", gsPort="14240", gsqlVersion=None, certPath=None):
        """

        :param str host:
        :param str username:
        :param str password:
        :param str gsqlVersion:
        :param bool useCert:
        :param str certPath:
        """
        self.tgLocation = urllib.parse.urlparse(host)
        self.host = self.tgLocation.netloc
        self.gsPort = str(gsPort)
        self.username = username
        self.password = password

        self.gsqlVersion = "3.0.0"
        if gsqlVersion:
            self.gsqlVersion = gsqlVersion
            if gsqlVersion in self.VERSION_MAPPING:  # Fall back to earlier version of no new GSQL client was released for the given database version
                self.gsqlVersion = self.VERSION_MAPPING[gsqlVersion]

        self.commitHash = ""
        if self.gsqlVersion in self.COMMIT_HASH:
            self.commitHash = self.COMMIT_HASH[self.gsqlVersion]

        if self.gsqlVersion and self.gsqlVersion >= "2.3.0":
            self.ABORT_SESSION = "abortclientsession"
        else:
            self.ABORT_SESSION = "abortloadingprogress"

        if certPath:
            self.sslContext = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
            self.sslContext.check_hostname = False
            self.sslContext.verify_mode = ssl.CERT_REQUIRED
            self.sslContext.load_verify_locations(certPath)
            self.protocol = "https"
        else:
            self.sslContext = None
            self.protocol = "http"

        self.credentials = base64.b64encode((self.username + ":" + self.password).encode("utf-8")).decode("utf-8")

        self.isLocal = self.host.startswith("127.0.0.1") or self.host.lower().startswith("localhost")

        if self.isLocal:
            self.gsPath = "/gsql/"
            self.gsURL = self.host + ":8123"
        else:
            self.gsPath = "/gsqlserver/gsql/"
            self.gsURL = self.host + ":" + self.gsPort
