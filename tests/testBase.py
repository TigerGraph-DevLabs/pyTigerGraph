from .context import pyTigerGraph as tg

import json

sep = "-" * 60

testMetadata = True
testOthers = False

def pr(fn, res):
    print(fn + "\n")
    print("==> " + str(type(res)))
    if isinstance(res, set):
        print(res)
    else:
        print(json.dumps(res, indent=4))
    print(sep)


print("Start\n" + ("-" * 100))

# Connection and setup =========================================================

conn = tg.TigerGraphBase(host="http://localhost", debug=True)

# Metadata related functions ===================================================

if testMetadata:
    # Get schema metadata
    pr("getSchema()", conn.getSchema())
    pr("getSchema(False)", conn.getSchema(False))
    pr("getSchemaVersion()", conn.getSchemaVersion())


# Other functions ==============================================================

if testOthers:
    # echo -- Diagnostics
    # https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#get-echo-and-post-echo
    # Expected response: {'error': False, 'message': 'Hello GSQL'}
    pr("echo()", conn.echo())

    # endpoints - List of the installed endpoints and their parameters
    # https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#get-endpoints
    pr("getEndpoints()", conn.getEndpoints())
    pr("getEndpoints(builtin=True, dynamic=True, static=True)", conn.getEndpoints(builtin=True, dynamic=True, static=True))
    pr("getEndpoints(dynamic=True)", conn.getEndpoints(dynamic=True))

    # /statistics
    # https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#get-statistics
    # Returns real-time query performance statistics over the given time period
    pr("getStatistics()", conn.getStatistics())

    # /version
    # https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#get-version
    # Returns the git versions of all components of the system
    pr("getVersion()", conn.getVersion())

    # Get version for a specific component; short or long form
    pr("getVer()", conn.getVer())
    pr("getVer(full=True)", conn.getVer(full=True))

    # Get edition information (Developer or Enterprise)
    pr("getEdition()", conn.getEdition())

    # Get license info (if available)
    pr("getLicenseInfo()", conn.getLicenseInfo())

# ===============================================================================

print("\n" + ("-" * 100) + "\nEnd")
