import getopt, sys
from .ui_lib import to_did, from_did, pretty_json

Usage = """
replica available [options] <rse> <file_namespace:file_name>        - add/modify available file replica
    -u <url>
    -p <path>
    -n <preference>     - default: 0
    -j                  - print final file info as JSON

replica unavailable <rse> <file_namespace:file_name>                - mark file replica unavailable

show file [-j] <file_namespace:file_name>                   - show file info (-j = as JSON)
"""

def replica_available(client, rest):
        opts, args = getopt.getopt(rest, "p:n:u:j")
        opts = dict(opts)
        if len(args) != 2:
            print(Usage)
            sys.exit(2)
        rse, did = args
        namespace, name = from_did(did)
        path = opts.get("-p")
        url = opts.get("-u")
        preference = int(opts.get("-n", 0))
        data = client.replica_available(namespace, name, rse, path=path, url=url, preference=preference)
        if "-j" in opts:
            print(pretty_json(data))

def replica_unavailable(client, rest):
        opts, args = getopt.getopt(rest, "j")
        opts = dict(opts)
        if len(args) != 2:
            print(Usage)
            sys.exit(2)
        rse, did = args
        namespace, name = from_did(did)
        data = client.replica_unavailable(namespace, name, rse)
        if "-j" in opts:
            print(pretty_json(data))
            
def show_file(client, rest):
        opts, args = getopt.getopt(rest, "j")
        did = args[0]
        namespace, name = from_did(did)
        data = client.get_file(namespace, name)
        if "-j" in opts:
            print(pretty_json(data))
        else:
            replicas = data["replicas"].items()
            replicas = sorted(replicas, key=lambda info: (not info[1].get("available"), info[0]))
            print("Namespace: ", namespace)
            print("Name:      ", name)
            print("Replicas:  ", len(replicas))
            for rse, info in replicas:
                print("%1s %-25s %s" % ("A" if info["available"] else "U", rse, info.get("url") or ""))
                if info.get("path"):
                    print("%27s %s" % ("", info["path"]))
