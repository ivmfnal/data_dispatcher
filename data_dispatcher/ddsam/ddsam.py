from data_dispatcher.api import DataDispatcherClient
from metacat.webapi import MetaCatClient 
import sys, getopt, os, json, pprint

from data_dispatcher.ui.cli import CLI, CLICommand, InvalidArguments

def chunked(iterable, n):
    if isinstance(iterable, (list, tuple)):
        for i in range(0, len(iterable), n):
            yield iterable[i:i+n]
    else:
        chunk = []
        for item in iterable:
            chunk.append(item)
            if len(chunk) >= n:
                yield chunk
                chunk = []
        if chunk:
            yield chunk

class LocationsCommand(CLICommand):
    
    Opts = "qs:r:jp query= schemes= rses= json pprint"
    
    Usage = """                 - print file replica locations
        [options] -q|--query <MQL query>
        [options] -q|--query @<query file>
        [options] <project id>
        [options] <file did> ...
        
        Options:
            -s|--schemes <schema>[,...]     - list of URL schemes. Default: all
            -r|--rses <rse>[,...]           - list of RSEs. Default: all
            -j|--json                       - print results as JSON
            -p|--pprint                     - print results as pprint
    """
    
    def __call__(self, command, context, opts, args):
        from rucio.client.replicaclient import ReplicaClient
        from metacat.webapi import MetaCatClient
        schemes = None if "-s" not in opts and "--schemes" not in opts else opts.get("-s", opts.get("--schemes")).split(",")
        rses = None if "-r" not in opts and "--rses" not in opts else opts.get("-r", opts.get("--rses")).split(",")

        dids = None
        if "-q" in opts or "--query" in opts:
            metacat_client = MetaCatClient()
            query = opts.get("-q", opts.get("--query"))
            if query.startswith('@'):
                query = open(query[1:], "r").read()
            else:
                query = " ".join([query] + args)
            dids = ({"scope":f["namespace"], "name":f["name"]} for f in metacat_client.query(query))
        elif len(args) == 1 and ':' not in args[0]:
            try:    project_id = int(args[0])
            except:
                print("Invadid project id format. Must be integer:", args[0], file=sys.stderr)
                sys.exit(1)
            dd_client = DataDispatcherClient()
            project_info = dd_client.get_project(project_id, with_files=True, with_replicas=False)
            if project_info is None:
                print("Project", project_id, "not found")
                sys.exit(1)
            dids = [{"scope":f["namespace"], "name":f["name"]} for f in project_info["file_handles"] 
                    if f["state"] not in ("active", "done") 
            ]
        elif all(':' in arg for arg in args):
            dids = [{"scope":pair[0], "name":pair[1]} for pair in 
                [arg.split(':', 1) for arg in args]
            ]
        else:
            raise InvalidArguments("--")

        if dids is None:
            raise InvalidArguments("File list must be specified")

        #dids = list(dids)
        #print("dids:", list(dids))

        rucio_client = ReplicaClient()
        out = {}

        for chunk in chunked(dids, 1000):
            replicas = rucio_client.list_replicas(chunk, schemes=schemes, all_states=False, ignore_availability=False)
            for r in replicas:
                namespace = r["scope"]
                name = r["name"]
                did = f"{namespace}:{name}"
                did_info = {
                    "namespace": namespace,
                    "name":name
                }
                out[did] = did_info
                did_info["replicas"] = did_replicas = {}
                for rse, urls in r["rses"].items():
                    if rses is None or rse in rses:
                        did_replicas[rse] = urls
        if "-j" in opts or "--json" in opts:
            print(json.dumps(out, indent=4, sort_keys=True))
        elif "-p" in opts or "--pprint" in opts:
            pprint.pprint(out)
        else:
            for did, did_info in sorted(out.items()):
                print(did)
                for rse, urls in sorted(did_info["replicas"].items()):
                    for url in urls:
                        print("    ", rse, url)

DD_SAM_CLI = CLI(
    "locations",   LocationsCommand()
)

def main():
    cli = DD_SAM_CLI
    cli.run(sys.argv, argv0="dd-sam")
        
if __name__ == "__main__":
    main()
        


