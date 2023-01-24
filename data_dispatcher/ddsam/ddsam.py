from data_dispatcher.api import DataDispatcherClient
from metacat.webapi import MetaCatClient 
import sys, getopt, os, json

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
    
    Usage = """
        [options] <MQL query>
        [options] -q <query file>
    """

    def __call__(self, command, context, opts, args):
        from rucio.client.replicaclient import ReplicaClient
        from metacat.webapi import MetaCatClient 
        
        query = None
        if "-q" in opts:
            query = open(opts["-q"], "r").read()
        elif args:
            query = " ".join(args)
        
        if query is None:
            raise InvalidArguments("No query given")
        
        metacat_client = MetaCatClient()
        rucio_client = ReplicaClient()
        files = list(metacat_client.query(query))
        for chunk in chunked(files, 100):
            dids = [{"scope":f["namespace"], "name":f["name"]} for f in chunk]
            rucio_replicas = rucio_client.list_replicas(dids, schemes=["https", "root", "http"],
                                    all_states=False, ignore_availability=False)
            for r in rucio_replicas:
                namespace = r["scope"]
                name = r["name"]
                print(f"{namespace}:{name}")
                for rse, urls in r["rses"].items():
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
        


