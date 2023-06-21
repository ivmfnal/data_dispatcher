import sys, time
from .ui_lib import to_did, from_did, pretty_json, print_handles
from .cli import CLI, CLICommand, InvalidOptions, InvalidArguments
from data_dispatcher.api import NotFoundError

class NextFileCommand(CLICommand):
    
    Opts = "jt:c:w:"
    MinArgs = 1
    Usage = """[options] <project_id> -- get next available file
             -w <worker id>     -- specify worker id
             -c <cpu site>      -- choose the file according to the CPU/RSE proximity map for the CPU site
             -j                 -- as JSON
             -t <timeout>       -- wait for next file until "timeout" seconds, 
                                   otherwise, wait until the project finishes
    """

    def __call__(self, command, client, opts, args):
        project_id = int(args[0])
        worker_id = opts.get("-w")
        as_json = "-j" in opts
        timeout = opts.get("-t")
        if timeout is not None: timeout = int(timeout)
        cpu_site = opts.get("-c")

        try:
            reply = client.next_file(project_id, cpu_site=cpu_site, worker_id=worker_id, timeout=timeout)
        except NotFoundError:
            print("project not found")
            sys.exit(1)

        if isinstance(reply, dict):
            if as_json:
                reply["replicas"] = sorted(reply["replicas"].values(), key=lambda r: 1000000 if r.get("preference") is None else r["preference"])
                print(pretty_json(reply))
            else:
                print("%s:%s" % (reply["namespace"], reply["name"]))
        else:
            print("timeout" if reply else "done")
            sys.exit(1)        # timeout
           

class DoneCommand(CLICommand):
    
    MinArgs = 2
    Usage = """<project id> (<DID>|all)                          -- mark a file as done
        "all" means mark all files reserved by the worker as done
    """

    def __call__(self, command, client, opts, args):
        project_id, did = args
        if did == "all":
            dids = [to_did(h["namespace"], h["name"]) for h in client.reserved_files(project_id)]
        else:
            dids = [did]
        for did in dids:
            client.file_done(int(project_id), did)
    
class FailedCommand(CLICommand):
    
    Opts = "f"
    MinArgs = 2
    Usage = """[-f] <project id> (<DID>|all)                      -- mark a file as failed
        -f            -- final, do not retry the file
        "all" means mark all files reserved by the worker as failed
    """
    
    def __call__(self, command, client, opts, args):
        project_id, did = args
        if did == "all":
            dids = [to_did(h["namespace"], h["name"]) for h in client.reserved_files(project_id)]
        else:
            dids = [did]
        for did in dids:
            client.file_failed(int(project_id), did, retry = not "-f" in opts)

class IDCommand(CLICommand):
    
    Opts = "n"
    Usage = """[-n|<worker id>]                                 -- set or print worker id
        -n          -- generate random worker id
        
        worker id will be saved in <CWD>/.worker_id
    """
    
    def __call__(self, command, client, opts, args):
        if "-n" in opts:
            worker_id = client.new_worker_id()
        elif args:
            worker_id = args[0]
            client.new_worker_id(worker_id)
        else:
            worker_id = client.WorkerID
        if not worker_id:
            print("worker id unknown")
            sys.exit(1)
        print(worker_id)
        
class ListReservedCommand(CLICommand):
    
    MinArgs = 1
    Opts = "j"
    Usage = """[-j] [-w <worker id>] <project id>              -- list files allocated to the worker
        -j                      -- as JSON
        -w <worker id>          -- specify worker id. Otherwise, use my worker id    
    """

    def __call__(self, command, client, opts, args):
        project_id = int(args[0])
        worker_id = opts.get("-w", client.WorkerID)
        as_json = "-j" in opts
        
        try:    handles = client.reserved_files(project_id, worker_id)
        except NotFoundError:
            print("project not found", file=sys.stderr)
            sys.exit(1)

        if as_json:
            print(pretty_json(handles))
        else:
            for h in handles:
                print(h["namespace"] + ':' + h["name"])


WorkerCLI = CLI(
    "id",       IDCommand(),
    "list",     ListReservedCommand(),
    "next",     NextFileCommand(),
    "done",     DoneCommand(),
    "failed",   FailedCommand()
)
