import getopt, json, time, pprint
from metacat.webapi import MetaCatClient
from .ui_lib import pretty_json, parse_attrs, print_handles

from .cli import CLI, CLICommand, InvalidOptions, InvalidArguments

class CreateCommand(CLICommand):
    
    Opts = "q:c:l:j:A:a:j"
    Usage = """[options] [inline MQL query]         -- create project

        Use an inline query or one of the following to provide file list:
        -l (-|<flat file with file list>)               - read "namespace:name [attr=value ...]" lines from the file 
        -j (-|<JSON file with file list>)               - read JSON file with file list {"namespace":...,"name"..., "attributes":{...}}
        -q <file with MQL query>                        - read MQL query from file instead
            -c <name>[,<name>...]                       - copy metadata attributes from the query results, 
                                                          use with -q or inline query

        -A @<file.json>                                 - JSON file with project attributes
        -A "name=value name=value ..."                  - project attributes
        -a @<file.json>                                 - JSON file with common file attributes
        -a "name=value name=value ..."                  - common file attributes

        -p (json|pprint|id)                             - print created project info as JSON, 
                                                          pprint or just project id (default)
    """
    
    def __call__(self, command, client, opts, args):
        if sum([len(args) > 0, "-l" in opts, "-j" in opts, "-q" in opts]) != 1:
            raise InvalidOptions("Use either -q or -l or -j to provide the file list")
        
        if args:
            query = " ".join(args)
        elif "-q" in args:
            query = open(opts["-q"], "r").read()

        common_attrs = {}
        if "-a" in opts:
            attr_src = opts["-a"]
            if attr_src[0] == '@':
                common_attrs = json.load(open(attr_src[1:], "r"))
            else:
                common_attrs = parse_attrs(attr_src)
 
        project_attrs = {}
        if "-A" in opts:
            attr_src = opts["-A"]
            if attr_src[0] == '@':
                project_attrs = json.load(open(attr_src[1:], "r"))
            else:
                project_attrs = parse_attrs(attr_src)

        if query:
            files = MetaCatClient().query(query, with_metadata = "-c" in opts)
            for info in files:
                info["attributes"] = common_attrs.copy()
            #
            # copy file attributes from metacat
            #
        
            if "-c" in opts:
                fields = opts["-c"].split(",")
                for info in files:
                    #print(info["metadata"])
                    attrs = {k:info["metadata"].get(k) for k in fields}
                    info["attributes"].update(attrs)    
        
        elif "-j" in opts:
            inp = opts["-j"]
            if inp == "-":
                inp = sys.stdin
            else:
                inp = open(inp, "r")
            files = json.load(inp)

        elif "-l" in opts:
            inp = opts["-l"]
            if inp == "-":
                inp = sys.stdin
            else:
                inp = open(inp, "r")
            files = []
            for line in inp:
                line = line.strip()
                if line:
                    did, rest = (tuple(line.split(None, 1)) + (None,))[:2]
                    namespace, name = did.split(":", 1)
                    files.append({"namespace":namespace, "name":name, "attributes":parse_attrs(rest)})

        #print("files:", files)
        info = client.create_project(files, common_attributes=common_attrs, project_attributes=project_attrs)
        printout = opts.get("-p", "id")
        if printout == "json":
            print(pretty_json(info))
        elif printout == "pprint":
            pprint.pprint(info)
        else:
            print(info["project_id"])
        
class ShowCommand(CLICommand):
    
    Opts = "arjf:"
    Usage = """[options] <project_id>               -- show project info (-j show as JSON)
        -a                                              - show project attributes only
        -r                                              - show replicas information
        -j                                              - show as JSON
        -f [active|ready|available|all|reserved|failed|done]    - list files (namespace:name) only
               all       - all files, including done and failed
               active    - all except done and failed
               ready     - ready files only
               available - available files only
               reserved  - reserved files only
               failed    - failed files only
               done      - done files only
    """
    MinArgs = 1
    
    def __call__(self, command, client, opts, args):
        project_id = args[0]
        info = client.get_project(project_id, with_files=True, with_replicas=True)
        if "-a" in opts:
            attrs = info.get("attributes", {})
            if "-j" in opts:
                print(pretty_json(attrs))
            else:
                for name, value in sorted(attrs.items()):
                    print(f"{name} {value}")
        else:
            if "-j" in opts:
                print(pretty_json(info))
            elif "-f" in opts:
                filter_state = opts["-f"]
                for h in info["file_handles"]:
                    did = h["namespace"] + ":" + h["name"]
                    state = h["state"]
                    rlist = h["replicas"].values()
                    available = state == "ready" and len([r for r in rlist if r["available"] and r["rse_available"]]) > 0
                    if filter_state == "all" or \
                                filter_state in ("done", "ready", "failed", "reserved") and state == filter_state or \
                                filter_state == "available" and available or \
                                filter_state == "active" and not state in ("done", "failed"):
                        print(did)
            else:
                print("Project ID:         ", project_id)
                print("Status:             ", info["state"])
                print("Project Attributes: ")
                for k, v in info.get("attributes", {}).items():
                    if not isinstance(v, (int, float, str, bool)):
                        v = json.dumps(v)
                    print("  %-15s = %s" % (k, v))
                print("Handles:")
                print_replicas = "-r" in opts
                if "file_handles" in info:
                    print_handles(info["file_handles"], print_replicas)

class ListCommand(CLICommand):
    Opts = "ju:s:a:"
    Usage = """[options]                            -- list projects
            -j                                          - JSON output
            -u <owner>                                  - filter by owner
            -s <state>                                  - filter by state
            -a "name=value name=value ..."              - filter by attributes
    """

    def __call__(self, command, client, opts, args):
        state = opts.get("-s")
        attributes = None
        if "-a" in opts:
            attributes = parse_attrs(opts["-a"])
        owner = opts.get("-u")
        lst = client.list_projects(state=state, attributes=attributes, owner=owner, with_files=True, with_replicas=False)
        if "-j" in opts:
            print(pretty_json(list(lst)))
        else:
            print("%-15s %-15s %-19s %-15s %17s" % ("project id", "owner", "created", "state", "done/failed/files"))
            print("%s %s %s %s %s" % ("-"*15, "-"*15, "-"*19, "-"*15, "-"*17))
            for prj in lst:
                ct = time.localtime(prj["created_timestamp"])
                ct = time.strftime("%Y-%m-%d %H:%M:%S", ct)
            
                nfiles = len(prj["file_handles"])
                ready_files = reserved_files = failed_files = done_files = available_files = 0
                for h in prj["file_handles"]:
                    if h["state"] == "ready":
                        ready_files += 1
                    elif h["state"] == "done":
                        done_files += 1
                    elif h["state"] == "failed":
                        failed_files += 1
                counts = "%d/%d/%d" % (done_files, failed_files, nfiles)
                print("%-15s %-15s %19s %15s %17s" % (prj["project_id"], prj["owner"], ct, prj["state"], counts))
            print("%s %s %s %s %s" % ("-"*15, "-"*15, "-"*19, "-"*15, "-"*17))

class CancelCommand(CLICommand):
    
    Opts = "j"
    Usage = """[-j] <project_id>                    -- cancel project"""
    MinArgs = 1
    
    def __call__(self, command, client, opts, args):
            project_id = int(args[0])
            out = client.cancel_project(project_id)
            if "-j" in opts:
                print(pretty_json(out))

class DeleteCommand(CLICommand):
    
    Usage = """<project_id>                         -- delete project"""
    MinArgs = 1
    
    def __call__(self, command, client, opts, args):
            project_id = int(args[0])
            client.delete_project(project_id)

ProjectCLI = CLI(
    "create",   CreateCommand(),
    "show",     ShowCommand(),
    "list",     ListCommand(),
    "cancel",   CancelCommand(),
    "delete",   DeleteCommand()
)

