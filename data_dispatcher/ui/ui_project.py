import getopt, json, time, pprint, textwrap, sys
from datetime import datetime
from metacat.webapi import MetaCatClient
from .ui_lib import pretty_json, parse_attrs, print_handles

from .cli import CLI, CLICommand, InvalidOptions, InvalidArguments

class CreateCommand(CLICommand):
    
    Opts = "q:c:l:j:A:a:t:p:w:u:r:"
    Usage = """[options] [inline MQL query]         -- create project

        Use an inline query or one of the following to provide file list:
        -l (-|<flat file with file list>)               - read "namespace:name [attr=value ...]" lines from the file 
        -j (-|<JSON file with file list>)               - read JSON file with file list {"namespace":...,"name"..., "attributes":{...}}

        -w (<worker timeout>[s|m|h|d] | none)           - worker timeout in seconds (minutes, hours, days), default: 12 hours
        -t (<idle timeout>[s|m|h|d] | none)             - worker timeout in seconds (minutes, hours, days), default: 72 hours

        -q <file with MQL query>                        - read MQL query from file instead
        -c <name>[,<name>...]                           - copy metadata attributes from the query results, 
                                                          use only with -q or inline query. Otherwise ignored

        -A @<file.json>                                 - JSON file with project attributes
        -A "name=value name=value ..."                  - project attributes
        -a @<file.json>                                 - JSON file with common file attributes
        -a "name=value name=value ..."                  - common file attributes
        
        -u <username>[,...]                             - add authorized users
        -r <role>[,...]                                 - add authorized roles

        -p (json|pprint|id)                             - print created project info as JSON, 
                                                          pprint or just project id (default)
    """
    
    FileProperties = "fid,namespace,name,checksums,size,creator,created_timestamp,parents,children,datasets".split(',') # from metacat.dbobjects2

    
    def __call__(self, command, client, opts, args):
        if sum([len(args) > 0, "-l" in opts, "-j" in opts, "-q" in opts]) != 1:
            raise InvalidOptions("Use either -q or -l or -j to provide the file list")

        query = None
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
            files = list(files)
            for info in files:
                info["attributes"] = common_attrs.copy()
            
            #
            # copy file attributes from metacat
            #
    
            if "-c" in opts:
                fields = opts["-c"].split(",")
                for info in files:
                    #print(info["metadata"])
                    attrs = {}
                    for k in fields:
                        if '.' not in k and k in self.FileProperties:
                            attrs[k] = info.get(k)
                        else:
                            attrs[k] = info["metadata"].get(k)
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
                    did, rest = (tuple(line.split(None, 1)) + ("",))[:2]
                    namespace, name = did.split(":", 1)
                    files.append({"namespace":namespace, "name":name, "attributes":parse_attrs(rest)})
                    
        users = [u.strip() for u in opts.get("-u", "").split(',') if u]
        roles = [r.strip() for r in opts.get("-r", "").split(',') if r]

        worker_timeout = opts.get("-w")
        if worker_timeout is not None:
            mult = 1
            if worker_timeout[-1].lower() in "smhd":
                worker_timeout, unit = worker_timeout[:-1], worker_timeout[-1].lower()
                mult = {'s':1, 'm':60, 'h':3600, 'd':24*3600}[unit]
            worker_timeout = float(worker_timeout)*mult
        else:
            worker_timeout = 12*3600

        idle_timeout = opts.get("-t")
        if idle_timeout is not None:
            mult = 1
            if idle_timeout[-1].lower() in "smhd":
                idle_timeout, unit = idle_timeout[:-1], idle_timeout[-1].lower()
                mult = {'s':1, 'm':60, 'h':3600, 'd':24*3600}[unit]
            idle_timeout = float(idle_timeout)*mult
        else:
            idle_timeout = 72*3600

        #print("files:", files)
        #print("calling API.client.create_project...")
        #print("")
        if not files:
            print("Empty file list", file=sys.stderr)
            sys.exit(1)
        info = client.create_project(files, common_attributes=common_attrs, project_attributes=project_attrs, query=query, 
                    worker_timeout=worker_timeout, idle_timeout=idle_timeout,
                    users=users, roles=roles)
        printout = opts.get("-p", "id")
        if printout == "json":
            print(pretty_json(info))
        elif printout == "pprint":
            pprint.pprint(info)
        else:
            print(info["project_id"])

class CopyCommand(CLICommand):
    MinArgs = 1
    Opts = "A:a:t:p:"
    Usage = """[options] <project id>               -- copy project

        -A @<file.json>                                 - JSON file with project attributes to override
        -A "name=value name=value ..."                  - project attributes to override
        -a @<file.json>                                 - JSON file with file attributes to override
        -a "name=value name=value ..."                  - file attributes to override

        -t <worker timeout>|none                        - worker timeout to override

        -p (json|pprint|id)                             - print created project info as JSON, 
                                                          pprint or just project id (default)
    """
    
    def __call__(self, command, client, opts, args):
        project_id = int(args[0])
        
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

        worker_timeout = opts.get("-t")
        if worker_timeout == "none":    
            worker_timeout = None
        if worker_timeout is not None:
            worker_timeout = float(worker_timeout)

        #print("files:", files)
        info = client.copy_project(project_id, common_attributes=common_attrs, project_attributes=project_attrs, worker_timeout=worker_timeout)
        printout = opts.get("-p", "id")
        if printout == "json":
            print(pretty_json(info))
        elif printout == "pprint":
            pprint.pprint(info)
        else:
            print(info["project_id"])


class RestartCommand(CLICommand):

    MinArgs = 1
    Opts = "adfr"
    Usage = """                                     -- restart project handles
    restart <project_id> <DID> [...]                  -- restart specific handles by DIDs

    restart [selection] <project_id>                  -- restart handles by state:
        -f                                              - restart failed handles
        -d                                              - restart done handles
        -r                                              - unreserve reserved handles
        -a                                              - same as -f -d -r
    """

    def __call__(self, command, client, opts, args):
        project_id = args[0]
        if args[1:]:
            for did in args[1:]:
                if len(did.split(':', 1)) != 2:
                    raise InvalidArguments("Invalid DID format: %s" % (did,))
            client.restart_handles(project_id, handles=args[1:])
        else:
            handle_states = []
            if "-a" in opts:    
                handle_states = ["all"]
            else:
                if "-f" in opts:    handle_states.append("failed")
                if "-d" in opts:    handle_states.append("done")
                if "-r" in opts:    handle_states.append("reserved")
            
            if not handle_states:
                raise InvalidOptions("One or more handle states need to be selected")
            
            handle_states = {s:True for s in handle_states}
            client.restart_handles(project_id, **handle_states)
            
class ActivateCommand(CLICommand):

    MinArgs = 1
    Opts = "j"
    Usage = """[-j] <project_id>                    -- re-activate an abandoned project
        -j                                              - print project info as JSON
    """

    def __call__(self, command, client, opts, args):
        project_id = int(args[0])
        client.activate_project(project_id)
            
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
        if info is None:
            print("Project", project_id, "not found")
            sys.exit(1)
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
                created_timestamp = datetime.utcfromtimestamp(info["created_timestamp"]).strftime("%Y/%m/%d %H:%M:%S UTC")
                ended_timestamp = info.get("ended_timestamp") or ""
                if ended_timestamp:
                    ended_timestamp = datetime.utcfromtimestamp(ended_timestamp).strftime("%Y/%m/%d %H:%M:%S UTC")
                print("Project ID:         ", project_id)
                print("Owner:              ", info["owner"])
                print("Created:            ", created_timestamp)
                print("Ended:              ", ended_timestamp)
                print("Query:              ", textwrap.indent(info.get("query") or "", " "*10).lstrip())
                print("Status:             ", info["state"])
                print("Worker timeout:     ", info.get("worker_timeout"))
                print("Idle timeout:       ", info.get("idle_timeout"))
                print("Authorized users:   ", ", ".join(sorted(info.get("users", []))))
                print("           roles:   ", ", ".join(sorted(info.get("roles", []))))
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
            -s <state>                                  - filter by state, default: active projects only
            -a "name=value name=value ..."              - filter by attributes
    """

    def __call__(self, command, client, opts, args):
        state = opts.get("-s", "active")
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

class SearchCommand(CLICommand):
    Opts = "ju:s:"
    Usage = """[options] (-q (<query file>|-) |<search query>)            -- search projects
            -j                                          - JSON output
            -u <owner>                                  - filter by owner
            -s (<state>|all)                            - filter by state, default: active projects only
            -q <query file>|-                           - read the search query from the file or stdin if "-"
    """

    def __call__(self, command, client, opts, args):
        state = opts.get("-s", "active")
        owner = opts.get("-u")
        if "-q" in opts:
            query_file = opts["-q"]
            if query_file == "-":
                query = sys.stdin.read()
            else:
                query = open(query_file, "r").read()
        else:
            if not args:
                raise InvalidArguments("Search query is not specified")
            query = " ".join(args)
        lst = client.search_projects(query, state=state, owner=owner, with_files=True, with_replicas=False)
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
    "copy",     CopyCommand(),
    "show",     ShowCommand(),
    "list",     ListCommand(),
    "search",   SearchCommand(),
    "restart",  RestartCommand(),
    "activate", ActivateCommand(),
    "cancel",   CancelCommand(),
    "delete",   DeleteCommand()
)

