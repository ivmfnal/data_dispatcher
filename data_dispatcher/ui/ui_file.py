import sys
from .ui_lib import to_did, from_did, pretty_json, print_handles
from .cli import CLI, CLICommand, InvalidOptions, InvalidArguments

class ShowCommand(CLICommand):
    
    Opts = "jp:"
    Usage = """[-j] [-p <project id>] <file DID>
        -j                  -- JSON output
        -p <project>        -- show file handle for the project
    """
    MinArgs = 1

    def show_file(self, client, did, opts):
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

    def show_handle(self, client, project_id, did, opts):
        namespace, name = from_did(did)
        handle = client.get_handle(project_id, namespace, name)
        if not handle:
            print(f"Handle {handle_id} not found")
            sys.exit(1)
        if "-j" in opts:
            print(pretty_json(handle))
        else:
            for name, val in handle.items():
                if name != "replicas":
                    print(f"{name:10s}: {val}")
            if "replicas" in handle:
                print("replicas:")
                replicas = handle["replicas"]
                for rse, r in replicas.items():
                    r["rse"] = rse
                replicas = sorted(replicas.values(), key=lambda r: (-r["preference"], r["rse"]))
                for r in replicas:
                    print("  Preference: ", r["preference"])
                    print("  RSE:        ", r["rse"])
                    print("  Path:       ", r["path"] or "")
                    print("  URL:        ", r["url"] or "")
                    print("  Available:  ", "yes" if r["available"] else "no")

    def __call__(self, command, client, opts, args):
        did = args[0]
        if "-p" in opts:
            project_id = int(opts["-p"])
            return self.show_project(client, project, did, opts)
        else:
            return self.show_file(client, did, opts)

class ListHandlesCommand(CLICommand):
    Opts = "r:s:"
    Usage = """[options] <project id>
        -j                  -- JSON output
        -s <handle state>   -- list handles in state
        -r <rse>            -- list handles with replicas in RSE
    """
    MinArgs = 1

    def __call__(self, command, client, opts, args):
        project_id = int(args[0])
        lst = client.list_handles(project_id=project_id, rse=opts.get("-r"), state=opts.get("-s"), with_replicas=True)
        if "-j" in opts:
            print(pretty_json(lst))
        else:
            print_handles(lst)


FileCLI = CLI(
    "show",    ShowCommand(),
    "list", ListHandlesCommand()
)
