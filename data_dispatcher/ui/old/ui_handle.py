import getopt, sys
from .ui_lib import print_handles, pretty_json

def show_handle(client, rest):
        opts, args = getopt.getopt(rest, "j")
        opts = dict(opts)
        if len(args) < 1:
            print(Usage)
            sys.exit(2)
        project_id, did = args
        project_id = int(project_id)
        namespace, name = did.split(":", 1)
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

def list_handles(client, rest):
        opts, args = getopt.getopt(rest, "p:s:r:")
        opts = dict(opts)
        project_id = opts.get("-p")
        if project_id is not None:
            project_id = int(project_id)
        lst = client.list_handles(project_id=project_id, rse=opts.get("-r"), state=opts.get("-s"), with_replicas=True)
        print_handles(lst)
