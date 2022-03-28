import getopt, sys
from .ui_lib import print_handles

def show_handle(client, rest):
        opts, args = getopt.getopt(rest, "j")
        opts = dict(opts)
        if len(args) < 1:
            print(Usage)
            sys.exit(2)
        did = args[0]
        handle = client.get_handle(handle_id)
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
                replicas = handle["replicas"] or []
                replicas = sorted(replicas, key=lambda r: (-r["preference"], r["rse"]))
                for r in handle["replicas"]:
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
        lst = client.list_handles(project_id=project_id, rse=opts.get("-r"), status=opts.get("-s"))
        print_handles(lst)
