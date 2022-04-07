import getopt, sys
from .ui_lib import pretty_json

Usage = """
show rse [-j] <name>                        - show RSE information
set rse -a (yes|no) <name>                  - set RSE availability (requires admin privileges)
"""

def show_rse(client, args):
    opts, args = getopt.getopt(args, "j")
    if not args:
        print(Usage)
        sys.exit(2)
    opts = dict(opts)
    name = args[0]
    rse_info = client.get_rse(name)
    if rse_info is None:
        print(f"RSE {name} not found")
        sys.exit(1)
    if "-j" in opts:
        print(pretty_json(rse_info))
    else:
        print("RSE:           ", name)
        print("Preference:    ", rse_info["preference"])
        print("Tape:          ", "yes" if rse_info["is_tape"] else "no")
        print("Available:     ", "yes" if rse_info["is_available"] else "no")
        print("Pin URL:       ", rse_info.get("pin_url") or "")
        print("Poll URL:      ", rse_info.get("poll_url") or "")
        print("Remove prefix: ", rse_info["remove_prefix"])
        print("Add prefix:    ", rse_info["add_prefix"])

def set_rse(client, args):
    opts, args = getopt.getopt(args, "a:")
    opts = dict(opts)
    yes_no = opts.get("-a")
    if not args or "-a" not in opts or yes_no not in ("yes", "no"):
        print(Usage)
        sys.exit(2)
    name = args[0]
    client.set_rse_availability(name, yes_no == "yes")
