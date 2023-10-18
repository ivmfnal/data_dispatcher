import getopt, sys
from .ui_lib import pretty_json
from .cli import CLI, CLICommand, InvalidOptions, InvalidArguments
from .cli.tabular import Table, Column

Usage = """
show rse [-j] <name>                        - show RSE information
set rse -a (yes|no) <name>                  - set RSE availability (requires admin privileges)
"""

class ShowCommand(CLICommand):
    
    Opts = "j"
    Usage = """[-j] <rse>
        -j          -- JSON output
    """
    MinArgs = 1
    
    def __call__(self, command, client, opts, args):
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

class SetAvailability(CLICommand):

    Usage = "(up|down) <rse>"
    MinArgs = 2
    
    def __call__(self, command, client, opts, args):
        up_down, name = args
        if up_down not in ("up", "down"):
            print(Usage)
            sys.exit(2)
        return client.set_rse_availability(name, up_down == "up")
    
class ListTabularCommand(CLICommand):

    Opts = "j"
    Usage = """[-j]             -- JSON output"""

    def __call__(self, command, client, opts, args):
        rses = sorted(client.list_rses(), key=lambda r: r["name"])
        if "-j" in opts:
            print(pretty_json(rses))
        elif rses:
            table = Table(Column("Name", min_width=30, left=True), 
                "Tape", "Status", 
                Column("Description", min_width=60, left=True)
            )

            for rse in rses:
                table.add_row(rse["name"],
                    "tape" if rse["is_tape"] else "",
                    "up" if rse["is_available"] else "down",
                    rse["description"])
            table.print()
    
class ListCommand(CLICommand):

    Opts = "j"
    Usage = """[-j]             -- JSON output"""

    def __call__(self, command, client, opts, args):
        rses = sorted(client.list_rses(), key=lambda r: r["name"])
        if "-j" in opts:
            print(pretty_json(rses))
        elif rses:
            print("%-40s %3s %6s %s" % (
                    "Name", "Tape", "Status", "Description"
                )) 
            print("%s" % ("-"*105,)) 

            for rse in rses:
                print("%-40s %3s %6s %s" % (
                    rse["name"],
                    "tape" if rse["is_tape"] else "    ",
                    "up" if rse["is_available"] else "down",
                    rse["description"]
                ))
            print("%s" % ("-"*105,)) 
    
RSECLI = CLI(
    "list",         ListTabularCommand(),
    "set",          SetAvailability(),
    "show",         ShowCommand()
)        
    
    
