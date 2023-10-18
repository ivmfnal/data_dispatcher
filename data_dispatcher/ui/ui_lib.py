import json
from .cli.tabular import Table, Column

def to_did(namespace, name):
    return f"{namespace}:{name}"

def from_did(did):
    return tuple(did.split(":", 1))

def pretty_json(data):
    return json.dumps(data, indent=2, sort_keys=True)
    
def parse_attrs(text):
    parts = [w.split("=",1) for w in text.split()]
    out = {}
    for k, v in parts:
        try:    v = int(v)
        except:
            try:    v = float(v)
            except:
                v = {"null":None, "true":True, "false":False}.get(v, v)
        out[k]=v
    return out
    
def print_handles(handles, print_replicas):
    
    state_order = {
        "initial":      0,
        "reserved":     1,
        "done":         3,
        "failed":       4
    }
    
    table = Table("Status", "Replicas", "Attempts", "Worker", 
            Column("File" + (" / RSE, avlbl, URL" if print_replicas else ""),
                left=True)
    )

    handles = list(handles)

    for h in handles:
        #print("print_handles: handle:", h)
        h["is_available"] = any(r["available"] and r.get("rse_available") for r in h["replicas"].values())

    handles = sorted(handles, key=lambda h: (0 if h["is_available"] else 1, state_order[h["state"]], h["attempts"], h["namespace"], h["name"]))

    for f in handles:
        rlist = f["replicas"].values()
        available_replicas = len([r for r in rlist if r["available"] and r["rse_available"]])
        nreplicas = len(rlist)
        state = f["state"]
        if state == "initial" and available_replicas:
            state = "available"
        table.add_row(
            state,
            "%4d/%-4d" % (available_replicas, nreplicas),
            f["attempts"],
            f["worker_id"],
            "%s:%s" % (f["namespace"], f["name"])
        )
        if print_replicas:
            for r in sorted(f["replicas"].values(), key=lambda r: r["preference"]):
                table.add_row(None, None, None, None,
                    " %-10s %-3s %s" % (r["rse"], 
                        "yes" if r["available"] else "no", r["url"] or ""
                    )
                )
    table.print()




