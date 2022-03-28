import json

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
        "ready":    0,
        "reserved":     1,
        "done":         3,
        "failed":       4
    }
    
    print("%10s %9s %8s %10s %s" % (
            "Status", "Replicas", "Attempts", "Worker", "File" + (" / replica pref., RSE, avlbl, URL" if print_replicas else "")
        )) 
    print("%s" % ("-"*110,)) 

    handles = list(handles)

    for h in handles:
        h["is_available"] = any(r["available"] for r in h["replicas"].values())
    
    handles = sorted(handles, key=lambda h: (0 if h["is_available"] else 1, state_order[h["state"]], h["attempts"], h["namespace"], h["name"]))
    
    for f in handles:
        rlist = f["replicas"].values()
        available_replicas = len([r for r in rlist if r["available"]])
        nreplicas = len(rlist)
        state = f["state"]
        if state == "ready" and available_replicas:
            state = "available"
        print("%10s %4d/%-4d %8s %10s %s:%s" % (
            state,
            available_replicas, nreplicas,
            f["attempts"],
            f["worker_id"] or "",
            f["namespace"],
            f["name"]                    
        ))
        if print_replicas:
            for r in f["replicas"].values():
                print("%40s %-4d %-10s %-3s %s" % ("", r["preference"], r["rse"], "yes" if r["available"] else "no", r["url"] or ""))
    print("%s" % ("-"*110,)) 




