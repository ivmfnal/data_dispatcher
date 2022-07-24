import json, sys, getopt

Usage="""
python json_extract.py [-n (null|omit|error|default:<value>)] <file.json> [<path item>/<path item>...]
"""

not_found_mode = "omit"
defaut_value = None

class Omit: pass

O = Omit()

class NotFound(Exception):
    def __init__(self, data, index):
        self.Data = data
        self.Index = index

def apply_default(data, index):
    if not_found_mode == "omit":      return O
    elif not_found_mode == "null":    return None
    elif not_found_mode == "default":    return defaut_value
    else:
        raise NotFound(data, index)

def getitem(data, index):
    try:
        if isinstance(data, (dict, list)):
            if isinstance(data, list):
                index = int(index)
            v = data[index]
        else:
            v = apply_default(data, index)
    except (ValueError, KeyError, IndexError):
        return apply_default(data, index)
    return v        

def extract(data, path):
    # not_found_mode can be "omit", "null", "error"
    #print("extract: data:", data)
    #print("         path:", path)
    if not path:
        return data
    index, down = path[0], path[1:]
    if not index:
        return data
    if isinstance(data, dict):
        if index == '*':
            out = {}
            for key, value in data.items():
                v = extract(value, down)
                if v is not O:
                    out[key] = v
            return out
        else:
            return extract(getitem(data, index), down)
    elif isinstance(data, list):
        if index == '*':
            return [v for v in [extract(value, down) for value in data] if v is not O]
        elif ':' in index:
            tail = index.split(':', 1)[1]
            if ':' in tail:
                i0, i1, i2 = [int(x) for x in index.split(':', 2)]
                return [v for v in [extract(value, down) for value in data[i0:i1:i2]] if v is not O]
            else:
                i0, i1 = [int(x) for x in index.split(':', 1)]
                return [v for v in [extract(value, down) for value in data[i0:i1]] if v is not O]
        else:
            return extract(getitem(data, index), down)
    else:
        return extract(getitem(data, index), down)

opts, args = getopt.getopt(sys.argv[1:], "n:qj")
opts = dict(opts)
quiet = '-q' in opts
force_json = '-j' in opts
not_found_mode=opts.get("-n", "omit")
if not_found_mode.startswith("default:"):
    not_found_mode, defaut_value = not_found_mode.split(':', 1)
    if defaut_value in ("null", "None"):
        defaut_value = None
    elif defaut_value.lower() in ("true", "false"):
        defaut_value = defaut_value.lower() == "true"
    elif defaut_value[0] in '\'"' and defaut_value[0] == defaut_value[-1]:
        default_value = default_value[1:-1]
    else:
        try:
            defaut_value = int(defaut_value)
        except ValueError:
            try:
                defaut_value = float(defaut_value)
            except:
                pass
            
if not args:
    print(Usage)
    sys.exit(2)
elif len(args) == 1:
    file_path = args[0]
    path = ""
else:
    file_path, path = args

if file_path == '-':
    data = json.load(sys.stdin)
else:
    data = json.load(open(file_path, "r"))

try:    extracted = extract(data, path.split('/'))
except NotFound as e:
    if not quiet:   print("Index", e.Index, "not found in:", json.dumps(e.Data, indent=4, sort_keys=True))
    sys.exit(1)
else:
    if not quiet:   
        if isinstance(extracted, (dict,list)) or force_json:
            print(json.dumps(extracted, indent=4, sort_keys=True))
        else:
            print(extracted)

