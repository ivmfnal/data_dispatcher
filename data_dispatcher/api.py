import requests, uuid, json, urllib.parse, os
from metacat.auth import TokenLib, TokenAuthClientMixin

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class ServerError(Exception):
    
    def __init__(self, url, status_code, message, body=""):
        self.URL = url
        self.StatusCode = status_code
        self.Message = message
        self.Body = to_str(body)
        
    def __str__(self):
        msg = f"DataDispatcherServer error:\n  URL: {self.URL}\n  HTTP status code: {self.StatusCode}\n  Message: {self.Message}"
        if self.Body:
            msg += "\nMessage from the server:\n"+self.Body+"\n"
        return msg

class APIError(ServerError):
    
    def __init__(self, url, status_code, body):
        ServerError.__init__(self, url, status_code, "", body)
    
    def json(self):
        #print("WebAPIError.json: body:", self.Body)
        return json.loads(self.Body)


def to_bytes(x):
    if not isinstance(x, bytes):
        x = x.encode("utf-8")
    return x
    
def to_str(x):
    if isinstance(x, bytes):
        x = x.decode("utf-8")
    return x
    
class HTTPClient(object):

    def __init__(self, server_url, token=None):
        self.ServerURL = server_url
        self.Token = token

    def get(self, uri_suffix):
        if not uri_suffix.startswith("/"):  uri_suffix = "/"+uri_suffix
        url = "%s%s" % (self.ServerURL, uri_suffix)
        headers = {}
        if self.Token is not None:
            headers["X-Authentication-Token"] = self.Token.encode()
        response = requests.get(url, headers =headers)
        #print("response status code:", response.status_code)
        if response.status_code != 200:
            raise APIError(url, response.status_code, response.text)
        if response.headers.get("Content-Type", "").startswith("text/json"):
            data = json.loads(response.text)
        else:
            data = response.text
        return data
        
    def post(self, uri_suffix, data):
        #print("post_json: data:", type(data), data)
        
        if not uri_suffix.startswith("/"):  uri_suffix = "/"+uri_suffix
        
        if data is None or isinstance(data, (dict, list)):
            data = json.dumps(data)
        else:
            data = to_bytes(data)
        #print("post_json: data:", type(data), data)
            
        url = "%s%s" % (self.ServerURL, uri_suffix)
        
        headers = {}
        if self.Token is not None:
            headers["X-Authentication-Token"] = self.Token.encode()
        #print("HTTPClient.post_json: url:", url)
        #print("HTTPClient.post_json: data:", data)
        
        response = requests.post(url, data = data, headers = headers)
        if response.status_code != 200:
            raise APIError(url, response.status_code, response.text)
        #print("response.text:", response.text)
        if response.headers.get("Content-Type", "").startswith("text/json"):
            data = json.loads(response.text)
        else:
            data = response.text
        return data
        
class DataDispatcherClient(HTTPClient, TokenAuthClientMixin):
    
    DefaultWorkerIDFile = ".data_dispatcher_worker_id"
    
    def __init__(self, server_url, worker_id=None, worker_id_file=None, token = None, token_file = None, auth_server_url=None):
        server_url = server_url or os.environ.get("DATA_DISPATCHER_URL")
        auth_server_url = auth_server_url or os.environ.get("DATA_DISPATCHER_AUTH_URL")
        TokenAuthClientMixin.__init__(self, server_url, token, token_file, auth_url=auth_server_url)

        #print("DataDispatcherClient: url:", server_url)

        worker_id_file = worker_id_file or self.DefaultWorkerIDFile
        if worker_id is None:
            if worker_id_file:
                try:    worker_id = open(worker_id_file, "r").read().strip()
                except: pass
        if worker_id is None:
            worker_id = self.gen_worker_id()
            if worker_id_file:
                open(worker_id_file, "w").write(worker_id)
        self.WorkerID = worker_id
        
        HTTPClient.__init__(self, server_url, self.token())
            
        #print("DataDispatcherClient: token:", token, "  user:", token["user"])
        
    def gen_worker_id(self):
        u = uuid.uuid4().bytes
        n = len(u)//4
        wid = [0]*n
        for i in range(n):
            for j in range(0, n, 4):
                wid[i] ^= u[j+i]
        out = bytes(wid).hex()
        #print("gen_worker_id:", out)
        return out
        
    def new_worker_id(self, new_id = None, worker_id_file=None):
        worker_id_file = worker_id_file or self.DefaultWorkerIDFile
        worker_id = new_id if new_id is not None else self.gen_worker_id()
        #print("generated worker_id:", worker_id)
        open(worker_id_file, "w").write(worker_id)
        self.WorkerID = worker_id
        return worker_id

    #
    # projects
    #
    def create_project(self, files, common_attributes={}, project_attributes={}):
        file_list = []
        for info in files:
            if not "namespace" in info or not "name" in info:
                raise ValueError("File specification must include namespace and name")
            attrs = common_attributes.copy()
            attrs.update(info.get("attributes") or {})
            item = {"name":info["name"], "namespace":info["namespace"], "attributes":attrs}
            file_list.append(item)
        return self.post("create_project", json.dumps(
                {   
                    "files":        file_list,
                    "project_attributes":   project_attributes
                }
            )
        )

    def delete_project(self, project_id):
        return self.get(f"delete_project?project_id={project_id}")
        
    def get_project(self, project_id, with_files=True, with_replicas=False):
        with_files = "yes" if with_files else "no"
        with_replicas = "yes" if with_replicas else "no"
        uri = f"project?project_id={project_id}&with_files={with_files}&with_replicas={with_replicas}"
        return self.get(uri)
    
    def list_projects(self, owner=None, state=None, not_state=None, attributes=None, with_files=True, with_replicas=False):
        suffix = "projects"
        args = []
        if owner: args.append(f"owner={owner}")
        if state: args.append(f"state={state}")
        if not_state: args.append(f"not_state={not_state}")
        args.append("with_handles=" + ("yes" if with_files else "no"))
        args.append("with_replicas=" + ("yes" if with_replicas else "no"))
        if attributes: args.append("attributes="+urllib.parse.quote(json.dumps(attributes)))
        args = "?" + "&".join(args) if args else ""
        return self.get(f"projects{args}")

    def next_file(self, project_id):
        if self.WorkerID is None:
            raise ValueError("DataDispatcherClient must be initialized with Worker ID")
        return self.get(f"next_file?project_id={project_id}&worker_id={self.WorkerID}")
        
    #
    # File handles
    #
    
    def list_handles(self, project_id=None, state=None, not_state=None, rse=None):
        args = []
        if rse: args.append(f"rse={rse}")
        if project_id: args.append(f"project_id={project_id}")
        if state: args.append(f"state={state}")
        if not_state: args.append(f"not_state={not_state}")
        args = "?" + "&".join(args) if args else ""
        return self.get(f"handles{args}")

    def find_handle(self, project_id, name=None, file_id=None, namespace=None):
        if file_id:
            suffix = f"file_handle?project_id={project_id}&file_id={file_id}"
        else:
            suffix = f"file_handle?project_id={project_id}&name={name}"
            if namespace:
                suffix += f"&namespace={namespace}"
        return self.get(suffix)

    def get_handle(self, handle_id):
        return self.get(f"file_handle?handle_id={handle_id}")

    def get_file(self, namespace, name):
        return self.get(f"file?namespace={namespace}&name={name}")

    def replica_available(self, namespace, name, rse, path=None, preference=0, url=None):
        data = {
            "path": path,
            "url": url,
            "preference": preference
        }
        suffix = f"replica_available?&rse={rse}&namespace={namespace}&name={name}"
        return self.post(suffix, data)

    def replica_unavailable(self, namespace, name, rse):
        suffix = f"replica_unavailable?&rse={rse}&namespace={namespace}&name={name}"
        return self.get(suffix)

    def file_done(self, project_id, did):
        handle_id = f"{project_id}:{did}"
        return self.get(f"release?handle_id={handle_id}&failed=no")

    def file_failed(self, project_id, did, retry=True):
        handle_id = f"{project_id}:{did}"
        retry = "yes" if retry else "no"
        return self.get(f"release?handle_id={handle_id}&failed=yes&retry={retry}")
        
    
        
        
        
