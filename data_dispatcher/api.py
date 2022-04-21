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

    def get(self, uri_suffix, none_if_not_found=False):
        if not uri_suffix.startswith("/"):  uri_suffix = "/"+uri_suffix
        url = "%s%s" % (self.ServerURL, uri_suffix)
        headers = {}
        if self.Token is not None:
            headers["X-Authentication-Token"] = self.Token.encode()
        response = requests.get(url, headers =headers)
        #print("response status code:", response.status_code)
        if response.status_code != 200:
            if none_if_not_found and response.status_code == 404:
                return None
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
    
    def __init__(self, server_url=None, auth_server_url=None, worker_id=None, worker_id_file=None, token = None, token_file = None):
        
        """Initializes the DataDispatcherClient object

        Keyword Arguments:
            server_url (str): The server endpoint URL. If unspecified, the value of the DATA_DISPATCHER_URL environment will be used
            auth_server_url (str): The endpoint URL for the Authentication server. If unspecified, the value of the DATA_DISPATCHER_AUTH_URL environment will be used
            worker_id_file (str): File path to read/store the worker ID. 
                Default: <cwd>/.data_dispatcher_worker_id
            worker_id (str): Worker ID to use when reserving next file. If unspecified, will be read from the worker ID file.
        """
        
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
        """Sets or generates new worker ID to be used for next file allocation.
        
        Keyword Arguments:
            new_id (str or None): New worker id to use. If None, a random worker_id will be generated.
            worker_id_file (str or None): Path to store the worker id. Default: <cwd>/.data_dispatcher_worker_id
        
        Returns:
            (str) assigned worker id
        """
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
        """Creates new project
        
        Args:
            files (list): each item in the list is either a dictionary with keys: "namespace", "name", "attributes" (optional) or a string "namespace:name"

        Keyword Arguments:
            common_attributes (dict): attributes to attach to each file, will be overridden by the individual file attribute values with the same key
            project_attributes (dict): attriutes to attach to the new project

        Returns:
            (dict) new project information
        """
        file_list = []
        for info in files:
            attrs = common_attributes.copy()
            if isinstance(info, dict):
                if not "namespace" in info or not "name" in info:
                    raise ValueError("File specification must include namespace and name")
                attrs.update(info.get("attributes") or {})
                name = info["name"]
                namespace = info["namespace"]
            elif isinstance(info, str):
                namespace, name = info.split(":", 1)
            else:
                raise ValueError(f"Unrecognized file info type: {info}")
            file_list.append({"name":name, "namespace":namespace, "attributes":attrs})
        return self.post("create_project", json.dumps(
                {   
                    "files":        file_list,
                    "project_attributes":   project_attributes
                }
            )
        )

    def delete_project(self, project_id):
        """Deletes a project by id

        Args:
            project_id (str): project id
        """
        return self.get(f"delete_project?project_id={project_id}")
        
    def cancel_project(self, project_id):
        """Cancels a project by id

        Args:
            project_id (str): project id

        Returns:
            (dict) project information
        """
        return self.get(f"cancel_project?project_id={project_id}")
        
    def get_project(self, project_id, with_files=True, with_replicas=False):
        """Gets information about the project
        
        Args:
            project_id (str): project id

        Keyword Arguments:
            with_files (boolean) : whether to include iformation about project files. Default: True
            with_replicas (boolean) : whether to include iformation about project file replicas. Default: False
    
        Returns:
            (dict) project information
        """
        with_files = "yes" if with_files else "no"
        with_replicas = "yes" if with_replicas else "no"
        uri = f"project?project_id={project_id}&with_files={with_files}&with_replicas={with_replicas}"
        return self.get(uri, none_if_not_found=True)
        
    def get_handle(self, project_id, namespace, name):
        """Gets information about a file handle
        
        Args:
            project_id (str): project id
            namespace (str): file namespace
            name (str): file name
    
        Returns:
            (dict) file handle information or None if not found
        """
        project_info = self.get_project(project_id, with_files=True, with_replicas=True)
        if project_info is None:
            return None
        for h in project_info.get("file_handles", []):
            if h["namespace"] == namespace and h["name"] == name:
                return h
        else:
            return None
    
    def list_projects(self, owner=None, state=None, not_state=None, attributes=None, with_files=True, with_replicas=False):
        """Lists existing projects
        
        Keyword Arguments:
            owner (str): Include only projects owned by the specified user. Default: all users
            state (str): Include only projects in specified state. Default: all states
            not_state (str): Exclude projects in the specified state. Default: do not exclude
            attributes (dict): Include only projects with specified attribute values. Default: do not filter by attributes
            with_files (boolean): Include information about files. Default: True
            with_replicas (boolean): Include information about file replics. Default: False
    
        Returns:
            list of dictionaries with information about projects selected
        """
        
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

    def next_file(self, project_id, cpu_site=None):
        """Reserves next available file from the project
        
        Args:
            project_id (int): project id to reserve a file from
            cpu_site (str): optional, if specified, the file will be reserved according to the CPU/RSE proximity map
        
        Returns:
            dictionary with file information, or None if no file was available to be reserved. The method does not block and always returns immediately.
            Use `get_project()` to see if the project is done.
        """
        
        if self.WorkerID is None:
            raise ValueError("DataDispatcherClient must be initialized with Worker ID")
        url_tail = f"next_file?project_id={project_id}&worker_id={self.WorkerID}"
        if cpu_site:
            url_tail += f"&cpu_site={cpu_site}"
        return self.get(url_tail)
        
    def get_file(self, namespace, name):
        """Gets information about a file
        
        Args:
            namespace (str): file namespace
            name (str): file name
    
        Returns:
            dictionary with the file information or None if not found
        """
        return self.get(f"file?namespace={namespace}&name={name}", none_if_not_found)

    def list_handles(self, project_id, state=None, not_state=None, rse=None, with_replicas=False):
        """Returns information about project file handles, selecting them by specified criteria
        
        Args:
            project_id (int): project id
        
        Keyword Arguments:
            state (str): select only handles in the specified state
            not_state (str): exclude handles in the specified state
            rse (str): include only handles with replicas in the specified RSE
            with_replicas (boolean): include information about replicas

        Returns:
            list of dictionaries with inofrmation about selected file handles
        """
        args = []
        if rse: args.append(f"rse={rse}")
        if project_id: args.append(f"project_id={project_id}")
        if state: args.append(f"state={state}")
        if not_state: args.append(f"not_state={not_state}")
        args = "?" + "&".join(args) if args else ""
        return self.get(f"handles{args}")
        
    def list_rses(self):
        """Return information about all RSEs
        
        Args:
        
        Returns:
            list of dictionaries with RSE information
        """
        
        return self.get(f"rses")


    def get_rse(self, name):
        """Returns information about RSE
        
        Args:
            name (str): RSE name
        
        Returns:
            dictionary with RSE information or None if not found
        """
        
        return self.get(f"get_rse?name={name}", none_if_not_found=True)

    def set_rse_availability(self, name, available):
        """Changes RSE availability flag. The user must be an admin.

        Args:
            name (str): RSE name
            available (boolean): RSE availability

        Returns:
            dictionary with updated RSE information or None if not found
        """

        available = "yes" if available else "no"
        return self.get(f"set_rse_availability?name={name}&available={available}", none_if_not_found=True)


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

    def _____reset_file(self, project_id, did, force=False):
        ""

        # not fully implemented. need to be careful with updating the project status

        """Re-submit the file for processing
        
        Args:
            project_id (str):   Project id
            did (str):          File DID (namespace:name)
            force (boolean):    Re-submit the file even if it is reserved by a worker
        
        Returns:
            dictionary with file handle information
        """
        handle_id = f"{project_id}:{did}"
        force = "yes" if force else "no"
        return self.get(f"reset_file?handle_id={handle_id}&force={force}")

