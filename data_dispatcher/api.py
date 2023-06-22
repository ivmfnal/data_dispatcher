import requests, uuid, json, urllib.parse, os, time, random
from metacat.common import TokenLib, HTTPClient, TokenAuthClientMixin

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

class NotFoundError(ServerError):

    def __init__(self, url, message):
        ServerError.__init__(self, url, 404, message, "")

class TimeoutError(Exception):
    pass

def to_bytes(x):
    if not isinstance(x, bytes):
        x = x.encode("utf-8")
    return x
    
def to_str(x):
    if isinstance(x, bytes):
        x = x.decode("utf-8")
    return x

class DataDispatcherClient(HTTPClient, TokenAuthClientMixin):
    
    DefaultWorkerIDFile = ".data_dispatcher_worker_id"
    
    def __init__(self, server_url=None, auth_server_url=None, worker_id=None, worker_id_file=None, 
            token = None, token_file = None, token_library = None, 
            cpu_site="DEFAULT", timeout=300):
        
        """Initializes the DataDispatcherClient object

        Keyword Arguments:
            server_url (str): The server endpoint URL. If unspecified, the value of the DATA_DISPATCHER_URL environment will be used
            auth_server_url (str): The endpoint URL for the Authentication server. If unspecified, the value of the DATA_DISPATCHER_AUTH_URL environment will be used
            worker_id_file (str): File path to read/store the worker ID. 
                Default: <cwd>/.data_dispatcher_worker_id
            worker_id (str): Worker ID to use when reserving next file. If unspecified, will be read from the worker ID file.
            cpu_site (str): Name of the CPU site where the client is running, optional. Will be used when reserving project files.
            timeout (float or int): Number of seconds to wait for a response.
        """
        
        server_url = server_url or os.environ.get("DATA_DISPATCHER_URL")
        auth_server_url = auth_server_url or os.environ.get("DATA_DISPATCHER_AUTH_URL")
        TokenAuthClientMixin.__init__(self, server_url, auth_server_url, token=token, token_file=token_file, token_library=token_library)

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
        self.CPUSite = cpu_site

        HTTPClient.__init__(self, server_url, token=self.token(), timeout=timeout)
        
    @staticmethod
    def random_worker_id(prefix=""):
        """
        Static method to generate random worker id
        """
        return prefix + uuid.uuid4().hex[:8]

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
        
    def version(self):
        """Returns the server version as a string
        """
        return self.get("version")

    #
    # projects
    #
    
    DEFAULT_IDLE_TIMEOUT = 72*3600      # 72 hors
    
    def create_project(self, files, common_attributes={}, project_attributes={}, query=None, worker_timeout=None,
            idle_timeout = DEFAULT_IDLE_TIMEOUT, users=[], roles=[]):
        """Creates new project
        
        Parameters
        ----------
        files : list
            Each item in the list is either a dictionary with keys: "namespace", "name", "attributes" (optional) or a string "namespace:name"
        common_attributes : dict
            attributes to attach to each file, will be overridden by the individual file attribute values with the same key
        project_attributes : dict
            attriutes to attach to the new project
        query : str 
            MQL query to be associated with the project. Thit attribute optiona and is not used by Data Dispatcher in any way.
            It is used for informational purposes only.
        worker_timeout : int or float
            If not None, all file handles will be automatically released if allocated by same worker for longer than the ``worker_timeout`` seconds
        idle_timeout : int or float
            If there is no file reserve/release activity for the specified time interval, the project goes into "abandoned" state.
            Default is 72 hours (3 days). If set to None, the project remains active until complete.
        users : list of strings
            List of users who can use the worker interface (next_file, done, failed...), in addition to the project
            creator.
        roles : list of strings
            List of roles, members of which are authorized to use the worker interface.

        Returns
        -------
        dict
            new project information
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
                    "files":                file_list,
                    "project_attributes":   project_attributes,
                    "query":                query,
                    "worker_timeout":       worker_timeout,
                    "idle_timeout":         idle_timeout,
                    "users":                users or [],
                    "roles":                roles or []
                }
            )
        )
        
    def copy_project(self, project_id, common_attributes={}, project_attributes={}, worker_timeout=None):
        """Creates new project
        
        Args:
            project_id (int): id of the project to copy
        
        Keyword Arguments:
            common_attributes (dict): file attributes to override
            project_attributes (dict): project attributes to override
            worker_timeout (int or float): worker timeout to override

        Returns:
            (dict) new project information
        """
        return self.post("copy_project", json.dumps(
                {   
                    "project_id":           project_id,
                    "file_attributes":      common_attributes,
                    "project_attributes":   project_attributes,
                    "worker_timeout":       worker_timeout
                }
            )
        )

    def restart_handles(self, project_id, done=False, failed=False, reserved=False, all=False, handles=[]):
        """Restart processing of project file handles
        
        Args:
            project_id (int): id of the project to restart

        Keyword Arguments:
            done (boolean): default=False, restart done handles
            reserved (boolean): default=False, restart reserved handles
            failed (boolean): default=False, restart failed handles
            all (boolean): default=False, restart all handles
            handles (list of DIDs): default=[], restart specific handles
        
        Returns:
            (dict) project information
        """
        if not handles:
            if all: done = failed = reserved = True
            selection = dict(project_id=project_id, done=done, failed=failed, reserved=reserved)
        else:
            selection = dict(project_id=project_id, handles=handles)
            
        return self.post("restart_handles", json.dumps(selection))

    def delete_project(self, project_id):
        return self.get(f"delete_project?project_id={project_id}")

    def cancel_project(self, project_id):
        """Cancels a project by id

        Args:
            project_id (str): project id

        Returns:
            (dict) project information
        """
        return self.get(f"cancel_project?project_id={project_id}")

    def activate_project(self, project_id):
        """
        Resets the state of an abandoned project back to "active"
        """
        return self.get(f"activate_project?project_id={project_id}")

    def get_project(self, project_id, with_files=True, with_replicas=False):
        """Gets information about the project
        
        Args:
            project_id (str): project id

        Keyword Arguments:
            with_files (boolean) : whether to include iformation about project files. Default: True
            with_replicas (boolean) : whether to include iformation about project file replicas. Default: False
    
        Returns:
            (dict) project information or None if project not found.

            The dictionary will include the following values:
        
                * project_id: numeric, project id
                * owner: str, project owner username,
                * state: str, current project state,
                * attributes: dict, project metadata attributes as set by the create_project(),
                * created_timestamp: numeric, timestamp for the project creation time,
                * ended_timestamp: numeric or None, project end timestamp,
                * active: boolean, whether the project is active - at least one handle is not done or failed,
                * query: str, MQL query string associated with the project,
                * worker_timeout: numeric or None, worker idle timeout, in seconds
                * idle_timeout: numeric or None, project inactivity timeout in seconds
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
    
    def list_projects(self, owner=None, state="active", not_state="abandoned", attributes=None, with_files=True, with_replicas=False):
        """Lists existing projects
        
        Keyword Arguments:
            owner (str): Include only projects owned by the specified user. Default: all users
            state (str): Include only projects in specified state. Default: active only
            not_state (str): Exclude projects in the specified state. Default: exclude abandoned
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

    def search_projects(self, search_query, owner=None, state="active", with_files=True, with_replicas=False):
        """Lists existing projects
        
        Arguments:
            search_query (str): project search query in subset of MQL
        
        Keyword Arguments:
            owner (str): Include only projects owned by the specified user. Default: all users
            with_files (boolean): Include information about files. Default: True
            with_replicas (boolean): Include information about file replics. Default: False
    
        Returns:
            list of dictionaries with information about projects found
        """
        
        info = {
            "query":    search_query,
            "with_handles": with_files,
            "with_replicas": with_replicas,
        }
        if state != "all":  info["state"] = state
        if owner:   info["owner"] = owner
        return self.post("search_projects", json.dumps(info))

    def __next_file(self, project_id, cpu_site, worker_id):
        if worker_id is None:
            raise ValueError("DataDispatcherClient must be initialized with Worker ID")
        url_tail = f"next_file?project_id={project_id}&worker_id={worker_id}"
        if cpu_site:
            url_tail += f"&cpu_site={cpu_site}"
        return self.get(url_tail)

    def next_file(self, project_id, cpu_site=None, worker_id=None, timeout=None, stagger=10):
        """Reserves next available file from the project

        Args:
            project_id (int): project id to reserve a file from
            cpu_site (str): optional, if specified, the file will be reserved according to the CPU/RSE proximity map
            timeout (int or float): optional, if specified, time to wait for a file to become available. Otherwise, will wait indefinitely
            stagger (int or float): optional, introduce a random delay between 0 and <stagger> seconds before sending first request. This will help mitigate the effect of synchronous stard of multiple workers. Default: 10

        Returns:
            Dictionary or boolean.
            If dictionary, the dictionary contains the reserved file information. "replicas" field will be a dictionary will contain a subdictionary with replicas information indexed by RSE name.
            If ``True``: the request timed out, but can be retried.
            If ``False``: the project has ended.
        """
        worker_id = worker_id or self.WorkerID
        cpu_site = cpu_site or self.CPUSite
        t1 = None if timeout is None else time.time() + timeout
        if stagger:
            time.sleep(stagger * random.random())
        retry = True
        while retry:
            reply = self.__next_file(project_id, cpu_site, worker_id)
            info = reply.get("handle")
            if info:
                return info         # allocated
            reason = reply.get("reason")
            retry = reply["retry"]
            if retry:
                if t1 is None or time.time() < t1:
                    dt = 60
                    if t1 is not None:
                        dt = min(dt, t1-time.time())
                    dt0 = min(dt, 1.0)
                    if dt > 0:
                        time.sleep(dt0 + (dt-dt0)*random.random())
                else:
                    break
        return retry            # True=try again later, False=project ended

    def reserved_handles(self, project_id, worker_id=None):
        """Returns list of file handles reserved in the project by given worker
        
        Args:
            project_id (int): Project id
            worker_id (str or None): Worker id. If None, client's worker id will be used
    
        Returns:
            list of dictionaries with the file handle information
        """
        worker_id = worker_id or self.WorkerID
        project = self.get_project(project_id)
        return [h for h in project["file_handles"]
            if h["state"] == "reserved" and h["worker_id"] == worker_id
        ]

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

    def file_done(self, project_id, did):
        """Notifies Data Dispatcher that the file was successfully processed and should be marked as "done".
        
        Args:
            project_id (int): project id
            did (str): file DID ("<namespace>:<name>")
        """
        handle_id = f"{project_id}:{did}"
        return self.get(f"release?handle_id={handle_id}&failed=no")

    def file_failed(self, project_id, did, retry=True):
        """Notifies Data Dispatcher that the file was successfully processed and should be marked as "done".
        
        Args:
            project_id (int): project id
            did (str): file DID ("<namespace>:<name>")
        """
        handle_id = f"{project_id}:{did}"
        retry = "yes" if retry else "no"
        return self.get(f"release?handle_id={handle_id}&failed=yes&retry={retry}")

    #
    # Deprecated, undocumented, unsupported
    #
    
    def get_file(self, namespace, name):
        """Deprecated
        """
        return self.get(f"file?namespace={namespace}&name={name}", none_if_not_found=True)

    def list_handles(self, project_id, state=None, not_state=None, with_replicas=False):
        """Deprecated
        """
        args = []
        if project_id: args.append(f"project_id={project_id}")
        if state: args.append(f"state={state}")
        if not_state: args.append(f"not_state={not_state}")
        args = "?" + "&".join(args) if args else ""
        return self.get(f"handles{args}")
        
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

    
