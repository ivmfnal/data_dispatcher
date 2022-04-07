from webpie import WPApp, WPHandler
from wsdbtools import ConnectionPool
from data_dispatcher.db import DBProject, DBFileHandle, DBFile, DBRSE
from data_dispatcher.logs import Logged, init_logger
from data_dispatcher import Version
from metacat.auth import SignedToken, SignedTokenExpiredError, SignedTokenImmatureError, \
    SignedTokenUnacceptedAlgorithmError, SignedTokenSignatureVerificationError
from metacat.auth.server import BaseHandler, BaseApp, AuthHandler
import json, urllib.parse, yaml, secrets, hashlib
import requests


def to_bytes(x):
    if not isinstance(x, bytes):
        x = x.encode("utf-8")
    return x
    
def to_str(x):
    if isinstance(x, bytes):
        x = x.decode("utf-8")
    return x
    
class Handler(BaseHandler):
    
    def __init__(self, request, app):
        BaseHandler.__init__(self, request, app)
        self.auth = AuthHandler(request, app)
        
    def version(self, request, relpath, **args):
        return Version
        
    def probe(self, request, relpath, **args):
        try:    db = self.App.db()
        except:
            return 500, "Database connection error"
        else:
            return "OK"
    
    def create_project(self, request, relpath, **args):
        user = self.authenticated_user()
        if user is None:
            return "Unauthenticated user", 403
        specs = json.loads(to_str(request.body))
        files = specs["files"]
        attributes = specs.get("project_attributes", {})
        #print(specs.get("files"))
        db = self.App.db()
        project = DBProject.create(db, user.Username, attributes=attributes)
        files_converted = []
        for f in files:
            if isinstance(f, str):
                try:
                    namespace, name = f.split(":")
                except:
                    return 400, f"Invalid file specification: {f} - can not parse as namespace:name"
                files_converted.append({"namespace": namespace, "name":name})
            elif isinstance(f, dict):
                if "namespace" not in f or "name" not in f:
                    return 400, f"Invalid file specification: {f} - namespace or name missing"
                attrs = f.get("attributes", {})
                if not isinstance(attrs, dict):
                    return 400, f"Invalid file specification: {f} - invalid file attributes specification, expected a dictionary"
                files_converted.append({
                    "namespace":f["namespace"],
                    "name":f["name"],
                    "attributes":attrs
                })
            else:
                return 400, f"Invalid file specification: {f} - unsupported type"
        project.add_files(files_converted)
        self.App.project_created(project.ID)
        return json.dumps(project.as_jsonable(with_handles=True)), "text/json"
        
    def delete_project(self, equest, relpath, project_id=None, **args):
        if not project_id:
            return 400, "Project id must be specified"
        user = self.authenticated_user()
        if user is None:
            return "Unauthenticated user", 403
        
        project_id = int(project_id)
        db = self.App.db()
        project = DBProject.get(db, project_id)
        if project is None:
            return 404, "Project not found"
        if user.Username != project.Owner and not user.is_admin():
            return 403, "Forbidden"
        project.delete()
        return "null", "text/json"
        
    def replica_available(self, request, relpath, namespace=None, name=None, rse=None, **args):
        user = self.authenticated_user()
        if user is None:
            return "Unauthenticated user", 403
        data = request.json
        db = self.App.db()
        if rse is None:             return 400, "RSE must be specified"
        if None in (namespace, name):       return 400, "File namespace and name must be specified"
        preference = data.get("preference", 0)
        url = data.get("url")
        path = data.get("path")
        f = DBFile.get(db, namespace, name)
        if not f:
            return 404, "File not found"
        f.create_replica(rse, path, url, preference=preference, available=True)
        return json.dumps(f.as_jsonable(with_replicas=True)), "text/json"
            
    def replica_unavailable(self, request, relpath, namespace=None, name=None, rse=None, **args):
        user = self.authenticated_user()
        if user is None:
            return "Unauthenticated user", 403
        db = self.App.db()
        if rse is None:             return 400, "RSE must be specified"
        if None in (namespace, name):       return 400, "File namespace and name must be specified"
        f = DBFile.get(db, namespace, name)
        if not f:
            return 404, "File not found"
        r = f.get_replica(rse)
        if r is None:
            return 404, "Replica not found"
        r.Available = False
        r.save()
        return json.dumps(f.as_jsonable(with_replicas=True)), "text/json"
        
    def file(self, request, relpath, namespace=None, name=None, **args):
        if None in (namespace, name):
            return 400, "File namespace and name must be specified"
        db = self.App.db()
        f = DBFile.get(db, namespace, name)
        if f is None:
            return 404, "File not found"
        return json.dumps(f.as_jsonable(with_replicas=True)), "text/json"

    def next_file(self, request, relpath, project_id=None, worker_id=None, **args):
        #print("next_file...")
        user = self.authenticated_user()
        if user is None:
            return "Unauthenticated user", 403
        if worker_id is None or project_id is None:
            return 400, "Project ID and Worker ID must be specified"
        db = self.App.db()
        project_id = int(project_id)
        project = DBProject.get(db, project_id)
        if not project:
            return 404, "Project not found"
        if not user.is_admin() and user.Username != project.Owner:
            return 403
        handle = project.reserve_next_file(worker_id)
        if handle is None:
            return "null", "text/json"
        info = handle.as_jsonable(with_replicas=True)
        info["project_attributes"] = project.Attributes or {}
        return json.dumps(info), "text/json"
        
    def release(self, request, relpath, handle_id=None, failed="no", retry="yes", **args):
        if handle_id is None:
            return 400, "File Handle ID (<project_id>:<namespace>:<name>) must be specified"
        user = self.authenticated_user()
        if user is None:
            return "Unauthenticated user", 403

        db = self.App.db()

        project_id, namespace, name = DBFileHandle.unpack_id(handle_id)

        project = DBProject.get(db, project_id)
        if project is None:
            return 404, "Project not found"

        if not user.is_admin() and user.Username != project.Owner:
            return 403
            
        failed = failed == "yes"
        retry = retry == "yes"
        
        handle = project.release_handle(namespace, name, failed, retry)
        if handle is None:
            return 404, "Handle not found"
        
        return json.dumps(handle.as_jsonable()), "text/json"
        
    def project_files(self, request, relpath, project_id=None, state=None, ready_only="no", **args):
        if project_id is None:
            return 400, "Project ID must be specified"
        ready_only = ready_only == "yes"
        db = self.App.db()
        project_id = int(project_id)
        project = DBProject.get(db, project_id)
        if not project: return 403
        handles = project.files(state=state, ready_only=ready_only)
        return json.dumps([h.as_jsonable(with_replicas=ready_only) for h in handles]), "text/json"

    def project(self, request, relpath, project_id=None, with_files="yes", with_replicas="yes", **args):
        with_files = with_files == "yes"
        with_replicas = with_replicas == "yes"
        db = self.App.db()
        project_id = int(project_id)
        project = DBProject.get(db, project_id)
        if not project:
            return 403
        #print("project(): with handles/replicas: ", with_files, with_replicas)
        jsonable = project.as_jsonable(with_handles=with_files, with_replicas=with_replicas)
        return json.dumps(jsonable), "text/json"
    
    def projects(self, request, relpath, state=None, not_state=None, owner=None, attributes="", 
                with_handles="yes", with_replicas="yes", **args):
        with_handles = with_handles == "yes"
        with_replicas = with_replicas == "yes"
        attributes = urllib.parse.unquote(attributes)
        attributes = json.loads(attributes) if attributes else None
        db = self.App.db()
        projects = DBProject.list(db, state=state, not_state=not_state, owner=owner, attributes=attributes)
        return json.dumps([p.as_jsonable(with_handles=with_handles, with_replicas=with_replicas) for p in projects]), "text/json"
        
    def file_handle(self, request, relpath, handle_id=None, project_id=None, file_id=None, name=None, namespace=None, **args):
        db = self.App.db()
        if handle_id:
            handle = DBFileHandle.get(db, int(handle_id))
        elif project_id is None:
            return 400, "At least handle_id or project_id must be specified"
        elif file_id:
            handle = DBFileHandle.get_for_project(db, int(project_id), file_id=file_id)
        elif name:
            if not namespace:
                if not ':' in name:
                    return 400, "File namespace unspecified"
                namespace, name = name.split(':', 1)
            handle = DBFileHandle.get_for_project(db, int(project_id), file_namespace=namespace, file_name=name)
        else:
            return 400, "Either file_id, or file_name='namespace:name' or file_namespace and file_name must be specified"
        if not handle:
            return "null", "text/json"
        return json.dumps(handle.as_jsonable(with_replicas=True)), "text/json"
    
    def handles(self, request, relpath, project_id=None, state=None, rse=None, not_state=None):
        db = self.App.db()
        project_id = int(project_id)
        lst = DBFileHandle.list(db, project_id=project_id, rse=rse, not_state=not_state, state=state)
        return json.dumps([h.as_jsonable() for h in lst]), "text/json"
    
    def get_rse(self, request, relpath, name=None, **args):
        name = name or relpath
        rse = DBRSE.get(self.App.db(), name)
        if rse is None:
            return 404, f"RSE {name} not found"
        return json.dumps(rse.as_jsonable()), "text/json"
    
    def set_rse_availability(self, request, relpath, name=None, available=None, **args):
        user = self.authenticated_user()
        if user is None or not user.is_admin():
            return "Not authorized", 403

        if available not in ("yes", "no"):
            return 400, 'availability value must be specified as "yes" or "no"'

        name = name or relpath
        rse = DBRSE.get(self.App.db(), name)
        if rse is None:
            return 404, f"RSE {name} not found"

        rse.Available = available == "yes"
        rse.save()
        
        return json.dumps(rse.as_jsonable()), "text/json"


class App(BaseApp, Logged):

    def __init__(self, config):
        Logged.__init__(self, "DataServer")
        BaseApp.__init__(self, config, Handler)
        self.DaemonURL = config.get("daemon_server", {}).get("url")
        log_out = config.get("web_server",{}).get("log","-")
        init_logger(log_out, True, log_out, log_out)
        
        
    def project_created(self, project_id):
        if self.DaemonURL:
            url = f"{self.DaemonURL}/add_project?project_id={project_id}"
            response = requests.get(url)
            #self.debug(f"project_created({project_id}): response:", response)
        else:
            #self.debug("Daemon web servier URL is not configured")
            pass

def create_application(config):
    if isinstance(config, str):
        config = yaml.load(open(config, "r"), Loader=yaml.SafeLoader)
    return App(config)
    
        
if __name__ == "__main__":
    import getopt, sys
    
    opts, args = getopt.getopt(sys.argv[1:], "c:")
    opts = dict(opts)
    config = yaml.load(open(opts["-c"], "r"), Loader=yaml.SafeLoader)
    server_config = config.get("web_server", {})
    app = create_application(config)
    port = server_config.get("data_port", 8088)
    app.run_server(port)
    
    
