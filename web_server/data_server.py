from webpie import WPApp, WPHandler
from wsdbtools import ConnectionPool
from data_dispatcher.db import DBProject, DBFileHandle, DBRSE, DBProximityMap
from data_dispatcher.logs import Logged, init_logger
from data_dispatcher import Version
from metacat.auth import SignedToken, SignedTokenExpiredError, SignedTokenImmatureError, \
    SignedTokenUnacceptedAlgorithmError, SignedTokenSignatureVerificationError
from metacat.auth.server import BaseHandler, BaseApp, AuthHandler
import json, urllib.parse, yaml, secrets, hashlib
import requests
from datetime import datetime, timedelta


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
        #print("create_project()...")
        user, error = self.authenticated_user()
        #print("authenticated user:", user)
        if user is None:
            print("returning 401", error)
            return 401, error
        specs = json.loads(to_str(request.body))
        #print("specs:", specs)
        files = specs["files"]
        query = specs.get("query")
        worker_timeout = specs.get("worker_timeout")
        if worker_timeout is not None:
            worker_timeout = timedelta(seconds=worker_timeout)
        idle_timeout = specs.get("idle_timeout")
        if idle_timeout is not None:
            idle_timeout = timedelta(seconds=idle_timeout)
        attributes = specs.get("project_attributes", {})
        #print("opening DB...")
        db = self.App.db()
        #print("calling DBProject.create()...")
        project = DBProject.create(db, user.Username, attributes=attributes, query=query, worker_timeout=worker_timeout,
                        idle_timeout=idle_timeout)
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
        
    def delete_project(self, request, relpath, project_id=None, **args):
        if not project_id:
            return 400, "Project id must be specified"
        user, error = self.authenticated_user()
        if user is None:
            return 401, error
        
        project_id = int(project_id)
        db = self.App.db()
        project = DBProject.get(db, project_id)
        if project is None:
            return 404, "Project not found"
        if user.Username != project.Owner and not user.is_admin():
            return 403, "Forbidden"
        project.delete()
        return "null", "text/json"
        
    def activate_project(self, request, relpath, project_id=None, **args):
        if not project_id:
            return 400, "Project id must be specified"
        user, error = self.authenticated_user()
        if user is None:
            return 401, error
           
        project_id = int(project_id)
        db = self.App.db()
        project = DBProject.get(db, project_id)
        if project is None:
            return 404, "Project not found"
        if user.Username != project.Owner and not user.is_admin():
            return 403, "Forbidden"
        project.activate()
        return json.dumps(project.as_jsonable(with_replicas=True)), "text/json"

    def restart_project(self, request, relpath, project_id=None, force="no", failed_only="yes", **args):
        force = force == "yes"
        failed_only = failed_only == "yes"
        if not project_id:
            return 400, "Project id must be specified"
        user, error = self.authenticated_user()
        if user is None:
            return 401, error
           
        project_id = int(project_id)
        db = self.App.db()
        project = DBProject.get(db, project_id)
        if project is None:
            return 404, "Project not found"
        if user.Username != project.Owner and not user.is_admin():
            return 403, "Forbidden"
        project.restart(force=force, failed_only=failed_only)
        return json.dumps(project.as_jsonable(with_replicas=True)), "text/json"
        
    def restart_handles(self, request, relpath, **args):
        params = json.loads(to_str(request.body))
        project_id = params.get("project_id")
        if not project_id:
            return 400, "Project id must be specified"
        project_id = int(project_id)
            
        user, error = self.authenticated_user()
        if user is None:
            return 401, error

        db = self.App.db()
        project = DBProject.get(db, project_id)
        if project is None:
            return 404, "Project not found"
        if user.Username != project.Owner and not user.is_admin():
            return 403, "Forbidden"

        dids = params.get("handles")
        if dids is not None:
            project.restart_handles(dids=dids)
        else:
            states = [s for s in DBFileHandle.States if params.get(s)]
            project.restart_handles(states=states)

        return json.dumps(project.as_jsonable(with_replicas=True)), "text/json"
        
    def copy_project(self, request, relpath, **args):
        user, error = self.authenticated_user()
        if user is None:
            return 401, error
        specs = json.loads(to_str(request.body))
        project_attributes = specs.get("project_attributes", {})
        file_attributes = specs.get("file_attributes", {})
        project_id = specs["project_id"]
        #print(specs.get("files"))
        db = self.App.db()
        original_project = DBProject.get(db, project_id)
        worker_timeout = specs.get("worker_timeout", original_project.WorkerTimeout)
        if original_project is None:
            return 404, "Project not found"
        files_updated = []
        for h in original_project.handles():
            attrs = h.Attributes.copy()
            attrs.update(file_attributes)
            files_updated.append(dict(
                namespace=h.Namespace,
                name=h.Name,
                attributes=attrs
            ))
        project_attrs = original_project.Attributes.copy()
        project_attrs.update(project_attributes)
        project = DBProject.create(db, user.Username, attributes=project_attrs, query=original_project.Query, worker_timeout=worker_timeout)
        project.add_files(files_updated)
        project.add_log("event", event="copied", source=project_id, override=dict(
            project=project_attrs, file=file_attributes
        ))
        self.App.project_created(project.ID)
        return json.dumps(project.as_jsonable(with_handles=True)), "text/json"

    def cancel_project(self, request, relpath, project_id=None, **args):
        if not project_id:
            return 400, "Project id must be specified"
        user, error = self.authenticated_user()
        if user is None:
            return 401, error
        
        project_id = int(project_id)
        db = self.App.db()
        project = DBProject.get(db, project_id)
        if project is None:
            return 404, "Project not found"
        if user.Username != project.Owner and not user.is_admin():
            return 403, "Forbidden"
        project.cancel()
        return json.dumps(project.as_jsonable(with_replicas=True)), "text/json"
        
    def next_file(self, request, relpath, project_id=None, worker_id=None, cpu_site=None, **args):
        #print("next_file...")
        user, error = self.authenticated_user()
        if user is None:
            return 401, error
        if worker_id is None or project_id is None:
            return 400, "Project ID and Worker ID must be specified"
        db = self.App.db()
        project_id = int(project_id)
        project = DBProject.get(db, project_id)
        if not project:
            return 404, "Project not found"
        if not user.is_admin() and user.Username != project.Owner:
            return 403, "Not authorized"
        if project.State == "abandoned":
            project.activate()
        elif project.State != "active":
            return 400, f"Inactive project. State={project.State}"
        handle, reason, retry = project.reserve_handle(worker_id)
        if handle is None:
            out = {
                "handle": None,
                "reason": reason,
                "retry": retry
            }
        else:
            pmap = self.App.proximity_map()
            info = handle.as_jsonable(with_replicas=True)
            info["replicas"] = {rse:r for rse, r in info["replicas"].items() if r["available"] and r["rse_available"]}
            for rse, r in info["replicas"].items():
                try:    proximity = pmap.proximity(cpu_site, rse)
                except KeyError:
                    proximity = None
                r["preference"] = proximity
            info["project_attributes"] = project.Attributes or {}
            out = {
                "handle": info,
                "reason": "reserved",
                "retry": False
            }
        return json.dumps(out), "text/json"

    def release(self, request, relpath, handle_id=None, failed="no", retry="yes", **args):
        if handle_id is None:
            return 400, "File Handle ID (<project_id>:<namespace>:<name>) must be specified"
        user, error = self.authenticated_user()
        if user is None:
            return 401, error

        db = self.App.db()

        project_id, namespace, name = DBFileHandle.unpack_id(handle_id)

        project = DBProject.get(db, project_id)
        if project is None:
            return 404, "Project not found"

        if not user.is_admin() and user.Username != project.Owner:
            return 403, "Not authorized"
            
        failed = failed == "yes"
        retry = retry == "yes"
        
        handle = project.release_handle(namespace, name, failed, retry)
        if handle is None:
            return 404, "Handle not found or was not reserved"
        if project.State == "abandoned":
            project.activate()
        return json.dumps(handle.as_jsonable()), "text/json"

    def ______reset_file(self, request, relpath, handle_id=None, force="no", **args):
        # not fully implemented. need to be careful with the project status update - possible race condition
        if handle_id is None:
            return 400, "File Handle ID (<project_id>:<namespace>:<name>) must be specified"
        user, error = self.authenticated_user()
        if user is None:
            return 401, error

        db = self.App.db()

        project_id, namespace, name = DBFileHandle.unpack_id(handle_id)

        project = DBProject.get(db, project_id)
        if project is None:
            return 404, "Project not found"

        if not user.is_admin() and user.Username != project.Owner:
            return 403, "Not authorized"
            
        force = force == "yes"

        handle = project.handle(namespace, name)
        if handle is None:
            return 404, "Handle not found"
        if not force and handle.is_reserved():
            return 400, "Handle is reserved"

        handle.reset()
        return json.dumps(handle.as_jsonable()), "text/json"
        
    def project_files(self, request, relpath, project_id=None, state=None, ready_only="no", **args):
        if project_id is None:
            return 400, "Project ID must be specified"
        ready_only = ready_only == "yes"
        db = self.App.db()
        project_id = int(project_id)
        project = DBProject.get(db, project_id)
        if not project: return 404, "Project not found"
        handles = project.files(state=state, ready_only=ready_only)
        return json.dumps([h.as_jsonable(with_replicas=ready_only) for h in handles]), "text/json"

    def project(self, request, relpath, project_id=None, with_files="yes", with_replicas="yes", **args):
        with_files = with_files == "yes"
        with_replicas = with_replicas == "yes"
        db = self.App.db()
        project_id = int(project_id)
        project = DBProject.get(db, project_id)
        if not project:
            return 404, "Project not found"
        #print("project(): with handles/replicas: ", with_files, with_replicas)
        jsonable = project.as_jsonable(with_handles=with_files, with_replicas=with_replicas)
        return json.dumps(jsonable), "text/json"

    def project_handles_log(self, request, relpath, project_id=None):
        project_id = int(project_id)
        project = DBProject.get(db, project_id)
        if not project:
            return 404, "Project not found"
        project_log = (x.as_jsonable() for x in project.handles_log())
        return json.dumps(project_log), "text/json"

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
    
    def handles(self, request, relpath, project_id=None, state=None, rse=None, not_state=None, with_replicas="no"):
        db = self.App.db()
        project_id = int(project_id)
        lst = DBFileHandle.list(db, project_id=project_id, rse=rse, not_state=not_state, state=state, with_replicas=with_replicas=="yes")
        return json.dumps([h.as_jsonable() for h in lst]), "text/json"
        
    def rses(self, request, relpath, **args):
        rses = DBRSE.list(self.App.db())
        return json.dumps([r.as_jsonable() for r in rses]), "text/json"
    
    def get_rse(self, request, relpath, name=None, **args):
        name = name or relpath
        rse = DBRSE.get(self.App.db(), name)
        if rse is None:
            return 404, f"RSE {name} not found"
        return json.dumps(rse.as_jsonable()), "text/json"
    
    def set_rse_availability(self, request, relpath, name=None, available=None, **args):
        user, error = self.authenticated_user()
        if user is None:
            return 401, error
        if not user.is_admin():
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

    def __init__(self, config, prefix):
        Logged.__init__(self, "DataServer")
        BaseApp.__init__(self, config, Handler, prefix=prefix)
        self.DaemonURL = config.get("daemon_server", {}).get("url")
        proximity_map_cfg = config.get("proximity_map", {})
        self.ProximityMapDefaults = proximity_map_cfg.get("defaults", {})
        self.ProximityMapOverrides = proximity_map_cfg.get("overrides", {})
        log_out = config.get("web_server",{}).get("log","-")
        init_logger(log_out, debug_enabled=True)
    
    def proximity_map(self):
        db = self.db()
        return DBProximityMap(db, defaults=self.ProximityMapDefaults, overrides=self.ProximityMapOverrides)

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
    prefix = config.get("web_server", {}).get("data_prefix", "")
    return App(config, prefix)
    
        
if __name__ == "__main__":
    import getopt, sys
    
    opts, args = getopt.getopt(sys.argv[1:], "c:")
    opts = dict(opts)
    config = yaml.load(open(opts["-c"], "r"), Loader=yaml.SafeLoader)
    server_config = config.get("web_server", {})
    app = create_application(config)
    port = server_config.get("data_port", 8088)
    print(f"Starting at port {port}...")
    app.run_server(port, logging=False)
    
    
