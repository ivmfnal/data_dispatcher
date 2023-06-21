import stompy, pprint, urllib, requests, json, time, traceback, textwrap, sys
from urllib.parse import urlparse
from data_dispatcher.db import DBProject, DBReplica, DBRSE, DBProximityMap
from data_dispatcher.logs import Logged
from daemon_web_server import DaemonWebServer
from tape_interfaces import get_interface

import pythreader
if pythreader.version_info < (2,10,0):
    print("Pythreader version 2.10 or newer is required. Installed:", pythreader.version_info, file=sys.stderr)
    sys.exit(1)

from pythreader import PyThread, Primitive, Scheduler, synchronized, TaskQueue, Task

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def to_did(namespace, name):
    return f"{namespace}:{name}"

def from_did(did):
    return tuple(did.split(":", 1))
    
def chunked(iterable, n):
    if isinstance(iterable, (list, tuple)):
        for i in range(0, len(iterable), n):
            yield iterable[i:i + n]
    else:
        it = iter(iterable)
        while True:
            chunk = list(itertools.islice(it, n))
            if not chunk:
                return
            yield chunk

class TaskQueueDelegate(object):

    def taskFailed(self, queue, task, exc_type, exc_value, tb):
        print(f"{queue}: task failed:", task, file=sys.stderr)
        traceback.print_exception(exc_type, exc_value, tb, file=sys.stderr)

delegate = TaskQueueDelegate()

SyncTaskQueue = TaskQueue(5, name="SyncTaskQueue", delegate=delegate)
GeneralTaskQueue = TaskQueue(100, name="GeneralTaskQueue", delegate=delegate)

class ProximityMapDownloader(PyThread, Logged):

    def __init__(self, db, url, interval=300):
        PyThread.__init__(self, name="ProximityMapDownloader", daemon=True)
        Logged.__init__(self, "ProximityMapDownloader")
        self.DB = db
        self.URL = url
        self.Interval = interval
        self.Stop = False
        self.LastMap = None

    def run(self):
        while not self.Stop:
            response = requests.get(self.URL)
            if response.status_code // 100 == 2:
                data = response.text
                proximity_map = []
                for line in data.split("\n"):
                    line = line.strip()
                    if line:
                        words = line.split(",")
                        if len(words) == 3:
                            cpu, rse, proximity = words
                            proximity = int(proximity)
                            proximity_map.append((cpu, rse, proximity))
                proximity_map = sorted(proximity_map)
                if self.LastMap is None or self.LastMap != proximity_map:
                    dbmap = DBProximityMap(self.DB, proximity_map)
                    dbmap.save()
                    self.log("Proximity map updated")
                else:
                    self.log("Proximity map unchanged")
            else:
                self.log(f"Error retrieving proximity map from {self.URL}:", response)
            if not self.Stop:
                time.sleep(self.Interval)


class RSEListLoader(PyThread, Logged):

    def __init__(self, db, rucio_client, interval=30):
        PyThread.__init__(self, daemon=True, name="RSEListLoader")
        Logged.__init__(self, name="RSEListLoader")
        self.DB = db
        self.RucioClient = rucio_client
        self.Interval = interval
        self.Stop = False

    def run(self):
        last_set = None
        while not self.Stop:
            rses = set(info["rse"] for info in self.RucioClient.list_rses())
            new_set = set(rses)
            if last_set is None or last_set != new_set:
                last_set = last_set or set()
                new_rses = new_set - last_set
                removed_rses = last_set - new_set
                DBRSE.create_many(self.DB, rses)
                last_set = new_set
                self.log("RSE list updated. New RSEs:", list(new_rses), "   removed RSEs:", list(removed_rses))
            else:
                # self.log("RSE list unchanged")
                pass
            if not self.Stop:
                self.sleep(self.Interval)

class RSEConfig(Logged):
    
    def __init__(self, config, db):
        Logged.__init__(self)
        self.Config = config
        #print("RSEConfig: config:")
        #pprint.pprint(config)
        self.DB = db

    def unview(self, rse):
        cfg = self.Config[rse]
        return cfg.get("view", rse)

    def is_view(self, rse):
        return self.Config[rse].get("view") is not None
        
    def get_actual_config(self, rse):
        dbrse = DBRSE.get(self.DB, rse)
        if dbrse is None:
            raise KeyError(f"RSE {rse} not in the database")
        dbcfg = dbrse.as_dict()

        cfg = self.Config.get(rse)
        if cfg is not None:
            dbcfg["ssl"] = cfg.get("ssl")
        return dbcfg

    __getitem__ = get_actual_config
    
    def keys(self):
        return (r.Name for r in DBRSE.list(self.DB))

    rses = keys

    def __contains__(self, rse):
        return rse in set(self.keys())
        
    def get(self, rse, default={}):
        if not rse in self: return default
        return self[rse]
        
    def is_tape(self, rse):
        return self.get(rse).get("is_tape", False)
        
    def type(self, rse):
        if not self.is_tape(rse):
            return None
        return self[rse]["type"]

    def ssl_config(self, rse):
        return self.get(rse).get("ssl")

    def pin_url(self, rse):
        url = self[rse]["pin_url"]
        return url
        
    def pin_prefix(self, rse):
        return self[rse].get("pin_prefix") or ""
        
    def poll_url(self, rse):
        return self[rse]["poll_url"]
        
    def remove_prefix(self, rse):
        return self[rse].get("remove_prefix", "")

    def add_prefix(self, rse):
        return self[rse].get("add_prefix", "")

    def url_to_path(self, rse, url):
        parts = urlparse(url)
        path = parts.path
        while "//" in path:
            path = path.replace("//", "/")
        if not path or path[0] != '/':
            path = "/" + path

        remove_prefix = self.remove_prefix(rse)
        add_prefix = self.add_prefix(rse)

        if remove_prefix and path.startswith(remove_prefix):
            path = path[len(remove_prefix):]
            
        if add_prefix:
            path = add_prefix + path

        return path

    def fix_url(self, rse, url):
        # make sure the URL is valid given the schema
        parts = list(urllib.parse.urlsplit(url))
        scheme = parts[0]
        if scheme in ("root", "xroot"):
            path = parts[2]
            if not path.startswith("//"):
                if path.startswith('/'):
                    path = '/'+path
                else:
                    path = '//' + path
            parts[2] = path
            url = urllib.parse.urlunsplit(parts)
        return url

    def preference(self, rse):
        return self.get(rse).get("preference", 0)

    def max_burst(self, rse):
        return self.get(rse).get("max_poll_burst", 100)

class ListReplicasTask(Task):
    
    def __init__(self, master, rucio_client, dids):
        Task.__init__(self)
        self.Master = master        # to synchronize access to the client
        self.Client = rucio_client
        self.DIDs = dids
        
    def run(self):
        self.Master.debug("ListReplicasTask started with", len(self.DIDs), "DIDs")
        try:
            with self.Master:
                return self.Client.list_replicas(self.DIDs, all_states=False, ignore_availability=False)
        finally:
            self.Master = None      # unlink

class ProjectMonitor(Primitive, Logged):
    
    UpdateInterval = 120        # replica availability update interval
    NewRequestInterval = 5      # interval to check on new pin request
    SyncInterval = 600          # interval to re-sync replicas with Rucio
    
    def __init__(self, master, project_id, db, rse_config, rucio_client):
        Logged.__init__(self, f"ProjectMonitor({project_id})")
        Primitive.__init__(self, name=f"ProjectMonitor({project_id})")
        self.Removed = False
        self.ProjectID = project_id
        self.DB = db
        self.RSEConfig = rse_config
        self.PinRequests = {}       # {rse -> PinRequest}
        self.Master = master
        self.RucioClient = rucio_client
        self.SyncReplicasJobID = f"sync_replicas_{project_id}"
        self.UpdateAvailabilityJobID = f"update_availability_{project_id}"
        self.CheckStateJobID = f"check_state_{project_id}"
        self.TapeRSEInterfaces = {}

        self.SyncTask = SyncTaskQueue.add(self.sync_replicas, interval=self.SyncInterval)
        self.CheckProjectTask = GeneralTaskQueue.add(self.check_project_state, interval=self.UpdateInterval)
        self.UpdateAvailabilityTask = None

    def tape_rse_interface(self, rse):
        interface = self.TapeRSEInterfaces.get(rse)
        if interface is None:
            self.TapeRSEInterfaces[rse] = interface = get_interface(rse, self.RSEConfig, self.DB)
        return interface
        
    def remove_me(self, reason):
        self.debug("remove_me():", reason)
        self.Removed = True
        self.CheckProjectTask.cancel()
        self.SyncTask.cancel()
        if self.UpdateAvailabilityTask is not None:
            self.UpdateAvailabilityTask.cancel()

        for rse, rse_interface in self.TapeRSEInterfaces.items():
            rse_interface.unpin_project(self.ProjectID)
            self.log("remove_me(): unpinned files in:", rse)
            self.debug("remove_me(): unpinned files in:", rse)

        self.Master.remove_project(self.ProjectID, reason)
        self.Master = None
        self.log("remove_me(): project monitor removed:", reason)

    def active_handles(self, as_dids = True):
        # returns {did -> handle} for all active handles, or None if the project is not found
        project = DBProject.get(self.DB, self.ProjectID)
        if project is None:
            return None
        return [h for h in project.handles(with_replicas=True) if h.is_active()]

    def tape_replicas_by_rse(self, active_handles):
        tape_replicas_by_rse = {}               # {rse -> {did -> replica}}
        for h in active_handles:
            for rse, replica in h.replicas().items():
                if self.RSEConfig.is_tape(rse):
                    #print("tape_replicas_by_rse(): Tape RSE:", rse)
                    tape_replicas_by_rse.setdefault(rse, {})[replica.did()] = replica
        return tape_replicas_by_rse
        
    @synchronized
    def check_project_state(self):
        if self.Removed:
            self.debug("check_project_state: already removed. skipping")
            return              # alredy removed
        self.debug("check_project_state...")
        project = DBProject.get(self.DB, self.ProjectID)
        if project is None or project.State != "active":
            if project is None:
                reason = "project disappeared"
            else:
                reason = f"project inactive ({project.State})"
            self.log("removing project:", reason)
            self.remove_me(reason)          # this will cancel this and other repeating task

    @synchronized
    def sync_replicas(self):
        if self.Removed:
            self.debug("sync_replicas: already removed. skipping")
            return              # alredy removed
        active_handles = self.active_handles()
        self.debug("sync_replicas(): active_handles:", None if active_handles is None else len(active_handles))

        if active_handles is None:
            self.remove_me("deleted")
            return "stop"
        elif not active_handles:
            self.remove_me("done")
            return "stop"
            
        try:
            dids = [{"scope":h.Namespace, "name":h.Name} for h in active_handles]
            ndids = len(dids)
            
            total_replicas = 0
            for chunk in chunked(dids, 100):
                nchunk = len(chunk)
                #self.debug("sync_replicas: blocking for Master...")
                with self.Master:
                    # locked, in case the client needs to refresh the token
                    #self.debug("sync_replicas: calling list_replicas...")
                    rucio_replicas = self.RucioClient.list_replicas(chunk, schemes=self.Master.URLSchemes,
                                            all_states=False, ignore_availability=False)
                rucio_replicas = list(rucio_replicas)
                n = len(rucio_replicas)
                total_replicas += n
                self.log(f"sync_replicas(): {n} replicas found for dids chunk {nchunk}")

                by_namespace_name_rse = {}
                for r in rucio_replicas:
                    namespace = r["scope"]
                    name = r["name"]
                    for rse, urls in r["rses"].items():
                        #self.debug(namespace, name, rse, urls)
                        if rse in self.RSEConfig:
                            #self.debug(f"RSE {rse} in config")
                            preference = self.RSEConfig.preference(rse)
                            available = not self.RSEConfig.is_tape(rse)
                            for url in urls:
                                parsed = urlparse(url)
                                scheme = parsed.scheme.lower()
                                if scheme in ("root", "xroot", "xrootd"):
                                    break
                            else:
                                # xrootd not found - use first URL on the list, assuming it's most preferred for the RSE
                                url = urls[0]
                            url = self.RSEConfig.fix_url(rse, url)
                            path = self.RSEConfig.url_to_path(rse, url)
                            data = dict(path=path, url=url, available=available, preference=preference)
                            #self.debug("replica data:", data)
                            by_namespace_name_rse.setdefault((namespace, name), {})[rse] = data
                            #self.debug("added replica:", data)
                        else:
                            pass
                DBReplica.sync_replicas(self.DB, by_namespace_name_rse)
            self.log(f"sync_replicas(): {total_replicas} replicas found for {ndids} dids")
        except Exception as e:
            print("Exception in sync_replicas:", e, file=sys.stderr)
            traceback.print_exc()
            self.error("exception in sync_replicas:", e)
            self.error(textwrap.indent(traceback.format_exc()), "  ")
        if self.UpdateAvailabilityTask is None:
            self.UpdateAvailabilityTask = GeneralTaskQueue.add(self.update_replicas_availability, interval=self.UpdateInterval)
        self.debug("update_replicas_availability task schduled")
        self.debug("sync_replicas done")

    @synchronized
    def update_replicas_availability(self):
        if self.Removed:
            self.debug("update_replicas_availability: already removed. skipping")
            return              # alredy removed
        self.debug("update_replicas_availability() ...")
        with self:
            #self.debug("update_replicas_availability(): entered")
            
            active_handles = self.active_handles()
            self.debug("update_replicas_availability(): active_handles:", None if active_handles is None else len(active_handles))

            if active_handles is None:
                self.remove_me("deleted")
                return "stop"
            elif not active_handles:
                self.remove_me("done")
                return "stop"

            #
            # Collect replica info on active replicas located in tape storages
            #

            tape_replicas_by_rse = self.tape_replicas_by_rse(active_handles)               # {rse -> {did -> replica}}
            self.debug("tape_replicas_by_rse:", [(rse, len(dids)) for rse, dids in tape_replicas_by_rse.items()])
        
            next_run = self.UpdateInterval

            #self.debug("tape_replicas_by_rse:", len(tape_replicas_by_rse))
            for rse, replicas in tape_replicas_by_rse.items():
                replica_paths = {did: r.Path for did, r in replicas.items()}
                rse_interface = self.tape_rse_interface(rse)
                self.debug("pinning", len(replica_paths), "in", rse)
                rse_interface.pin_project(self.ProjectID, replica_paths)
                
            project = DBProject.get(self.DB, self.ProjectID)
            if project is not None and project.WorkerTimeout is not None:
                self.debug("releasing timed-out handles, timeout=", project.WorkerTimeout)
                n = project.release_timed_out_handles()
                if n:
                    self.log(f"released {n} timed-out handles")
            self.debug("update_replicas_availability(): done")
            return next_run

class ProjectMaster(PyThread, Logged):
    
    RunInterval = 10        # seconds       - new project discovery latency
    PurgeInterval = 30
    
    def __init__(self, db, rse_config, rucio_client, url_schemes):
        Logged.__init__(self, "ProjectMaster")
        PyThread.__init__(self, name="ProjectMaster")
        self.DB = db
        self.Monitors = {}      # project id -> ProjectMonitor
        self.Stop = False
        self.RSEConfig = rse_config
        self.RucioClient = rucio_client
        self.URLSchemes = url_schemes or None

    def clean(self):
        self.debug("cleaner...")
        try:
            #nprojects = DBProject.purge(self.DB)       never purge projects
            nabandoned = DBProject.mark_abandoned(self.DB)
            nreplicas = DBReplica.purge(self.DB)
            self.log("abandoned projects:", nabandoned, ", purged replicas:", nreplicas)
        except:
            self.error("Exception in ProjectMaster.clean():", "\n" + traceback.format_exc())

    def run(self):
        GeneralTaskQueue.append(self.clean, interval = self.PurgeInterval)
        while not self.Stop:
            try:
                active_projects = set(p.ID for p in DBProject.list(self.DB, state="active", with_handle_counts=True) if p.is_active())
                #self.debug("run: active projects:", len(active_projects),"   known projects:", len(monitor_projects))
                with self:
                    monitor_projects = set(self.Monitors.keys())
                    #for project_id in monitor_projects - active_projects:
                    #    self.remove_project(project_id, "inactive")
                    for project_id in active_projects - monitor_projects:
                        self.log("new project discovered:", project_id)
                        self.add_project(project_id)
            except Exception as e:
                self.error("exception in run():\n", traceback.format_exc())
            #self.debug("sleeping...")
            self.sleep(self.RunInterval)

    @synchronized
    def add_project(self, project_id):
        if not project_id in self.Monitors:
            # check if new project
            project = DBProject.get(self.DB, project_id)
            if project is not None:
                files = ({"namespace":f.Namespace, "name":f.Name} for f in project.handles(with_replicas=False))
                monitor = self.Monitors[project_id] = ProjectMonitor(self, project_id, self.DB, self.RSEConfig, self.RucioClient)
            self.log("project added:", project_id)

    @synchronized
    def remove_project(self, project_id, reason):
        monitor = self.Monitors.pop(project_id, None)
        self.log("project removed:", project_id, "  reason:", reason)

"""
Sample message from Rucio

{'created_at': '2022-01-31 16:58:17.235625',
 'event_type': 'transfer-done',
 'payload': {'account': None,
             'activity': 'User Subscriptions',
             'bytes': 1024000000,
             'checksum-adler': '93b40001',
             'checksum-md5': 'b5c667a723a10a3485a33263c4c2b978',
             'created_at': None,
             'dst-rse': 'DUNE_FR_CCIN2P3_XROOTD',
             'dst-type': 'DISK',
             'dst-url': 'root://ccxroot.in2p3.fr:1097/xrootd/in2p3.fr/tape/dune/rucio/test/c5/2e/1gbtestfilea.20220131',
             'duration': 239,
             'file-size': 1024000000,
             'guid': None,
             'name': '1gbtestfilea.20220131',
             'previous-request-id': None,
             'protocol': 'root',
             'reason': '',
             'request-id': 'b4ccdac3a01146a7859bd5458da9808e',
             'scope': 'test',
             'src-rse': 'MANCHESTER',
             'src-type': 'DISK',
             'src-url': 'root://bohr3226.tier2.hep.manchester.ac.uk/dune/RSE/test/c5/2e/1gbtestfilea.20220131',
             'started_at': '2022-01-31 16:53:42',
             'submitted_at': '2022-01-31 16:53:40.324004',
             'tool-id': 'rucio-conveyor',
             'transfer-endpoint': 'https://fts3-public.cern.ch:8446',
             'transfer-id': '57517b00-82b6-11ec-ba1a-fa163ecc10d8',
             'transfer-link': 'https://fts3-public.cern.ch:8449/fts3/ftsmon/#/job/57517b00-82b6-11ec-ba1a-fa163ecc10d8',
             'transferred_at': '2022-01-31 16:57:41'}}
"""
            
class RucioListener(PyThread, Logged):
    
    def __init__(self, db, rucio_config, rse_config):
        Logged.__init__(self, "RucioListener")
        PyThread.__init__(self, name="RucioListener")
        self.Config = rucio_config
        self.MessageBrokerConfig = rucio_config.get("message_broker")
        self.SSLConfig = self.MessageBrokerConfig.get("ssl", {})
        self.DB = db
        self.RSEConfig = rse_config

    def add_replica(self, scope, name, replica_info):
        # replica_info is supposed to be the payload dictionary received from Rucio with the following fields:
        #   scope, name, dst-rse, dst-url, dst-type
        rse = replica_info["dst-rse"]
        if rse not in self.RSEConfig:
            return
        url = self.RSEConfig.fix_url(rse, replica_info["dst-url"])
        path = self.RSEConfig.url_to_path(rse, url)
        available = not self.RSEConfig.is_tape(rse)         # do not trust dst-type from Rucio
        DBReplica.create(self.DB, scope, name, rse, path, url, available=available)
        self.debug("added replica:", scope, name, rse, url, path)

    def run(self):
        if self.MessageBrokerConfig is None:
            self.log("Message broker is not configured. Stopping the RucioListener thread.")
            return
        broker_addr = (self.MessageBrokerConfig["host"], self.MessageBrokerConfig["port"])
        cert_file = self.SSLConfig.get("cert")
        key_file = self.SSLConfig.get("key")
        ca_file = self.SSLConfig.get("ca_bundle")
        vhost = self.MessageBrokerConfig.get("vhost", "/")
        subscribe = self.MessageBrokerConfig["subscribe"]
        connection = stompy.connect(broker_addr, cert_file=cert_file, key_file=key_file, ca_file=ca_file, host=vhost)
        connection.subscribe(subscribe)
        for frame in connection:
            if frame.Command == "MESSAGE":
                data = frame.json
                event_type = data.get("event_type")
                if event_type == "transfer-done":
                    payload = data.get("payload", {})
                    rse = payload.get("dst-rse")
                    if rse and rse in self.RSEConfig:
                        scope = payload.get("scope")
                        name = payload.get("name")
                        self.debug(f"Transfer done: RSE:{rse} DID:{scope}:{name}")
                        self.add_replica(scope, name, payload)


def main():
    import sys, yaml, getopt, os
    from wsdbtools import ConnectionPool
    from rucio.client.replicaclient import ReplicaClient
    from rucio.client.rseclient import RSEClient
    from data_dispatcher.logs import init_logger

    opts, args = getopt.getopt(sys.argv[1:], "c:d")
    opts = dict(opts)
    config = opts.get("-c") or os.environ.get("DATA_DISPATCHER_CFG")
    config = yaml.load(open(config, "r"), Loader=yaml.SafeLoader)

    dbconfig = config["database"]
    connection_pool = ConnectionPool(postgres=dbconfig, max_connections=dbconfig.get("max_connections"))

    rse_config = RSEConfig(config.get("rses", {}), connection_pool)
    ssl_config = config.get("ssl", {})
    rucio_config = config.get("rucio", {})
    
    logging_config = config.get("logging", {})
    log_out = logging_config.get("log", "-")

    debug_out = "-" if "-d" in opts else logging_config.get("debug", False)
    debug_enabled = "-d" in opts or not not debug_out 
    if debug_enabled: debug_out = debug_out or None

    print("debug_enabled:", debug_enabled, "   debug_out:", debug_out)
    
    error_out = logging_config.get("errors", "-")
    
    default_logger = init_logger(log_out, debug_out=debug_out, error_out=error_out, debug_enabled=debug_enabled)

    # test the DB connection
    try:
        connection = connection_pool.connect()
    except Exception as e:
        print("Error connection to the DD database:", e)
        sys.exit(1)
    else:
        connection.close()
        del connection

    replica_client = ReplicaClient()        # read standard Rucio config file for now
    rse_client = RSEClient()
    
    proximity_map_loader = None
    proximity_map_url = config.get("proximity_map", {}).get("url")
    if proximity_map_url:
        proximity_map_loader_interval = int(config.get("proximity_map_loader_interval", 60))
        proximity_map_loader = ProximityMapDownloader(
                connection_pool, proximity_map_url,
                proximity_map_loader_interval
        )
        proximity_map_loader.start()
    else:
        print("Proximity map URL is not configured")

    rse_list_loader = RSEListLoader(connection_pool, rse_client)
    rse_list_loader.start()
    
    rucio_listener = RucioListener(connection_pool, rucio_config, rse_config)
    rucio_listener.start()
        

    schemes = config.get("replica_url_schemes")

    project_master = ProjectMaster(connection_pool, rse_config, replica_client, schemes)
    project_master.start()

    server_config = config.get("daemon_server", {})
    web_server = None
    if "port" in server_config:
	    web_server = DaemonWebServer(server_config, project_master)
	    web_server.start()
    
    rucio_listener.join()
    project_master.join()
    if web_server is not None:
        web_server.join()
    
    
if __name__ == "__main__":
	main() 
