import stompy, pprint, urllib, requests, json, time, traceback, textwrap

from data_dispatcher.db import DBFile, DBProject, DBReplica, DBRSE, DBProximityMap
from pythreader import PyThread, Primitive, Scheduler, synchronized, LogFile, LogStream, TaskQueue, Task
from data_dispatcher.logs import Logged
from daemon_web_server import DaemonWebServer
from dcache import DCachePinner, DCachePoller

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

class JobDelegate(object):
    
    def jobFailed(self, scheduler, job_id, exc_type, exc_value, tb):
        print(f"{scheduler}: job failed:", job_id)
        traceback.print_exception(exc_type, exc_value, tb)

delegate = JobDelegate()

TaskScheduler = Scheduler(name="TaskScheduler", delegate=delegate)
SyncScheduler = Scheduler(5, name="SyncScheduler", delegate=delegate)

TaskScheduler.start()
SyncScheduler.start()

Queue = TaskQueue(10, stagger=0.1)

class ProximityMapDownloader(PyThread, Logged):

    def __init__(self, db, url, interval=30):
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
        self.CfgCache = {}      # rse -> (expiration, DBRSE object)

    def unview(self, rse):
        cfg = self.Config[rse]
        return cfg.get("view", rse)

    def is_view(self, rse):
        return self.Config[rse].get("view") is not None
    
    CACHE_EXPIRATION = 60
    
    def get_actual_config(self, rse):
        tup = self.CfgCache.get(rse)
        dbrse = None
        if tup and tup[0] >= time.time():
            dbrse = tup[1]
        if dbrse is None:
            dbrse = DBRSE.get(self.DB, rse)
            if dbrse is not None:
                self.CfgCache[rse] = (time.time() + self.CACHE_EXPIRATION, dbrse)
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
        parts = urllib.parse.urlparse(url)
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
    
    UpdateInterval = 30         # replica availability update interval
    NewRequestInterval = 5      # interval to check on new pin request
    SyncInterval = 600          # interval to re-sync replicas with Rucio
    
    def __init__(self, master, scheduler, project_id, db, rse_config, pollers, pinners, rucio_client):
        Logged.__init__(self, f"ProjectMonitor({project_id})")
        Primitive.__init__(self, name=f"ProjectMonitor({project_id})")
        self.ProjectID = project_id
        self.DB = db
        self.RSEConfig = rse_config
        self.Pinners = pinners
        self.Pollers = pollers
        self.Master = master
        self.Scheduler = scheduler
        self.RucioClient = rucio_client

    def remove_me(self, reason):
        self.log("remove me:", reason)
        for pinner in self.Pinners.values():
            del pinner[self.ProjectID]
        self.Master.remove_project(self.ProjectID, reason)
        self.Master = None
        self.log("removed:", reason)

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
                    replica.Handle = h
                    tape_replicas_by_rse.setdefault(rse, {})[replica.did()] = replica
        return tape_replicas_by_rse

    @synchronized
    def sync_replicas(self):

        self.debug("sync_replicas...")

        project = DBProject.get(self.DB, self.ProjectID)
        if project is None:
            self.remove_me("project deleted")
            return "stop"

        active_handles = self.active_handles()
        self.debug("sync_replicas(): active_handles:", None if active_handles is None else len(active_handles))

        if not active_handles:
            self.remove_me("no active handles")
            return "stop"
            
        try:
            dids = [{"scope":h.Namespace, "name":h.Name} for h in active_handles]
            ndids = len(dids)
            
            total_replicas = 0
            for chunk in chunked(dids, 100):
                nchunk = len(chunk)
                with self.Master:
                    # locked, in case the client needs to refresh the token
                    rucio_replicas = self.RucioClient.list_replicas(chunk, all_states=False, ignore_availability=False)
                rucio_replicas = list(rucio_replicas)
                n = len(rucio_replicas)
                total_replicas += n
                self.log(f"sync_replicas(): {n} replicas found for dids chunk {nchunk}")

                by_namespace_name_rse = {}
                for r in rucio_replicas:
                    namespace = r["scope"]
                    name = r["name"]
                    for rse, urls in r["rses"].items():
                        if rse in self.RSEConfig:
                            preference = self.RSEConfig.preference(rse)
                            available = not self.RSEConfig.is_tape(rse)
                            url = self.RSEConfig.fix_url(rse, urls[0])           # assume there is only one
                            path = self.RSEConfig.url_to_path(rse, url)          
                            by_namespace_name_rse.setdefault((namespace, name), {})[rse] = dict(path=path, url=url, available=available, preference=preference)
                        else:
                            pass
                DBReplica.sync_replicas(self.DB, by_namespace_name_rse)
            self.log(f"sync_replicas(): done: {total_replicas} replicas found for {ndids} dids")
        except Exception as e:
            traceback.print_exc()
            self.error("exception in sync_replicas:", e)
            self.error(textwrap.indent(traceback.format_exc()), "  ")
 
    LOW_WATER_PRESTAGE = 10     # unreserved files to keep prestaged per tape RSE

    @synchronized
    def update_replicas_availability(self):
        
        project = DBProject.get(self.DB, self.ProjectID)
        if project is None:
            self.remove_me("project deleted")
            return "stop"
        
        active_handles = self.active_handles()
        reserved_handles = {h.did():h for h in active_replicas if h.State == "reserved"}

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

        tape_replicas_by_rse = self.tape_replicas_by_rse(active_handles)               # {rse -> {did -> replica}}, will set the replica.Handle pointer
        
        next_run = self.UpdateInterval

        self.debug("tape_replicas_by_rse:", len(tape_replicas_by_rse))
        for rse, replicas in tape_replicas_by_rse.items():
            self.debug("sending %d replicas to %s pinner" % (len(replicas, rse)))
            self.Pinners[rse][self.ProjectID] = replicas
        
        #
        # Release timed-out handles
        #
        if project.WorkerTimeout is not None:
            self.debug("releasing timed-out handles, timeout=", project.WorkerTimeout)
            n = project.release_timed_out_handles()
            if n:
                self.log(f"released {n} timed-out handles")
        self.debug("update_replicas_availability(): done")
        return next_run

class ProjectMaster(PyThread, Logged):
    
    RunInterval = 10        # seconds       - new project discovery latency
    PurgeInterval = 30*60
    
    def __init__(self, db, scheduler, rse_config, pollers, pinners, rucio_client):
        Logged.__init__(self, "ProjectMaster")
        PyThread.__init__(self, name="ProjectMaster")
        self.DB = db
        self.Monitors = {}      # project id -> ProjectMonitor
        self.Stop = False
        self.Scheduler = scheduler
        self.RSEConfig = rse_config
        self.Pollers = pollers
        self.Pinners = pinners
        self.RucioClient = rucio_client
        self.Scheduler.add(self.clean, id="cleaner")

    def clean(self):
        self.debug("cleaner...")
        try:
            nprojects = DBProject.purge(self.DB)
            nfiles = DBFile.purge(self.DB)
            self.log("purged projects:", nprojects, ", files:", nfiles)
        except:
            self.error("Exception in ProjectMaster.clean():", "\n" + traceback.format_exc())
        return self.PurgeInterval

    def run(self):
        while not self.Stop:
            try:
                active_projects = set(p.ID for p in DBProject.list(self.DB, state="active", with_handle_counts=True) if p.is_active())
                #self.debug("run: active projects:", len(active_projects),"   known projects:", len(monitor_projects))
                with self:
                    monitor_projects = set(self.Monitors.keys())
                    #for project_id in monitor_projects - active_projects:
                    #    self.remove_project(project_id, "inactive")
                    for project_id in active_projects - monitor_projects:
                        self.add_project(project_id)
            except Exception as e:
                self.error("exception in run():\n", traceback.format_exc())
            self.sleep(self.RunInterval)

    @synchronized
    def add_project(self, project_id):
        if not project_id in self.Monitors:
            # check if new project
            project = DBProject.get(self.DB, project_id)
            if project is not None:
                files = ({"namespace":f.Namespace, "name":f.Name} for f in project.files())
                monitor = self.Monitors[project_id] = ProjectMonitor(self, self.Scheduler, project_id, self.DB, 
                    self.RSEConfig, self.Pollers, self.Pinners, self.RucioClient)
                SyncScheduler.add(monitor.sync_replicas, id=f"sync_{project_id}", t0=time.time(), interval=ProjectMonitor.SyncInterval)
                TaskScheduler.add(monitor.update_replicas_availability, id=f"update_{project_id}", 
                    t0 = time.time() + 10,          # run a bit after first sync, but it's ok to run before
                    interval=ProjectMonitor.UpdateInterval)
                
            self.log("project added:", project_id)

    @synchronized
    def remove_project(self, project_id, reason):
        monitor = self.Monitors.pop(project_id, None)
        SyncScheduler.remove(f"sync_{project_id}")
        TaskScheduler.remove(f"update_{project_id}")
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
        self.MessageBrokerConfig = rucio_config["message_broker"]
        self.SSLConfig = self.MessageBrokerConfig.get("ssl", {})
        self.DB = db
        self.RSEConfig = rse_config

    def add_replica(self, scope, name, replica_info):
        # replica_info is supposed to be the payload dictionary received from Rucio with the following fields:
        #   scope, name, dst-rse, dst-url, dst-type
        rse = replica_info["dst-rse"]
        if rse not in self.RSEConfig:
            return
        f = DBFile.get(self.DB, scope, name)
        if f is None:
            return
        url = self.RSEConfig.fix_url(rse, replica_info["dst-url"])
        path = self.RSEConfig.url_to_path(rse, url)
        preference = self.RSEConfig.preference(rse)
        available = not self.RSEConfig.is_tape(rse)         # do not trust dst-type from Rucio
        f.create_replica(rse, path, url, preference, available)
        #self.log("added replica:", scope, name, rse, url, path)

    def run(self):
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
    connstr="host=%(host)s port=%(port)s dbname=%(dbname)s user=%(user)s password=%(password)s" % dbconfig
    connection_pool = ConnectionPool(postgres=connstr)

    rse_config = RSEConfig(config.get("rses", {}), connection_pool)
    ssl_config = config.get("ssl", {})
    rucio_config = config["rucio"]
    
    logging_config = config.get("logging", {})
    log_out = logging_config.get("log", "-")

    debug_out = logging_config.get("debug", False)
    debug_enabled = "-d" in opts or not not debug_out 
    if debug_enabled: debug_out = debug_out or None

    print("debug_enabled:", debug_enabled, "   debug_out:", debug_out)
    
    error_out = logging_config.get("errors")
    
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
    
    pollers = {}
    pinners = {}

    for rse in rse_config.rses():
        #
        # remove all replicas to reset old status
        # done in ProjectMonitor.init()
        #DBReplica.remove_bulk(connection_pool, rse=rse)
        #default_logger.log("all replicas removed for RSE", rse, who="main()")

        if rse_config.is_tape(rse):
            poller = pollers[rse] = DCachePoller(rse, connection_pool, 
                 rse_config.poll_url(rse), rse_config.max_burst(rse), rse_config.ssl_config(rse)
            )
            poller.start()
            pollers[rse] = poller
            pinner = pinners[rse] = DCachePinner(rse, connection_pool, rse_config.pin_url(rse), rse_config.pin_prefix(rse),
                rse_config.ssl_config(rse), poller)
            pinner.start()

    proximity_map_loader = None
    proximity_map_url = config.get("proximity_map", {}).get("url")
    if proximity_map_url:
        proximity_map_loader_interval = int(config.get("proximity_map_loader_interval", 60))
        proximity_map_loader = ProximityMapDownloader(
                connection_pool, proximity_map_url,
                proximity_map_loader_interval
        )
        proximity_map_loader.start()


    rse_list_loader = RSEListLoader(connection_pool, rse_client)
    rse_list_loader.start()
    
    #max_threads = config.get("max_threads", 100)
    scheduler = Scheduler()
    scheduler.start()
    
    rucio_listener = RucioListener(connection_pool, rucio_config, rse_config)
    rucio_listener.start()

    project_master = ProjectMaster(connection_pool, scheduler, rse_config, pollers, pinners, replica_client)
    project_master.start()

    server_config = config.get("daemon_server", {})
    web_server = None
    if "port" in server_config:
	    web_server = DaemonWebServer(server_config, project_master)
	    web_server.start()
    
    rucio_listener.join()
    project_master.join()
    scheduler.join()
    [poller.join() for poller in pollers.values()]
    if web_server is not None:
        web_server.join()
    
    
if __name__ == "__main__":
	main() 
