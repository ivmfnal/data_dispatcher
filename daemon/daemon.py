import stompy, pprint, urllib, requests, json, time
from data_dispatcher.db import DBFile, DBProject, DBReplica, DBRSE
from pythreader import PyThread, Primitive, Scheduler, synchronized, LogFile, LogStream
from data_dispatcher.logs import Logged
from daemon_web_server import DaemonWebServer

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def to_did(namespace, name):
    return f"{namespace}:{name}"

def from_did(did):
    return tuple(did.split(":", 1))

TaskScheduler = Scheduler()

class DCachePoller(PyThread, Logged):
    
    STAGGER = 0.1
    
    def __init__(self, rse, db, base_url, max_burst, ssl_config):
        PyThread.__init__(self, name=f"DCachePoller({rse})")
        Logged.__init__(self, f"DCachePoller({rse})")
        self.MaxBurst = max_burst
        self.DB = db
        self.RSE = rse
        self.BaseURL = base_url
        self.Files = {}                 # { did -> path }
        self.Stop = False
        ssl_config = ssl_config or {}
        self.Cert = ssl_config.get("cert")
        self.Key = ssl_config.get("key")
        #self.CA_Bundle = ssl_config.get("ca_bundle")
        
    @synchronized
    def submit(self, dids_paths):
        self.Files.update(dict(dids_paths))
        self.wakeup()
        
    def run(self):
        while not self.Stop:
            while self.Files:
                with self:
                    items = list(self.Files.items())
                    n = len(items)
                    nburst = min(self.MaxBurst, max(10, n//10))
                    burst, items = items[:nburst], items[nburst:]
                    self.Files = dict(items)
                headers = { "accept" : "application/json",
                        "content-type" : "application/json"}
                available_dids = []
                unavailable_dids = []
                remove_dids = []
                for did, path in burst:
                    url = self.BaseURL + path + "?locality=true"
                    cert = None if self.Cert is None else (self.Cert, self.Key)
                    response = requests.get(url, headers=headers, cert=cert, verify=False)
                    #self.debug("response:", response.status_code, response.text)
                    if response.status_code == 404:
                        #self.debug(f"file not found (status 404): {did} - removing")
                        self.log("Replica not found:", path)
                        remove_dids.append(did)
                    elif response.status_code//100 != 2:
                        continue
                    else:
                        data = response.json()
                        available = "ONLINE" in data.get("fileLocality", "").upper()
                        if available:
                            self.log("Replica available:", did, path)
                            available_dids.append(did)
                        else:
                            unavailable_dids.append(did)
                            self.log("Replica unavailable:", did, path)
                DBReplica.update_availability_bulk(self.DB, True, self.RSE, available_dids)
                DBReplica.update_availability_bulk(self.DB, False, self.RSE, unavailable_dids)
                DBReplica.remove_bulk(self.DB, self.RSE, remove_dids)
                time.sleep(self.STAGGER)
            self.sleep(10)

class PinRequest(Logged):
    
    # dCache version
    # see https://docs.google.com/document/d/14sdrRmJts5JYBFKSvedKCxT1tcrWtWchR-PJhxdunT8/edit?usp=sharing
    
    PinLifetime = 3600
    
    def __init__(self, project_id, url, ssl_config, replicas):
        Logged.__init__(self, f"PinRequest({project_id})")
        self.BaseURL = url
        self.Replicas = replicas.copy()             # {did -> path}
        self.SSLConfig = ssl_config
        self.URL = None
        self.Cert = self.SSLConfig.get("cert")
        self.Key = self.SSLConfig.get("key")
        self.CertTuple = (self.Cert, self.Key) if self.Cert else None
        self.Error = False
        self.ErrorText = True

    def send(self):
        headers = { "accept" : "application/json",
                    "content-type" : "application/json"}
        data =  {
            "target" : json.dumps(list(self.Replicas.values())),
            "activity" : "PIN",
            "clearOnSuccess" : False, 
            "clearOnFailure" : False, 
            "expandDirectories" : None,
            "arguments": {
                "lifetime": self.PinLifetime,
                "lifetime-unit": "SECONDS"
            }
        }
        #self.debug("request data:", json.dumps(data))
        r = requests.post(self.BaseURL, data = json.dumps(data), headers=headers, 
                verify=False, cert = self.CertTuple)

        #self.debug("send(): response:", r)
        r.raise_for_status()
        self.URL = r.headers['request-url']
        self.Expiration = time.time() + self.PinLifetime
        self.log("Pin request sent")

    def status(self):
        assert self.URL is not None
        headers = { "accept" : "application/json",
                    "content-type" : "application/json"}
        r = requests.get(self.URL, headers=headers, verify=False, cert = self.CertTuple)
        #self.debug("status(): response:", r)
        if r.status_code // 100 == 4:
            self.Error = True
            self.ErrorText = r.text
            return "ERROR"
        r.raise_for_status()
        #self.debug("request status response:", r.json())
        self.log("Pin request status:", r.json()["status"])
        return r.json()["status"]

    def error(self):
        return self.Error

    def complete(self):
        return self.status().upper() == "COMPLETED"

    def same_files(self, replicas):
        return set(replicas.keys()) == set(self.Replicas.keys())

class RSEConfig(Logged):
    
    def __init__(self, config, db):
        Logged.__init__(self)
        self.Config = config
        self.DB = db

    def unview(self, rse):
        cfg = self.Config[rse]
        return cfg.get("view", rse)

    def is_view(self, rse):
        return self.Config[rse].get("view") is not None
        
    def get_actual_config(self, rse):
        cfg = self.Config[rse]
        add_prefix = cfg.get("add_prefix")
        remove_prefix = cfg.get("remove_prefix")
        actual_rse = self.unview(rse)

        dbrse = DBRSE.get(self.DB, actual_rse)
        if dbrse is None:
            raise KeyErorr(f"RSE {dbrse} aliased as {rse} not found")
            
        cfg = dbrse.as_dict()
        if add_prefix is not None:
            cfg["add_prefix"] = add_prefix
        if remove_prefix is not None:
            cfg["remove_prefix"] = remove_prefix
        return cfg
        
    __getitem__ = get_actual_config
    
    def __contains__(self, rse):
        return rse in self.Config
        
    def get(self, rse, default={}):
        if not rse in self: return default
        return self[rse]
        
    def rses(self):
        return list(self.Config.keys())
        
    def is_tape(self, rse):
        return self.get(rse).get("is_tape", False)

    def ssl_config(self, rse):
        return self.get(rse).get("ssl")

    def pin_url(self, rse):
        return self[rse]["pin_url"]
        
    def poll_url(self, rse):
        return self[rse]["query_url"]
        
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

    def preference(self, rse):
        return self.get(rse).get("preference", 0)

    def max_burst(self, rse):
        return self.get(rse).get("max_poll_burst", 100)

class ProjectMonitor(Logged):
    
    Interval = 30             # regular check interval
    NewRequestInterval = 5    # interval to check on new pin request
    
    def __init__(self, master, scheduler, project_id, db, rse_config, pollers, rucio_client):
        Logged.__init__(self, f"ProjectMonitor({project_id})")
        #Primitive.__init__(self, name=f"ProjectMonitor({project_id})")
        self.ProjectID = project_id
        self.DB = db
        self.RSEConfig = rse_config
        self.PinRequests = {}       # {rse -> PinRequest}
        self.Pollers = pollers
        self.Master = master
        self.Scheduler = scheduler
        self.RucioClient = rucio_client

    def remove_me(self, reason):
        self.Master.remove_project(self.ProjectID, reason)
        self.Master = None

    def active_handles(self, as_dids = True):
        # returns {did -> handle} for all active handles, or None if the project is not found
        project = DBProject.get(self.DB, self.ProjectID)
        if project is None:
            return None

        return [h for h in project.handles(with_replicas=True) if h.is_active()]

    def tape_replicas_by_rse(self, active_handles):
        tape_replicas_by_rse = {}               # {rse -> {did -> path}}
        for h in active_handles:
            for rse, replica in h.replicas().items():
                if self.RSEConfig.is_tape(rse):
                    tape_replicas_by_rse.setdefault(rse, {})[replica.did()] = replica.Path
        return tape_replicas_by_rse
        
    def init(self):
        active_handles = self.active_handles()
        if active_handles is None:
            self.remove_me("deleted")
            return "stop"
            
        dids = [{"scope":h.Namespace, "name":h.Name} for h in active_handles]

        self.log("rucio replica loader started for", len(dids), "files")
        with self.Master:
            # locked, in case the client needs to refresh the token
            rucio_replicas = self.RucioClient.list_replicas(dids, all_states=False, ignore_availability=False)

        by_rse = {}             # {rse -> {(namespace, name) -> path}
        for r in rucio_replicas:
            namespace = r["scope"]
            name = r["name"]
            for rse, urls in r["rses"].items():
                if rse in self.RSEConfig:
                    url = urls[0]           # assume there is only one
                    path = self.RSEConfig.url_to_path(rse, url)          
                    by_rse_dict = by_rse.setdefault(rse, {})
                    by_rse_dict[(namespace, name)] = dict(path=path, url=url)
                    #self.debug("added for rse:", rse, "  ", namespace, name, "  data:", by_rse_dict[(namespace, name)])
                else:
                    pass
        for rse, replicas in by_rse.items():
            preference = self.RSEConfig.preference(rse)
            #self.debug(f"replicas for {rse}")
            #for k, v in replicas.items():
            #    self.debug(k, v) 
            DBReplica.create_bulk(self.DB, rse, preference, replicas)
            #self.debug("replicas found in", rse, " : ", len(replicas))
        self.log(f"project loading done")
        self.Scheduler.add(self.run)
 
    def create_pin_request(self, rse, replicas):
        ssl_config = self.RSEConfig.ssl_config(rse)
        self.PinRequests[rse] = pin_request = PinRequest(self.ProjectID, self.RSEConfig.pin_url(rse), 
            ssl_config, replicas
        )
        pin_request.send()
        return pin_request

    def run(self):
        
        active_handles = self.active_handles()
        if active_handles is None:
            self.remove_me("deleted")
            return "stop"

        elif not active_handles:
            self.remove_me("done")
            return "stop"

        #
        # Collect replica info on active replicas located in tape storages
        #

        tape_replicas_by_rse = self.tape_replicas_by_rse(active_handles)               # {rse -> {did -> path}}
        
        next_run = self.Interval

        for rse, replicas in tape_replicas_by_rse.items():
            pin_request = self.PinRequests.get(rse)
            if pin_request is None or not pin_request.same_files(replicas):
                self.create_pin_request(rse, replicas)
                next_run = self.NewRequestInterval     # this is new pin request. Check it in 20 seconds
            elif pin_request.complete():
                self.log("pin request COMPLETE. No need to poll")
                DBReplica.update_availability_bulk(self.DB, True, rse, list(replicas.keys()))
            elif pin_request.error():
                self.debug("ERROR in pin request. Creating new one\n", pin_request.ErrorText)
                self.create_pin_request(rse, replicas)
                next_run = self.NewRequestInterval     # this is new pin request. Check it in 20 seconds
            else:
                poller = self.Pollers[rse]
                dids_paths = [(did, path) for did, path in replicas.items()]
                n = len(dids_paths)
                self.log("sending", len(dids_paths), "dids/paths to poller for RSE", rse)
                poller.submit(dids_paths)
                
        return next_run

class ProjectMaster(PyThread, Logged):
    
    RunInterval = 10        # seconds       - new project discovery latency
    
    def __init__(self, db, scheduler, rse_config, pollers, rucio_client):
        Logged.__init__(self, "ProjectMaster")
        PyThread.__init__(self, name="ProjectMaster")
        self.DB = db
        self.Monitors = {}      # project id -> ProjectMonitor
        self.Stop = False
        self.Scheduler = scheduler
        self.RSEConfig = rse_config
        self.Pollers = pollers
        self.RucioClient = rucio_client

    def run(self):
        while not self.Stop:
            active_projects = DBProject.list(self.DB, state="active", with_handle_counts=True)
            active_projects = set(p.ID for p in active_projects if p.HandleCounts.get("ready",0) + p.HandleCounts.get("reserved",0) > 0)
            monitor_projects = set(self.Monitors.keys())
            #self.debug("run: active projects:", len(active_projects),"   known projects:", len(monitor_projects))
            with self:
                for project_id in monitor_projects - active_projects:
                    self.remove_project(project_id, "inactive")
                for project_id in active_projects - monitor_projects:
                    self.add_project(project_id)
                self.sleep(self.RunInterval)
    
    @synchronized
    def add_project(self, project_id):
        if not project_id in self.Monitors:
            # check if new project
            project = DBProject.get(self.DB, project_id)
            if project is not None:
                files = ({"namespace":f.Namespace, "name":f.Name} for f in project.files())
                monitor = self.Monitors[project_id] = ProjectMonitor(self, self.Scheduler, project_id, self.DB, 
                    self.RSEConfig, self.Pollers, self.RucioClient)
                self.Scheduler.add(monitor.init, id=project_id)
            self.log("project added:", project_id)

    @synchronized
    def remove_project(self, project_id, reason):
        monitor = self.Monitors.pop(project_id, None)
        self.Scheduler.remove(project_id)
        self.log("project removed:", project_id, "  reason:", reason)
        

class RSEMonitor(Logged):
    Interval = 60             # regular check interval
    ReadAvailabilityMask = 4
    
    def __init__(self, master, rse, rucio_client):
        self.Master = master
        self.RSE = rse
        self.RucioClient = rucio_client
        self.LastAvailability = None
        
    def run(self):
        try:
            rse_info = self.RucioClient.get_rse(self.RSE)
            available = (rse_info["availability"] & self.ReadAvailabilityMask) != 0
        except Exception as e:
            self.error(f"Can not get RSE {self.RSE} availability: {e}")
        else:
            if self.LastAvailability != available:
                self.LastAvailability = available
                self.Master.change_availability(self.RSE, available)


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
        url = replica_info["dst-url"]
        path = self.RSEConfig.url_to_path(rse, url)
        preference = self.RSEConfig.preference(rse)
        available = not self.RSEConfig.is_tape(rse)         # do not trust dst-type from Rucio
        f.create_replica(rse, path, url, preference, available)
        self.log("added replica:", scope, name, rse, url, path)

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
    from data_dispatcher.logs import init_logger

    opts, args = getopt.getopt(sys.argv[1:], "c:")
    opts = dict(opts)
    config = opts.get("-c") or os.environ.get("DATA_DISPATCHER_CFG")
    config = yaml.load(open(config, "r"), Loader=yaml.SafeLoader)

    rse_config = RSEConfig(config.get("rses", {}))
    ssl_config = config.get("ssl", {})
    rucio_config = config["rucio"]
    
    logging_config = config.get("logging", {})
    log_out = logging_config.get("log", "-")

    debug_out = logging_config.get("debug", False)
    debug_enabled = not not debug_out
    
    error_out = logging_config.get("errors", "-")
    
    init_logger(log_out, debug_enabled, debug_out, error_out)
    
    replica_client = ReplicaClient()        # read standard Rucio config file for now
    
    dbconfig = config["database"]
    connstr="host=%(host)s port=%(port)s dbname=%(dbname)s user=%(user)s password=%(password)s" % dbconfig
    connection_pool = ConnectionPool(postgres=connstr)

    pollers = {}

    for rse in rse_config.rses():
        if rse_config.is_tape(rse):
            poller = pollers[rse] = DCachePoller(rse, connection_pool, 
                 rse_config.poll_url(rse), rse_config.max_burst(rse), rse_config.ssl_config(rse)
            )
            poller.start()

    # test the connection
    try:
        connection = connection_pool.connect()
    except Exception as e:
        print("Error connection to the DD database:", e)
        sys.exit(1)
    else:
        connection.close()
        del connection

    #max_threads = config.get("max_threads", 100)
    scheduler = Scheduler()
    scheduler.start()
    
    rucio_listener = RucioListener(connection_pool, rucio_config, rse_config)
    rucio_listener.start()

    project_master = ProjectMaster(connection_pool, scheduler, rse_config, pollers, replica_client)
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
