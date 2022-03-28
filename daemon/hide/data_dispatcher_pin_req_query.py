import stompy
from data_dispatcher.db import DBFile
from pythreader import PyThread, Primitive, Scheduler, synchronized

TaskScheduler = Scheduler()

class PinRequest(object):
    
    # dCache version
    # see https://docs.google.com/document/d/14sdrRmJts5JYBFKSvedKCxT1tcrWtWchR-PJhxdunT8/edit?usp=sharing
    
    PinLifetime = 3600
    
    def __init__(self, url, ssl_config, replicas, staged):
        self.EndPoint = url
        self.Replicas = replicas.copy()             # {did -> replica_info(including path)}
        self.SSLConfig = ssl_config
        self.ID = None
        self.Staged = staged
        
    def send(self):
        session = requests.Session()
        session.verify = self.SSLConfig.get("ca_bundle")
        session.cert = self.SSLConfig.get("cert")
        session.key = self.SSLConfig.get("key")

        headers = { "accept" : "application/json",
                    "content-type" : "application/json"}

        data =  {
            "target" : list(r["path"] for r in self.Replicas.values()),
            "activity" : "PIN",
            "clearOnSuccess" : True, 
            "clearOnFailure" : True, 
            "expandDirectories" : None,
            "arguments": {
                "lifetime": self.PinLifetime,
                "lifetime-unit": "SECONDS"
            }
        }
        r = session.post(self.EndPoint, data = json.dumps(data), headers=headers)
        r.raise_for_status()
        self.Expiration = time.time() + self.PinLifetime
        
    def query(self):
        pass
        
    def same_files(self, dids):
        return set(dids) == set(self.Replicas.keys())

def did(namespace, name):
    return f"{namespace}:{name}"

def undid(did):
    return tuple(did.split(":", 1))
    
class RSEConfig(object):
    
    def __init__(self, config):
        self.Config = config
    
    def __contains__(self, rse):
        return rse in self.Config
    
    def is_tape_rse(self, rse):
        return self.Config.get(rse, {}).get("is_tape", False)
        
    def pin_interfce_url(self, rse):
        return self.Config[rse]["pin_interface_url"]
        
    def url_to_path(self, rse, url):
        root = self.Config.get(rse, {}).get("root", "/")
        parts = urllib.parse.urlparse(url)
        path = parts.path
        if path.startswith(root) and root != "/":
            path = path[len(root):]
        while "//" in path:
            path = path.replace("//", "/")
        if not path or path[0] != '/':
            path = "/" + path
        return path
        
    def preference(self, rse):
        return self.Config.get(rse, {}).get("preference", 0)
        
class FileMonitor(Primitive):
    

    def __init__(self, project_monitor, rse, endpoint, stagger=0.1):
        self.RSE = rse
        self.Monitor = 
        self.Tasks = TaskQueue(1, stagger=stagger)
        
    def submit(self, rse, url, files):
        self.Tasks.addTaask(UpdateFiles())
        
class UpdateFiles(Task):
    
    def __init__(self, monitor, rse, url, files):
        Task.__init__(self)
        self.RSE = rse
        self.BaseURL = url
        self.Files = files      # { did -> {"path":..., ...}}
        self.Monitor = monitor
        
    def run(self):
        availability = []           # [(did, path, available), ...]
        for did, info in self.Files.items():
            path = info["path"]
            url = self.BaseURL + "/" + path + "?locality=true"
            response = requests.get(url, verify=False)
            data = response.json()
            availability.append((did, "ONLINE" in data.get("fileLocality", "").upper())
        self.Monitor.update_availability(self.RSE, availability)

class ProjectMonitor(Primitive):
    
    Interval = 600        # 10 minutes
    
    def __init__(self, project_id, db, rse_config, ssl_config):
        #Primitive.__init__(self, name=f"ProjectMonitor({project_id})")
        self.ProjectID = project_id
        self.DB = db
        self.RSEConfig = rse_config
        self.SSLConfig = ssl_config
        self.PinRequests = {}       # {rse -> PinRequest}
        self.UpdateTaskQueue = TaskQueue(5, stagget=0.1)
        
    def __update_availability(self, rse, pin_request, replicas):
        #
        # Update file availability
        #
        # replicas: active replicas for single RSE, {did -> replica_info}
        #
        update_availability = {}            # files became available/unavailable, {did -> {rse -> bool}}
        file_status = pin_request.query()   # {did -> staged: True/False}
        set_true = []                       # [did, ...]
        set_false = []                      # [did, ...]
        for did, replica_info in replicas.items():
            if replica_info["available"] and not file_status.get(did):
                set_true.append(did)
            elif not replica_info["available"] and file_status.get(did):
                set_false.append(did)
                
        DBFile.update_availability(self.DB, rse, set_true, True)
        DBFile.update_availability(self.DB, rse, set_false, False)

    @synchronized
    def update_files_availability(self, rse, availability):
        # availability: [(did, available)]
        available = []
        unavailable = []
        for did, status in availability:
            if status:  available.append(did)
            else:       unavailable.append(did)
        DBFile.update_availability(self.DB, rse, available, True)
        DBFile.update_availability(self.DB, rse, unavailable, True)

    def run(self):
        project = DBProject.get(self.DB, self.ProjectID)
        if project is None:
            self.remove_me(None, "deleted")        # deleted
            return "stop"       # for the Scheduler
            
        handles = project.file_handles(with_replicas=True)
        active_handles = {did(h.Namespace, h.Name): h for h in handles if h.is_active()}
        
        if not handles:
            self.remove_me(project, "finished")
            return "stop"

        #
        # Collect replica info on active replicas located in tape storages
        #

        tape_replicas_by_rse = {}               # {rse -> {did -> replica_info_dict(including path)}}
        for did, h in active_handles.items():
            for rse, replica_info in h.replicas().items():
                if self.RSEConfig.is_tape_rse(rse):
                    tape_replicas_by_rse.setdefault(rse, {})[did] = replica_info
        
        for rse, replicas_dict in tape_replicas_by_rse.items():
            pin_request = self.PinRequests.get(rse)
            updated = False
            if pin_request is None or not pin_request.same_files(replicas_dict.keys()):
                staged = set() if pin_request is None else pin_request.Staged
                self.PinRequests[rse] = pin_request = PinRequest(self.RSEConfig.pin_interface_url(rse), 
                    self.SSLConfig, replicas_dict
                )
                pin_request.send()
            self.update_availability(rse, pin_request, replicas_dict)
            if pin_request.Expiration < time.time() + self.Interval*2:
                pin_request.send()

    __call__ = run

class ProjectMaster(PyThread):
    
    RunInterval = 10        # seconds       - new project discovery latency
    
    def __init__(self, db, scheduler, rse_config, ssl_config):
        PyThread.__init__(self, name="ProjectMaster")
        self.DB = db
        self.Monitors = {}      # project id -> ProjectMonitor
        self.Stop = False
        self.Scheduler = scheduler
        self.RSEConfig = rse_config
        self.SSLConfig = ssl_config

    def run(self):
        while not self.Stop:
            projects = DBProject.list(self.DB, state="active")
            for project in projects:
                if not project.ID in self.Monitors:
                    self.add_project(project.ID)
            self.sleep(self.RunInterval)
    
    @synchronized
    def add_project(self, project_id):
        if not project_id in self.Monitors:
            monitor = self.Monitors[project_id] = ProjectMonitor(project_id, self.DB, self.RSEConfig, self.SSLConfig)
        self.Scheduler.add(monitor)

    @synchronized
    def remove_project(self, project_id):
        monitor = self.Monitors.pop(project_id, None)
        if monitor is not None:
            self.Scheduler.remove(project_id)

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

class RucioListener(PyThread):
    
    def __init__(self, db, rucio_config, ssl_config):
        PyThread.__init__(self, name="RucioListener")
        self.Config = config
        self.SSLConfig = config.get("ssl", {})
        self.MessageBrokerConfig = rucio_config["message_broker"]
        self.SSLConfig = ssl_config
        self.DB = db
        self.RSEConfig = config["rse"]

    def add_replica(self, scope, name, replica_info):
        # replica_info is supposed to be the payload dictionary received from Rucio with the following fields:
        #   scope, name, dst-rse, dst-url, dst-type
        f = DBFile.get(db, scope, name)
        if f is None:
            return
        rse = replica_info["dst-rse"]
        transport_url = replica_info["dst-url"]
        path = self.RSEConfig.url_to_path(rse, transport_url)
        preference = self.RSEConfig.preference(rse)
        available = not self.RSEConfig.is_tape_rse(rse)         # do not trust dst-type from Rucio
        f.update_replica(rse, path, preference, available)

    def run(self):
        broker_addr = (self.MessageBrokerConfig["host"], self.MessageBrokerConfig["port"])
        cert_file = self.SSLConfig.get("cert")
        key_file = self.SSLConfig.get("key")
        ca_file = self.SSLConfig.get("ca_bundle")
        vhost = self.MessageBrokerConfig.get("vhost", "/")
        subscribe = self.MessageBrokerConfig.get["subscribe"]
        connection = stompy.connect(broker_addr, cert_file=cert_file, key_file=key_file, ca_file=ca_file, host=vhost)
        connection.subscribe(subscribe)
        for frame in connection:
            if frame.Command == "MESSAGE":
                data = frame.json
                event_type = data.get("event_type")
                if event_type == "transfer-done":
                    payload = data.get("payload", {})
                    rse = payload.get("dst-rse")
                    if rse in self.RSEConfig:
                        scope = payload.get("scope")
                        name = payload.get("name")
                        if event_type == "request-done" and rse and self.Updater.has_rse(rse):
                            self.add_replica(scope, name, payload)

def main():
    import sys, yaml, getopt, os
    from wsdbtools import ConnectionPool

    opts, args = getopt.getopt(sys.argv[1:], "c:")
    opts = dict(opts)
    config = opts.get("-c") or os.environ.get("DATA_DISPATCHER_CFG")
    config = yaml.load(open(config, "r"), Loader=yaml.SafeLoader)
    
    rse_config = RSEConfig(config.get("rse", {}))
    ssl_config = config.get("ssl", {})
    rucio_config = config["rucio"]
    
    dbconfig = config["database"]
    connstr = dbconfig["connect"]
    connection_pool = ConnectionPool(postgres=connstr)

    # test the connection
    try:
        connection = ConnectionPool.connect()
    except Exception as e:
        print("Error connection to the DD database:", e)
        sys.exit(1)
    else:
        connection.close()
        del connection
    
    max_threads = config.get("max_threads", 100)
    scheduler = Scheduler(max_threads)
    scheduler.start()
    
    rucio_listener = RucioListener(connection_pool, rucio_config, ssl_config)
    rucio_listener.start()
    
    project_master = ProjectMaster(connection_pool, scheduler, rse_config, ssl_config)
    project_master.start()
    
    rucio_listener.join()
    project_master.join()
    scheduler.join()
    
    
    
    
    
