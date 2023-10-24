import traceback, requests, time, json, sys
from data_dispatcher.db import DBReplica
from pythreader import Primitive, Scheduler, synchronized, PyThread
from data_dispatcher.logs import Logged

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
                    nburst = min(self.MaxBurst, n)
                    burst, items = items[:nburst], items[nburst:]
                    self.Files = dict(items)
                headers = { 
                    "accept" : "application/json",
                    "content-type" : "application/json"
                }
                available_dids = []
                unavailable_dids = []
                remove_dids = []
                for did, path in burst:
                    url = self.BaseURL + path + "?locality=true"
                    cert = None if self.Cert is None else (self.Cert, self.Key)
                    #self.debug("dCache poll URL:", url)
                    response = requests.get(url, headers=headers, cert=cert, verify=False)
                    #self.debug("response:", response.status_code, response.text)
                    if response.status_code == 404:
                        self.debug(f"file not found (status 404): {did} {path} - removing")
                        #self.log("Replica not found:", path)
                        remove_dids.append(did)
                    elif response.status_code//100 != 2:
                        continue
                    else:
                        data = response.json()
                        available = "ONLINE" in data.get("fileLocality", "").upper()
                        if available:
                            #self.log("Replica available:", did, path)
                            available_dids.append(did)
                        else:
                            unavailable_dids.append(did)
                            #self.log("Replica unavailable:", did, path)
                self.debug("out of %d replicas: available: %d, unavailable: %d, not found:%d" % 
                    (len(burst), len(available_dids), len(unavailable_dids), len(remove_dids))
                )
                DBReplica.update_availability_bulk(self.DB, True, self.RSE, available_dids)
                DBReplica.update_availability_bulk(self.DB, False, self.RSE, unavailable_dids)
                DBReplica.remove_bulk(self.DB, self.RSE, remove_dids)
                time.sleep(self.STAGGER)
            self.sleep(10)

class PinRequest(Logged):
    
    # dCache version
    # see https://docs.google.com/document/d/14sdrRmJts5JYBFKSvedKCxT1tcrWtWchR-PJhxdunT8/edit?usp=sharing
    
    PinLifetime = 3600
    
    def __init__(self, rse, url, pin_prefix, ssl_config, paths, mode="dcache"):
        Logged.__init__(self, f"PinRequest({rse})")
        self.BaseURL = url
        self.Paths = set(paths)
        self.SSLConfig = ssl_config
        self.URL = None
        self.PinPrefix = pin_prefix
        self.Cert = self.SSLConfig.get("cert")
        self.Key = self.SSLConfig.get("key")
        self.CertTuple = (self.Cert, self.Key) if self.Cert else None
        self.Complete = False
        self.Expiration = None
        self.Error = None
        self.StagedReplicas = set()         # set of paths
        self.debug("created for", len(paths),"replicas")
        self.Mode = mode            # dcache or WLCG

    def __len__(self):
        return len(self.Paths)

    def send(self):
        self.StagedReplicas = set()
        self.Expiration = time.time() + self.PinLifetime
        headers = { "accept" : "application/json",
                    "content-type" : "application/json"}
        if self.Mode == "dcache":
            data =  {
                "target" : json.dumps(list(self.Paths)),
                "activity" : "PIN",
                "clearOnSuccess" : "false",             # 6/30/22: dCache will accept strings instead of booleans for a while. In the future it will start accepting both 
                "clearOnFailure" : "false", 
                "expandDirectories" : None,
                "arguments": {
                    "lifetime": str(self.PinLifetime),  # 6/30/22: dCache will accept strings instead of ints
                    "lifetime-unit": "SECONDS"
                }
            }
            if self.PinPrefix:
                data["target-prefix"] = self.PinPrefix
            url = self.BaseURL
        elif self.Mode == "WLCG":
            if self.PinLifetime >= 3600:
                lifetime = "PT%dH" % ((self.PinLifetime + 3599)//3600,)
            elif self.PinLifetime >= 60:
                lifetime = "PT%dM" % ((self.PinLifetime + 59)//60,)
            else:
                lifetime = "PT%dS" % (self.PinLifetime,)
            data =  {
                "files": [
                    {"path": path, "diskLifetime": lifetime}
                ]
            }
            url = self.BaseURL + ("/" if not self.BaseURL.endswith("/") else "") + "stage"
        else:
            print("Unknown Pin Request mode:", mode, file=sys.stderr)
            sys.exit(1)
            
        self.debug("request data:", json.dumps(data, indent="  "))
        r = requests.post(url, data = json.dumps(data), headers=headers, 
                    verify=False, cert = self.CertTuple)
        self.debug("response:", r)
        self.debug("response headers:")
        for name, value in r.headers.items():
            self.debug("  %s: %s" % (name, value))
        self.debug("response text:")
        self.debug(r.text)
        self.URL = r.headers.get('request-url') or r.headers.get("Location")            # dCache or WLCG format
        result = True

        if r.status_code // 100 != 2:
            self.Error = f"Bad HTTP status code: {r.status_code}"
            self.error("Error sendig pin request:\n    HTTP status:", r.status_code, "\n    response headers:", r.headers,"\n    response text:", r.text)
            result = False
        elif not self.URL:
            self.Error = f"No pin request URL provided in the response headers"
            self.error("Error sendig pin request. No URL returned.\n    response headers:", r.headers)
            result = False
        self.debug("send() result -> ", result, "  error:", self.Error or "(no error)")
        return result
        
    def query(self):
        assert self.URL is not None
        headers = { "accept" : "application/json" }
        r = requests.get(self.URL, headers=headers, verify=False, cert = self.CertTuple)
        #self.debug("status(): response:", r)
        if r.status_code // 100 == 4:
            self.log("query: HTTP status:", r.status_code, " -- ERROR")
            self.Error = r.text
            return "ERROR"
        data = r.json()
        self.debug("My URL:", self.URL, "   query response:", r)
        self.debug("response text:", "\n"+r.text)
        r.raise_for_status()
        data = r.json()
        if data.get("status", "").upper() == "COMPLETED":
            self.Complete = True
            self.StagedReplicas = self.Paths
        elif "targets" in data:
            # dCache format
            self.StagedReplicas = set(f for f in
                (item.get("target") for item in data["targets"] if item.get("state") == "COMPLETED")
                if f
            )
        elif "files" in data:
            # WLCG format
            self.StagedReplicas = set(item["path"] for item in data["files"]
                    if item.get("onDisk") or item.get("state", "").upper() == "COMPLETED"
            )
        return data
        
    def staged_replicas(self):
        return self.StagedReplicas

    def delete(self):
        assert self.URL is not None
        headers = { "accept" : "application/json" }
        r = requests.delete(self.URL, headers=headers, verify=False, cert = self.CertTuple)
        #self.debug("status(): response:", r)
        if r.status_code // 100 == 4:
            self.Error = r.text
            return "ERROR"
        r.raise_for_status()
        self.debug("delete: my URL:", self.URL, "   response:", r.text)
        self.StagedReplicas = set()
        #return r.json()

    def status(self):
        return self.query()["status"]

    def complete(self):
        self.debug("previous complete status:", self.Complete)
        if not self.Complete:
            info = self.query()
            self.Complete = info.get("status").upper() == "COMPLETED" and len(info.get("failures", {}).get("failures",{})) == 0
        return self.Complete

    def update_staged_set(self):
        # query and update staged set unless already complete, return stged set
        _ = self.complete()
        return self.StagedReplicas

    def will_expire(self, t):
        return self.Expiration is None or time.time() >= self.Expiration - t

    def same_files(self, paths):
        paths = set(paths)
        same = paths == self.Paths
        return same

class DCachePinner(PyThread, Logged):

    InitialSleepInterval = 60
    UpdateInterval = 60         # replica availability update interval

    def __init__(self, rse, db, url, prefix, ssl_config, poller):
        PyThread.__init__(self, name=f"DCachePinner({rse})")
        Logged.__init__(self, name=f"DCachePinner({rse})")
        self.URL = url
        self.debug("url:", self.URL)
        self.RSE = rse
        self.Prefix = prefix
        self.SSLConfig = ssl_config
        self.FilesPerProject = {}        # {project_id -> {did: path, ...}}
        self.PinRequest = None
        self.DB = db
        self.Poller = poller
        self.Stop = False
        self.StagedReplicas = []

    @synchronized
    def pin_project(self, project_id, replicas):
        self.log(f"pin_project({project_id}):", len(replicas), "replicas")
        self.FilesPerProject[project_id] = replicas.copy()

    @synchronized
    def unpin_project(self, project_id):
        self.log(f"unpin_project({project_id})")
        self.FilesPerProject.pop(project_id, None)

    def run(self):
        time.sleep(self.InitialSleepInterval)          # initial sleep so pprojects have a chance to send their pin requests
        self.debug("run...")
        while not self.Stop:
            next_run = self.UpdateInterval
            try:
                with self:
                    all_files = {}              # {did -> path} for all projects, combined
                    for project_files in self.FilesPerProject.values():
                        all_files.update(project_files)
                    all_paths = set(all_files.values())
                    
                    self.debug("existing pin request:", None if self.PinRequest is None else self.PinRequest.URL)
                    
                    if self.PinRequest is not None:
                        if not self.PinRequest.same_files(all_paths):
                            self.log("file set changed -- deleting pin request")
                            self.debug("file set changed -- deleting pin request")
                            # debug
                            for path in sorted(all_paths - self.PinRequest.Paths):
                                self.debug(" +", path)
                            for path in sorted(self.PinRequest.Paths - all_paths):
                                self.debug(" -", path)
                            self.PinRequest.delete()
                            self.PinRequest = None
                        elif self.PinRequest.will_expire(self.UpdateInterval*3):
                            self.debug("pin request is about to expire -- deleting pin request")
                            self.log("pin request is about to expire -- deleting pin request")
                            try:    self.PinRequest.delete()
                            except Exception as e:
                                self.error("Exception deleting pin request:", e, "   -- ignoring, the pin request will expire")
                                self.debug("Exception deleting pin request:", e, "   -- ignoring, the pin request will expire")
                            self.PinRequest = None
                    if all_files:       # anything to pin ??
                        if self.PinRequest is None:
                            self.debug("sending pin request for", len(all_paths), "replicas...")
                            pin_request = PinRequest(self.RSE, self.URL, self.Prefix, self.SSLConfig, all_paths)
                            try:
                                created = pin_request.send()
                            except Exception as e:
                                self.error("exception sending pin request: " + traceback.format_exc())
                                self.log("Failed to create pin request because of exception:", e)
                            else:
                                if created:
                                    self.PinRequest = pin_request
                                    self.debug("pin request created for %d files. URL:%s" % (len(all_paths), self.PinRequest.URL))
                                    self.log("pin request created for %d files. URL:%s" % (len(all_paths), self.PinRequest.URL))
                                    next_run = 5            # check request status kinda soon
                                else:
                                    self.log("error sending pin request:", pin_request.Error)
                                    self.error("error sending pin request:", pin_request.Error)
                        else:
                            staged_paths = self.PinRequest.update_staged_set()
                            staged_dids = [did for did, path in all_files.items() if path in staged_paths]
                            pending_dids_paths = [(did, path) for did, path in all_files.items() if path not in staged_paths]
                            self.log("files staged:", len(staged_dids), "    still pending:", len(pending_dids_paths))
                            if staged_dids:
                                DBReplica.update_availability_bulk(self.DB, True, self.RSE, staged_dids)
                            if pending_dids_paths:
                                self.debug("sending", len(pending_dids_paths), "dids/paths to poller")
                                self.Poller.submit(pending_dids_paths)
                    else:
                        #self.debug("no files to pin")
                        pass
            except Exception as e:
                self.error("exception in run:\n", traceback.format_exc())
            time.sleep(next_run)       

class DCacheInterface(Primitive, Logged):
    
    def __init__(self, rse, db, rse_config):
        Primitive.__init__(self, name=f"DCacheInterface({rse})")
        Logged.__init__(self, name=f"DCacheInterface({rse})")
        self.Poller = DCachePoller(rse, db, rse_config.poll_url(rse), rse_config.max_burst(rse), rse_config.ssl_config(rse))
        self.Pinner = DCachePinner(rse, db, rse_config.pin_url(rse), rse_config.pin_prefix(rse), rse_config.ssl_config(rse), self.Poller)
        self.Poller.start()
        self.Pinner.start()

    def pin_project(self, project_id, files):
        # files: {did -> path}
        return self.Pinner.pin_project(project_id, files)

    def unpin_project(self, project_id):
        return self.Pinner.unpin_project(project_id)

    def poll(self, files):
        # files: {did -> path}
        self.Poller.submit(files)
