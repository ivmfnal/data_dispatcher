import traceback, requests, time, json, sys
from data_dispatcher.db import DBReplica
from pythreader import Primitive, Scheduler, synchronized, PyThread
from data_dispatcher.logs import Logged
from dcache import DCachePoller

print("wlcg module importing...")


class WLCGPinRequest(Logged):
    
    # WLCG pin interface
    # see https://docs.google.com/document/d/1Zx_H5dRkQRfju3xIYZ2WgjKoOvmLtsafP2pKGpHqcfY/edit
    
    PinLifetime = 3600
    
    def __init__(self, rse, url, pin_prefix_ignored, ssl_config, paths):
        Logged.__init__(self, f"WLCGPinRequest({rse})")
        self.EndpointURL = url
        self.Paths = set(paths)
        self.SSLConfig = ssl_config
        self.URL = None
        self.RequestID = None
        self.Cert = self.SSLConfig.get("cert")
        self.Key = self.SSLConfig.get("key")
        self.CertTuple = (self.Cert, self.Key) if self.Cert else None
        self.Complete = False
        self.Expiration = None
        self.Error = None
        self.StagedReplicas = set()         # set of paths
        self.debug("created for", len(paths),"replicas")

    def __len__(self):
        return len(self.Paths)

    def send(self):
        self.StagedReplicas = set()
        self.Expiration = time.time() + self.PinLifetime
        headers = { "accept" : "application/json",
                    "content-type" : "application/json"}
        if self.PinLifetime >= 3600:
            lifetime = "PT%dH" % ((self.PinLifetime + 3599)//3600,)
        elif self.PinLifetime >= 60:
            lifetime = "PT%dM" % ((self.PinLifetime + 59)//60,)
        else:
            lifetime = "PT%dS" % (self.PinLifetime,)
        data =  {
            "files": [
                {"path": path, "diskLifetime": lifetime} for path in self.Paths
            ]
        }
        url = self.EndpointURL + ("/" if not self.EndpointURL.endswith("/") else "") + "stage"
            
        self.debug("WLCG pin request: URL:", url, "  request data:\n", json.dumps(data, indent="  "))
        r = requests.post(url, data = json.dumps(data), headers=headers, verify=False, cert = self.CertTuple)
        self.debug("response:", r)
        self.debug("response headers:")
        for name, value in r.headers.items():
            self.debug("  %s: %s" % (name, value))
        self.debug("response text:")
        self.debug(r.text)
        self.URL = r.headers.get("Location")
        result = True

        if r.status_code // 100 != 2:
            self.Error = f"Bad HTTP status code: {r.status_code}"
            self.error("Error sendig pin request:\n    HTTP status:", r.status_code, "\n    response headers:", r.headers,"\n    response text:", r.text)
            result = False
        elif not self.URL:
            self.Error = f"No pin request URL provided in the response headers"
            self.error("Error sendig pin request. No URL returned.\n    response headers:", r.headers)
            result = False
        self.RequestID = r.json().get("requestId")
        self.debug("send() result -> ", result, "  error:", self.Error or "(no error)", "  request id:", self.RequestID)
        return result
        
    def query(self):
        assert self.URL is not None
        headers = { "accept" : "application/json" }
        r = requests.get(self.URL, headers=headers, verify=False, cert = self.CertTuple)
        #self.debug("status(): response:", r)
        if r.status_code // 100 != 2:
            self.log("query: HTTP status:", r.status_code, " -- ERROR")
            self.Error = r.text
            return "ERROR"
        data = r.json()
        self.debug("My URL:", self.URL, "   query response:", r)
        self.debug("response text:", "\n"+r.text)
        r.raise_for_status()
        self.StagedReplicas = set(
            item["path"] for item in data["files"]
            if item.get("onDisk") or item.get("state", "").upper() == "COMPLETED"
        )
        self.Complete = data.get("completedAt", time.time() + 1000000) < time.time()
        return data
        
    def staged_replicas(self):
        return self.StagedReplicas

    def delete(self):
        assert self.URL is not None
        headers = { "accept" : "application/json" }
        r = requests.delete(self.URL, headers=headers, verify=False, cert = self.CertTuple)
        #self.debug("status(): response:", r)
        if r.status_code // 100 != 2:
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
            self.query()
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

class WLCGPinner(PyThread, Logged):

    InitialSleepInterval = 60
    UpdateInterval = 60         # replica availability update interval

    def __init__(self, rse, db, url, prefix, ssl_config, poller):
        PyThread.__init__(self, name=f"WLCGPinner({rse})")
        Logged.__init__(self, name=f"WLCGPinner({rse})")
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
        # replicas: {did: path}
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
                            pin_request = WLCGPinRequest(self.RSE, self.URL, self.Prefix, self.SSLConfig, all_paths)
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
        pin_url = rse_config.pin_url(rse)
        pin_url = "https://fndca1.fnal.gov:3880/.well-known/wlcg-tape-rest-api"
        if "/.well-known/" in pin_url:
            pin_url = poll_url = self.discover(pin_url)
            self.debug(f"WLCG interface discovered at:", pin_url)
        else:
            poll_url = pin_urlrse_config.poll_url(rse)
        self.Poller = DCachePoller(rse, db, poll_url, rse_config.max_burst(rse), rse_config.ssl_config(rse))
        self.Pinner = WLCGPinner(rse, db, pin_url, rse_config.pin_prefix(rse), rse_config.ssl_config(rse), self.Poller)
        self.Poller.start()
        self.Pinner.start()
        self.log("WLCG DCacheInterface created at:\n    pin URL:", pin_url, "\n    poll URL:", poll_url)
        
    def discover(self, url):
        if "/.well-known/" in url:
            response = requests.get(url, verify=False)
            response.raise_for_status()
            url = response.json()["endpoints"][0]["uri"]
        return url

    def pin_project(self, project_id, files):
        # files: {did -> path}
        return self.Pinner.pin_project(project_id, files)

    def unpin_project(self, project_id):
        return self.Pinner.unpin_project(project_id)

    def poll(self, files):
        # files: {did -> path}
        self.Poller.submit(files)


print("wlcg module imported")
