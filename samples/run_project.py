from data_dispatcher.api import DataDispatcherClient
import sys, os, getopt, random, time
from pythreader import PyThread

class WorkerThread(PyThread):
    
    def __init__(self, project_id, nfiles, tmin, tmax, failure_rate, cpu_site="DEFAULT"):
        self.MyWorkerID = my_worker_id = DataDispatcherClient.random_worker_id()
        PyThread.__init__(self, name=f"Worker {my_worker_id}")
        self.Client = DataDispatcherClient(worker_id=my_worker_id)
        self.TMin = tmin
        self.TMax = tmax
        self.FailureRate = failure_rate
        self.NFiles = nfiles
        self.CPUSite = cpu_site
        self.ProjectID = project_id

    def __str__(self):
        return f"Worker {self.MyWorkerID}"
        
    def log(self, *message):
        print(self, *message)
    
    def run(self):
        self.log("started")
        for ifile in range(self.NFiles):
            file_info = None
            while not file_info:
                file_info = self.Client.next_file(self.ProjectID, cpu_site=self.CPUSite, stagger=2)
                if isinstance(file_info, dict) or not file_info:
                    break
                self.log("time-out reserving a file")
                time.sleep(1.0)
            if not file_info:
                self.log("project ended")
                break   # project is over
            
            did = file_info["namespace"] + ":" + file_info["name"]
            # simulate file processing
            self.log("starting processing file", did, "...")
            time.sleep(self.TMin + random.random()*(self.TMax - self.TMin))
            if random.random() < self.FailureRate:
                self.Client.file_failed(self.ProjectID, did, retry=random.random() < 0.0)
                self.log("failed processing file", did, "...")
            else:
                self.Client.file_done(self.ProjectID, did)
                self.log("done processing file", did, "...")
        else:
            self.log("maximum number of files processed")


opts, args = getopt.getopt(sys.argv[1:], "m:n:t:")
opts = dict(opts)

nworkers = int(opts.get("-m", 3))
files_per_worker = int(opts.get("-n", 2))
min_processing_time = int(opts.get("-t", 10))
max_processing_time = min_processing_time*2
failure_rate = 0.1

project_id = args[0]

workers = [WorkerThread(project_id, files_per_worker, min_processing_time, max_processing_time, failure_rate) for _ in range(nworkers)]
[w.start() for w in workers]
[w.join() for w in workers]



                
            