from rucio.client.replicaclient import ReplicaClient
import sys

dids = sys.argv[1:]
rucio_replicas = self.RucioClient.list_replicas(dids, all_states=False, ignore_availability=False)
print(rucio_replicas)