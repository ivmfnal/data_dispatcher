import json
from kafka import KafkaConsumer
from urllib.parse import urlparse, parse_qs

"""
msgType:
  remove
  request
  restore
  store
  transfer

"""

_STORAGE_GROUP = "mu2e"

def main():

    consumer = KafkaConsumer("ingest.dcache.billing",
                             bootstrap_servers="lssrv03:9092,lssrv04:9092,lssrv05:9092",
                             value_deserializer=lambda m: json.loads(m.decode("ascii")))
    for msg in consumer:
        message = msg.value
        msgType = message["msgType"]

        pnfsid = message.get("pnfsid")
        pool = message.get("cellName")

        if msgType == "restore":
            if 'locations' not in message:
                continue
            storage_info = message.get("storageInfo")
            transfer_time = message.get("transferTime")
            file_size = message.get("fileSize")
            storage_info = message.get("storageInfo")
            speed = file_size / float ( 1 << 20 ) / transfer_time * 1000.
            locations = message.get("locations")
            loc = locations[0]
            url = urlparse(loc)
            qs = parse_qs(url.query)
            volume = qs.get("volume")[0]
            path = qs.get("original_name")[0]
            print ("File ", path, " restored from volume ", volume, " to pool ", pool, "speed %.2f MiB/s" % speed)
        elif msgType == "store":
            storage_info = message.get("storageInfo")
            transfer_time = message.get("transferTime")
            file_size = message.get("fileSize")
            speed = file_size / float ( 1 << 20 ) / transfer_time * 1000.
            path = message.get("billingPath")
            print ("File ", path, " stored from pool ", pool, " speed %.2f MiB/s" % speed)
        elif msgType == "remove":
            #'status': {'msg': 'sweeper making space for new data', 'code': 0}}
            status = message.get("status")
            if status.get("msg") == "sweeper making space for new data" and \
                   status.get("code") == 0:
                print (pnfsid, " went OFFLINE")

if __name__ == "__main__":
    main()