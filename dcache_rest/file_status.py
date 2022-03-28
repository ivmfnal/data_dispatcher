import json, pprint
import requests
import sys, os

base_url = "https://fndca3b.fnal.gov:3880/api/v1/namespace"
key = "certs/ivm@fnal.gov_key.pem"
cert = "certs/ivm@fnal.gov_cert.pem"

headers = { "accept" : "application/json",
                "content-type" : "application/json"}

path = sys.argv[1]
url = base_url + "/" + path + "?locality=true"
r = requests.get(url, headers=headers, cert=(cert, key), verify=False)
r.raise_for_status()
data = r.json()
pprint.pprint(data)

