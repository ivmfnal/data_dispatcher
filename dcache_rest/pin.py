#!/usr/bin/env python

import json 
import requests
import sys

base_url = "https://fndca3b.fnal.gov:3880/api/v1/bulk-requests"

if __name__ == "__main__":
    session = requests.Session()
    session.verify = "/etc/grid-security/certificates"
    session.cert = "/tmp/x509up_u8637"
    session.key = "/tmp/x509up_u8637"

    headers = { "accept" : "application/json",
                "content-type" : "application/json"}

    data =  {
    	"target" : sys.argv[1], 
        "activity" : "PIN",
        "clearOnSuccess" : True, 
        "clearOnFailure" : True, 
        "expandDirectories" : None,
        "arguments": {
            "lifetime": 24,
            "lifetime-unit": "HOURS"
        }
    }
    

    r = session.post(base_url, data = json.dumps(data), headers=headers)
    r.raise_for_status()
    print (r.status_code, r.headers['request-url'])

    
