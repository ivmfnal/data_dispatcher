from data_dispatcher.api import DataDispatcherClient
from data_dispatcher import Version
from metacat.webapi import MetaCatClient 
import sys, getopt, os, json
from .ui_project import create_project, show_project, list_projects
from .ui_handle import show_handle
from .ui_lib import pretty_json
from .ui_file import show_file, replica_available, replica_unavailable
import time

server_url = os.environ.get("DATA_DISPATCHER_URL")
if not server_url:
    print("Data Dispatcher Server URL undefined. Use DATA_DISPATCHER_URL environment variable")
    sys.exit(2)

my_name = sys.argv[0]

Usage = f"""
Data Dispatcher version: {Version}

Usage:

dd <commnd> <subcommand> <options> <args>

Commands:

    worker [-n|<worker id>]                                - set or print my worker_id 
                                                             -n generates new one
                                                             worker id will be saved in <CWD>/.worker_id
    create project ...
    show project ...
    list projects ...
    
    show file [-j] <namespace>:<name>

    list handles [-s <status>] <project_id>                - list file handles, -s filters handles by status
    show handle [-j] <project_id> <namespace>:<name>       - show file handle, -j - as JSON

    next [-j] [-t <timeout>] <project_id>                  - get next available file, 
                                                             -j - as JSON
                                                             -t - wait for next file until "timeout" seconds, 
                                                                  otherwise, wait until the project finishes
    done <project_id> <namespace>:<name>                   - mark the file as successfully processed
    failed [-f] <project_id> <namespace>:<name>            - mark the file as failed, -f means "final", no retries

    delete project <project_id>
    
    login x509 <user> <cert> <key>
    login password <user>
"""

def main():
    client = DataDispatcherClient(server_url)

    if len(sys.argv[1:]) < 1 or "-\?" in sys.argv[1:] or "--help" in sys.argv[1:]:
        print(Usage)
        sys.exit(2)
    
    command = sys.argv[1]
    rest = sys.argv[2:]

    if command == "worker":
        opts, args = getopt.getopt(rest, "n")
        opts = dict(opts)
        if "-n" in opts:
            worker_id = client.new_worker_id()
        elif args:
            worker_id = args[0]
            client.new_worker_id(worker_id)
        else:
            worker_id = client.WorkerID
        if not worker_id:
            print("worker id unknown")
            sys.exit(1)
        print(worker_id)
        
    elif command == "create":
        subcommand, rest = rest[0], rest[1:]
        if subcommand != "project":
            print(Usage)
            sys.exit(2)
        create_project(client, rest)
    
    elif command == "show":
        subcommand, rest = rest[0], rest[1:]
        if subcommand not in ("file", "project", "handle"):
            print(Usage)
            sys.exit(2)
        if subcommand == "project":
            show_project(client, rest)
        elif subcommand == "handle":
            show_handle(client, rest)
        elif subcommand == "file":
            show_file(client, rest)
        else:
            print(Usage)
            sys.exit(2)
        
    elif command == "replica":
        subcommand, rest = rest[0], rest[1:]
        if subcommand == "available":
            replica_available(client, rest)
        elif subcommand == "unavailable":
            replica_unavailable(client, rest)
        else:
            print(Usage)
            sys.exit(2)

    elif command == "list":
        if not rest:
            print(Usage)
            sys.exit(2)
        subcommand, rest = rest[0], rest[1:]
        if subcommand == "projects":
            list_projects(client, rest)
        elif subcommand == "handles":
            list_handles(client, rest)
        else:
            print(Usage)
            sys.exit(2)
        
    elif command == "delete":
        subcommand, rest = rest[0], rest[1:]
        if subcommand == "project":
            project_id = rest[0]
            client.delete_project(project_id)
        else:
            print(Usage)
            sys.exit(2)
        

    elif command == "next":
        opts, args = getopt.getopt(rest, "jt:")
        opts = dict(opts)
        if not args:
            print(Usage)
            sys.exit(2)
        project_id = args[0]
        as_json = "-j" in opts
        timeout = int(opts.get("-t", -1))
        done = False
        t0 = time.time()
        t1 = t0 + timeout
        while not done:
            info = client.next_file(project_id)
            if info:
                if as_json:
                    print(pretty_json(info))
                else:
                    print("%s:%s" % (info["namespace"], info["name"]))
                sys.exit(0)
            if timeout < 0 or (timeout > 0 and time.time() < t1):
                project_info = client.get_project(project_id)
                if not project_info["active"]:
                    sys.exit(10)    # project finished
                dt = min(5, t1-time.time())
                time.sleep(dt)
            else:
                sys.exit(11)        # timeout

    elif command == "done":
        if len(rest) != 2:
            print(Usage)
            sys.exit(2)
        project_id, did = rest
        client.file_done(project_id, did)
    
    elif command == "failed":
        opts, args = getopt.getopt(rest, "f")
        opts = dict(opts)
        if len(args) != 2:
            print(Usage)
            sys.exit(2)
        project_id, did = args
        client.file_failed(project_id, did, retry = not "-f" in opts)
    
    elif command == "login":
        if len(rest) < 2:
            print(Usage)
            sys.exit(2)
        subcommand, rest = rest[0], rest[1:]
        if subcommand == "x509":
            username, cert, key = rest
            user, expiration = client.login_x509(username, cert, key)
        elif subcommand == "password":
            import getpass
            username = rest[0]
            password = getpass.getpass("Password:")
            user, expiration = client.login_password(username, password)
        else:
            print(f"Unknown authentication mechanism {subcommand}\n")
            print(Usage)
            sys.exit(2)

        print ("User:   ", user)
        print ("Expires:", time.ctime(expiration))
    
    else:
        print(Usage)
        sys.exit(2)

if __name__ == "__main__":
    main()
        
        
    
            
            
        
        
        
        
        
