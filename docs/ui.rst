Data Dispatcher User Interface
==============================

Setting up the environment
--------------------------

After installing the Data Dispatcher client, you will need to define the following environment variables:

    * DATA_DISPATCHER_URL - URL for Data Dispatcher data server
    * DATA_DISPATCHER_AUTH_URL - URL for Data Dispatcher authentication server
    * METACAT_SERVER_URL - URL for MetaCat data srever

Data Dispatcher Commands
------------------------

Logging in
~~~~~~~~~~

Before using Data Dispatcher UI, the user needs to log in. Logging in essentially means obraining an authentication/authorization token from
the token issuer and storing it in local file system for use by the Data Dispatcher UI commands.

Currently, Data Dispatcher supports 2 modes of authentication:

    .. code-block:: shell

        $ dd login password <username>                                  # login using LDAP password
        Password: ...
        
        $ dd login x509 <username> <cert_file.pem> <key_file.pem>       # login using X.509 authentication
        $ dd login x509 <username> <proxy_file>

Projects
~~~~~~~~

Creating project
................

A Data Dispatcher project is a collection of files to process. There are two ways to provide the list of files to process.
One is to specify a MetaCat query and create the project from the resulting file set:

    .. code-block:: shell
    
        $ dd create project <inline MetaCat query>

        # Examples:
        $ dd create project files from dune:all limit 100
        $ dd create project files from dune:all where 'namespace="protodune-sp"' skip 3000 limit 10

        $ dd create project -q <file with MetaCat MQL query>

A project can be created with explicit list of files, specified either as a list of their DIDs (namespace:name):


    .. code-block:: shell

        $ cat > file_list << _EOF_
        protodune-sp:np04_raw_run006833_0001_dl10.root
        protodune-sp:np04_raw_run006833_0001_dl1.root
        protodune-sp:np04_raw_run006833_0001_dl6.root
        _EOF_
        $ dd create project -l file_list


or JSON-formatted list. The list is composed of items of two types:

    - file DID as string
    - a dictionary with keys "namespace", "name" and optional "attributes":

    .. code-block:: shell

        $ cat /tmp/file_list.json 
        [
            "protodune-sp:np04_raw_run006834_0009_dl2.root",
            { 
                "namespace":"protodune-sp", 
                "name":"np04_raw_run006834_0009_dl6.root" 
            },
            { 
                "namespace":  "protodune-sp", 
                "name":       "np04_raw_run006834_0010_dl10.root", 
                "attributes": {"debug":true} 
            }
        ]
        $ dd create project /tmp/file_list.json

The "dd create project" command prints information about the created project in 3 different formats, depending on "-p" option:

    .. code-block:: shell

        $ dd create project ...
        123         # default: just the project ID
        
        $ dd create project -p json ... # print project information as JSON
        {
            "project_id": 123,
            "file_handles": [
                ...
            ]
            ...
        }
        
        $ dd create project -p pprint ... # print project information using Python pprint
        {
         'project_id': 123,
         'file_handles': [
            ...
         ]
         ...
        }


Project and project file attributes
...................................

Data Dispatcher provides a way to pass some arbitrary metadata about the project as a whole and/or each individual project file to the worker.
The metadata is attached to the project and/or project files at the time of the project creation. Project and file metadata can be any JSON dictionary. 
If the project is created using a MetaCat query, Data Dispatcher can copy some portions of file metadata from MetaCat to avoid unnecessary
querying MetaCat at the run time.
When the worker asks for the next file to process, the Data Dispatcher responds with the file information, which includes the project and the 
file metadata.

Note that the project file attributes defined at the project creation time do not get stored in MetaCat. Also, because file
attributes are associated with project file handles instead of files, if two projects include the same
file, they can define file attributes independently without interfering with each other.

There are several ways to specify project level metadata attributes:

    .. code-block:: shell

        # inline:
        $ dd create project -A "email_errors=user@fnal.gov step=postprocess" ...
        
        # as a JSON file:
        $ cat project_attrs.json
        {
            "email_errors": "user@fnal.gov",
            "step": "postprocess"
        }
        $ dd create project -A @project_attrs.json
        
To copy some metadata attributes from MetaCat:

    .. code-block:: shell

        $ dd create project -c core.runs files from ...
        $ dd create project -c detector.hv_value,core.data_tier files from ...

To associate common attributes with each file in the project, use ``-a`` option:

    .. code-block:: shell

        $ dd create project -a "name1=value1 name2=value2" ...
        $ dd create project -a @<JSON file>

If the file list is specified explicitly using JSON file, then each file dictionary may optionally include file attributes:

    .. code-block:: shell

        $ cat /tmp/file_list.json 
        [
            { "namespace":"protodune-sp", "name":"np04_raw_run006834_0009_dl2.root", 
                    "attributes":   {   "pi":3.14, "debug":true } 
            },
            { "namespace":"protodune-sp", "name":"np04_raw_run006834_0009_dl6.root",
                    "attributes":   {   "skip_events": 10   }
            },
            { "namespace":"protodune-sp", "name":"np04_raw_run006834_0010_dl10.root" }
        ]
        $ dd create project -j /tmp/file_list.json
        
When the worker gets next file to process, the JSON representation of file inofrmation includes project and project file attributes:

    .. code-block:: shell

        $ dd next -j 70
        {
          "attempts": 1,
          "attributes": {                   # file attributes
            "pi": 3.14,
            "debug": true,
            "detector.hv_value": 37.7801,   # copied from MetaCat
            "core.runs": [ 1789, 1795 ]
          },
          "name": "np04_raw_run006834_0009_dl2.root",
          "namespace": "protodune-sp",
          "project_attributes": {           # project attributes
            "email_errors": "user@fnal.gov",
            "step": "postprocess"
          },
          "project_id": 70,
          "replicas": [
            {
              "available": true,
              "name": "np04_raw_run006834_0009_dl2.root",
              "namespace": "protodune-sp",
              "path": "/pnfs/fnal.gov/usr/...",
              "preference": 0,
              "rse": "FNAL_DCACHE",
              "rse_available": true,
              "url": "root://fndca1.fnal.gov:1094/pnfs/fnal.gov/usr/..."
            }
          ],
          "state": "reserved",
          "worker_id": "fnpc123_pid4563"
        }

        
Viewing projects
................

    .. code-block:: shell

        $ dd list projects
            -j                                              - JSON output
            -u <owner>                                      - filter by project owner
            -a "name1=value1 name2=value2 ..."              - filter by project attributes

        $ dd show project [options] <project_id>            - show project info (-j show as JSON)
                -a                                          - show project attributes only
                -r                                          - show replicas information
                -j                                          - show as JSON
                -f [active|initial|available|all|reserved|failed|done]   - list files (namespace:name) only
                   all       - all files, including done and failed
                   active    - all except done and failed
                   initial   - in initial state
                   available - available files only
                   reserved  - reserved files only
                   failed    - failed files only
                   done      - done files only

Cancelling project
..................

    .. code-block:: shell
    
        $ dd cancel project [-j] <project id>
        
``-j`` will print the project information in JSON format
    

Workflow
~~~~~~~~

The following commands are used by the worker process. The worker is assumed to know the id of the project it is working on.


Setting worker id
.................

Each worker is identified by a unique worker id.
Data Dispatcher does not use the worker id in any way other than to inform the user which file is reserved by which worker. 
That is why the Data Dispatcher does not maintain the list of worker ids nor does it ensure their uniqueness.
It is the responsibility of the worker to choose a unique worker id, which has some meaning for the user.

The worker can either assign a worker id explicitly, or have the Data Dispatcher client generate a random worker id.
In both cases, the worker id will be stored in CWD/.worker_id file and will be used to identify the worker in the future interactions with the
Data Dispatcher.

    .. code-block:: shell
        
        $ dd worker -n          # - generate random worker id
        9e0124f8
        
        $ dd worker <assigned worker id>
        # example
        $ my_id=`hostname`_`date +%s`
        $ dd worker $my_id
        fnpc123_1645849756
        
        $ dd worker             # - prints current worker id
        fnpc123_1645849756

Getting next file to process
............................

    .. code-block:: shell

       $ dd next [-j] [-t <timeout>] [-c <cpu_site>] <project_id>  - get next available file
             -c - choose the file according to the CPU/RSE proximity map for the CPU site
             -j - as JSON
             -t - wait for next file until "timeout" seconds, 
                  otherwise, wait until the project finishes
                                                                  
In case when no file is available to be processed, but the project has not finished yet (not all files are done or failed permanently),
the "dd next" command will block until a file becomes available for consumption. If "-t" is specified, the "dd next" command will block
for the specified amount of time. Depending on the outcome, the command will:

    * If a file becomes available
    
        * print file info as JSON if "-j" was specified or just file DID (namespace:name) otherwise
        * exit with 0 (success) code
       
    * If the command times out
    
        * print "timeout"
        * exit with code 1
        
    * If the project finishes (all the files are either done or failed permanently)
    
        * print "done"
        * exit with code 1
        
Here is an example of using this command:

    .. code-block:: shell

        #!/bin/bash
        
        ...
        
        out=$(dd next -j $my_project)
        if [ $? -eq 0 ]
        then
             # process the file using $out as the JSON data
        else
            case $out in
                done)
                    # project is done
                    ;;
                timeout)
                    # timed out
                    ;;
            esac
        fi
        
If "-j" option is used, then the JSON output will represent complete information about the file handle, including the list of
available replicas sorted by the RSE preference as well as the file and project attributes defined at the time of the project creation. 
Replicas located in unavailable RSEs will _not_ be included, even if they are known to be staged in the RSE.

    .. code-block:: shell

        $ dd next -j 70
        {
          "attempts": 1,
          "attributes": {
            "core.runs": [
              6534
            ]
          },
          "name": "np04_raw_run006534_0005_dl1_reco_16440189_0_20190217T040518.root",
          "namespace": "np04_reco_keepup",
          "project_attributes": {
            "pi": 3.14,
            "x": "y"
          },
          "project_id": 70,
          "replicas": [
            {
              "available": true,
              "name": "np04_raw_run006535_0087_dl8_reco_16217100_0_20190217T105045.root",
              "namespace": "np04_reco_keepup",
              "path": "/pnfs/fnal.gov/usr/...",
              "preference": 0,
              "rse": "FNAL_DCACHE",
              "rse_available": true,
              "url": "root://fndca1.fnal.gov:1094/pnfs/fnal.gov/usr/..."
            }
          ],
          "state": "reserved",
          "worker_id": "hello_there_123"
        }

        
Releasing the file
..................

If the file was processed successfully, the worker issues "done" command:

    .. code-block:: shell

        $ dd done <project_id> <file namespace>:<file name>
        
If the file processing failes, the worker issues "failed" command. "-f" option is used to signal that the file has failed permanently and should
not be retried. Otherwise, the failed file will be moved to the back of the project's file list and given to a worker for consumption in the future.

    .. code-block:: shell

        $ dd failed [-f] <project_id> <file namespace>:<file name>
            

RSEs
~~~~

Data Dispatcher maintains minimal set of information about known RSEs, including the RSE availability state.

Listing known RSEs
..................

    .. code-block:: shell
    
        $ dd list rses -j
        [
          {
            "add_prefix": "",
            "description": "FNAL dCache",
            "is_available": true,
            "is_tape": true,
            "name": "FNAL_DCACHE",
            "pin_url": null,
            "poll_url": null,
            "preference": 0,
            "remove_prefix": ""
          },
          {
            "add_prefix": "",
            "description": "",
            "is_available": true,
            "is_tape": true,
            "name": "FNAL_DCACHE_TEST",
            "pin_url": null,
            "poll_url": null,
            "preference": 0,
            "remove_prefix": ""
          }
        ]
        
        $ dd list rses
        Name                 Pref Tape Status Description
        --------------------------------------------------------------------------------------------------------------
        FNAL_DCACHE             0 tape     up FNAL dCache
        FNAL_DCACHE_TEST        0 tape     up 
        
Showing information about particular RSE
........................................

    .. code-block:: shell
    
        $ dd show rse FNAL_DCACHE
        RSE:            FNAL_DCACHE
        Preference:     0
        Tape:           yes
        Available:      yes
        Pin URL:        
        Poll URL:       
        Remove prefix:  
        Add prefix:     
        
        $ dd show rse -j FNAL_DCACHE
        {
          "add_prefix": "",
          "description": "FNAL dCache",
          "is_available": true,
          "is_tape": true,
          "name": "FNAL_DCACHE",
          "pin_url": null,
          "poll_url": null,
          "preference": 0,
          "remove_prefix": ""
        }

Changing RSE availability
.........................

This command requires admin privileges.

    .. code-block:: shell

        $ dd set rse -a down FNAL_DCACHE
        $ dd show rse FNAL_DCACHE
        RSE:            FNAL_DCACHE
        Preference:     0
        Tape:           yes
        Available:      no
        ...
        
When an RSE is unavailable (down), replicas in this RSE are considered unavailable even if this is a disk RSE or they are known to be staged in a tape RSE.
