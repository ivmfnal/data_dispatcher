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
        
Software versions
~~~~~~~~~~~~~~~~~

To print client and server versions, use the version command:

    .. code-block:: shell

        $ dd version
        Server URL:     http://host.domain:8080/dd/data
        Server version: 1.3.2
        Client version: 1.3.1


Projects
~~~~~~~~

Creating project
................

A Data Dispatcher project is a collection of files to process. There are three ways to provide the list of files to process.
One is to specify a MetaCat query and create the project from the resulting file set:

    .. code-block:: shell
    
        $ dd project create <inline MetaCat query>

        # Examples:
        $ dd project create files from dune:all limit 100
        $ dd project create files from dune:all where 'namespace="protodune-sp"' skip 3000 limit 10

        $ dd project create -q <file with MetaCat MQL query>

A project can be created with explicit list of files, specified as a text file with list of DIDs (namespace:name), one
DID per line:

    .. code-block:: shell

        $ cat > file_list << _EOF_
        protodune-sp:np04_raw_run006833_0001_dl10.root
        protodune-sp:np04_raw_run006833_0001_dl1.root
        protodune-sp:np04_raw_run006833_0001_dl6.root
        _EOF_
        $ dd project create -l file_list


Third way is to yse JSON-formatted file list. The list is composed of items of one of two types:

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
        $ dd project create -j /tmp/file_list.json
        
Hyphen can be used as the value for ``-j`` and ``-l`` options to read the list from stdin:

    .. code-block:: shell

        $ dd project create -l - << _EOF_ 
        protodune-sp:np04_raw_run006833_0001_dl10.root
        protodune-sp:np04_raw_run006833_0001_dl1.root
        protodune-sp:np04_raw_run006833_0001_dl6.root
        _EOF_

Optionally, use ``-t <timeout in seconds>`` to specify the worker timeout. If a worker allocates a file and does not release it
for longer than the specified time interval, Data Dispatcher will automatically release the file and make it available for
another worker to allocate. Make sure the specified interval is long enough to avoid processing of the same file by multiple
workers.

The "dd project create" command prints information about the created project in 3 different formats, depending on 
the value of the ``-p`` option:

    .. code-block:: shell

        $ dd project create -p id ...                   # -p id is default
        123                                             # print the project id only
        
        $ dd project create -p json ... # print project information as JSON
        {
            "project_id": 123,
            "file_handles": [
                ...
            ]
            ...
        }
        
        $ dd project create -p pprint ... # print project information using Python pprint
        {
         'project_id': 123,
         'file_handles': [
            ...
         ]
         ...
        }

Project time-outs
.................

There are 2 parameters which control the behavior of the Data Dispatcher with respect to idle projects: *worker timeout* and *project idle timeout*.

Worker timeout parameter tells the Data Dispatcher that if a worker reserves a file and does not release it for too long, the Data Dispatcher will 
assume that the worker which holds the file has died witout releasing the file and the Data Dispatcher will
release it and make available for another (or the same) worker to allocate. This parameter can be set using ``-w`` option when creating the project:

    .. code-block:: shell
    
        $ dd project create -w (<worker timeout>[s|m|h|d] | none)
        
the timeout value is numeric with optional suffix ``s``, ``m``, ``h`` or ``d``. If a suffix is used, then the timeout is set to the specified
number of seconds, minutes, hours or days respectively. ``none`` can be used to create a project without any worker timeout. Default worker timeout
is 12 hours.

Project idle timeout applies in the case, when a there is no worker file reserve/release activity for the project for the specified time interval.
In this case, the Data Dispatcher moves the project into ``abandoned`` state. In this state, the Data Dispatcher stops updaitng file replica
availability information for the project. Use ``-t`` option to specify this timeout:

    .. code-block:: shell
    
        $ dd project create -t (<idle timeout>[s|m|h|d] | none)

Default value for the project idle timeout is 72 hours.

To reactivate an abandoned project, use ``activate`` subcommand:

    .. code-block:: shell
    
        $ dd project activate <project id>

An abandoned project will also be re-activated if a worker releases or tries to reserve a file.

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
        $ dd project create -A "email_errors=user@fnal.gov step=postprocess" ...
        
        # as a JSON file:
        $ cat project_attrs.json
        {
            "email_errors": "user@fnal.gov",
            "step": "postprocess"
        }
        $ dd project create -A @project_attrs.json
        
To copy some metadata attributes from MetaCat:

    .. code-block:: shell

        $ dd project create -c core.runs files from ...
        $ dd project create -c detector.hv_value,core.data_tier files from ...

To associate common attributes with each file in the project, use ``-a`` option:

    .. code-block:: shell

        $ dd project create -a "name1=value1 name2=value2" ...
        $ dd project create -a @<JSON file>

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
        $ dd project create -j /tmp/file_list.json
        
When the worker gets next file to process, the JSON representation of file inofrmation includes project and project file attributes:

    .. code-block:: shell

        $ dd worker next -j 70
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

        
Listing projects
................

    .. code-block:: shell

        $ dd project list
            -j                                              - JSON output
            -u <owner>                                      - filter by project owner
            -a "name1=value1 name2=value2 ..."              - filter by project attributes

Viewing projects
................

    .. code-block:: shell

        $ dd project show [options] <project_id>            - show project info (-j show as JSON)
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

Searching projects
..................

    .. code-block:: shell

        $ dd project search [oprions] -q -              - read search query from stdin
        $ dd project search [oprions] -q <file path>    - read search query from a file
        $ dd project search [oprions] <search query>    - inline search query
        
        Options:
            -j                                          - JSON output
            -u <owner>                                  - filter by owner
            -s (<state>|all)                            - filter by state, default: active projects only

See :ref:`Searching Projects <SearchQL>` for details on search query language

Copying project
...............

    .. code-block:: shell

        dd project copy [options] <project id>               -- copy project
  
          -A @<file.json>                                 - JSON file with project attributes to override
          -A "name=value name=value ..."                  - project attributes to override
          -a @<file.json>                                 - JSON file with file attributes to override
          -a "name=value name=value ..."                  - file attributes to override
  
          -p (json|pprint|id)                             - print created project info as JSON, 
                                                            pprint or just project id (default)

This command allows the user to create a new project with the same files as an existing one. The project and file attributes
can be copied to the new project or overriden.

Restarting files in a project
.............................

There are 2 ways to reset some files of the project to the initial state, making them available for re-processing within the same
project.

    .. code-block:: shell

        $ dd project restart <project_id> <did> ...
        
This command will reset the state of the files specified with their DIDs regardless of their current state.

    .. code-block:: shell
    
        $ dd restart [options] <project_id>
              -f                                              - restart failed files
              -d                                              - restart done files
              -r                                              - unreserve reserved files
              -a                                              - restart all files
  
This command will reset all the files in the project in given set or states. Options ``-f``, ``-r``, ``-d`` can be combined. 
``-a`` will reset all files in the project.


Cancelling project
..................

    .. code-block:: shell
    
        $ dd project cancel [-j] <project id>
        
``-j`` will print the project information in JSON format
    
Deleting project
................

    .. code-block:: shell
    
        $ dd project delete <project id>


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
        
        $ dd worker id -n          # - generate random worker id
        9e0124f8
        
        $ dd worker id <assigned worker id>
        # example
        $ my_id=`hostname`_`date +%s`
        $ dd worker id $my_id
        fnpc123_1645849756
        
        $ dd worker id            # - prints current worker id
        fnpc123_1645849756

Getting next file to process
............................

    .. code-block:: shell

       $ dd worker next [-j] [-t <timeout>] [-c <cpu_site>] [-w <worker_id>] <project_id>  - get next available file
             -c - choose the file according to the CPU/RSE proximity map for the CPU site
             -j - as JSON
             -t - wait for next file until "timeout" seconds, 
                  otherwise, wait until the project finishes
             -w - override worker id
                                                                  
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
        
        out=$(dd worker next -j $my_project)
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

        $ dd worker next -j 70
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

        $ dd worker done <project_id> <file namespace>:<file name>
        
If the file processing failes, the worker issues "failed" command. "-f" option is used to signal that the file has failed permanently and should
not be retried. Otherwise, the failed file will be moved to the back of the project's file list and given to a worker for consumption in the future.

    .. code-block:: shell

        $ dd worker failed [-f] <project_id> <file namespace>:<file name>
            

RSEs
~~~~

Data Dispatcher maintains minimal set of information about known RSEs, including the RSE availability state.

Listing known RSEs
..................

    .. code-block:: shell
    
        $ dd rse list -j
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
        
        $ dd rse list
        Name                                     Pref Tape Status Description
        --------------------------------------------------------------------------------------------------------------
        FNAL_DCACHE                                 0 tape     up FNAL dCache
        FNAL_DCACHE_PERSISTENT                      0 tape     up 
        FNAL_DCACHE_STAGING                         0 tape     up 
        FNAL_DCACHE_TEST                            0 tape     up 
        LANCASTER                                   0          up 
        TEST_RSE                                    0          up Test RSE
        --------------------------------------------------------------------------------------------------------------
        
        
Showing information about particular RSE
........................................

    .. code-block:: shell
    
        $ dd rse show FNAL_DCACHE
        RSE:            FNAL_DCACHE
        Preference:     0
        Tape:           yes
        Available:      yes
        Pin URL:        
        Poll URL:       
        Remove prefix:  
        Add prefix:     
        
        $ dd rse show -j FNAL_DCACHE
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

        $ dd rse set -a down FNAL_DCACHE
        $ dd rse show FNAL_DCACHE
        RSE:            FNAL_DCACHE
        Preference:     0
        Tape:           yes
        Available:      no
        ...
        
When an RSE is unavailable (down), replicas in this RSE are considered unavailable even if this is a disk RSE or they are known to be staged in a tape RSE.
