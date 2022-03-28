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

Alternatively, a project can be created with explicit list of files, specified either as a list of their DIDs (namespace:name), or
using JSON expression of the file list:

    .. code-block:: shell

        $ cat > file_list << _EOF_
        protodune-sp:np04_raw_run006833_0001_dl10.root
        protodune-sp:np04_raw_run006833_0001_dl1.root
        protodune-sp:np04_raw_run006833_0001_dl6.root
        _EOF_
        $ dd create project -l file_list

        $ cat /tmp/file_list.json 
        [
            { "namespace":"protodune-sp", "name":"np04_raw_run006834_0009_dl2.root" },
            { "namespace":"protodune-sp", "name":"np04_raw_run006834_0009_dl6.root" },
            { "namespace":"protodune-sp", "name":"np04_raw_run006834_0010_dl10.root" }
        ]
        $ dd create project -j /tmp/file_list.json

Project and file attributes
...........................

Optionally, the project and/or each file of the project can carry some metadata, which can be retrieved by the worker when the project runs.

There are several ways to specify project level metadata attributes:

    .. code-block:: shell

        # inline:
        $ dd create project -A "name1=value1 name2=value2" ...
        
        # as a JSON file:
        $ dd create project -A @<JSON file>
        
File attributes can be copied from MetaCat and/or set when the project is created. To copy some metadata attributes from MetaCat:

    .. code-block:: shell

        $ dd create project -c core.runs files from ...
        $ dd create project -c detector.hv_value,core.data_tier files from ...

Also, common file attributes can be added using "-a" option:

    .. code-block:: shell

        $ dd create project -a "name1=value name1=value" ...
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

Viewing projects
................

    .. code-block:: shell

        $ dd list projects
            -j                                              - JSON output
            -u <owner>                                      - filter by project owner
            -a "name=value name=value ..."                  - filter by project attributes

        $ dd show project [options] <project_id>            - show project info (-j show as JSON)
                -a                                          - show project attributes only
                -r                                          - show replicas information
                -j                                          - show as JSON
                -f [active|ready|available|all|reserved|failed|done]    - list files (namespace:name) only
                                                               all       - all files, including done and failed
                                                               active    - all except done and failed
                                                               ready     - ready files only
                                                               available - available files only
                                                               reserved  - reserved files only
                                                               failed    - failed files only
                                                               done      - done files only

Workflow
~~~~~~~~

The following commands are used by the worker process. The worker is assumed to know the id of the project it is working on.


Setting worker id
.................

Each worker is identified by a unique worker id. Worker id can be either generated randomly by the Data Dispatcher UI command or assigned by the client.
In case when the worker id is assigned by the client, it is the client responsibility to make sure the worker id is unique.
In both cases, the worker id will be stored in CWD/.worker_id file and will be used to identify the worker in the future interactions with the
Data Dispatcher.

    .. code-block:: shell
        
        $ dd worker -n          # - generate random worker id
        
        $ dd worker <assigned worker id>
        # example
        $ my_id=`hostname`_`date +%s`
        $ dd worker $my_id
        
        $ dd worker             # - prints current worker id

Getting next file to process
............................

    .. code-block:: shell

        $ dd next [-j] [-t <timeout>] <project_id>           - reserve next available file 
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
    
        * produce empty output
        * exit with code 11
        
    * If the project finishes (all the files are either done or failed permanently)
    
        * produce empty output
        * exit with code 10
        
Releasing the file
..................

If the file was processed successfully, the worker issues "done" command:

    .. code-block:: shell

        $ dd done <project_id> <file namespace>:<file name>
        
If the file processing failes, the worker issues "failed" command. "-f" option is used to signal that the file has failed permanently and should
not be retried. Otherwise, the failed file will be moved to the back of the project's file list and given to a worker for consumption in the future.

    .. code-block:: shell

        $ dd failed [-f] <project_id> <file namespace>:<file name>
            


