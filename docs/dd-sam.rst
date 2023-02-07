DD-SAM
======

``dd-sam`` is a collecton command line tools implementing portions of SAM functionality, which require communication with more than one of
the 3 components - Data Dispatcher, MetaCat and Rucio. Strictly speaking, Data Dispatcher interface already communicates with MetaCat and
Data Dispatcher server when it is creating a project from the result of a MetaCat query.

DD-SAM configuration
--------------------

Becaue ``dd-sam`` uses Rucio, MetaCat and Data Dispatcher clients, all 3 of them need to be configured properly. The following
environment variables can be used for that:

     * METACAT_SERVER_URL - MetaCat server URL
     * DATA_DISPATCHER_URL - DataDisatcher server URL
     * RUCIO_CONFIG - file with Rucio client configuration

Replica locations
-----------------

``dd-sam locations`` command can be used to get real-time information about file replica locations.

Getting locations for files selected by a MetaCat query:

    .. code-block:: shell

        $ dd-sam locations [options] -q|--query <inline MQL query>
        $ dd-sam locations [options] -q|--query @<file with MQL query>
        
This command will run the MetaCat query and get real time file replica locations from Rucio.

Getting locations for project files:

    .. code-block:: shell

        $ dd-sam locations [options] <DD project id>

Getting locations for specific files:

    .. code-block:: shell

        $ dd-sam locations [options] <file DID> ...

Common options for the command:

    .. code-block:: shell

            -s|--schemes <schema>[,...]     - list of URL schemes. Default: all
            -r|--rses <rse>[,...]           - list of RSEs. Default: all
            -j|--json                       - print results as JSON
            -p|--pprint                     - print results as pprint
