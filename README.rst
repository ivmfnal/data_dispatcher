Data Dispatcher
===============

.. image:: https://readthedocs.org/projects/data-dispatcher/badge/?version=latest
  :target: https://data-dispatcher.readthedocs.io/en/latest/?badge=latest
  :alt: Documentation Status

`Docuemntation <http://data-dispatcher.readthedocs.io/>`_

.. code-block:: shell

    # install Data Dispatcher Client
    $ pip install --user datadispatcher

    # set up the environment
    $ export DATA_DISPATCHER_URL=...
    $ export DATA_DISPATCHER_AUTH_URL=...
    $ export METACAT_SERVER_URL=...
    
.. code-block:: shell

    $ dd

    Data Dispatcher version: 1.2.1

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

    


