Data Dispatcher Client Installation and Configuration
=====================================================

Installation
------------

To install the Data Dispatcher client, use pip command:

    .. code-block:: shell
    
        $ pip install --user datadispatcher

Alternatively:

    .. code-block:: shell
    
        $ git clone https://github.com/ivmfnal/metacat.git
        $ cd metacat
        $ python setup.py install --user
        $ cd ..
        $ git clone https://github.com/ivmfnal/data_dispatcher.git
        $ cd data_dispatcher
        $ python setup.py install --user

If using ``--user``, make sure your ``~/.local/bin`` is in your PATH 


Configuration
-------------

To configure the client, you need to define the following environment variables:

    * DATA_DISPATCHER_URL - URL for Data Dispatcher data server
    * DATA_DISPATCHER_AUTH_URL - URL for Data Dispatcher authentication server
    * METACAT_SERVER_URL - URL for MetaCat data srever

For DUNE, the values are:

    * DATA_DISPATCHER_URL = https://metacat.fnal.gov:9443/dune/dd/data
    * DATA_DISPATCHER_AUTH_URL - https://metacat.fnal.gov:8143/auth/dune
    * METACAT_SERVER_URL - https://metacat.fnal.gov:9443/dune_meta_demo/app
    

