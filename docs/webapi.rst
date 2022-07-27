Python API
==========

Python API is recommended way for a client side Python appications to communicate with the Data Dispatcher server.
To use the API, you need to install Data Dispatcher client module:

.. code-block:: shell
    
        $ pip install --user datadispatcher

Then import the API module and create a ``DataDispatcherClient`` object:

.. code-block:: python

    from data_dispatcher.api import DataDispatcherClient
    client = DataDispatcherClient("http://server.host.domain:8080/dd/data")

.. autoclass:: data_dispatcher.api.DataDispatcherClient
   :members:
   :noindex:
   :inherited-members: