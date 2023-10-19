File Processing with Data Dispatcher
====================================

Basic file processing workflow looks like this:

  1. Attempt to reserve a file using ``next file`` finction

    * If reserved:

      #. Process the file
      #. Release the file

        * if processed successfully, use ``file done`` function
        * if processing failed, use ``file failed`` function

    * If project is done, stop
    * If timed out, go back to (1)

Using Python API
----------------

.. code-block:: python

  # The worker is assumed to know its project id and CPU site name
  project_id = ...
  cpu_site = ...
  
  # create DD client object
  from data_dispatcher import DataDispatcherClient
  client = DataDispatcherClient(server_url)
  
  #
  # Reserve a file to process
  #
  
  project_done = False
  reserved_file = None
  while not project_done and not reserved_file:
      # reserve the next file to process
      result = client.next_file(project_id, cpu_site=cpu_site, timeout=timeout)
      if isinstance(result, dict):
          # file reserved
          reserved_file = result
      elif result:
          # reservation timed out, but the project is not over yet
          ...
      else:
          project_done = True
          
  if reserved_file is not None:
      #
      # A file was successfully reserved
      #
      did = reserved_file["namespace"] + ':' + reserved_file["name"]
      replica = reserved_file["replicas"][0]         # get the closest replica
      url = reserved_file["url"]
  
      # process the file ...
  
      #
      # Release the file
      #

      if processing_successful:
          client.file_done(project_id, did)
      else:
          # proessing failed
          if retry_later:
              client.file_failed(project_id, did, True)
          else:
              client.file_failed(project_id, did, False)
      
Using Data Dispatcher CLI
-------------------------
.. code-block:: shell

    #!/bin/bash
    
    my_project=...
    my_cpu_site=...
    
    #
    # Attempt to reserve a file
    #
    out=$(ddisp worker next -j file_info.json -c $my_cpu_site $my_project)
    if [ $? -eq 0 ]
    then
         did=$out           # reserved file DID (namespace:name)

         #
         # Process the file using the contents of file_info.json
         #
         if [ $processed_successfully ]; then
             ddisp worker done $my_project $did
         else
             if [ $retry ]; then
                 ddisp worker failed $my_project $did
             else
                 ddisp worker failed -f $my_project $did
             fi
         fi
    else
        case $out in
            done)
                # project is done
                ;;
            timeout)
                # timed out, can try to reserve again
                ;;
        esac
    fi

      