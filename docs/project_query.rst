.. _SearchQL:

Searching Projects
==================

Data Dispatcher provides a way to search projetcs by their attributes and metadata.
Each project has the following attributes:

    * owner - string - username of the user who created the project
    * state - string - current state of the project. Can be ``active``, ``abandoned``, ``cancelled``, ``done``, ``failed``
    * created - datetime - date/time when the project was created
    * id - integer - project id
    * query - string - MetaCat query used to select files for the project

In addition, a project may have an arbitrary disctiopnary with user-defined project metadata. For example:

   .. code-block::
   
       {
           "reco-version": "1.23.7",
           "mode": "debug",
           "events_per_file": 1024,
           "MaxE": 3.14
       }

The user can query Data Dispatcher for projects writing the query in a subset of MQL. Project query is a logical expression,
built from operations on project attributes and metadata fields. If an attribute name appears in a query, it is assumed to
be referring to the project attribute, not the project metadata.

The project creation datetime attribute `created` is a date-time field. To etner its value in the query, use string in the format:

    .. code-block::
    
        YYYY-MM-DD[ HH:MM:SS[+/-HH:MM]]


The following operations are supported:

    * <field> present                                           -- checks if the field is present in the project metadata
    * <field> (< | = | > | >= | <= | !=) <value>                -- comparison
    * <field> ~ "<regular expression>"                          -- string matching
    * <field> in ( <constant>,<constant>,... )                  -- enumerated set of values 
    * <field> in <constant>:<constant>                          -- range of values
    * <constant> in <field>                                     -- if the <field> is an array, checks if the <constant> is in the array
    * "<constant>" in <field>                                   -- if the <field> is a string, checks if the "<constant>" a substring
    
The query language also supports ``not`` variations of some operations:

    * <field> not in ( <constant>,<constant>,... )
    * <field> not in <constant>:<constant>
    * <constant> not in <field>

For array metadata fields, the language supports indexing:

    * <field>[<index>] (< | = | > | >= | <= | !=) <value>              
    * <field>[<index>] ~ "<regular expression>"                     
    * <field>[<index>] in ( <constant>,<constant>,... )             
    * <field>[<index>] in <constant>:<constant>                   
    * <constant> in <field>[<index>]                              
    * "<constant>" in <field>[<index>]                            
    
Here, ``index`` can be one of:
    
    * integer - for arrays
    * string - for dictionaries
    * ``any``
    * ``all``
    
Keywords ``any`` and ``all`` can be used with dictionaries and arrays to apply the operation to any or all collection members. For example:

    .. code-block::

        events[any] = 32768
        events[all] in (32174, 32128)
        bits[any] = "on"
    
Atomic operations can be combined into more complex expressions using Boolean algebra:

    .. code-block::
    
        state=active and ! (version >= "1.23" and bits[any] = "on")

"Safe" string constants (string, which consist only of letters, digits and ``%$@_^.%*?-`` do not need to be enclosed in quotes.
If an unquoted string can be interpreted as an integer or a floating point number, it will be converted to the number.
If you need such a string to remain a string, it needs to be enclosed in quotes.

Unquoted strings ``null``, ``true`` and ``false`` are treated as null and boolean constants.
