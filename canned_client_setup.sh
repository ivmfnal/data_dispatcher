# edit and soutrce this file

export DATA_DISPATCHER_URL=...
export METACAT_SERVER_URL=...
export DATA_DISPATCHER_AUTH_URL=...

CLIENT_ROOT=...         # where you untared the metacat_client_...tar

# to use dependencies installed with pip or otherwise
export PYTHONPATH=${CLIENT_ROOT}

# to use canned dependencies:
# export PYTHONPATH=${CLIENT_ROOT}/lib:${METACAT_CLIENT_ROOT}/dependencies


export PATH=${CLIENT_ROOT}/bin