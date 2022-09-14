#!/bin/bash

# Usage: worker.sh <nfiles> <project_id>

if [ $1 == "" ]; then
        echo Usage: run_project.sh \<nfiles> \<project_id\>
        exit 2
fi

nfiles=$1
project_id=$2

cert=${HOME}/certs/ivm@fnal.gov_cert.pem
key=${HOME}/certs/ivm@fnal.gov_key.pem

my_id=`dd worker id dc4_$$`
echo My worker id: $my_id
info_file=/tmp/${my_id}.json
tmpfile=/tmp/${my_id}.data

i=0
n_failures=0
max_failures=5

until [ $i -gt $nfiles ]
do
	    dd worker next -w $my_id -j $project_id > $info_file
        if [ "$?" != "0" ]; then
            # likely the project is done
            done="true"
            cat $info_file
            rm -f $info_file
        else
            url=`python json_extract.py $info_file replicas/0/url`
            namespace=`python json_extract.py $info_file namespace`
            name=`python json_extract.py $info_file name`
            did=${namespace}:${name}

            echo
            echo ------ $did ...

            # download the replica using the URL from the DD
            case $url in
                root\:*|xroot:*)
                    xrdcp --force $url $tmpfile
                    status=$?
                    ;;
                http\:*)
                    curl -L -o $tmpfile "$url"
                    status=$?
                    ;;
                https\:*)
                    curl -L -k --cert $cert --key $key -o $tmpfile "$url"
                    status=$?
                    ;;
                *)
                    echo Unknown URL schema: $url
                    exit 1
                    ;;
            esac
            
            if [ $status != 0 ]; do
                dd worker failed $project_id $did
                ((n_failures=n_failures+1))
                if[ $n_failures -ge $max_failures ]; do
                    break
                fi
            else
                ((i=i+1))
                n_failures=0
            fi
            rm -f $tmpfile $info_file
            dd worker done $project_id $did
            echo
    	fi
done
