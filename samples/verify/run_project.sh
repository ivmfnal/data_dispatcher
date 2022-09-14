#!/bin/bash

# Usage: run_project.sh <project_id>

if [ "$1" == "" ]; then
        echo Usage: run_project.sh \<project_id\>
        exit 2
fi

project_id=$1

cert=${HOME}/certs/ivm@fnal.gov_cert.pem
key=${HOME}/certs/ivm@fnal.gov_key.pem

my_id=`dd worker id checksums_$$`
echo My worker id: $my_id
info_file=/tmp/${my_id}.json
tmpfile=/tmp/${my_id}.data

done="false"
while [ $done == "false" ]; do
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
            
            # checksum type from project attributes
            checksum_type=`python json_extract.py \
                $info_file project_attributes/checksum_type`

            # size and checksum from MetaCat via file attributes
            meta_checksum=`python json_extract.py \
                $info_file attributes/checksums/$checksum_type`
            meta_size=`python json_extract.py $info_file attributes/size`

            echo
            echo ------ $did ...

            # download the replica using the URL from the DD
            case $url in
                root\:*|xroot:*)
                    xrdcp --force $url $tmpfile
                    ;;
                http\:*)
                    curl -L -o $tmpfile "$url"
                    ;;
                https\:*)
                    curl -L -k --cert $cert --key $key -o $tmpfile "$url"
                    ;;
                *)
                    echo Unknown URL schema: $url
                    exit 1
                    ;;
            esac
            
            # calculate the checksum and stat the file size
            checksum=`python checksum.py $checksum_type $tmpfile`
            size=`stat -c %s $tmpfile`
            
            # compare and print results
            ok=ok
            if [ "$size" != $meta_size ]; then
                echo File size mismatch for $did
                echo "  metadata:  " $meta_size
                echo "  downloaded:" $size
                ok=""
            fi
            if [ "$checksum" != $meta_checksum ]; then
                echo Checksum mismatch for $did
                echo "  metadata:  " $meta_checksum
                echo "  downloaded:" $checksum
                ok=""
            fi
            if [ "$ok" == "ok" ]; then
                echo OK: $did 
                echo "  size:    " $size 
                echo "  checksum:" $checksum "($checksum_type)" 
            fi
            rm -f $tmpfile $info_file
            dd worker done $project_id $did
            echo
    	fi
done
