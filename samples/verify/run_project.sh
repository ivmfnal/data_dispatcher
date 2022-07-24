#!/bin/bash

# Usage: checksums.sh <project_id>

if [ $1 == "" ]; then
        echo Usage: checksums.sh \<project_id\>
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
        #cat $info_file
        sts=$?
        #echo "dd worker next status:" $sts
        if [ "$sts" != "0" ]; then
            done="true"
            cat $info_file
            rm -f $info_file
        else
            url=`python json_extract.py $info_file replicas/0/url`
            namespace=`python json_extract.py $info_file namespace`
            name=`python json_extract.py $info_file name`
            checksum_type=`python json_extract.py $info_file project_attributes/checksum_type`

            # size and checksum from MetaCat
            meta_checksum=`python json_extract.py $info_file attributes/checksums/$checksum_type`
            meta_size=`python json_extract.py $info_file attributes/size`

            did=${namespace}:${name}

            echo
            echo ------ $did
            echo downloading...
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
            checksum=`python checksum.py $checksum_type $tmpfile`
            size=`stat -c %s $tmpfile`
            ok=ok
            if [ "$size" != $meta_size ]; then
                echo File size mismatch for $did: metadata: $meta_size, downloaded: $size
                ok=""
            fi
            if [ "$checksum" != $meta_checksum ]; then
                echo Checksum mismatch for $did: metadata: $meta_checksum, downloaded: $checksum
                ok=""
            fi
            if [ "$ok" == "ok" ]; then
                echo $did:  OK: size=$size $checksum_type=$checksum
            fi
            rm -f $tmpfile
            dd worker done $project_id $did
            echo
    	fi
done
	


