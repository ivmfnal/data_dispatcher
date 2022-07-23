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
		type=`python json_extract.py $info_file project_attributes/checksum_type`
                run_number=`python json_extract.py $info_file attributes/core.runs/0`
		did=${namespace}:${name}


		echo downloading $namespace:$name ...
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
                ls -l $tmpfile
		checksum=`python samples/checksum.py $type $tmpfile`
                rm -f $tmpfile
		echo $name:  $type=$checksum run=$run_number
		dd worker done $project_id $did
	fi
done
	


