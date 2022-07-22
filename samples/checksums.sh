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

done="false"
while [ $done == "false" ]; do
	dd worker next -j $project_id > info.json
        sts=$?
        #echo "dd worker next status:" $sts
        if [ "$sts" != "0" ]; then
		done="true"
                cat info.json
        else
		url=`python ui/json_extract.py info.json replicas/0/url`
		namespace=`python ui/json_extract.py info.json namespace`
		name=`python ui/json_extract.py info.json name`
		type=`python ui/json_extract.py info.json project_attributes/checksum_type`
                run_number=`python ui/json_extract.py info.json attributes/core.runs/0`
		did=${namespace}:${name}

                tmpfile=/tmp/tmpdata_$$

		echo downloading $url
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
		checksum=`python checksum.py $type $tmpfile`
                rm -f $tmpfile
		echo $name : $type=$checksum $run=$run_number
                echo
		dd worker done $project_id $did
	fi
done
	


