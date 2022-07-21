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
echo My id: $my_id

cat << _EOF_ > /tmp/checksum.py
import zlib, sys
a = zlib.adler32(b"")
data=sys.stdin.buffer.read(10000)
while data:
    a = zlib.adler32(data, a)
    data=sys.stdin.buffer.read(10000)
print("%x" % (a & 0xffffffff,))
_EOF_

done="false"
while [ $done == "false" ]; do
	dd worker next -j $project_id > info.json
        sts=$?
        #echo "dd worker next status:" $sts
        if [ "$sts" != "0" ]; then
		done="true"
                cat info.json
        else
		url=`python ui/json_extract.py info.json replicas.0.url`
		namespace=`python ui/json_extract.py info.json namespace`
		name=`python ui/json_extract.py info.json name`
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
                ls -l $tmpfile
		checksum=`python /tmp/checksum.py < $tmpfile`
                rm -f $tmpfile
		echo $name : Adler32=$checksum
                echo
		dd worker done $project_id $did
	fi
done
	


