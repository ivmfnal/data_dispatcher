#!/bin/bash

# Usage: create_project.sh <checksum type> <MQL query>

if [ $1 == "" ]; then
        echo Usage: create_project.sh \<checksum type\> \<MQL query\>
        exit 2
fi

checksum_type=$1
shift

project_id=`dd project create -A checksum_type=$checksum_type -c core.runs $@`
echo Project created: $project_id
