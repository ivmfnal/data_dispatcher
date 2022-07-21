#!/bin/bash

# Usage: create_project.sh <MQL query>

if [ $1 == "" ]; then
        echo Usage: create_project.sh <MQL query>
        exit 2
fi

project_id=`dd project create $@`
echo Project created: $project_id
