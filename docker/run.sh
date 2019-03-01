#!/bin/bash

# * Copyright (c) 2009-2019. Authors: see NOTICE file.
# *
# * Licensed under the Apache License, Version 2.0 (the "License");
# * you may not use this file except in compliance with the License.
# * You may obtain a copy of the License at
# *
# *      http://www.apache.org/licenses/LICENSE-2.0
# *
# * Unless required by applicable law or agreed to in writing, software
# * distributed under the License is distributed on an "AS IS" BASIS,
# * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# * See the License for the specific language governing permissions and
# * limitations under the License.


bash /tmp/addHosts.sh

if [[ $# -ne 2 ]]; then
    echo "Not enough parameters to run project migrator. Exiting"
    exit
fi

if [[ $1 == "import" ]]; then
    if [[ ${2:(-4):4} == ".txt" ]]; then
        IFS=$'\n' read -d '' -r -a lines < $2
    else
        lines=($2)
    fi

    for i in "${lines[@]}"; do
        echo $i
        python /app/cytomineprojectmigrator/importer.py \
        --host $CORE_URL \
        --host_upload $UPLOAD_URL \
        --public_key $PUBLIC_KEY \
        --private_key $PRIVATE_KEY \
        --project_path $i
    done
    echo "Finished."
elif [[ $1 == "export" ]]; then
    echo $2
    echo "Not yet implemented."
fi