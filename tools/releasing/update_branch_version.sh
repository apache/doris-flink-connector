#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

##############################################################
# This script is update branch version in pom.xml
##############################################################

RELEASE_VERSION=${RELEASE_VERSION}

if [ -z "${RELEASE_VERSION}" ]; then
    echo "RELEASE_VERSION was not set."
    exit 1
fi

# fail immediately
set -o errexit
set -o nounset

CURR_DIR=`pwd`
BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
PROJECT_ROOT="${BASE_DIR}/../../"

# Sanity check to ensure that resolved paths are valid; a LICENSE file should aways exist in project root
if [ ! -f ${PROJECT_ROOT}/LICENSE.txt ]; then
    echo "Project root path ${PROJECT_ROOT} is not valid; script may be in the wrong directory."
    exit 1
fi

cd ${PROJECT_ROOT}/flink-doris-connector
mvn versions:set -DgenerateBackupPoms=false -DnewVersion=${RELEASE_VERSION}
mvn versions:set-property -DgenerateBackupPoms=false -Dproperty=revision -DnewVersion=${RELEASE_VERSION}

git commit -am "[release] Update version to ${RELEASE_VERSION}"

RELEASE_VERSION_COMMIT_HASH=`git rev-parse HEAD`

echo "Done. Created a new commit for the new version ${RELEASE_VERSION}, with hash ${RELEASE_VERSION_COMMIT_HASH}"
echo "If this is a new version to be released (or a candidate to be voted on), don't forget to create a signed release tag on GitHub and push the changes."

cd ${CURR_DIR}