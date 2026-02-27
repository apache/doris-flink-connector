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
# This script is deploy stage jars to repository.apache.org
##############################################################

MVN=${MVN:-mvn}
CUSTOM_OPTIONS=${CUSTOM_OPTIONS:-}

# fail immediately
set -o errexit
set -o nounset

CURR_DIR=`pwd`
BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
PROJECT_ROOT="${BASE_DIR}/../../"

# Sanity check to ensure that resolved paths are valid; a LICENSE file should always exist in project root
if [ ! -f ${PROJECT_ROOT}/LICENSE.txt ]; then
    echo "Project root path ${PROJECT_ROOT} is not valid; script may be in the wrong directory."
    exit 1
fi

###########################

# Sanity check to ensure that current JDK is version 17
JAVA_VERSION=$(java -version 2>&1 | awk -F'"' '/version/ {print $2}')
if [[ "${JAVA_VERSION}" != 17* ]]; then
    echo "Error: This script requires JDK 17, but found version '${JAVA_VERSION}'."
    echo "Please switch to JDK 17 before running this script."
    exit 1
fi
echo "JDK version check passed: ${JAVA_VERSION}"

cd ${PROJECT_ROOT}/flink-doris-connector

echo "Deploying to repository.apache.org for Flink 2.x"

echo "Deploying Flink 2.0..."
${MVN} clean deploy -Papache-release -DretryFailedDeploymentCount=10 -pl flink-doris-connector-flink2 -am -Pflink2 -Dflink.version=2.0.0 -Dflink.major.version=2.0 -DskipTests=true

echo "Deploying Flink 2.1..."
${MVN} clean deploy -Papache-release -DretryFailedDeploymentCount=10 -pl flink-doris-connector-flink2 -am -Pflink2 -Dflink.version=2.1.0 -Dflink.major.version=2.1 -DskipTests=true

echo "Deploying Flink 2.2..."
${MVN} clean deploy -Papache-release -DretryFailedDeploymentCount=10 -pl flink-doris-connector-flink2 -am -Pflink2 -Dflink.version=2.2.0 -Dflink.major.version=2.2 -DskipTests=true

echo "Deploy jar with jdk17 finished."
cd ${CURR_DIR}