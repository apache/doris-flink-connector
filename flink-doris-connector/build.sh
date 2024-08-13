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
# This script is used to compile Flink-Doris-Connector
# Usage:
#    sh build.sh
#
##############################################################

# Bugzilla 37848: When no TTY is available, don't output to console
have_tty=0
# shellcheck disable=SC2006
if [[ "`tty`" != "not a tty" ]]; then
    have_tty=1
fi

 # Only use colors if connected to a terminal
if [[ ${have_tty} -eq 1 ]]; then
  PRIMARY=$(printf '\033[38;5;082m')
  RED=$(printf '\033[31m')
  GREEN=$(printf '\033[32m')
  YELLOW=$(printf '\033[33m')
  BLUE=$(printf '\033[34m')
  BOLD=$(printf '\033[1m')
  RESET=$(printf '\033[0m')
else
  PRIMARY=""
  RED=""
  GREEN=""
  YELLOW=""
  BLUE=""
  BOLD=""
  RESET=""
fi

echo_r () {
    # Color red: Error, Failed
    [[ $# -ne 1 ]] && return 1
    # shellcheck disable=SC2059
    printf "[%sDoris%s] %s$1%s\n"  $BLUE $RESET $RED $RESET
}

echo_g () {
    # Color green: Success
    [[ $# -ne 1 ]] && return 1
    # shellcheck disable=SC2059
    printf "[%sDoris%s] %s$1%s\n"  $BLUE $RESET $GREEN $RESET
}

echo_y () {
    # Color yellow: Warning
    [[ $# -ne 1 ]] && return 1
    # shellcheck disable=SC2059
    printf "[%sDoris%s] %s$1%s\n"  $BLUE $RESET $YELLOW $RESET
}

echo_w () {
    # Color yellow: White
    [[ $# -ne 1 ]] && return 1
    # shellcheck disable=SC2059
    printf "[%sDoris%s] %s$1%s\n"  $BLUE $RESET $WHITE $RESET
}

# OS specific support.  $var _must_ be set to either true or false.
cygwin=false
os400=false
# shellcheck disable=SC2006
case "`uname`" in
CYGWIN*) cygwin=true;;
OS400*) os400=true;;
esac

# resolve links - $0 may be a softlink
PRG="$0"

while [[ -h "$PRG" ]]; do
  # shellcheck disable=SC2006
  ls=`ls -ld "$PRG"`
  # shellcheck disable=SC2006
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '/.*' > /dev/null; then
    PRG="$link"
  else
    # shellcheck disable=SC2006
    PRG=`dirname "$PRG"`/"$link"
  fi
done

# Get standard environment variables
# shellcheck disable=SC2006
ROOT=$(cd "$(dirname "$PRG")" &>/dev/null && pwd)
export DORIS_HOME=$(cd "$ROOT/../" &>/dev/null && pwd)

. "${DORIS_HOME}"/env.sh

# include custom environment variables
if [[ -f ${DORIS_HOME}/custom_env.sh ]]; then
    . "${DORIS_HOME}"/custom_env.sh
fi

selectFlink() {
  echo 'Flink-Doris-Connector supports multiple versions of flink. Which version do you need ?'
  select flink in "1.15.x" "1.16.x" "1.17.x" "1.18.x" "1.19.x" "1.20.x"
  do
    case $flink in
      "1.15.x")
        return 1
        ;;
      "1.16.x")
        return 2
        ;;
      "1.17.x")
        return 3
        ;;
      "1.18.x")
        return 4
        ;;
      "1.19.x")
        return 5
        ;;
      "1.20.x")
        return 6
        ;;
      *)
        echo "invalid selected, exit.."
        exit 1
        ;;
    esac
  done
}

FLINK_VERSION=0
selectFlink
flinkVer=$?
FLINK_PYTHON_ID="flink-python"
if [ ${flinkVer} -eq 1 ]; then
    FLINK_VERSION="1.15.0"
    FLINK_PYTHON_ID="flink-python_2.12"
elif [ ${flinkVer} -eq 2 ]; then
    FLINK_VERSION="1.16.0"
elif [ ${flinkVer} -eq 3 ]; then
    FLINK_VERSION="1.17.0"
elif [ ${flinkVer} -eq 4 ]; then
    FLINK_VERSION="1.18.0"
elif [ ${flinkVer} -eq 5 ]; then
    FLINK_VERSION="1.19.0"
elif [ ${flinkVer} -eq 6 ]; then
    FLINK_VERSION="1.20.0"
fi

# extract major version:
# eg: 3.1.2 -> 3
FLINK_MAJOR_VERSION=0
[ ${FLINK_VERSION} != 0 ] && FLINK_MAJOR_VERSION=${FLINK_VERSION%.*}

echo_g " flink version: ${FLINK_VERSION}, major version: ${FLINK_MAJOR_VERSION}"
echo_g " build starting..."

${MVN_BIN} clean package -Dflink.version=${FLINK_VERSION} -Dflink.major.version=${FLINK_MAJOR_VERSION} -Dflink.python.id=${FLINK_PYTHON_ID} "$@"

EXIT_CODE=$?
if [ $EXIT_CODE -eq 0 ]; then
  DIST_DIR=${DORIS_HOME}/dist
  [ ! -d "$DIST_DIR" ] && mkdir "$DIST_DIR"
  dist_jar=$(ls "${ROOT}"/target | grep "flink-doris-" | grep -v "sources.jar" | grep -v "original-")
  rm -rf "${DIST_DIR}"/"${dist_jar}"
  cp "${ROOT}"/target/"${dist_jar}" "$DIST_DIR"
  echo_g "*****************************************************************"
  echo_g "Successfully build Flink-Doris-Connector"
  echo_g "dist: $DIST_DIR/$dist_jar "
  echo_g "*****************************************************************"
  exit 0
else
  echo_w "Failed build Flink-Doris-Connector"
  exit $EXIT_CODE
fi
