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

# check DORIS_HOME
export LC_ALL=C

if [[ -z ${DORIS_HOME} ]]; then
    echo "Error: DORIS_HOME is not set"
    exit 1
fi

# check OS type
if [[ ! -z "$OSTYPE" ]]; then
    if [[ ${OSTYPE} != "linux-gnu" ]] && [[ ${OSTYPE:0:6} != "darwin" ]]; then
        echo "Error: Unsupported OS type: $OSTYPE"
        exit 1
    fi
fi

# include custom environment variables
if [[ -f ${DORIS_HOME}/custom_env.sh ]]; then
    . ${DORIS_HOME}/custom_env.sh
fi

thrift_help() {
    echo "You can rename 'custom_env.sh.tpl' to 'custom_env.sh' and set THRIFT_BIN to the thrift binary"
    echo "For example: "
    echo "    THRIFT_BIN=/path/to/thrift/bin/thrift"
    echo ""
    echo "You can install thrift@v0.13 by yourself, or if you have compiled the Doris core source file,"
    echo "there is thrift in 'thirdparty/installed/bin/'"
}

# check thrift
if [ -z "$THRIFT_BIN" ]; then
    thrift_help
    exit 1
fi

if ! ${THRIFT_BIN} --version; then
    thrift_help
    exit 1
fi

# check java home
if [ -z "$JAVA_HOME" ]; then
    export JAVACMD=$(which java)
    JAVAP=$(which javap)
else
    export JAVA="${JAVA_HOME}/bin/java"
    JAVAP="${JAVA_HOME}/bin/javap"
fi

if [ ! -x "$JAVA" ]; then
    echo "The JAVA_HOME environment variable is not defined correctly"
    echo "This environment variable is needed to run this program"
    echo "NB: JAVA_HOME should point to a JDK not a JRE"
    exit 1
fi

JAVA_VER=$(${JAVAP} -verbose java.lang.String | grep "major version" | cut -d " " -f5)
if [[ $JAVA_VER -lt 52 ]]; then
    echo "Error: require JAVA with JDK version at least 1.8"
    exit 1
fi

# check maven
if [ -z "$MVN_BIN" ]; then
    export MVN_BIN=$(which mvn)
fi
if ! ${MVN_BIN} --version; then
    echo "Error: mvn is not found"
    echo "You can rename 'custom_env.sh.tpl' to 'custom_env.sh' and set MVN_BIN to the mvn binary"
    echo "For example:"
    echo "    export MVN_BIN=/path/to/maven/bin/mvn"
    exit 1
fi
