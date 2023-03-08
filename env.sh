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

thrift_failed() {
    echo "You can rename 'custom_env.sh.tpl' to 'custom_env.sh' and set THRIFT_BIN to the thrift binary"
    echo "For example: "
    echo "    THRIFT_BIN=/path/to/thrift/bin/thrift"
    echo ""
    echo "You can install thrift@v0.13.0 by yourself, or if you have compiled the Doris core source file,"
    echo "there is thrift in 'thirdparty/installed/bin/'"
    exit 1
}

# check thrift
[ -z "$THRIFT_BIN" ] && export THRIFT_BIN=$(which thrift)
$THRIFT_BIN --version >/dev/null 2>&1
[ $? -eq 127 ] && thrift_failed
THRIFT_VER=$($THRIFT_BIN --version | awk '{print $3}')
if [ x"${THRIFT_VER}" != x"0.13.0" ]; then
    echo "oh, thrift version must be v0.13.0, please reinstall thrift@v0.13.0"
    exit 1
fi

# check java home
# Make sure prerequisite environment variables are set
if [ -z "$JAVA_HOME" ] && [ -z "$JRE_HOME" ]; then
  if $darwin; then
    # Bugzilla 54390
    if [ -x '/usr/libexec/java_home' ] ; then
      export JAVA_HOME=$(/usr/libexec/java_home)
    # Bugzilla 37284 (reviewed).
    elif [ -d "/System/Library/Frameworks/JavaVM.framework/Versions/CurrentJDK/Home" ]; then
      export JAVA_HOME="/System/Library/Frameworks/JavaVM.framework/Versions/CurrentJDK/Home"
    fi
  else
    JAVA_PATH=$(which java 2>/dev/null)
    if [ "x$JAVA_PATH" != "x" ]; then
      JAVA_PATH=$(dirname "$JAVA_PATH" 2>/dev/null)
      JRE_HOME=$(dirname "$JAVA_PATH" 2>/dev/null)
    fi
    if [ "x$JRE_HOME" = "x" ]; then
      # XXX: Should we try other locations?
      if [ -x /usr/bin/java ]; then
        JRE_HOME=/usr
      fi
    fi
  fi
  if [ -z "$JAVA_HOME" ] && [ -z "$JRE_HOME" ]; then
    echo "Neither the JAVA_HOME nor the JRE_HOME environment variable is defined"
    echo "At least one of these environment variable is needed to run this program"
    exit 1
  fi
fi

# Set standard commands for invoking javap, if not already set.
[ -z "$_RUNJAVAP" ] && _RUNJAVAP="$JAVA_HOME"/bin/javap

JAVA_VER=$(${_RUNJAVAP} -verbose java.lang.String | grep "major version" | cut -d " " -f5)
if [[ $JAVA_VER -lt 52 ]]; then
    echo "Error: require JAVA with JDK version at least 1.8"
    exit 1
fi

# check maven
[ -z "$MVN_BIN" ] && export MVN_BIN=$(which mvn)
${MVN_BIN} --version >/dev/null 2>&1
[ $? -ne 0 ] && export MVN_BIN=${DORIS_HOME}/mvnw
