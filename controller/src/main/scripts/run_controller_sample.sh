#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# CHANGE THIS TO THE ROOT OF THE DIRECTORY CONTAINING EXTRACTED TERRAPIN JARS.
BASE_DIR=/var

JAVA_HOME=/usr
JAVA=${JAVA_HOME}/bin/java

# CHANGE THESE TO POINT TO APPROPRIATE LOG DIRECTORY OR RUN DIRECTORY.
LOG_DIR=${BASE_DIR}/log
RUN_DIR=${BASE_DIR}/run
PIDFILE=${RUN_DIR}/terrapin-controller.pid

LIB=${BASE_DIR}
CP=${LIB}:${LIB}/*:${LIB}/lib/*

JVM_MEMORY=2G

# YOUR CLUSTER PROPERTIES FILE HERE. MUST BE ON CLASSPATH.
TERRAPIN_CONFIG=sample.properties

DAEMON_OPTS="-server -Xmx${JVM_MEMORY} -Xms${JVM_MEMORY} -XX:NewSize=512M -verbosegc -Xloggc:${LOG_DIR}/gc.log \
-XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=100 -XX:GCLogFileSize=2M \
-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintClassHistogram \
-XX:+HeapDumpOnOutOfMemoryError -XX:+UseConcMarkSweepGC -XX:+UseParNewGC \
-XX:ErrorFile=${LOG_DIR}/jvm_error.log -XX:CMSInitiatingOccupancyFraction=70 \
-cp ${CP} -Dlog4j.configuration=log4j.controller.properties \
-Dterrapin.config=${TERRAPIN_CONFIG} \
-Djute.maxbuffer=10485760 \
com.pinterest.terrapin.controller.TerrapinControllerMain"


function server_start {
   echo -n "Starting ${NAME}: "
   mkdir -p ${LOG_DIR}
   chmod 755 ${LOG_DIR}
   mkdir -p ${RUN_DIR}
   touch ${PIDFILE}
   chmod 755 ${RUN_DIR}
   start-stop-daemon --start --quiet --umask 007 --pidfile ${PIDFILE} --make-pidfile \
           --exec ${JAVA} -- ${DAEMON_OPTS} 2>&1 < /dev/null &
   echo "${NAME} started."
}

function server_stop {
    echo -n "Stopping ${NAME}: "
    start-stop-daemon --stop --quiet --pidfile ${PIDFILE} --exec ${JAVA}
    echo "${NAME} stopped."
    rm -f ${RUN_DIR}/*
}

case "$1" in

    start)
    server_start
    ;;

    stop)
    server_stop
    ;;

    restart)
    server_stop
    server_start
    ;;

esac

exit 0
