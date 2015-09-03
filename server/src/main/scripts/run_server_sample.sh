#!/bin/bash

# CHANGE THIS TO THE ROOT OF THE DIRECTORY CONTAINING EXTRACTED TERRAPIN JARS.
BASE_DIR=/var

JAVA_HOME=/usr
JAVA=${JAVA_HOME}/bin/java

# CHANGE THESE TO POINT TO APPROPRIATE LOG DIRECTORY OR RUN DIRECTORY.
LOG_DIR=${BASE_DIR}/log
RUN_DIR=${BASE_DIR}/run
PIDFILE=${RUN_DIR}/terrapin-server.pid

# CHANGE THIS TO POINT TO LOCATION FOR NATIVE SNAPPY.SO FILE.
JAVA_LIB_PATH=/usr/lib/hadoop/lib/native

LIB=${BASE_DIR}
CP=${LIB}:${LIB}/*:${LIB}/lib/*

JVM_MEMORY=5G

# YOUR CLUSTER PROPERTIES FILE HERE. MUST BE ON CLASSPATH.
TERRAPIN_CONFIG=sample.properties
DAEMON_OPTS="-server -Xmx${JVM_MEMORY} -Xms${JVM_MEMORY} -XX:NewSize=512M -XX:MaxNewSize=512M \
-verbosegc -Xloggc:${LOG_DIR}/gc.log -XX:+UseGCLogFileRotation \
-XX:NumberOfGCLogFiles=100 -XX:GCLogFileSize=2M -XX:+PrintGCDetails \
-XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintClassHistogram \
-XX:+HeapDumpOnOutOfMemoryError -XX:+UseConcMarkSweepGC -XX:+UseParNewGC \
-XX:ParallelGCThreads=8 -XX:ConcGCThreads=4 -XX:ErrorFile=${LOG_DIR}/jvm_error.log \
-XX:CMSInitiatingOccupancyFraction=80 -cp ${CP} -Djava.library.path=${JAVA_LIB_PATH} \
-Dlog4j.configuration=log4j.server.properties \
-Dterrapin.config=${TERRAPIN_CONFIG} \
-Djute.maxbuffer=10485760 \
com.pinterest.terrapin.server.TerrapinServerMain"


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
