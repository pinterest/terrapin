#!/bin/bash

export JAVA_HOME=/usr/lib/jvm/java-6-sun
JAVA=${JAVA_HOME}/bin/java

SERVICENAME=terrapin

LIB=/mnt/$SERVICENAME
LOG_DIR=/mnt/log/$SERVICENAME
RUN_DIR=/var/run/$SERVICENAME
PIDFILE=${RUN_DIR}/terrapin-server.pid
CP=${LIB}:${LIB}/*:${LIB}/lib/*

# TODO: set server memory based on available memory
# or have this info in puppet.
DAEMON_OPTS="-server -Xmx30000m -Xms30000m -XX:NewSize=512M -XX:MaxNewSize=512M \
-verbosegc -Xloggc:${LOG_DIR}/gc.log -XX:+UseGCLogFileRotation \
-XX:NumberOfGCLogFiles=100 -XX:GCLogFileSize=2M -XX:+PrintGCDetails \
-XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintClassHistogram \
-XX:+HeapDumpOnOutOfMemoryError -XX:+UseConcMarkSweepGC -XX:+UseParNewGC \
-XX:ParallelGCThreads=8 -XX:ConcGCThreads=4 -XX:ErrorFile=${LOG_DIR}/jvm_error.log \
-XX:CMSInitiatingOccupancyFraction=80 -cp ${CP} -Djava.library.path=/usr/lib/hadoop/lib/native \
-Dlog4j.configuration=log4j.server.properties \
-Dterrapin.config=${TERRAPIN_CONFIG} \
-Djute.maxbuffer=10485760 \
com.pinterest.terrapin.server.TerrapinServerMain"

ulimit -n 65536

function server_start {
   echo -n "Starting ${NAME}: "
   mkdir -p ${LOG_DIR}
   chown -R hdfs:hdfs ${LOG_DIR}
   chmod 755 ${LOG_DIR}
   mkdir -p ${RUN_DIR}
   touch ${PIDFILE}
   chown -R hdfs:hdfs ${RUN_DIR}
   chmod 755 ${RUN_DIR}
   start-stop-daemon --start --quiet --umask 007 --pidfile ${PIDFILE} --make-pidfile \
           --chuid hdfs:hdfs --exec ${JAVA} -- ${DAEMON_OPTS} 2>&1 < /dev/null &
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
