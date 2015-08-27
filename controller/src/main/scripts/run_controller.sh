#!/bin/bash

export JAVA_HOME=/usr/lib/jvm/java-6-sun
JAVA=${JAVA_HOME}/bin/java

SERVICENAME=terrapin

LIB=/mnt/$SERVICENAME
LOG_DIR=/mnt/log/$SERVICENAME
RUN_DIR=/var/run/$SERVICENAME
PIDFILE=${RUN_DIR}/terrapin-controller.pid
CP=${LIB}:${LIB}/*:${LIB}/lib/*

DAEMON_OPTS="-server -Xmx5G -Xms5G -XX:NewSize=512M -verbosegc -Xloggc:${LOG_DIR}/gc.log \
-XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=100 -XX:GCLogFileSize=2M \
-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintClassHistogram \
-XX:+HeapDumpOnOutOfMemoryError -XX:+UseConcMarkSweepGC -XX:+UseParNewGC \
-XX:ErrorFile=${LOG_DIR}/jvm_error.log -XX:CMSInitiatingOccupancyFraction=70 \
-cp ${CP} -Dlog4j.configuration=log4j.controller.properties \
-Dterrapin.config=${TERRAPIN_CONFIG} \
-Djute.maxbuffer=10485760 \
com.pinterest.terrapin.controller.TerrapinControllerMain"

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
