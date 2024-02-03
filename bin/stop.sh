#!/bin/bash

PROM2CLICK_HOME=/opt/prom2click
PROM2CLICK_LOG=/var/log/prom2click
PROM2CLICK_PID=$PROM2CLICK_HOME/prom2click.pid

function wait_for_server_to_die() {
  local pid
  local count
  pid=$1
  timeout=$2
  count=0
  timeoutTime=$(date "+%s")
  let "timeoutTime+=$timeout"
  currentTime=$(date "+%s")
  forceKill=1

  while [[ $currentTime -lt $timeoutTime ]]; do
    $(kill ${pid} > /dev/null 2> /dev/null)
    if kill -0 ${pid} > /dev/null 2>&1; then
      sleep 10
    else
      forceKill=0
      break
    fi
    currentTime=$(date "+%s")
  done

  if [[ forceKill -ne 0 ]]; then
    $(kill -9 ${pid} > /dev/null 2> /dev/null)
  fi
}


if [[ ! -f "${PROM2CLICK_PID}" ]]; then
  echo "prome2click is not running"
else
  pid=$(cat ${PROM2CLICK_PID})
  if [[ -z "${pid}" ]]; then
    echo "prom2click is not running"
  else
    wait_for_server_to_die $pid 40
    $(rm -f ${PROM2CLICK_PID})
    echo "prom2click is stopped."
  fi
fi


