#!/bin/bash

PROM2CLICK_HOME=/opt/prom2click
PROM2CLICK_LOG=/var/log/prom2click/prom2click.log
PROM2CLICK_PID=$PROM2CLICK_HOME/prom2click.pid
CK_SERVER=10.110.6.180:8123,10.110.6.181:8123
CK_USER=fide_w
CK_PASSWORD=fide@1234
CK_DB=fidedw
CK_TABLE=ods_app_metrics_ed
CK_BATCH_SIZE=10000

function start()
{
  echo "Start to check whether the prom2click is running"
  if [[ -f "${PROM2CLICK_PID}" ]]; then
      pid=$(cat ${PROM2CLICK_PID})
      if kill -0 ${pid} >/dev/null 2>&1; then
        echo "prom2click is already running."
        exit 1
      fi
  fi

  nohup $PROM2CLICK_HOME/prom2click "-ch.addr" "$CK_SERVER" "-ch.user" "$CK_USER" "-ch.password" "$CK_PASSWORD" "-ch.db" "$CK_DB" "-ch.writeTable" "$CK_TABLE" "-ch.batch" "$CK_BATCH_SIZE" > $PROM2CLICK_LOG 2>&1 &

  pid=$!
  sleep 2
  if [[ -z "${pid}" ]]; then
    echo "prom2click start failed!"
    exit 1
  else
    echo "prom2click start succeeded!"
    echo $pid > $PROM2CLICK_PID
  fi
}


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
      sleep 3
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

function stop()
{
  if [[ ! -f "${PROM2CLICK_PID}" ]]; then
      echo "prom2click is not running"
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
}

function restart()
{
    stop
    sleep 10
    start
}


status()
{
  if [[ ! -f "${PROM2CLICK_PID}" ]]; then
      echo "prom2click is stopped"
      exit 1
  else
      pid=$(cat ${PROM2CLICK_PID})
      if [[ -z "${pid}" ]]; then
        echo "prom2click is not running"
        exit 1
      fi
      ps -ax | awk '{ print $1 }' | grep -e "^${pid}$"
      flag=$?
      if [ $flag != 0 ]; then
        echo "prom2click is not running"
        exit 1
      fi
      echo "prom2click is running."
  fi

}

COMMAND=$1
case $COMMAND in
  start|stop|restart|status)
    $COMMAND
    ;;
  *)
    print_usage
    exit 2
    ;;
esac
