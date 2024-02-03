#!/bin/bash

PROM2CLICK_HOME=/opt/prom2click
PROM2CLICK_LOG=/var/log/prom2click
PROM2CLICK_PID=$PROM2CLICK_HOME/prom2click.pid

nohup $PROM2CLICK_HOME/prom2click "-ch.addr" "10.110.6.181:8123" "-ch.user" "fide_w" "-ch.password" "fide@1234" "-ch.db" "fidedw" "-ch.writeTable" "ods_app_metrics_ed" "-ch.batch" "10000" > $PROM2CLICK_LOG/prom2click.log 2>&1 &

pid=$!
sleep 2
if [[ -z "${pid}" ]]; then
  echo "prom2click start failed!"
  exit 1
else
  echo "prom2click start succeeded!"
  echo $pid > $PROM2CLICK_PID
fi
