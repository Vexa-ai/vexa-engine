#!/bin/bash

case "$1" in
    start)
        supervisorctl start all
        ;;
    stop)
        supervisorctl stop all
        ;;
    restart)
        supervisorctl restart all
        ;;
    status)
        supervisorctl status
        ;;
    *)
        echo "Usage: $0 {start|stop|restart|status}"
        exit 1
        ;;
esac 