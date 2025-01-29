#!/bin/bash

# Create logs directory if it doesn't exist
mkdir -p /app/logs

wait_for_status() {
    local target_status=$1
    local max_attempts=30
    local attempt=1
    while [ $attempt -le $max_attempts ]; do
        current_status=$(supervisorctl status | grep -c "$target_status")
        if [ "$current_status" -eq 3 ]; then
            return 0
        fi
        echo "Waiting for all services to be $target_status... (attempt $attempt/$max_attempts)"
        sleep 2
        ((attempt++))
    done
    return 1
}

case "$1" in
    start)
        echo "Starting all services..."
        supervisorctl start all
        wait_for_status "RUNNING" || echo "Warning: Not all services started successfully"
        supervisorctl status
        ;;
    stop)
        echo "Stopping all services..."
        supervisorctl stop all
        wait_for_status "STOPPED" || echo "Warning: Not all services stopped successfully"
        supervisorctl status
        ;;
    restart)
        echo "Restarting all services..."
        supervisorctl restart all
        wait_for_status "RUNNING" || echo "Warning: Not all services restarted successfully"
        supervisorctl status
        ;;
    status)
        supervisorctl status
        ;;
    logs)
        echo "Available logs:"
        ls -l /app/logs/
        echo -e "\nUse 'tail -f /app/logs/<logfile>' to follow a specific log"
        ;;
    *)
        echo "Usage: $0 {start|stop|restart|status|logs}"
        exit 1
        ;;
esac 