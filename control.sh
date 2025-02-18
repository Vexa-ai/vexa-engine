#!/bin/bash

# Create required directories
mkdir -p /app/logs
mkdir -p /var/run/supervisor

# Function to ensure supervisord is running
ensure_supervisord() {
    if ! pgrep supervisord > /dev/null; then
        echo "Starting supervisord..."
        supervisord -c /app/supervisord.conf
        sleep 2  # Give it time to start
    fi
}

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
        ensure_supervisord
        echo "Starting all services..."
        supervisorctl start all
        wait_for_status "RUNNING" || echo "Warning: Not all services started successfully"
        supervisorctl status
        ;;
    stop)
        ensure_supervisord
        echo "Stopping all services..."
        supervisorctl stop all
        wait_for_status "STOPPED" || echo "Warning: Not all services stopped successfully"
        supervisorctl status
        ;;
    restart)
        ensure_supervisord
        echo "Restarting all services..."
        supervisorctl restart all
        wait_for_status "RUNNING" || echo "Warning: Not all services restarted successfully"
        supervisorctl status
        ;;
    status)
        ensure_supervisord
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



# # Start all services (API, worker, and monitor)
# ./control.sh start

# # Stop all services
# ./control.sh stop

# # Restart all services
# ./control.sh restart

# # Check status of all services
# ./control.sh status

# # View available log files and their locations
# ./control.sh logs