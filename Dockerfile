FROM python:3.11-slim

# Install supervisor and other essential packages
RUN apt-get update && apt-get install -y \
    supervisor \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy and install requirements
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy application code
COPY . .

# Setup supervisor
#COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

# Start with root user explicitly
USER root

# Create directories and set permissions
RUN mkdir -p /var/log/supervisor /var/run && \
    touch /var/log/supervisor/supervisord.log && \
    chown -R root:root /var/log/supervisor /var/run && \
    chmod -R 777 /var/log/supervisor /var/run /app

# If you have a specific user for running the app (optional)
# USER appuser

# Command to run supervisor
#CMD ["/usr/bin/supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf"]