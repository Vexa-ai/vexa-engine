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
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

# Create and switch to non-root user
RUN useradd -m appuser && chown -R appuser:appuser /app
USER appuser

# Command to run supervisor
CMD ["/usr/bin/supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf"]