# Use NVIDIA CUDA base image for GPU support
FROM nvidia/cuda:12.6.2-runtime-ubuntu22.04 AS cuda-base

# Set noninteractive installation
ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=Etc/UTC

# Install Python and system dependencies
RUN apt-get update && apt-get install -y \
    software-properties-common \
    tzdata \
    && ln -fs /usr/share/zoneinfo/Etc/UTC /etc/localtime \
    && dpkg-reconfigure -f noninteractive tzdata \
    && add-apt-repository -y ppa:deadsnakes/ppa \
    && apt-get update \
    && apt-get install -y \
    python3.11 \
    python3.11-dev \
    python3.11-distutils \
    python3-pip \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install pip for Python 3.11
RUN curl -sS https://bootstrap.pypa.io/get-pip.py | python3.11

# Set Python aliases
RUN update-alternatives --install /usr/bin/python python /usr/bin/python3.11 1 \
    && update-alternatives --install /usr/bin/pip pip /usr/local/bin/pip3.11 1

# Set working directory
WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies (only missing ones)
RUN pip install  -r requirements.txt

# Copy the rest of the application
COPY . .

# Create a non-root user
RUN useradd -m appuser && chown -R appuser:appuser /app
USER appuser

# Expose the API port
EXPOSE 8765

# Start the FastAPI application
#CMD ["python", "-m", "uvicorn", "api:app", "--host", "0.0.0.0", "--port", "8765", "--reload"]