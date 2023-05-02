# Use the official Apache Airflow base image
FROM apache/airflow:2.5.3

# Switch to root user
USER root

# Install required packages for Docker
RUN apt-get update && \
    apt-get install -y \
    apt-transport-https \
    ca-certificates \
    curl \
    gnupg-agent \
    software-properties-common \
    lsb-release

# Add Docker's official GPG key and repository
RUN curl -fsSL https://download.docker.com/linux/ubuntu/gpg | apt-key add - && \
    add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu \
   xenial \
   stable"

# Install Docker
RUN apt-get update && \
    apt-get install -y docker-ce docker-ce-cli containerd.io

# Add 'airflow' user to the 'docker' group
RUN usermod -aG docker airflow

# Switch back to 'airflow' user
USER airflow

# Add requirements.txt and install Python packages
ADD requirements.txt .

RUN pip install --upgrade pip \
    pip install -r requirements.txt

# Create 'downloads' directory
RUN mkdir downloads