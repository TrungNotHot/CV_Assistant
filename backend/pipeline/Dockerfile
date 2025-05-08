FROM python:3.12.10-slim

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

USER root 

# Install only essential dependencies
RUN apt-get update --yes && \
    apt-get install --yes --no-install-recommends \
    curl && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/dagster/app/pipeline

# Add repository code
COPY requirements.txt /opt/dagster/app/pipeline
# Use pip for installation
RUN pip install --upgrade pip && pip install -r requirements.txt
COPY . /opt/dagster/app/pipeline
CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-m", "pipeline"]