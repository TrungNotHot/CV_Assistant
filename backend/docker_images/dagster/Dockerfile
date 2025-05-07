# Dagster libraries to run both dagit and the dagster-daemon. Does not
# need to have access to any pipeline code.
FROM python:3.12.10-slim

# Set $DAGSTER_HOME and copy dagster instance and workspace YAML there
ENV DAGSTER_HOME=/opt/dagster/dagster_home

# Install dependencies
RUN apt-get update --yes && \
    apt-get install --yes --no-install-recommends \
    curl && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR $DAGSTER_HOME
COPY requirements.txt $DAGSTER_HOME
# Use pip for installation
RUN pip install --upgrade pip && pip install -r requirements.txt