#!/bin/bash

echo "Waiting for Kafka Connect to be ready..."

echo "Creating PostgreSQL CDC Source Connector..."
curl -X POST -H "Content-Type: application/json" \
--data @pg-source.json \
http://localhost:8083/connectors

echo -e "\n\nCreating PostgreSQL JDBC Sink Connector..."
curl -X POST -H "Content-Type: application/json" \
--data @pg-sink.json \
http://localhost:8083/connectors

# echo -e "\n\nCreating MongoDB Sink Connector..."
# curl -X POST -H "Content-Type: application/json" \
# --data @mongo-sink.json \
# http://localhost:8083/connectors

echo -e "\n\nDone!"