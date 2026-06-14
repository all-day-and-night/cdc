#!/bin/bash

# MongoDB Consumer를 로컬에서 실행하는 스크립트

echo "🚀 Starting MongoDB Consumer (Faust) locally..."
echo ""

# 환경 변수 설정
export KAFKA_BROKER="localhost:9092"
export MONGO_URI="mongodb://root:rootpass@localhost:27017/?authSource=admin"

echo "📥 Kafka Broker: $KAFKA_BROKER"
echo "📦 MongoDB: $MONGO_URI"
echo "🌐 Web UI: http://localhost:6066"
echo ""
echo "Press Ctrl+C to stop"
echo ""

# Faust worker 실행
cd "$(dirname "$0")"
faust -A kafka_streams worker -l info

