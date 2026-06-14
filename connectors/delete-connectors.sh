#!/bin/bash

echo "================================================"
echo "Kafka Connect - Connector 삭제 스크립트"
echo "================================================"

# Kafka Connect 준비 대기
echo "Checking Kafka Connect status..."
until curl -s http://localhost:8083/ > /dev/null; do
    echo "Waiting for Kafka Connect..."
    sleep 2
done
echo "✅ Kafka Connect is ready"
echo ""

# 현재 등록된 Connector 목록 조회
echo "📋 Current Connectors:"
curl -s http://localhost:8083/connectors | jq
echo ""

# 삭제할 Connector 목록
CONNECTORS=(
    "postgres-cdc-source"
    "postgres-jdbc-sink"
    # "mongodb-cdc-sink"
)

# 각 Connector 삭제
for connector in "${CONNECTORS[@]}"; do
    echo "🗑️  Deleting connector: $connector"
    
    # Connector 존재 여부 확인
    if curl -s http://localhost:8083/connectors/$connector > /dev/null 2>&1; then
        # 삭제 실행
        response=$(curl -s -X DELETE http://localhost:8083/connectors/$connector)
        
        if [ -z "$response" ]; then
            echo "   ✅ Successfully deleted: $connector"
        else
            echo "   ❌ Failed to delete: $connector"
            echo "   Response: $response"
        fi
    else
        echo "   ⚠️  Connector not found: $connector"
    fi
    echo ""
done

echo "================================================"
echo "📋 Remaining Connectors:"
curl -s http://localhost:8083/connectors | jq
echo ""
echo "✅ Deletion complete!"
echo "================================================"

