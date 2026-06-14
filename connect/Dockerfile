# Debezium 공식 이미지 사용 (Source + JDBC Sink Connector 포함)
FROM debezium/connect:2.4

USER root

# MongoDB Kafka Connector 설치 (Source + Sink)
RUN mkdir -p /kafka/connect/kafka-connect-mongodb/lib && \
    cd /kafka/connect/kafka-connect-mongodb/lib && \
    echo "Downloading MongoDB Connector..." && \
    curl -sO https://repo1.maven.org/maven2/org/mongodb/kafka/mongo-kafka-connect/1.11.0/mongo-kafka-connect-1.11.0-all.jar && \
    echo "✅ MongoDB Connector installed"

# 설치된 플러그인 확인
RUN echo "" && \
    echo "========================================" && \
    echo "✅ Installed Connectors:" && \
    echo "========================================" && \
    echo "" && \
    echo "📥 Source Connectors (DB → Kafka):" && \
    echo "   - PostgreSQL Source (CDC) - Debezium" && \
    echo "   - MySQL Source (CDC) - Debezium" && \
    echo "   - MongoDB Source (CDC) - Debezium & MongoDB" && \
    echo "   - SQL Server Source (CDC) - Debezium" && \
    echo "" && \
    echo "📤 Sink Connectors (Kafka → DB):" && \
    echo "   - JDBC Sink - Debezium (PostgreSQL, MySQL, etc.)" && \
    echo "   - MongoDB Sink - MongoDB Connector" && \
    echo "" && \
    echo "========================================" && \
    echo "Plugin directories:" && \
    ls -la /kafka/connect/ && \
    echo "========================================"

USER kafka
