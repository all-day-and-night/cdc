# 🔄 PostgreSQL CDC with Kafka Connector

PostgreSQL의 변경 데이터를 Kafka를 통해 실시간으로 캡처하는 CDC(Change Data Capture) 시스템 구축 가이드

## 📋 목차

1. [아키텍처 개요](#아키텍처-개요)
2. [사전 요구사항](#사전-요구사항)
3. [PostgreSQL 설정](#postgresql-설정)
4. [Kafka 환경 구성](#kafka-환경-구성)
5. [Debezium Connector 설정](#debezium-connector-설정)
6. [CDC 테스트](#cdc-테스트)
7. [트러블슈팅](#트러블슈팅)

## 🏗️ 아키텍처 개요

```
┌─────────────────────┐
│   PostgreSQL        │
│   (Source DB)       │
│   - WAL enabled     │
└──────────┬──────────┘
           │ WAL (Write-Ahead Log)
           ↓
┌─────────────────────┐
│ Debezium Connector  │
│ (Kafka Connect)     │
│ - PostgreSQL Source │
└──────────┬──────────┘
           ↓
┌─────────────────────┐
│   Kafka Cluster     │
│   - Topics          │
│   - Partitions      │
└──────────┬──────────┘
           ↓
┌─────────────────────┐
│   Consumers         │
│   - Applications    │
│   - Analytics       │
│   - Data Warehouse  │
└─────────────────────┘
```

### 주요 컴포넌트

- **PostgreSQL**: 소스 데이터베이스 (WAL 기반 CDC)
- **Kafka**: 메시지 브로커 및 이벤트 스트림
- **Kafka Connect**: 데이터 통합 프레임워크
- **Debezium**: PostgreSQL용 CDC Connector
- **Zookeeper**: Kafka 메타데이터 관리 (Kafka 3.x 이상에서는 선택사항)

## 📋 사전 요구사항

### 필수 도구

- Docker & Docker Compose
- PostgreSQL 10 이상
- Java 11 이상 (Kafka 실행용)
- Python 3.8 이상 (Consumer 애플리케이션용)

### 권장 환경

- CPU: 2 Core 이상
- RAM: 4GB 이상
- Disk: 10GB 이상 여유 공간

## 🐘 PostgreSQL 설정

### 1. WAL (Write-Ahead Logging) 활성화

PostgreSQL에서 CDC를 사용하려면 WAL을 논리 복제 모드로 설정해야 합니다.

#### postgresql.conf 수정

```ini
# WAL 레벨을 logical로 설정
wal_level = logical

# 최대 복제 슬롯 개수
max_replication_slots = 4

# 최대 WAL sender 프로세스
max_wal_senders = 4

# 논리 복제 워커
max_logical_replication_workers = 4
```

#### pg_hba.conf 수정

```
# Debezium이 복제 연결을 할 수 있도록 허용
# TYPE  DATABASE        USER            ADDRESS                 METHOD
host    replication     postgres        0.0.0.0/0               md5
host    all             postgres        0.0.0.0/0               md5
```

### 2. 복제 사용자 생성

```sql
-- 복제 권한이 있는 사용자 생성
CREATE USER debezium_user WITH REPLICATION PASSWORD 'debezium_password';

-- 필요한 권한 부여
GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium_user;
GRANT USAGE ON SCHEMA public TO debezium_user;

-- 복제 슬롯 생성 (Debezium이 자동으로 생성하지만 수동으로도 가능)
SELECT pg_create_logical_replication_slot('debezium_slot', 'pgoutput');
```

### 3. 테스트 테이블 생성

```sql
-- CDC 테스트용 테이블
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- 샘플 데이터 삽입
INSERT INTO users (name, email) VALUES 
    ('John Doe', 'john@example.com'),
    ('Jane Smith', 'jane@example.com');
```

## 🚀 Kafka 환경 구성

### Docker Compose로 전체 스택 실행

`docker-compose-cdc.yml` 파일 생성:

```yaml
version: '3.8'

services:
  # Zookeeper (Kafka 2.x 이하에서 필수, 3.x 이상은 KRaft 모드 사용 가능)
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    container_name: zookeeper
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    ports:
      - "2181:2181"
    volumes:
      - zookeeper-data:/var/lib/zookeeper/data
      - zookeeper-logs:/var/lib/zookeeper/log

  # Kafka Broker
  kafka:
    image: confluentinc/cp-kafka:7.5.0
    container_name: kafka
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
      - "9093:9093"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092,PLAINTEXT_INTERNAL://kafka:9093
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_INTERNAL:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT_INTERNAL
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
    volumes:
      - kafka-data:/var/lib/kafka/data

  # Kafka Connect with Debezium
  kafka-connect:
    image: debezium/connect:2.4
    container_name: kafka-connect
    depends_on:
      - kafka
      - postgres
    ports:
      - "8083:8083"
    environment:
      BOOTSTRAP_SERVERS: kafka:9093
      GROUP_ID: debezium-cluster
      CONFIG_STORAGE_TOPIC: debezium_configs
      OFFSET_STORAGE_TOPIC: debezium_offsets
      STATUS_STORAGE_TOPIC: debezium_statuses
      CONFIG_STORAGE_REPLICATION_FACTOR: 1
      OFFSET_STORAGE_REPLICATION_FACTOR: 1
      STATUS_STORAGE_REPLICATION_FACTOR: 1
      KEY_CONVERTER: org.apache.kafka.connect.json.JsonConverter
      VALUE_CONVERTER: org.apache.kafka.connect.json.JsonConverter
      INTERNAL_KEY_CONVERTER: org.apache.kafka.connect.json.JsonConverter
      INTERNAL_VALUE_CONVERTER: org.apache.kafka.connect.json.JsonConverter

  # PostgreSQL Database
  postgres:
    image: postgres:15-alpine
    container_name: postgres-cdc
    ports:
      - "5432:5432"
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
      POSTGRES_DB: testdb
    command:
      - "postgres"
      - "-c"
      - "wal_level=logical"
      - "-c"
      - "max_replication_slots=4"
      - "-c"
      - "max_wal_senders=4"
    volumes:
      - postgres-data:/var/lib/postgresql/data

  # Kafka UI (선택사항 - 모니터링용)
  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    container_name: kafka-ui
    depends_on:
      - kafka
    ports:
      - "8080:8080"
    environment:
      KAFKA_CLUSTERS_0_NAME: local
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:9093
      KAFKA_CLUSTERS_0_ZOOKEEPER: zookeeper:2181

volumes:
  zookeeper-data:
  zookeeper-logs:
  kafka-data:
  postgres-data:
```

### 실행

```bash
# CDC 스택 시작
docker-compose -f docker-compose-cdc.yml up -d

# 상태 확인
docker-compose -f docker-compose-cdc.yml ps

# 로그 확인
docker-compose -f docker-compose-cdc.yml logs -f
```

## 🔌 Debezium Connector 설정

### 1. Kafka Connect 상태 확인

```bash
# Kafka Connect가 실행 중인지 확인
curl http://localhost:8083/

# 설치된 플러그인 확인
curl http://localhost:8083/connector-plugins
```

### 2. Debezium PostgreSQL Connector 생성

`debezium-postgres-connector.json` 파일 생성:

```json
{
  "name": "postgres-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "postgres",
    "database.password": "postgres",
    "database.dbname": "testdb",
    "database.server.name": "dbserver1",
    "table.include.list": "public.users",
    "plugin.name": "pgoutput",
    "slot.name": "debezium_slot",
    "publication.name": "dbz_publication",
    "publication.autocreate.mode": "filtered",
    "topic.prefix": "cdc",
    "schema.history.internal.kafka.bootstrap.servers": "kafka:9093",
    "schema.history.internal.kafka.topic": "schema-changes.testdb"
  }
}
```

### 3. Connector 등록

```bash
# Connector 생성
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @debezium-postgres-connector.json

# Connector 상태 확인
curl http://localhost:8083/connectors/postgres-connector/status

# 모든 Connector 목록
curl http://localhost:8083/connectors
```

## 🧪 CDC 테스트

### 1. Kafka Topics 확인

```bash
# Kafka 컨테이너 접속
docker exec -it kafka bash

# 토픽 목록 확인
kafka-topics --list --bootstrap-server localhost:9093

# 예상 토픽:
# - cdc.public.users (users 테이블 변경사항)
# - schema-changes.testdb (스키마 변경사항)
```

### 2. Consumer로 메시지 확인

```bash
# users 테이블 변경사항 구독
kafka-console-consumer \
  --bootstrap-server localhost:9093 \
  --topic cdc.public.users \
  --from-beginning

# 또는 포맷팅된 출력
kafka-console-consumer \
  --bootstrap-server localhost:9093 \
  --topic cdc.public.users \
  --from-beginning \
  --property print.key=true \
  --property print.timestamp=true
```

### 3. 데이터 변경 테스트

#### INSERT 테스트

```sql
-- PostgreSQL에서 실행
INSERT INTO users (name, email) 
VALUES ('Alice Johnson', 'alice@example.com');
```

**Kafka 메시지 (예시)**:
```json
{
  "before": null,
  "after": {
    "id": 3,
    "name": "Alice Johnson",
    "email": "alice@example.com",
    "created_at": 1704085200000,
    "updated_at": 1704085200000
  },
  "source": {
    "version": "2.4.0.Final",
    "connector": "postgresql",
    "name": "dbserver1",
    "ts_ms": 1704085200123,
    "snapshot": "false",
    "db": "testdb",
    "schema": "public",
    "table": "users",
    "txId": 123,
    "lsn": 456
  },
  "op": "c",
  "ts_ms": 1704085200456
}
```

#### UPDATE 테스트

```sql
-- PostgreSQL에서 실행
UPDATE users 
SET name = 'Alice Cooper', updated_at = NOW() 
WHERE id = 3;
```

**Kafka 메시지 (예시)**:
```json
{
  "before": {
    "id": 3,
    "name": "Alice Johnson",
    "email": "alice@example.com",
    "created_at": 1704085200000,
    "updated_at": 1704085200000
  },
  "after": {
    "id": 3,
    "name": "Alice Cooper",
    "email": "alice@example.com",
    "created_at": 1704085200000,
    "updated_at": 1704085300000
  },
  "source": { ... },
  "op": "u",
  "ts_ms": 1704085300456
}
```

#### DELETE 테스트

```sql
-- PostgreSQL에서 실행
DELETE FROM users WHERE id = 3;
```

**Kafka 메시지 (예시)**:
```json
{
  "before": {
    "id": 3,
    "name": "Alice Cooper",
    "email": "alice@example.com",
    "created_at": 1704085200000,
    "updated_at": 1704085300000
  },
  "after": null,
  "source": { ... },
  "op": "d",
  "ts_ms": 1704085400456
}
```

## 🐍 Python Consumer 예제

### 설치

```bash
pip install kafka-python
```

### CDC Consumer 코드

`cdc_consumer.py`:

```python
from kafka import KafkaConsumer
import json

# Kafka Consumer 생성
consumer = KafkaConsumer(
    'cdc.public.users',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='cdc-consumer-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("🔄 Listening for CDC events...")

for message in consumer:
    event = message.value
    
    # 이벤트 타입
    op = event.get('op')
    before = event.get('before')
    after = event.get('after')
    
    print(f"\n{'='*60}")
    print(f"📨 Event Type: {op}")
    print(f"⏰ Timestamp: {event.get('ts_ms')}")
    
    if op == 'c':  # CREATE
        print(f"✅ INSERT: {after}")
    elif op == 'u':  # UPDATE
        print(f"🔄 UPDATE:")
        print(f"   Before: {before}")
        print(f"   After:  {after}")
    elif op == 'd':  # DELETE
        print(f"❌ DELETE: {before}")
    elif op == 'r':  # READ (initial snapshot)
        print(f"📸 SNAPSHOT: {after}")
    
    print(f"{'='*60}\n")
```

### 실행

```bash
python cdc_consumer.py
```

## 📊 Kafka UI로 모니터링

브라우저에서 접속:

```
http://localhost:8080
```

확인 가능한 정보:
- 토픽 목록 및 메시지 수
- 컨슈머 그룹 상태
- 메시지 내용
- 파티션 정보
- 브로커 상태

## 🛠️ 유용한 명령어

### Connector 관리

```bash
# Connector 삭제
curl -X DELETE http://localhost:8083/connectors/postgres-connector

# Connector 일시 중지
curl -X PUT http://localhost:8083/connectors/postgres-connector/pause

# Connector 재개
curl -X PUT http://localhost:8083/connectors/postgres-connector/resume

# Connector 재시작
curl -X POST http://localhost:8083/connectors/postgres-connector/restart
```

### Topic 관리

```bash
# Topic 상세 정보
kafka-topics --describe \
  --topic cdc.public.users \
  --bootstrap-server localhost:9093

# Topic 삭제
kafka-topics --delete \
  --topic cdc.public.users \
  --bootstrap-server localhost:9093

# 메시지 개수 확인
kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list localhost:9093 \
  --topic cdc.public.users
```

### PostgreSQL 복제 슬롯 관리

```sql
-- 복제 슬롯 목록
SELECT * FROM pg_replication_slots;

-- 복제 슬롯 삭제 (필요시)
SELECT pg_drop_replication_slot('debezium_slot');

-- WAL 사용량 확인
SELECT 
    slot_name,
    pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS retained_wal
FROM pg_replication_slots;
```

## 🐛 트러블슈팅

### 1. Connector가 시작되지 않음

```bash
# 로그 확인
docker logs kafka-connect -f

# Connector Task 확인
curl http://localhost:8083/connectors/postgres-connector/tasks
```

**일반적인 원인:**
- PostgreSQL WAL 레벨이 logical이 아님
- 복제 권한이 없음
- 네트워크 연결 문제

### 2. 메시지가 Kafka에 안 들어옴

```bash
# Connector 상태 확인
curl http://localhost:8083/connectors/postgres-connector/status

# PostgreSQL 로그 확인
docker logs postgres-cdc
```

**체크리스트:**
- [ ] WAL 레벨 = logical
- [ ] 복제 슬롯 생성됨
- [ ] 테이블이 include 리스트에 있음
- [ ] PostgreSQL 연결 정보 정확

### 3. Consumer Lag 발생

```bash
# Consumer Group 상태 확인
kafka-consumer-groups \
  --bootstrap-server localhost:9093 \
  --describe \
  --group cdc-consumer-group
```

**해결 방법:**
- Consumer 개수 증가 (파티션 수에 맞춰)
- 처리 로직 최적화
- 배치 처리 사용

### 4. 디스크 공간 부족

```bash
# Kafka 데이터 정리
kafka-configs \
  --bootstrap-server localhost:9093 \
  --alter \
  --entity-type topics \
  --entity-name cdc.public.users \
  --add-config retention.ms=86400000  # 1일

# PostgreSQL WAL 정리
# pg_replication_slots에서 사용하지 않는 슬롯 삭제
```

## 📈 성능 최적화

### 1. Kafka 설정

```yaml
# docker-compose-cdc.yml에서 Kafka 환경변수 추가
KAFKA_NUM_PARTITIONS: 3  # 파티션 수 증가
KAFKA_LOG_RETENTION_HOURS: 24  # 보관 기간 설정
KAFKA_COMPRESSION_TYPE: lz4  # 압축 사용
```

### 2. Connector 설정

```json
{
  "config": {
    ...
    "max.batch.size": 2048,
    "max.queue.size": 8192,
    "poll.interval.ms": 1000
  }
}
```

### 3. Consumer 설정

```python
consumer = KafkaConsumer(
    ...,
    max_poll_records=500,  # 한 번에 가져올 레코드 수
    fetch_min_bytes=1024,  # 최소 fetch 크기
    session_timeout_ms=30000
)
```

## 📚 다음 단계

1. **다중 테이블 CDC**: 여러 테이블 동시 모니터링
2. **변환 로직**: Kafka Streams로 데이터 변환
3. **Sink Connector**: 다른 데이터베이스로 복제
4. **모니터링**: Prometheus + Grafana 연동
5. **알림**: Slack/Email 알림 설정

## 🔗 참고 자료

- [Debezium Documentation](https://debezium.io/documentation/)
- [Kafka Connect](https://docs.confluent.io/platform/current/connect/index.html)
- [PostgreSQL Logical Replication](https://www.postgresql.org/docs/current/logical-replication.html)

---

**CDC로 데이터 변경사항을 실시간으로 캡처하세요! 🚀**

