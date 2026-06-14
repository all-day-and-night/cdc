CDC의 주요 사용 사례
CDC는 데이터를 실시간으로 동기화해야 하는 거의 모든 현대적인 데이터 아키텍처에서 핵심적인 역할을 합니다.

1. 데이터 복제 및 동기화
목적: 메인 운영 DB(OLTP)의 데이터를 실시간으로 백업 DB나 재해 복구 시스템으로 동기화합니다.

효과: 항상 최신 데이터를 유지하여 데이터 손실 위험을 최소화합니다.

2. 데이터 웨어하우스/레이크 로딩 (ETL/ELT)
목적: 트랜잭션 DB의 변경 데이터만 추출하여 분석용 데이터 웨어하우스(예: Snowflake, Redshift)에 반영합니다.

효과: 전체 데이터를 주기적으로 로드하는 대신, **변경분(델타)**만 로드하여 데이터 웨어하우스의 로딩 시간과 비용을 획기적으로 줄입니다.

3. 마이크로서비스 간 통신 (Event Sourcing)
목적: 한 마이크로서비스의 DB 변경 사항을 **이벤트 스트림(예: Kafka)**으로 변환하여, 다른 서비스가 이 이벤트를 구독하고 자신의 DB를 업데이트하도록 합니다.

효과: 서비스 간의 결합도를 낮추고 데이터 일관성을 유지하며 확장성 있는 아키텍처를 구축할 수 있습니다. (예: 주문 서비스에서 '주문 완료' 이벤트가 발생하면, 재고 서비스가 이를 구독하여 재고를 차감)

개념 및 예시 
https://suminii.tistory.com/entry/CDCChange-Data-Capture-%EA%B0%9C%EB%85%90%EA%B3%BC-%EA%B5%AC%EC%B6%95%ED%95%B4%EB%B3%B4%EA%B8%B0


제공해주신 docker-compose.yml 파일은 CDC를 구현하기 위한 핵심적인 서비스들(PostgreSQL 소스/복제, MongoDB, Kafka, Connect)을 잘 구성하고 있습니다. ✅

이 구성을 기반으로 CDC 기능을 완전히 구현하고 검증하기 위해 필요한 다음 단계의 작업 목록을 정리해 드립니다.

1. 커넥터 배포 및 생성 (CDC 핵심)
connect 서비스가 시작된 후, Kafka Connect의 REST API를 사용하여 소스 DB(postgres-source)의 변경 사항을 캡처하고 이를 Kafka 토픽으로 전송하는 커넥터 인스턴스를 생성해야 합니다.

a. PostgreSQL Source 커넥터 생성 (Debezium)
connect 컨테이너의 8083 포트로 아래와 같은 내용의 POST 요청을 보냅니다. (예: curl, Postman 이용)

역할: postgres-source의 변경사항을 캡처하여 Kafka에 토픽(postgres.public.테이블명)으로 스트리밍합니다.

핵심 설정:

connector.class: io.debezium.connector.postgresql.PostgresConnector

database.hostname: postgres-source (서비스 이름)

database.server.name: postgres (Kafka 토픽 접두사로 사용됨)

slot.name: debezium_slot (논리적 복제 슬롯 이름)


```
# 예시: 터미널에서 실행 (Connect 컨테이너가 시작된 후)
curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" http://localhost:8083/connectors/ -d '{
  "name": "postgres-source-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "tasks.max": "1",
    "database.hostname": "postgres-source",
    "database.port": "5432",
    "database.user": "postgres",
    "database.password": "postgres",
    "database.dbname": "mydb",
    "database.server.name": "postgres",
    "slot.name": "debezium_slot",
    "plugin.name": "pgoutput",
    "table.include.list": "public.*" 
  }
}'
```