# 생성 
docker-compose build --no-cache connect
docker-compose down -v
docker-compose up -d

# srcdb 먼저 세팅 
# connectors 생성 
cd connectors
./init-connectors.sh

# 데이터 sync 작업 수행 


# kafka-streams 실행 
cd kafka-streams
./run_local.sh

