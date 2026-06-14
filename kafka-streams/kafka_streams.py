#!/usr/bin/env python3
"""
MongoDB Consumer with Faust - Debezium CDC 직접 처리
PostgreSQL → Debezium → Kafka → Faust → MongoDB
"""

import faust
from motor.motor_asyncio import AsyncIOMotorClient
import os
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:29092')
MONGO_URI = os.getenv('MONGO_URI', 'mongodb://root:rootpass@mongodb:27017/?authSource=admin')
MONGO_DB = 'targetdb'

app = faust.App(
    'mongodb-consumer',
    broker=f'kafka://{KAFKA_BROKER}',
    value_serializer='raw',
    web_port=6066,
    store='memory://',
)

logger.info("=" * 60)
logger.info("🚀 MongoDB Consumer (Faust) - Direct Debezium Processing")
logger.info("=" * 60)
logger.info(f"📥 Kafka Broker: {KAFKA_BROKER}")
logger.info(f"📦 MongoDB: {MONGO_DB}")
logger.info(f"🌐 Web UI: http://localhost:6066")
logger.info("=" * 60)

mongo_client = AsyncIOMotorClient(MONGO_URI)
db = mongo_client[MONGO_DB]

# Debezium CDC 토픽 직접 구독
customers_topic = app.topic('src_server.public.customers')
orders_topic = app.topic('src_server.public.orders')
products_topic = app.topic('src_server.public.products')

def parse_debezium_message(event_bytes):
    """
    Debezium CDC 메시지 파싱
    
    Returns:
        (action, data) - action: 'delete' | 'upsert' | None
    """
    try:
        if event_bytes is None:
            return None, None
        
        message = json.loads(event_bytes)
        
        if 'payload' not in message:
            return None, None
        
        payload = message['payload']
        if payload is None:
            return None, None
        
        op = payload.get('op')
        
        # DELETE
        if op == 'd':
            before = payload.get('before', {})
            doc_id = before.get('id')
            return 'delete', {'id': doc_id} if doc_id else None
        
        # INSERT, UPDATE, SNAPSHOT
        elif op in ('c', 'u', 'r'):
            after = payload.get('after', {})
            return 'upsert', after if after else None
        
        return None, None
    
    except Exception as e:
        logger.error(f"❌ Parse error: {e}")
        return None, None

async def save_to_mongo(collection_name: str, event_bytes):
    """MongoDB에 저장"""
    try:
        action, data = parse_debezium_message(event_bytes)
        
        if not action or not data:
            return
        
        if action == 'delete':
            doc_id = data.get('id')
            if doc_id:
                result = await db[collection_name].delete_one({'id': doc_id})
                if result.deleted_count > 0:
                    logger.info(f"🗑️  Deleted {collection_name}: id={doc_id}")
                else:
                    logger.warning(f"⚠️  Delete failed {collection_name}: id={doc_id} not found")
        
        elif action == 'upsert':
            doc_id = data.get('id')
            if doc_id:
                result = await db[collection_name].replace_one(
                    {'id': doc_id}, 
                    data, 
                    upsert=True
                )
                if result.modified_count > 0:
                    logger.info(f"✅ Updated {collection_name}: id={doc_id}")
                elif result.upserted_id:
                    logger.info(f"✅ Inserted {collection_name}: id={doc_id}")
            else:
                # ID 없으면 그냥 삽입
                result = await db[collection_name].insert_one(data)
                logger.info(f"✅ Inserted {collection_name} (no id): {result.inserted_id}")
    
    except Exception as e:
        logger.error(f"❌ Error in {collection_name}: {e}")
        logger.error(f"   Event: {event_bytes[:200] if event_bytes else None}")

# Stream Agents
@app.agent(customers_topic)
async def process_customers(stream):
    """Customers 스트림 처리"""
    async for event in stream:
        await save_to_mongo('customers', event)

@app.agent(orders_topic)
async def process_orders(stream):
    """Orders 스트림 처리"""
    async for event in stream:
        await save_to_mongo('orders', event)

@app.agent(products_topic)
async def process_products(stream):
    """Products 스트림 처리"""
    async for event in stream:
        await save_to_mongo('products', event)

# Health check & Stats
@app.page('/health/')
async def health_check(self, request):
    """Health check endpoint"""
    return self.json({
        'status': 'healthy',
        'app': 'mongodb-consumer',
        'mongodb': MONGO_DB,
        'topics': [
            'src_server.public.customers',
            'src_server.public.orders',
            'src_server.public.products'
        ]
    })

@app.page('/stats/')
async def stats(self, request):
    """통계 정보"""
    try:
        counts = {}
        for collection_name in ['customers', 'orders', 'products']:
            count = await db[collection_name].count_documents({})
            counts[collection_name] = count
        
        return self.json({
            'status': 'ok',
            'collections': counts
        })
    except Exception as e:
        return self.json({
            'status': 'error',
            'message': str(e)
        })

if __name__ == '__main__':
    app.main()
