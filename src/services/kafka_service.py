"""
Kafka Service Implementation for Aiven.
Handles asynchronous message publishing to Kafka topics.
"""

import json
import logging
import ssl
from typing import Any, Dict, Optional
from aiokafka import AIOKafkaProducer

from src.config.secrets import get_secrets_config

logger = logging.getLogger(__name__)

class KafkaService:
    """
    Service to handle Kafka operations (publishing logs, insights, etc.)
    """
    
    _instance: Optional['KafkaService'] = None
    _producer: Optional[AIOKafkaProducer] = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(KafkaService, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        # Prevent re-initialization if already done
        if hasattr(self, '_initialized') and self._initialized:
            return
            
        self.config = get_secrets_config()
        self.bootstrap_servers = self.config.KAFKA_BOOTSTRAP_SERVERS
        self._initialized = True
        logger.info(f"[Kafka] Service initialized. Bootstrap servers: {self.bootstrap_servers}")

    async def start(self):
        """Start the Kafka producer."""
        if self._producer is not None:
            return

        if not self.bootstrap_servers:
            logger.warning("[Kafka] KAFKA_BOOTSTRAP_SERVERS not configured. Kafka functions will be disabled.")
            return

        try:
            # Setup SSL context for Aiven
            ssl_context = None
            if self.config.KAFKA_SSL_CA_CERT_PATH:
                ssl_context = ssl.create_default_context(cafile=self.config.KAFKA_SSL_CA_CERT_PATH)
                if self.config.KAFKA_SSL_ACCESS_CERT_PATH and self.config.KAFKA_SSL_ACCESS_KEY_PATH:
                    ssl_context.load_cert_chain(
                        certfile=self.config.KAFKA_SSL_ACCESS_CERT_PATH,
                        keyfile=self.config.KAFKA_SSL_ACCESS_KEY_PATH
                    )
                logger.info("[Kafka] SSL context configured for Aiven.")

            self._producer = AIOKafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                security_protocol="SSL" if ssl_context else "PLAINTEXT",
                ssl_context=ssl_context,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            await self._producer.start()
            logger.info("[Kafka] Producer started successfully.")
        except Exception as e:
            logger.error(f"[Kafka] Failed to start producer: {e}")
            self._producer = None

    async def stop(self):
        """Stop the Kafka producer."""
        if self._producer:
            await self._producer.stop()
            self._producer = None
            logger.info("[Kafka] Producer stopped.")

    async def publish(self, topic: str, message: Dict[str, Any], key: Optional[str] = None):
        """
        Publish a message to a Kafka topic.
        
        Args:
            topic: Target Kafka topic
            message: Dictionary to be JSON-serialized
            key: Optional message key for partitioning
        """
        if self._producer is None:
            # Try to start if not running
            await self.start()
            
        if self._producer is None:
            raise RuntimeError(f"Kafka producer is not available for topic {topic}")

        try:
            # Encode key if provided
            encoded_key = key.encode('utf-8') if key else None
            await self._producer.send_and_wait(topic, value=message, key=encoded_key)
            logger.debug(f"[Kafka] Published message to {topic}")
        except Exception as e:
            logger.error(f"[Kafka] Error publishing to {topic}: {e}")
            error_text = str(e).lower()
            if "not found in cluster metadata" in error_text or "unknown topic" in error_text:
                raise RuntimeError(
                    f"Kafka topic '{topic}' does not exist. Create it in Aiven before uploading documents."
                ) from e
            raise

# Singleton instance
_kafka_service = None

async def get_kafka_service() -> KafkaService:
    """Get the Kafka service singleton instance, ensuring it is started."""
    global _kafka_service
    if _kafka_service is None:
        _kafka_service = KafkaService()
        await _kafka_service.start()
    return _kafka_service
