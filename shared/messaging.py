"""
Redis Pub/Sub Message Broker for inter-engine communication.
"""
import os
import json
import redis


class MessageBroker:
    """Handles Redis Pub/Sub messaging between engines."""
    
    CHANNEL_PCA_ANOMALIES = "pca_anomalies"
    CHANNEL_EXECUTION_SIGNALS = "execution_signals"
    
    def __init__(self):
        """Initialize connection to Redis."""
        self.redis_host = os.getenv("REDIS_HOST", "redis")
        self.redis_port = int(os.getenv("REDIS_PORT", 6379))
        self.client = redis.Redis(
            host=self.redis_host,
            port=self.redis_port,
            decode_responses=True
        )
        print(f"MessageBroker connected to Redis at {self.redis_host}:{self.redis_port}")
    
    def publish_anomalies(self, tickers_data: dict) -> int:
        """
        Publish anomaly data to the pca_anomalies channel.
        
        Args:
            tickers_data: Dictionary containing ticker anomaly information.
            
        Returns:
            Number of subscribers that received the message.
        """
        message = json.dumps(tickers_data)
        subscribers = self.client.publish(self.CHANNEL_PCA_ANOMALIES, message)
        print(f"Published to {self.CHANNEL_PCA_ANOMALIES}: {tickers_data} ({subscribers} subscribers)")
        return subscribers

    def publish_execution_signal(self, trade_data: dict) -> int:
        """
        Publish trade-ready data to the execution_signals channel.

        Args:
            trade_data: Dictionary containing execution signal payload.

        Returns:
            Number of subscribers that received the message.
        """
        message = json.dumps(trade_data)
        subscribers = self.client.publish(self.CHANNEL_EXECUTION_SIGNALS, message)
        print(
            f"Published to {self.CHANNEL_EXECUTION_SIGNALS}: "
            f"{trade_data} ({subscribers} subscribers)"
        )
        return subscribers

    def _parse_message_data(self, raw_data):
        """Parse Redis pub/sub payloads robustly for string/bytes/dict inputs."""
        if isinstance(raw_data, dict):
            return raw_data

        if isinstance(raw_data, bytes):
            raw_data = raw_data.decode("utf-8", errors="replace")

        if isinstance(raw_data, str):
            try:
                return json.loads(raw_data)
            except json.JSONDecodeError:
                print(f"Failed to decode Redis message as JSON: {raw_data}")
                return None

        print(f"Unsupported Redis message payload type: {type(raw_data)}")
        return None
    
    def subscribe_to_anomalies(self, callback_function):
        """
        Subscribe to the pca_anomalies channel and process messages.
        
        Args:
            callback_function: Function to call with parsed JSON data when message is received.
        """
        pubsub = self.client.pubsub()
        pubsub.subscribe(self.CHANNEL_PCA_ANOMALIES)
        print(f"Subscribed to channel: {self.CHANNEL_PCA_ANOMALIES}")
        
        for message in pubsub.listen():
            if message["type"] == "message":
                data = self._parse_message_data(message.get("data"))
                if data is not None:
                    callback_function(data)

    def subscribe_to_execution_signals(self, callback_function):
        """
        Subscribe to execution_signals channel and process messages.

        Args:
            callback_function: Function to call with parsed JSON data.
        """
        pubsub = self.client.pubsub()
        pubsub.subscribe(self.CHANNEL_EXECUTION_SIGNALS)
        print(f"Subscribed to channel: {self.CHANNEL_EXECUTION_SIGNALS}")

        for message in pubsub.listen():
            if message["type"] == "message":
                data = self._parse_message_data(message.get("data"))
                if data is not None:
                    callback_function(data)