"""
Bronze Layer Consumer - Kafka to Parquet Ingestion
Consumes telemetry data from Kafka and stores in Bronze layer as Parquet files
Compatible with Python 3.11+
"""
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Any, Optional
import pandas as pd
import signal
import threading
import time

# Kafka import with error handling
try:
    from kafka import KafkaConsumer
    from kafka.errors import KafkaError
except ImportError as e:
    print(f"Error importing kafka-python: {e}")
    print("Please install kafka-python: pip install kafka-python")
    sys.exit(1)

# Add parent directory to path for config import
sys.path.append(str(Path(__file__).parent.parent))
from config.config import *

# Configure logging
logging.basicConfig(level=getattr(logging, LOG_LEVEL), format=LOG_FORMAT)
logger = logging.getLogger(__name__)

class BronzeLayerConsumer:
    def __init__(self):
        """Initialize the Bronze layer consumer"""
        self.consumer: Optional[KafkaConsumer] = None
        self.running = False
        self.message_buffer: List[Dict[str, Any]] = []
        self.buffer_lock = threading.Lock()
        self.last_flush_time = time.time()
        
        # Ensure directories exist (already done in config)
        logger.info("Bronze layer consumer initialized")
        
    def connect_kafka(self) -> bool:
        """Connect to Kafka consumer"""
        try:
            self.consumer = KafkaConsumer(
                KAFKA_TOPICS['telemetry'],
                bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                key_deserializer=lambda k: k.decode('utf-8') if k else None,
                group_id='bronze_telemetry_consumer',
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                auto_commit_interval_ms=1000,
                max_poll_records=CONSUMER_BATCH_SIZE,
                api_version=(0, 10, 1)  # Specify API version for compatibility
            )
            logger.info("Connected to Kafka consumer successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Kafka consumer: {e}")
            return False
    
    def validate_message(self, message: Dict[str, Any]) -> bool:
        """Validate incoming message structure"""
        required_fields = [
            'device_id', 'device_type', 'user_id', 'timestamp',
            'temperature', 'power_usage', 'energy_consumption_wh',
            'status', 'alert'
        ]
        
        try:
            # Check if all required fields are present
            for field in required_fields:
                if field not in message:
                    logger.warning(f"Missing required field: {field}")
                    return False
            
            # Validate data types
            if not isinstance(message['temperature'], (int, float)):
                return False
            if not isinstance(message['power_usage'], (int, float)):
                return False
            if not isinstance(message['energy_consumption_wh'], (int, float)):
                return False
            
            # Validate timestamp format
            datetime.fromisoformat(message['timestamp'].replace('Z', '+00:00'))
            
            return True
            
        except Exception as e:
            logger.warning(f"Message validation failed: {e}")
            return False
    
    def process_message(self, message: Dict[str, Any]) -> bool:
        """Process a single message and add to buffer"""
        try:
            # Add ingestion timestamp
            message['ingestion_time'] = datetime.now(timezone.utc).isoformat()
            
            # Validate message
            if not self.validate_message(message):
                logger.warning(f"Invalid message skipped: {message.get('device_id', 'unknown')}")
                return False
            
            # Add to buffer
            with self.buffer_lock:
                self.message_buffer.append(message)
            
            return True
            
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            return False
    
    def get_output_path(self, timestamp_str: str) -> Path:
        """Generate output path based on timestamp"""
        try:
            # Parse timestamp
            dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            
            # Create path: bronze/telemetry/YYYY/MM/DD/
            year = dt.strftime('%Y')
            month = dt.strftime('%m')
            day = dt.strftime('%d')
            
            output_dir = BRONZE_TELEMETRY_DIR / year / month / day
            output_dir.mkdir(parents=True, exist_ok=True)
            
            # Generate filename with hour and minute for better partitioning
            hour = dt.strftime('%H')
            minute = dt.strftime('%M')
            filename = f"telemetry_{hour}{minute}_{int(time.time())}.parquet"
            
            return output_dir / filename
            
        except Exception as e:
            logger.error(f"Error generating output path: {e}")
            # Fallback to simple timestamp
            timestamp = int(time.time())
            output_dir = BRONZE_TELEMETRY_DIR / 'fallback'
            output_dir.mkdir(parents=True, exist_ok=True)
            return output_dir / f"telemetry_{timestamp}.parquet"
    
    def flush_buffer(self, force: bool = False) -> None:
        """Flush message buffer to Parquet files"""
        current_time = time.time()
        
        # Check if we should flush (buffer size or time threshold)
        should_flush = (
            force or 
            len(self.message_buffer) >= CONSUMER_BATCH_SIZE or
            (current_time - self.last_flush_time) > 30  # Flush every 30 seconds
        )
        
        if not should_flush or len(self.message_buffer) == 0:
            return
        
        try:
            with self.buffer_lock:
                messages_to_write = self.message_buffer.copy()
                self.message_buffer.clear()
            
            if not messages_to_write:
                return
            
            # Convert to DataFrame
            df = pd.DataFrame(messages_to_write)
            
            # Group by timestamp (day) for better partitioning
            df['date'] = pd.to_datetime(df['timestamp']).dt.date
            
            for date, group_df in df.groupby('date'):
                # Get output path based on first message timestamp
                first_timestamp = group_df.iloc[0]['timestamp']
                output_path = self.get_output_path(first_timestamp)
                
                # Remove the temporary date column
                group_df = group_df.drop('date', axis=1)
                
                # Write to Parquet
                group_df.to_parquet(output_path, index=False, engine='pyarrow')
                logger.info(f"Wrote {len(group_df)} records to {output_path}")
            
            self.last_flush_time = current_time
            logger.info(f"Flushed {len(messages_to_write)} messages to Bronze layer")
            
        except Exception as e:
            logger.error(f"Error flushing buffer: {e}")
            # Put messages back in buffer if write failed
            with self.buffer_lock:
                self.message_buffer.extend(messages_to_write)
    
    def consume_messages(self) -> None:
        """Main message consumption loop"""
        if not self.connect_kafka():
            logger.error("Cannot start consumer - Kafka connection failed")
            return
        
        logger.info("Starting Bronze layer consumer")
        self.running = True
        message_count = 0
        
        try:
            if self.consumer:
                for message in self.consumer:
                    if not self.running:
                        break
                    
                    # Process the message
                    if self.process_message(message.value):
                        message_count += 1
                    
                    # Periodic flush
                    if message_count % 10 == 0:  # Check flush every 10 messages
                        self.flush_buffer()
                    
                    # Log progress
                    if message_count % 100 == 0:
                        logger.info(f"Processed {message_count} messages")
                        
        except KeyboardInterrupt:
            logger.info("Consumer stopped by user")
        except Exception as e:
            logger.error(f"Consumer error: {e}")
        finally:
            # Final flush
            self.flush_buffer(force=True)
            
            if self.consumer:
                self.consumer.close()
            
            logger.info(f"Consumer ended. Total messages processed: {message_count}")
    
    def stop(self) -> None:
        """Stop the consumer gracefully"""
        logger.info("Stopping Bronze layer consumer...")
        self.running = False

def signal_handler(signum, frame):
    """Handle shutdown signals"""
    logger.info(f"Received signal {signum}, shutting down...")
    if hasattr(signal_handler, 'consumer'):
        signal_handler.consumer.stop()

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Bronze Layer Consumer')
    parser.add_argument('--verbose', action='store_true', help='Enable verbose logging')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Set up signal handlers
    consumer = BronzeLayerConsumer()
    signal_handler.consumer = consumer
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Start consuming
    consumer.consume_messages()

if __name__ == "__main__":
    main()

