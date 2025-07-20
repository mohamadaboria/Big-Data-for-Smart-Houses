"""
IoT Telemetry Simulator - Producer
Simulates smart home devices sending telemetry data to Kafka
Compatible with Python 3.11+
"""
import json
import time
import random
import logging
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional
import pandas as pd
import sys
from pathlib import Path

# Kafka import with error handling for different versions
try:
    from kafka import KafkaProducer
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

class IoTTelemetrySimulator:
    def __init__(self):
        """Initialize the IoT telemetry simulator"""
        self.producer: Optional[KafkaProducer] = None
        self.devices: List[Dict[str, Any]] = self.load_device_catalog()
        self.device_states: Dict[str, Dict[str, Any]] = {}
        self.initialize_device_states()
        
    def load_device_catalog(self) -> List[Dict[str, Any]]:
        """Load device catalog from CSV"""
        try:
            devices_df = pd.read_csv(DEVICE_CATALOG_PATH)
            logger.info(f"Loaded {len(devices_df)} devices from catalog")
            return devices_df.to_dict('records')
        except Exception as e:
            logger.error(f"Error loading device catalog: {e}")
            return []
    
    def initialize_device_states(self) -> None:
        """Initialize device states for realistic simulation"""
        for device in self.devices:
            device_id = device['device_id']
            device_type = device['device_type']
            
            # Initialize with random but realistic starting values
            ranges = DEVICE_RANGES.get(device_type, DEVICE_RANGES['smart_plug'])
            
            self.device_states[device_id] = {
                'last_temperature': random.uniform(*ranges['temperature']),
                'last_power': random.uniform(*ranges['power_usage']),
                'last_update': datetime.now(timezone.utc),
                'alert_probability': 0.05,  # 5% chance of alert
                'status': random.choice(['online', 'online', 'online', 'maintenance'])  # Mostly online
            }
    
    def connect_kafka(self) -> bool:
        """Connect to Kafka broker"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
                value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                retries=3,
                retry_backoff_ms=1000,
                acks='all',
                api_version=(0, 10, 1)  # Specify API version for compatibility
            )
            logger.info("Connected to Kafka successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            return False
    
    def generate_telemetry_data(self, device: Dict[str, Any]) -> Dict[str, Any]:
        """Generate realistic telemetry data for a device"""
        device_id = device['device_id']
        device_type = device['device_type']
        user_id = device['user_id']
        
        # Get device ranges and current state
        ranges = DEVICE_RANGES.get(device_type, DEVICE_RANGES['smart_plug'])
        state = self.device_states[device_id]
        
        # Generate temperature with some continuity (gradual changes)
        temp_change = random.uniform(-1, 1)
        new_temperature = max(ranges['temperature'][0], 
                            min(ranges['temperature'][1], 
                                state['last_temperature'] + temp_change))
        
        # Generate power usage with some correlation to device type
        if device_type == 'thermostat':
            # Thermostat power varies with temperature difference from target (22°C)
            target_temp = 22
            temp_diff = abs(new_temperature - target_temp)
            power_factor = 1 + (temp_diff / 10)  # More power needed for bigger differences
            base_power = ranges['energy_base']
            new_power = base_power * power_factor + random.uniform(-200, 200)
        else:
            # Other devices have more random power usage
            power_change = random.uniform(-0.1, 0.1) * state['last_power']
            new_power = max(ranges['power_usage'][0],
                          min(ranges['power_usage'][1],
                              state['last_power'] + power_change))
        
        # Calculate energy consumption (Wh) based on time elapsed and power
        time_elapsed_hours = (datetime.now(timezone.utc) - state['last_update']).total_seconds() / 3600
        energy_consumption = new_power * time_elapsed_hours
        
        # Generate alerts based on thresholds
        alert = "none"
        if new_temperature > ALERT_THRESHOLDS['high_temperature']:
            alert = "high_temperature"
        elif new_temperature < ALERT_THRESHOLDS['low_temperature']:
            alert = "low_temperature"
        elif new_power > ALERT_THRESHOLDS['high_power']:
            alert = "high_power"
        elif random.random() < state['alert_probability']:
            alert = random.choice(["maintenance_required", "low_battery", "connectivity_issue"])
        
        # Update device state
        state['last_temperature'] = new_temperature
        state['last_power'] = new_power
        state['last_update'] = datetime.now(timezone.utc)
        
        # Create telemetry message
        telemetry = {
            "device_id": device_id,
            "device_type": device_type,
            "user_id": user_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "temperature": round(new_temperature, 2),
            "power_usage": round(new_power, 2),
            "energy_consumption_wh": round(energy_consumption, 3),
            "status": state['status'],
            "alert": alert,
            "location": device.get('location', 'unknown'),
            "manufacturer": device.get('manufacturer', 'unknown'),
            "model": device.get('model', 'unknown')
        }
        
        return telemetry
    
    def send_telemetry(self, telemetry_data: Dict[str, Any]) -> bool:
        """Send telemetry data to Kafka topic"""
        try:
            device_id = telemetry_data['device_id']
            future = self.producer.send(
                KAFKA_TOPICS['telemetry'],
                key=device_id,
                value=telemetry_data
            )
            
            # Wait for send to complete
            record_metadata = future.get(timeout=10)
            logger.debug(f"Sent telemetry for {device_id} to partition {record_metadata.partition}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send telemetry: {e}")
            return False
    
    def run_simulation(self, duration_minutes: Optional[int] = None) -> None:
        """Run the telemetry simulation"""
        if not self.connect_kafka():
            logger.error("Cannot start simulation - Kafka connection failed")
            return
        
        logger.info(f"Starting telemetry simulation for {len(self.devices)} devices")
        logger.info(f"Sending data every {PRODUCER_INTERVAL} seconds")
        
        start_time = time.time()
        message_count = 0
        
        try:
            while True:
                # Check if duration limit reached
                if duration_minutes and (time.time() - start_time) > (duration_minutes * 60):
                    logger.info(f"Simulation duration of {duration_minutes} minutes reached")
                    break
                
                # Generate and send telemetry for each device
                for device in self.devices:
                    telemetry = self.generate_telemetry_data(device)
                    if self.send_telemetry(telemetry):
                        message_count += 1
                
                # Flush producer to ensure delivery
                if self.producer:
                    self.producer.flush()
                
                # Log progress
                if message_count % (len(self.devices) * 10) == 0:  # Every 10 cycles
                    logger.info(f"Sent {message_count} telemetry messages")
                
                # Wait before next cycle
                time.sleep(PRODUCER_INTERVAL)
                
        except KeyboardInterrupt:
            logger.info("Simulation stopped by user")
        except Exception as e:
            logger.error(f"Simulation error: {e}")
        finally:
            if self.producer:
                self.producer.close()
            logger.info(f"Simulation ended. Total messages sent: {message_count}")

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='IoT Telemetry Simulator')
    parser.add_argument('--duration', type=int, help='Simulation duration in minutes')
    parser.add_argument('--verbose', action='store_true', help='Enable verbose logging')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    simulator = IoTTelemetrySimulator()
    simulator.run_simulation(duration_minutes=args.duration)

if __name__ == "__main__":
    main()

