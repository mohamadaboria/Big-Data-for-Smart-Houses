"""
Configuration settings for Smart House Data Platform
Compatible with Python 3.11+
"""
import os
from pathlib import Path
from typing import Dict, Any, Tuple

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPICS = {
    'telemetry': 'smart_home.telemetry',
    'billing': 'billing.raw'
}

# Data Paths - Using pathlib for better cross-platform compatibility
BASE_DIR = Path(__file__).parent.parent.absolute()
DATA_DIR = BASE_DIR / 'data'
BRONZE_DIR = DATA_DIR / 'bronze'
SILVER_DIR = DATA_DIR / 'silver'
GOLD_DIR = DATA_DIR / 'gold'

# Specific data paths
BRONZE_TELEMETRY_DIR = BRONZE_DIR / 'telemetry'
BRONZE_BILLING_DIR = BRONZE_DIR / 'billing'
BRONZE_DEVICES_DIR = BRONZE_DIR / 'devices'

SILVER_ENERGY_DIR = SILVER_DIR / 'energy_usage'
SILVER_BILLING_DIR = SILVER_DIR / 'billing_summary'

GOLD_DAILY_ENERGY_DIR = GOLD_DIR / 'daily_energy_consumption'
GOLD_DEVICE_HEALTH_DIR = GOLD_DIR / 'device_health_metrics'
GOLD_BUSINESS_SUMMARY_DIR = GOLD_DIR / 'daily_business_summary'

# Configuration files
CONFIG_DIR = BASE_DIR / 'config'
DEVICE_CATALOG_PATH = CONFIG_DIR / 'device_catalog.csv'
BILLING_DATA_PATH = CONFIG_DIR / 'billing_data.csv'

# Processing Configuration
PRODUCER_INTERVAL = 1  # seconds between messages
CONSUMER_BATCH_SIZE = 100
ETL_BATCH_SIZE = 1000

# Energy pricing
ENERGY_RATE_PER_KWH = 0.12  # $0.12 per kWh
BASE_CHARGE = 15.00  # $15 base charge

# Device simulation ranges
DEVICE_RANGES: Dict[str, Dict[str, Any]] = {
    'thermostat': {
        'temperature': (18, 26),  # Celsius
        'power_usage': (1500, 3000),  # Watts
        'energy_base': 2000  # Base energy consumption
    },
    'smart_bulb': {
        'temperature': (20, 25),
        'power_usage': (8, 15),
        'energy_base': 10
    },
    'smart_plug': {
        'temperature': (20, 25),
        'power_usage': (0, 1500),
        'energy_base': 500
    },
    'security_camera': {
        'temperature': (15, 30),
        'power_usage': (5, 12),
        'energy_base': 8
    },
    'motion_sensor': {
        'temperature': (18, 25),
        'power_usage': (0.1, 0.5),
        'energy_base': 0.3
    }
}

# Alert thresholds
ALERT_THRESHOLDS: Dict[str, float] = {
    'high_temperature': 35,  # Celsius
    'high_power': 3500,  # Watts
    'low_temperature': 5,   # Celsius
    'device_offline': 300   # seconds
}

# Dashboard configuration
DASHBOARD_REFRESH_INTERVAL = 30  # seconds
MAX_LIVE_CHART_RECORDS = 100
CACHE_TTL = 30  # seconds

# Logging configuration
LOG_LEVEL = 'INFO'
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

# Ensure all directories exist
def ensure_directories():
    """Create all necessary directories"""
    directories = [
        DATA_DIR, BRONZE_DIR, SILVER_DIR, GOLD_DIR,
        BRONZE_TELEMETRY_DIR, BRONZE_BILLING_DIR, BRONZE_DEVICES_DIR,
        SILVER_ENERGY_DIR, SILVER_BILLING_DIR,
        GOLD_DAILY_ENERGY_DIR, GOLD_DEVICE_HEALTH_DIR, GOLD_BUSINESS_SUMMARY_DIR
    ]
    
    for directory in directories:
        directory.mkdir(parents=True, exist_ok=True)

# Call this when module is imported
ensure_directories()

