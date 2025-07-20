"""
Silver to Gold ETL Processor
Creates business aggregations and analytics-ready datasets from Silver layer
Compatible with Python 3.11+
"""
import sys
import logging
import pandas as pd
import numpy as np
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import List, Dict, Any
import time

# Add parent directory to path for config import
sys.path.append(str(Path(__file__).parent.parent))
from config.config import *

# Configure logging
logging.basicConfig(level=getattr(logging, LOG_LEVEL), format=LOG_FORMAT)
logger = logging.getLogger(__name__)

class SilverToGoldETL:
    def __init__(self):
        """Initialize the Silver to Gold ETL processor"""
        logger.info("Silver to Gold ETL processor initialized")
        
    def get_silver_files(self, days_back: int = 7) -> List[str]:
        """Get Silver layer files from the last N days"""
        try:
            files = []
            end_date = datetime.now(timezone.utc).date()
            
            for days in range(days_back):
                check_date = end_date - timedelta(days=days)
                date_str = check_date.strftime('%Y-%m-%d')
                
                day_dir = SILVER_ENERGY_DIR / date_str
                if day_dir.exists():
                    for file_path in day_dir.glob('*.parquet'):
                        files.append(str(file_path))
            
            logger.info(f"Found {len(files)} Silver files to process")
            return sorted(files)
            
        except Exception as e:
            logger.error(f"Error getting Silver files: {e}")
            return []
    
    def load_silver_data(self, file_paths: List[str]) -> pd.DataFrame:
        """Load and combine Silver layer data"""
        if not file_paths:
            logger.warning("No Silver files to load")
            return pd.DataFrame()
        
        try:
            dataframes = []
            
            for file_path in file_paths:
                try:
                    df = pd.read_parquet(file_path)
                    dataframes.append(df)
                    logger.debug(f"Loaded {len(df)} records from {file_path}")
                except Exception as e:
                    logger.warning(f"Error loading file {file_path}: {e}")
            
            if not dataframes:
                return pd.DataFrame()
            
            # Combine all dataframes
            combined_df = pd.concat(dataframes, ignore_index=True)
            
            # Ensure timestamp is datetime
            combined_df['timestamp'] = pd.to_datetime(combined_df['timestamp'], utc=True)
            
            logger.info(f"Loaded total {len(combined_df)} records from Silver layer")
            return combined_df
            
        except Exception as e:
            logger.error(f"Error loading Silver data: {e}")
            return pd.DataFrame()
    
    def create_daily_energy_consumption(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create daily energy consumption aggregations"""
        if df.empty:
            return pd.DataFrame()
        
        try:
            logger.info("Creating daily energy consumption aggregations...")
            
            # Add date column
            df['date'] = df['timestamp'].dt.date
            
            # Group by device and date
            daily_agg = df.groupby(['device_id', 'device_type', 'user_id', 'date']).agg({
                'energy_consumption_wh': ['sum', 'mean', 'max', 'count'],
                'power_usage': ['mean', 'max'],
                'temperature': ['mean', 'min', 'max'],
                'quality_score': 'mean',
                'is_valid': 'mean',
                'alert_frequency_1h': 'max'
            }).round(3)
            
            # Flatten column names
            daily_agg.columns = [f"{col[0]}_{col[1]}" for col in daily_agg.columns]
            daily_agg = daily_agg.reset_index()
            
            # Calculate energy cost estimate
            daily_agg['energy_cost_estimate'] = (
                daily_agg['energy_consumption_wh_sum'] / 1000 * ENERGY_RATE_PER_KWH
            ).round(2)
            
            # Calculate efficiency metrics
            daily_agg['avg_efficiency'] = np.where(
                daily_agg['power_usage_mean'] > 0,
                daily_agg['energy_consumption_wh_mean'] / daily_agg['power_usage_mean'],
                0
            ).round(3)
            
            # Add data quality indicators
            daily_agg['data_completeness'] = (daily_agg['is_valid_mean'] * 100).round(1)
            daily_agg['total_readings'] = daily_agg['energy_consumption_wh_count']
            
            logger.info(f"Created daily energy consumption for {len(daily_agg)} device-days")
            return daily_agg
            
        except Exception as e:
            logger.error(f"Error creating daily energy consumption: {e}")
            return pd.DataFrame()
    
    def create_device_health_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create device health and reliability metrics"""
        if df.empty:
            return pd.DataFrame()
        
        try:
            logger.info("Creating device health metrics...")
            
            # Group by device
            device_health = df.groupby(['device_id', 'device_type']).agg({
                'quality_score': 'mean',
                'is_valid': 'mean',
                'alert_frequency_1h': 'mean',
                'processing_delay_hours': 'mean',
                'is_late_event': 'mean',
                'timestamp': ['count', 'min', 'max']
            }).round(3)
            
            # Flatten column names
            device_health.columns = [f"{col[0]}_{col[1]}" for col in device_health.columns]
            device_health = device_health.reset_index()
            
            # Calculate health score (0-1 scale)
            device_health['health_score'] = (
                device_health['quality_score_mean'] * 0.4 +  # 40% quality
                device_health['is_valid_mean'] * 0.3 +        # 30% validity
                (1 - device_health['is_late_event_mean']) * 0.2 +  # 20% timeliness
                (1 - np.minimum(device_health['alert_frequency_1h_mean'] / 10, 1)) * 0.1  # 10% alerts
            ).round(3)
            
            # Calculate failure probability based on various factors
            device_health['failure_probability'] = np.minimum(
                (1 - device_health['health_score']) * 0.7 +  # Health impact
                device_health['alert_frequency_1h_mean'] / 20 +  # Alert impact
                device_health['is_late_event_mean'] * 0.3,  # Latency impact
                1.0
            ).round(3)
            
            # Calculate data reliability
            device_health['data_reliability'] = (
                device_health['is_valid_mean'] * 0.6 +
                (1 - device_health['is_late_event_mean']) * 0.4
            ).round(3)
            
            # Count total alerts (approximate)
            device_health['total_alerts'] = (
                device_health['alert_frequency_1h_mean'] * 
                device_health['timestamp_count'] / 60  # Assuming 1-minute intervals
            ).round(0).astype(int)
            
            # Calculate uptime percentage
            expected_readings = (
                device_health['timestamp_max'] - device_health['timestamp_min']
            ).dt.total_seconds() / 60  # Expected readings per minute
            
            device_health['uptime_percentage'] = np.minimum(
                device_health['timestamp_count'] / expected_readings * 100, 100
            ).round(1)
            
            # Add timestamp
            device_health['created_at'] = datetime.now(timezone.utc)
            
            # Select final columns
            final_columns = [
                'device_id', 'device_type', 'health_score', 'failure_probability',
                'data_reliability', 'total_alerts', 'uptime_percentage', 'created_at'
            ]
            
            device_health = device_health[final_columns]
            
            logger.info(f"Created health metrics for {len(device_health)} devices")
            return device_health
            
        except Exception as e:
            logger.error(f"Error creating device health metrics: {e}")
            return pd.DataFrame()
    
    def create_daily_business_summary(self, daily_energy_df: pd.DataFrame, device_health_df: pd.DataFrame) -> pd.DataFrame:
        """Create daily business summary metrics"""
        try:
            logger.info("Creating daily business summary...")
            
            if daily_energy_df.empty:
                return pd.DataFrame()
            
            # Group by date for business summary
            business_summary = daily_energy_df.groupby('date').agg({
                'energy_consumption_wh_sum': 'sum',
                'energy_cost_estimate': 'sum',
                'device_id': 'nunique',
                'user_id': 'nunique',
                'data_completeness': 'mean',
                'total_readings': 'sum'
            }).round(2)
            
            business_summary = business_summary.reset_index()
            
            # Rename columns for clarity
            business_summary = business_summary.rename(columns={
                'energy_consumption_wh_sum': 'total_energy_wh',
                'energy_cost_estimate': 'total_cost_estimate',
                'device_id': 'active_devices',
                'user_id': 'active_users',
                'data_completeness': 'avg_data_quality',
                'total_readings': 'total_data_points'
            })
            
            # Convert energy to kWh
            business_summary['total_energy_kwh'] = (
                business_summary['total_energy_wh'] / 1000
            ).round(3)
            
            # Add device health summary if available
            if not device_health_df.empty:
                health_summary = device_health_df.agg({
                    'health_score': 'mean',
                    'failure_probability': 'mean',
                    'total_alerts': 'sum'
                }).round(3)
                
                # Add health metrics to each date
                business_summary['avg_device_health'] = health_summary['health_score']
                business_summary['avg_failure_risk'] = health_summary['failure_probability']
                business_summary['total_alerts'] = health_summary['total_alerts']
            else:
                business_summary['avg_device_health'] = 0.0
                business_summary['avg_failure_risk'] = 0.0
                business_summary['total_alerts'] = 0
            
            # Calculate efficiency metrics
            business_summary['cost_per_kwh'] = np.where(
                business_summary['total_energy_kwh'] > 0,
                business_summary['total_cost_estimate'] / business_summary['total_energy_kwh'],
                0
            ).round(3)
            
            # Add timestamp
            business_summary['created_at'] = datetime.now(timezone.utc)
            
            logger.info(f"Created business summary for {len(business_summary)} days")
            return business_summary
            
        except Exception as e:
            logger.error(f"Error creating business summary: {e}")
            return pd.DataFrame()
    
    def save_gold_data(self, daily_energy_df: pd.DataFrame, device_health_df: pd.DataFrame, business_summary_df: pd.DataFrame) -> None:
        """Save all Gold layer datasets"""
        timestamp = int(time.time())
        
        try:
            # Save daily energy consumption
            if not daily_energy_df.empty:
                filename = f"daily_energy_consumption_{timestamp}.parquet"
                output_path = GOLD_DAILY_ENERGY_DIR / filename
                daily_energy_df.to_parquet(output_path, index=False, engine='pyarrow')
                logger.info(f"Saved daily energy consumption: {output_path}")
            
            # Save device health metrics
            if not device_health_df.empty:
                filename = f"device_health_metrics_{timestamp}.parquet"
                output_path = GOLD_DEVICE_HEALTH_DIR / filename
                device_health_df.to_parquet(output_path, index=False, engine='pyarrow')
                logger.info(f"Saved device health metrics: {output_path}")
            
            # Save business summary
            if not business_summary_df.empty:
                # Partition by date
                for date, group_df in business_summary_df.groupby('date'):
                    date_str = date.strftime('%Y-%m-%d')
                    date_dir = GOLD_BUSINESS_SUMMARY_DIR / date_str
                    date_dir.mkdir(parents=True, exist_ok=True)
                    
                    filename = f"business_summary_{date_str}_{timestamp}.parquet"
                    output_path = date_dir / filename
                    group_df.to_parquet(output_path, index=False, engine='pyarrow')
                    logger.info(f"Saved business summary: {output_path}")
            
            logger.info("Successfully saved all Gold layer datasets")
            
        except Exception as e:
            logger.error(f"Error saving Gold data: {e}")
    
    def process_silver_to_gold(self, days_back: int = 7) -> None:
        """Main processing function"""
        logger.info("Starting Silver to Gold ETL process")
        
        try:
            # Get Silver files
            silver_files = self.get_silver_files(days_back)
            if not silver_files:
                logger.info("No Silver files to process")
                return
            
            # Load Silver data
            silver_df = self.load_silver_data(silver_files)
            if silver_df.empty:
                logger.info("No data loaded from Silver layer")
                return
            
            # Create Gold layer datasets
            logger.info("Creating daily energy consumption...")
            daily_energy_df = self.create_daily_energy_consumption(silver_df)
            
            logger.info("Creating device health metrics...")
            device_health_df = self.create_device_health_metrics(silver_df)
            
            logger.info("Creating business summary...")
            business_summary_df = self.create_daily_business_summary(daily_energy_df, device_health_df)
            
            # Save to Gold layer
            logger.info("Saving to Gold layer...")
            self.save_gold_data(daily_energy_df, device_health_df, business_summary_df)
            
            logger.info("Silver to Gold ETL process completed successfully")
            
        except Exception as e:
            logger.error(f"Error in Silver to Gold ETL: {e}")

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Silver to Gold ETL Processor')
    parser.add_argument('--days', type=int, default=7, help='Days of data to process (default: 7)')
    parser.add_argument('--verbose', action='store_true', help='Enable verbose logging')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    etl = SilverToGoldETL()
    etl.process_silver_to_gold(days_back=args.days)

if __name__ == "__main__":
    main()

