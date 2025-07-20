"""
Bronze to Silver ETL Processor
Validates, cleans, and enriches Bronze layer data for Silver layer
Compatible with Python 3.11+
"""
import sys
import logging
import pandas as pd
import numpy as np
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import List, Dict, Any, Optional
import time

# Add parent directory to path for config import
sys.path.append(str(Path(__file__).parent.parent))
from config.config import *

# Configure logging
logging.basicConfig(level=getattr(logging, LOG_LEVEL), format=LOG_FORMAT)
logger = logging.getLogger(__name__)

class BronzeToSilverETL:
    def __init__(self):
        """Initialize the Bronze to Silver ETL processor"""
        self.device_catalog: Optional[pd.DataFrame] = None
        self.load_device_catalog()
        
    def load_device_catalog(self) -> None:
        """Load device catalog for enrichment"""
        try:
            self.device_catalog = pd.read_csv(DEVICE_CATALOG_PATH)
            logger.info(f"Loaded device catalog with {len(self.device_catalog)} devices")
        except Exception as e:
            logger.error(f"Error loading device catalog: {e}")
            self.device_catalog = pd.DataFrame()
    
    def get_bronze_files(self, hours_back: int = 24) -> List[str]:
        """Get Bronze layer files from the last N hours"""
        try:
            # Calculate time range
            end_time = datetime.now(timezone.utc)
            start_time = end_time - timedelta(hours=hours_back)
            
            files = []
            
            # Search through date-partitioned directories
            for days_back in range(hours_back // 24 + 2):  # Include extra day for safety
                check_date = end_time - timedelta(days=days_back)
                year = check_date.strftime('%Y')
                month = check_date.strftime('%m')
                day = check_date.strftime('%d')
                
                day_dir = BRONZE_TELEMETRY_DIR / year / month / day
                if day_dir.exists():
                    for file_path in day_dir.glob('*.parquet'):
                        try:
                            file_mtime = datetime.fromtimestamp(file_path.stat().st_mtime, tz=timezone.utc)
                            if start_time <= file_mtime <= end_time:
                                files.append(str(file_path))
                        except Exception as e:
                            logger.warning(f"Error checking file time {file_path}: {e}")
            
            # Also check fallback directory
            fallback_dir = BRONZE_TELEMETRY_DIR / 'fallback'
            if fallback_dir.exists():
                for file_path in fallback_dir.glob('*.parquet'):
                    try:
                        file_mtime = datetime.fromtimestamp(file_path.stat().st_mtime, tz=timezone.utc)
                        if start_time <= file_mtime <= end_time:
                            files.append(str(file_path))
                    except Exception as e:
                        logger.warning(f"Error checking fallback file time {file_path}: {e}")
            
            logger.info(f"Found {len(files)} Bronze files to process")
            return sorted(files)
            
        except Exception as e:
            logger.error(f"Error getting Bronze files: {e}")
            return []
    
    def load_bronze_data(self, file_paths: List[str]) -> pd.DataFrame:
        """Load and combine Bronze layer data"""
        if not file_paths:
            logger.warning("No Bronze files to load")
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
            logger.info(f"Loaded total {len(combined_df)} records from Bronze layer")
            
            return combined_df
            
        except Exception as e:
            logger.error(f"Error loading Bronze data: {e}")
            return pd.DataFrame()
    
    def validate_and_clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate and clean the Bronze data"""
        if df.empty:
            return df
        
        logger.info(f"Starting validation of {len(df)} records")
        original_count = len(df)
        
        try:
            # Convert timestamp to datetime
            df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
            
            # Remove duplicates based on device_id and timestamp
            df = df.drop_duplicates(subset=['device_id', 'timestamp'])
            
            # Remove records with null critical fields
            critical_fields = ['device_id', 'device_type', 'user_id', 'timestamp']
            df = df.dropna(subset=critical_fields)
            
            # Validate numeric fields
            numeric_fields = ['temperature', 'power_usage', 'energy_consumption_wh']
            for field in numeric_fields:
                if field in df.columns:
                    # Convert to numeric, invalid values become NaN
                    df[field] = pd.to_numeric(df[field], errors='coerce')
                    
                    # Remove obviously invalid values
                    if field == 'temperature':
                        df = df[(df[field] >= -50) & (df[field] <= 100)]  # Reasonable temperature range
                    elif field == 'power_usage':
                        df = df[(df[field] >= 0) & (df[field] <= 10000)]  # Max 10kW
                    elif field == 'energy_consumption_wh':
                        df = df[df[field] >= 0]  # Energy can't be negative
            
            # Remove records with all NaN numeric values
            df = df.dropna(subset=numeric_fields, how='all')
            
            # Add data quality score
            df['quality_score'] = 1.0  # Start with perfect score
            
            # Reduce quality score for missing optional fields
            optional_fields = ['location', 'manufacturer', 'model']
            for field in optional_fields:
                if field in df.columns:
                    df.loc[df[field].isna(), 'quality_score'] -= 0.1
            
            # Reduce quality score for extreme values
            if 'temperature' in df.columns:
                extreme_temp = (df['temperature'] < 0) | (df['temperature'] > 50)
                df.loc[extreme_temp, 'quality_score'] -= 0.2
            
            if 'power_usage' in df.columns:
                extreme_power = df['power_usage'] > 5000  # > 5kW is unusual for home devices
                df.loc[extreme_power, 'quality_score'] -= 0.2
            
            # Add validation flag
            df['is_valid'] = df['quality_score'] >= 0.5
            
            logger.info(f"Validation complete: {len(df)}/{original_count} records passed ({len(df)/original_count*100:.1f}%)")
            
            return df
            
        except Exception as e:
            logger.error(f"Error in data validation: {e}")
            return df
    
    def enrich_with_device_catalog(self, df: pd.DataFrame) -> pd.DataFrame:
        """Enrich data with device catalog information"""
        if df.empty or self.device_catalog is None or self.device_catalog.empty:
            return df
        
        try:
            # Merge with device catalog
            enriched_df = df.merge(
                self.device_catalog[['device_id', 'location', 'installation_date', 'manufacturer', 'model']],
                on='device_id',
                how='left',
                suffixes=('', '_catalog')
            )
            
            # Use catalog values if original values are missing
            for field in ['location', 'manufacturer', 'model']:
                catalog_field = f"{field}_catalog"
                if catalog_field in enriched_df.columns:
                    enriched_df[field] = enriched_df[field].fillna(enriched_df[catalog_field])
                    enriched_df = enriched_df.drop(catalog_field, axis=1)
            
            # Add device age in days
            if 'installation_date' in enriched_df.columns:
                enriched_df['installation_date'] = pd.to_datetime(enriched_df['installation_date'])
                enriched_df['device_age_days'] = (
                    enriched_df['timestamp'] - enriched_df['installation_date']
                ).dt.days
            
            logger.info(f"Enriched {len(enriched_df)} records with device catalog")
            return enriched_df
            
        except Exception as e:
            logger.error(f"Error enriching with device catalog: {e}")
            return df
    
    def detect_late_events(self, df: pd.DataFrame, watermark_hours: int = 48) -> pd.DataFrame:
        """Detect late-arriving events"""
        if df.empty:
            return df
        
        try:
            # Calculate processing time delay
            df['processing_delay_hours'] = (
                pd.to_datetime(df['ingestion_time'], utc=True) - df['timestamp']
            ).dt.total_seconds() / 3600
            
            # Flag late events
            df['is_late_event'] = df['processing_delay_hours'] > watermark_hours
            
            late_count = df['is_late_event'].sum()
            if late_count > 0:
                logger.warning(f"Detected {late_count} late events (>{watermark_hours}h delay)")
            
            return df
            
        except Exception as e:
            logger.error(f"Error detecting late events: {e}")
            df['is_late_event'] = False
            df['processing_delay_hours'] = 0
            return df
    
    def calculate_derived_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate derived metrics for Silver layer"""
        if df.empty:
            return df
        
        try:
            # Sort by device and timestamp for window calculations
            df = df.sort_values(['device_id', 'timestamp'])
            
            # Calculate rolling averages (1-hour window) - simplified approach
            df['temp_1h_avg'] = df.groupby('device_id')['temperature'].transform(
                lambda x: x.rolling(window=60, min_periods=1).mean()
            )
            
            df['power_1h_avg'] = df.groupby('device_id')['power_usage'].transform(
                lambda x: x.rolling(window=60, min_periods=1).mean()
            )
            
            # Calculate energy efficiency (energy per unit power)
            df['energy_efficiency'] = np.where(
                df['power_usage'] > 0,
                df['energy_consumption_wh'] / df['power_usage'],
                0
            )
            
            # Calculate alert frequency (alerts per hour)
            df['has_alert'] = df['alert'] != 'none'
            df['alert_frequency_1h'] = df.groupby('device_id')['has_alert'].transform(
                lambda x: x.rolling(window=60, min_periods=1).sum()
            )
            
            logger.info("Calculated derived metrics for Silver layer")
            return df
            
        except Exception as e:
            logger.error(f"Error calculating derived metrics: {e}")
            return df
    
    def save_silver_data(self, df: pd.DataFrame) -> None:
        """Save processed data to Silver layer"""
        if df.empty:
            logger.warning("No data to save to Silver layer")
            return
        
        try:
            # Create timestamp-based partitions
            df['date'] = df['timestamp'].dt.date
            
            # Group by date for partitioning
            for date, group_df in df.groupby('date'):
                # Create output directory
                date_str = date.strftime('%Y-%m-%d')
                output_dir = SILVER_ENERGY_DIR / date_str
                output_dir.mkdir(parents=True, exist_ok=True)
                
                # Remove temporary columns
                save_df = group_df.drop(['date'], axis=1)
                
                # Generate filename
                timestamp = int(time.time())
                filename = f"energy_usage_{date_str}_{timestamp}.parquet"
                output_path = output_dir / filename
                
                # Save to Parquet
                save_df.to_parquet(output_path, index=False, engine='pyarrow')
                logger.info(f"Saved {len(save_df)} records to {output_path}")
            
            logger.info(f"Successfully saved {len(df)} records to Silver layer")
            
        except Exception as e:
            logger.error(f"Error saving Silver data: {e}")
    
    def process_bronze_to_silver(self, hours_back: int = 24) -> None:
        """Main processing function"""
        logger.info("Starting Bronze to Silver ETL process")
        
        try:
            # Get Bronze files
            bronze_files = self.get_bronze_files(hours_back)
            if not bronze_files:
                logger.info("No Bronze files to process")
                return
            
            # Load Bronze data
            bronze_df = self.load_bronze_data(bronze_files)
            if bronze_df.empty:
                logger.info("No data loaded from Bronze layer")
                return
            
            # Process the data
            logger.info("Validating and cleaning data...")
            cleaned_df = self.validate_and_clean_data(bronze_df)
            
            logger.info("Enriching with device catalog...")
            enriched_df = self.enrich_with_device_catalog(cleaned_df)
            
            logger.info("Detecting late events...")
            late_detected_df = self.detect_late_events(enriched_df)
            
            logger.info("Calculating derived metrics...")
            final_df = self.calculate_derived_metrics(late_detected_df)
            
            # Save to Silver layer
            logger.info("Saving to Silver layer...")
            self.save_silver_data(final_df)
            
            logger.info("Bronze to Silver ETL process completed successfully")
            
        except Exception as e:
            logger.error(f"Error in Bronze to Silver ETL: {e}")

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Bronze to Silver ETL Processor')
    parser.add_argument('--hours', type=int, default=24, help='Hours of data to process (default: 24)')
    parser.add_argument('--verbose', action='store_true', help='Enable verbose logging')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    etl = BronzeToSilverETL()
    etl.process_bronze_to_silver(hours_back=args.hours)

if __name__ == "__main__":
    main()

