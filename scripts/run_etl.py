"""
ETL Runner Script
Periodically runs Bronze to Silver and Silver to Gold ETL processes
Compatible with Python 3.11+
"""
import sys
import time
import logging
import threading
from datetime import datetime
from pathlib import Path
from typing import Optional

# Schedule import with error handling
try:
    import schedule
except ImportError as e:
    print(f"Error importing schedule: {e}")
    print("Please install schedule: pip install schedule")
    sys.exit(1)

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent))

# Import ETL processors
from consumer.bronze_to_silver import BronzeToSilverETL
from consumer.silver_to_gold import SilverToGoldETL
from config.config import LOG_LEVEL, LOG_FORMAT

# Configure logging
logging.basicConfig(level=getattr(logging, LOG_LEVEL), format=LOG_FORMAT)
logger = logging.getLogger(__name__)

class ETLRunner:
    def __init__(self):
        """Initialize the ETL runner"""
        self.bronze_to_silver = BronzeToSilverETL()
        self.silver_to_gold = SilverToGoldETL()
        self.running = False
        
    def run_bronze_to_silver(self) -> None:
        """Run Bronze to Silver ETL"""
        try:
            logger.info("Starting Bronze to Silver ETL...")
            self.bronze_to_silver.process_bronze_to_silver(hours_back=2)  # Process last 2 hours
            logger.info("Bronze to Silver ETL completed")
        except Exception as e:
            logger.error(f"Error in Bronze to Silver ETL: {e}")
    
    def run_silver_to_gold(self) -> None:
        """Run Silver to Gold ETL"""
        try:
            logger.info("Starting Silver to Gold ETL...")
            self.silver_to_gold.process_silver_to_gold(days_back=1)  # Process last day
            logger.info("Silver to Gold ETL completed")
        except Exception as e:
            logger.error(f"Error in Silver to Gold ETL: {e}")
    
    def run_full_etl(self) -> None:
        """Run both ETL processes in sequence"""
        logger.info("Starting full ETL pipeline...")
        
        # Run Bronze to Silver first
        self.run_bronze_to_silver()
        
        # Wait a bit before running Silver to Gold
        time.sleep(10)
        
        # Run Silver to Gold
        self.run_silver_to_gold()
        
        logger.info("Full ETL pipeline completed")
    
    def setup_schedule(self) -> None:
        """Setup the ETL schedule"""
        # Run Bronze to Silver every 2 minutes (for real-time processing)
        schedule.every(2).minutes.do(self.run_bronze_to_silver)
        
        # Run Silver to Gold every 5 minutes
        schedule.every(5).minutes.do(self.run_silver_to_gold)
        
        # Run full ETL every hour (for comprehensive processing)
        schedule.every().hour.do(self.run_full_etl)
        
        logger.info("ETL schedule configured:")
        logger.info("- Bronze to Silver: Every 2 minutes")
        logger.info("- Silver to Gold: Every 5 minutes")
        logger.info("- Full ETL: Every hour")
    
    def start(self) -> None:
        """Start the ETL runner"""
        logger.info("Starting ETL Runner...")
        self.running = True
        
        # Setup schedule
        self.setup_schedule()
        
        # Run initial ETL to process any existing data
        logger.info("Running initial ETL...")
        self.run_full_etl()
        
        # Start scheduled execution
        logger.info("Starting scheduled ETL execution...")
        
        try:
            while self.running:
                schedule.run_pending()
                time.sleep(30)  # Check every 30 seconds
                
        except KeyboardInterrupt:
            logger.info("ETL Runner stopped by user")
        except Exception as e:
            logger.error(f"ETL Runner error: {e}")
        finally:
            self.running = False
            logger.info("ETL Runner stopped")
    
    def stop(self) -> None:
        """Stop the ETL runner"""
        logger.info("Stopping ETL Runner...")
        self.running = False

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='ETL Runner')
    parser.add_argument('--verbose', action='store_true', help='Enable verbose logging')
    parser.add_argument('--once', action='store_true', help='Run ETL once and exit')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    runner = ETLRunner()
    
    if args.once:
        # Run ETL once and exit
        runner.run_full_etl()
    else:
        # Run continuously
        runner.start()

if __name__ == "__main__":
    main()

