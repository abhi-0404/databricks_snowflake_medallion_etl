"""
Main ETL Pipeline Orchestrator
Coordinates Bronze -> Silver -> Gold data flow
"""
import argparse
import logging
from datetime import datetime
from pathlib import Path
import yaml

from utils.spark_session import get_spark_session
from ingestion.bronze_loader import BronzeLoader
from transform.silver_cleaner import SilverCleaner
from transform.gold_builder import GoldBuilder
from quality.validators import DataQualityValidator


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'logs/pipeline_{datetime.now().strftime("%Y%m%d")}.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


class CitibikePipeline:
    """Main pipeline orchestrator"""
    
    def __init__(self, config_path: str = "config/config.yaml"):
        """Initialize pipeline with configuration"""
        self.config = self._load_config(config_path)
        self.spark = get_spark_session(config_path)
        
        # Initialize components
        self.bronze_loader = BronzeLoader(self.spark, self.config)
        self.silver_cleaner = SilverCleaner(self.spark, self.config)
        self.gold_builder = GoldBuilder(self.spark, self.config)
        self.validator = DataQualityValidator(self.spark, self.config)
        
        logger.info("Pipeline initialized successfully")
    
    @staticmethod
    def _load_config(config_path: str) -> dict:
        """Load configuration from YAML"""
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    
    def run_bronze_layer(self, source_path: str, ingestion_date: str) -> dict:
        """
        Execute Bronze layer ingestion
        
        Args:
            source_path: Path to source data files
            ingestion_date: Date of ingestion (YYYY-MM-DD)
        
        Returns:
            Metrics dictionary
        """
        logger.info("=" * 80)
        logger.info("BRONZE LAYER - Raw Data Ingestion")
        logger.info("=" * 80)
        
        metrics = self.bronze_loader.load_trips_pipeline(source_path, ingestion_date)
        
        logger.info(f"Bronze layer completed: {metrics}")
        return metrics
    
    def run_silver_layer(self) -> dict:
        """
        Execute Silver layer cleansing
        
        Returns:
            Metrics dictionary
        """
        logger.info("=" * 80)
        logger.info("SILVER LAYER - Data Cleansing & Validation")
        logger.info("=" * 80)
        
        metrics = self.silver_cleaner.process_trips_to_silver("bronze_trips_raw")
        
        # Generate quality report
        df_silver = self.spark.read.format("delta").load(
            f"{self.config['paths']['silver']}/silver_trips_clean"
        )
        
        quality_report = self.validator.generate_quality_report(
            df_silver,
            "SILVER",
            key_columns=["trip_sk"]
        )
        
        metrics["quality_report"] = quality_report
        
        logger.info(f"Silver layer completed: {metrics}")
        return metrics
    
    def run_gold_layer(self) -> dict:
        """
        Execute Gold layer star schema build
        
        Returns:
            Metrics dictionary
        """
        logger.info("=" * 80)
        logger.info("GOLD LAYER - Star Schema Build")
        logger.info("=" * 80)
        
        # Read Silver data
        df_trips = self.spark.read.format("delta").load(
            f"{self.config['paths']['silver']}/silver_trips_clean"
        )
        
        # Build dimensions
        logger.info("Building dimension tables...")
        df_dim_station = self.gold_builder.build_dim_station(df_trips)
        df_dim_date = self.gold_builder.build_dim_date("2020-01-01", "2025-12-31")
        df_dim_user_type = self.gold_builder.build_dim_user_type()
        
        # Write dimensions
        self.gold_builder.write_to_gold(df_dim_station, "dim_station")
        self.gold_builder.write_to_gold(df_dim_date, "dim_date")
        self.gold_builder.write_to_gold(df_dim_user_type, "dim_user_type")
        
        # Build fact table
        logger.info("Building fact table...")
        df_fact_trips = self.gold_builder.build_fact_trips(
            df_trips,
            df_dim_station,
            df_dim_date,
            df_dim_user_type
        )
        
        # Write fact table
        self.gold_builder.write_to_gold(
            df_fact_trips,
            "fact_trips",
            partition_cols=["year", "month"]
        )
        
        # Validate Gold layer
        validation_report = self.validator.validate_gold_layer(
            df_fact_trips,
            df_dim_station,
            df_dim_date,
            df_dim_user_type
        )
        
        metrics = {
            "status": "success",
            "dim_station_count": df_dim_station.count(),
            "dim_date_count": df_dim_date.count(),
            "dim_user_type_count": df_dim_user_type.count(),
            "fact_trips_count": df_fact_trips.count(),
            "validation_report": validation_report
        }
        
        logger.info(f"Gold layer completed: {metrics}")
        return metrics
    
    def run_full_pipeline(self, source_path: str, ingestion_date: str) -> dict:
        """
        Execute complete pipeline: Bronze -> Silver -> Gold
        
        Args:
            source_path: Path to source data
            ingestion_date: Date of ingestion
        
        Returns:
            Complete pipeline metrics
        """
        start_time = datetime.now()
        logger.info("=" * 80)
        logger.info(f"STARTING FULL PIPELINE - {start_time}")
        logger.info("=" * 80)
        
        try:
            # Execute layers sequentially
            bronze_metrics = self.run_bronze_layer(source_path, ingestion_date)
            silver_metrics = self.run_silver_layer()
            gold_metrics = self.run_gold_layer()
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            pipeline_metrics = {
                "status": "SUCCESS",
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
                "duration_seconds": duration,
                "bronze_metrics": bronze_metrics,
                "silver_metrics": silver_metrics,
                "gold_metrics": gold_metrics
            }
            
            logger.info("=" * 80)
            logger.info(f"PIPELINE COMPLETED SUCCESSFULLY in {duration:.2f} seconds")
            logger.info("=" * 80)
            
            return pipeline_metrics
            
        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}", exc_info=True)
            return {
                "status": "FAILED",
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
    
    def cleanup(self):
        """Cleanup resources"""
        if self.spark:
            self.spark.stop()
            logger.info("Spark session stopped")


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='NYC Citibike ETL Pipeline')
    parser.add_argument(
        '--mode',
        choices=['full', 'bronze', 'silver', 'gold'],
        default='full',
        help='Pipeline execution mode'
    )
    parser.add_argument(
        '--source-path',
        default='data/raw/trips.csv',
        help='Path to source data files'
    )
    parser.add_argument(
        '--date',
        default=datetime.now().strftime('%Y-%m-%d'),
        help='Ingestion date (YYYY-MM-DD)'
    )
    parser.add_argument(
        '--config',
        default='config/config.yaml',
        help='Path to configuration file'
    )
    
    args = parser.parse_args()
    
    # Initialize pipeline
    pipeline = CitibikePipeline(args.config)
    
    try:
        # Execute based on mode
        if args.mode == 'full':
            metrics = pipeline.run_full_pipeline(args.source_path, args.date)
        elif args.mode == 'bronze':
            metrics = pipeline.run_bronze_layer(args.source_path, args.date)
        elif args.mode == 'silver':
            metrics = pipeline.run_silver_layer()
        elif args.mode == 'gold':
            metrics = pipeline.run_gold_layer()
        
        # Print summary
        print("\n" + "=" * 80)
        print("PIPELINE EXECUTION SUMMARY")
        print("=" * 80)
        print(f"Status: {metrics.get('status', 'UNKNOWN')}")
        print(f"Mode: {args.mode}")
        print(f"Date: {args.date}")
        
        if metrics.get('status') == 'SUCCESS':
            print(f"Duration: {metrics.get('duration_seconds', 0):.2f} seconds")
        
        print("=" * 80)
        
    finally:
        pipeline.cleanup()


if __name__ == "__main__":
    main()
