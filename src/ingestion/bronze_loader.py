"""
Bronze Layer - Raw Data Ingestion
Loads data from source with minimal transformation
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_timestamp, lit, input_file_name
from delta.tables import DeltaTable
from pathlib import Path
import logging


class BronzeLoader:
    """Handles raw data ingestion into Bronze layer"""
    
    def __init__(self, spark: SparkSession, config: dict):
        self.spark = spark
        self.config = config
        self.bronze_path = config['paths']['bronze']
        self.logger = logging.getLogger(__name__)
    
    def load_trips_csv(self, source_path: str, ingestion_date: str) -> DataFrame:
        """
        Load trip data from CSV files
        
        Args:
            source_path: Path to source CSV files
            ingestion_date: Date of ingestion (YYYY-MM-DD)
        
        Returns:
            DataFrame with raw trip data
        """
        self.logger.info(f"Loading trips from {source_path}")
        
        # Read CSV with schema inference
        df = self.spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(source_path)
        
        # Add metadata columns
        df_with_metadata = df \
            .withColumn("ingestion_date", lit(ingestion_date)) \
            .withColumn("ingestion_timestamp", current_timestamp()) \
            .withColumn("source_file", input_file_name())
        
        record_count = df_with_metadata.count()
        self.logger.info(f"Loaded {record_count} records from source")
        
        return df_with_metadata
    
    def write_to_bronze(
        self, 
        df: DataFrame, 
        table_name: str,
        partition_cols: list = None
    ) -> None:
        """
        Write DataFrame to Bronze layer as Delta table
        
        Args:
            df: DataFrame to write
            table_name: Name of the Bronze table
            partition_cols: Columns to partition by
        """
        output_path = f"{self.bronze_path}/{table_name}"
        
        self.logger.info(f"Writing to Bronze: {output_path}")
        
        # Check if table exists
        if DeltaTable.isDeltaTable(self.spark, output_path):
            # Append to existing table
            df.write \
                .format("delta") \
                .mode("append") \
                .save(output_path)
            self.logger.info(f"Appended {df.count()} records to {table_name}")
        else:
            # Create new table
            writer = df.write.format("delta").mode("overwrite")
            
            if partition_cols:
                writer = writer.partitionBy(*partition_cols)
            
            writer.save(output_path)
            self.logger.info(f"Created new table {table_name} with {df.count()} records")
    
    def load_trips_pipeline(self, source_path: str, ingestion_date: str) -> dict:
        """
        Complete pipeline for loading trip data to Bronze
        
        Returns:
            Dictionary with pipeline metrics
        """
        try:
            # Load data
            df = self.load_trips_csv(source_path, ingestion_date)
            
            # Write to Bronze
            self.write_to_bronze(
                df, 
                "bronze_trips_raw",
                partition_cols=["ingestion_date"]
            )
            
            return {
                "status": "success",
                "records_loaded": df.count(),
                "table": "bronze_trips_raw",
                "ingestion_date": ingestion_date
            }
            
        except Exception as e:
            self.logger.error(f"Bronze load failed: {str(e)}")
            return {
                "status": "failed",
                "error": str(e)
            }
