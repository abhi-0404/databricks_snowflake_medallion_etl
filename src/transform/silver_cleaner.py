"""
Silver Layer - Data Cleansing and Validation
Applies business rules and quality checks
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, sha2, concat_ws, when, trim, upper,
    year, month, dayofmonth, to_date
)
from delta.tables import DeltaTable
import logging


class SilverCleaner:
    """Handles data cleansing for Silver layer"""
    
    def __init__(self, spark: SparkSession, config: dict):
        self.spark = spark
        self.config = config
        self.silver_path = config['paths']['silver']
        self.quarantine_path = config['paths']['quarantine']
        self.logger = logging.getLogger(__name__)
    
    def clean_trips(self, df_bronze: DataFrame) -> tuple[DataFrame, DataFrame]:
        """
        Clean and validate trip data
        
        Returns:
            Tuple of (clean_df, rejected_df)
        """
        self.logger.info("Starting trip data cleansing")
        
        # Remove exact duplicates
        df_deduped = df_bronze.dropDuplicates()
        
        # Define validation rules
        df_validated = df_deduped.withColumn(
            "is_valid",
            (col("tripduration").isNotNull()) &
            (col("tripduration") > 0) &
            (col("starttime").isNotNull()) &
            (col("stoptime").isNotNull()) &
            (col("start_station_id").isNotNull()) &
            (col("end_station_id").isNotNull()) &
            (col("bikeid").isNotNull())
        )
        
        # Separate good and bad records
        df_clean = df_validated.filter(col("is_valid") == True).drop("is_valid")
        df_rejected = df_validated.filter(col("is_valid") == False).drop("is_valid")
        
        # Log metrics
        total_records = df_bronze.count()
        clean_records = df_clean.count()
        rejected_records = df_rejected.count()
        
        self.logger.info(f"Total: {total_records}, Clean: {clean_records}, Rejected: {rejected_records}")
        
        return df_clean, df_rejected
    
    def generate_surrogate_keys(self, df: DataFrame) -> DataFrame:
        """
        Generate deterministic surrogate keys using SHA-256
        
        Args:
            df: DataFrame with natural keys
        
        Returns:
            DataFrame with surrogate key column
        """
        df_with_sk = df.withColumn(
            "trip_sk",
            sha2(
                concat_ws(
                    "|",
                    col("starttime"),
                    col("stoptime"),
                    col("start_station_id"),
                    col("end_station_id"),
                    col("bikeid")
                ),
                256
            ).cast("long")
        )
        
        return df_with_sk
    
    def standardize_columns(self, df: DataFrame) -> DataFrame:
        """
        Standardize column names and data types
        
        Args:
            df: DataFrame with raw column names
        
        Returns:
            DataFrame with standardized columns
        """
        df_standardized = df \
            .withColumnRenamed("tripduration", "trip_duration") \
            .withColumnRenamed("starttime", "start_time") \
            .withColumnRenamed("stoptime", "stop_time") \
            .withColumnRenamed("bikeid", "bike_id") \
            .withColumnRenamed("usertype", "user_type") \
            .withColumn("user_type", upper(trim(col("user_type")))) \
            .withColumn("year", year(col("start_time"))) \
            .withColumn("month", month(col("start_time"))) \
            .withColumn("day", dayofmonth(col("start_time")))
        
        return df_standardized
    
    def write_to_silver(self, df: DataFrame, table_name: str) -> None:
        """
        Write cleaned data to Silver layer using merge (upsert)
        
        Args:
            df: Cleaned DataFrame
            table_name: Target table name
        """
        output_path = f"{self.silver_path}/{table_name}"
        
        self.logger.info(f"Writing to Silver: {output_path}")
        
        if DeltaTable.isDeltaTable(self.spark, output_path):
            # Merge into existing table (idempotent)
            delta_table = DeltaTable.forPath(self.spark, output_path)
            
            delta_table.alias("target").merge(
                df.alias("source"),
                "target.trip_sk = source.trip_sk"
            ).whenMatchedUpdateAll() \
             .whenNotMatchedInsertAll() \
             .execute()
            
            self.logger.info(f"Merged records into {table_name}")
        else:
            # Create new table
            df.write \
                .format("delta") \
                .mode("overwrite") \
                .partitionBy("year", "month") \
                .save(output_path)
            
            self.logger.info(f"Created new table {table_name}")
    
    def quarantine_bad_records(self, df_rejected: DataFrame, reason: str) -> None:
        """
        Write rejected records to quarantine
        
        Args:
            df_rejected: DataFrame with bad records
            reason: Rejection reason
        """
        if df_rejected.count() > 0:
            quarantine_path = f"{self.quarantine_path}/rejected_trips"
            
            df_rejected \
                .withColumn("rejection_reason", lit(reason)) \
                .withColumn("quarantine_timestamp", current_timestamp()) \
                .write \
                .format("delta") \
                .mode("append") \
                .save(quarantine_path)
            
            self.logger.warning(f"Quarantined {df_rejected.count()} records")
    
    def process_trips_to_silver(self, bronze_table: str) -> dict:
        """
        Complete pipeline for processing trips to Silver
        
        Returns:
            Dictionary with pipeline metrics
        """
        try:
            # Read from Bronze
            df_bronze = self.spark.read.format("delta").load(
                f"{self.config['paths']['bronze']}/{bronze_table}"
            )
            
            # Clean data
            df_clean, df_rejected = self.clean_trips(df_bronze)
            
            # Quarantine bad records
            self.quarantine_bad_records(df_rejected, "Failed validation rules")
            
            # Standardize columns
            df_standardized = self.standardize_columns(df_clean)
            
            # Generate surrogate keys
            df_with_sk = self.generate_surrogate_keys(df_standardized)
            
            # Write to Silver
            self.write_to_silver(df_with_sk, "silver_trips_clean")
            
            return {
                "status": "success",
                "bronze_records": df_bronze.count(),
                "silver_records": df_with_sk.count(),
                "rejected_records": df_rejected.count(),
                "rejection_rate": df_rejected.count() / df_bronze.count()
            }
            
        except Exception as e:
            self.logger.error(f"Silver processing failed: {str(e)}")
            return {
                "status": "failed",
                "error": str(e)
            }
