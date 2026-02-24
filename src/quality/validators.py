"""
Data Quality Validation Framework
Implements comprehensive quality checks across all layers
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, count, when, isnan, isnull
import logging
from typing import Dict, List


class DataQualityValidator:
    """Validates data quality across pipeline layers"""
    
    def __init__(self, spark: SparkSession, config: dict):
        self.spark = spark
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.quality_thresholds = config['quality']
    
    def calculate_null_rate(self, df: DataFrame) -> Dict[str, float]:
        """
        Calculate null percentage for each column
        
        Returns:
            Dictionary mapping column name to null percentage
        """
        total_count = df.count()
        null_rates = {}
        
        for column in df.columns:
            null_count = df.filter(
                col(column).isNull() | isnan(col(column))
            ).count()
            null_rates[column] = null_count / total_count if total_count > 0 else 0
        
        return null_rates
    
    def detect_duplicates(self, df: DataFrame, key_columns: List[str]) -> int:
        """
        Count duplicate records based on key columns
        
        Args:
            df: DataFrame to check
            key_columns: Columns that define uniqueness
        
        Returns:
            Number of duplicate records
        """
        total_count = df.count()
        distinct_count = df.select(key_columns).distinct().count()
        duplicate_count = total_count - distinct_count
        
        return duplicate_count
    
    def validate_foreign_keys(
        self,
        fact_df: DataFrame,
        dim_df: DataFrame,
        fact_fk_col: str,
        dim_pk_col: str
    ) -> Dict[str, any]:
        """
        Validate foreign key integrity
        
        Returns:
            Dictionary with validation results
        """
        # Get distinct foreign keys from fact
        fact_keys = fact_df.select(fact_fk_col).distinct()
        
        # Get primary keys from dimension
        dim_keys = dim_df.select(dim_pk_col).distinct()
        
        # Find orphan records (FK not in dimension)
        orphan_records = fact_keys.subtract(dim_keys)
        orphan_count = orphan_records.count()
        
        return {
            "fact_fk_column": fact_fk_col,
            "dim_pk_column": dim_pk_col,
            "orphan_count": orphan_count,
            "is_valid": orphan_count == 0
        }
    
    def validate_record_counts(
        self,
        source_df: DataFrame,
        target_df: DataFrame,
        layer_name: str
    ) -> Dict[str, any]:
        """
        Reconcile record counts between layers
        
        Returns:
            Dictionary with reconciliation results
        """
        source_count = source_df.count()
        target_count = target_df.count()
        variance = abs(source_count - target_count) / source_count if source_count > 0 else 0
        
        max_variance = self.quality_thresholds['max_variance_rate']
        is_valid = variance <= max_variance
        
        return {
            "layer": layer_name,
            "source_count": source_count,
            "target_count": target_count,
            "variance": variance,
            "max_allowed_variance": max_variance,
            "is_valid": is_valid
        }
    
    def validate_business_rules(self, df: DataFrame) -> Dict[str, any]:
        """
        Validate business-specific rules
        
        Returns:
            Dictionary with validation results
        """
        total_count = df.count()
        
        # Rule 1: Trip duration must be positive
        invalid_duration = df.filter(col("trip_duration") <= 0).count()
        
        # Rule 2: Trip amount must be positive
        invalid_amount = df.filter(col("trip_amount") <= 0).count()
        
        # Rule 3: Start time must be before stop time
        invalid_time_order = df.filter(
            col("start_time") >= col("stop_time")
        ).count()
        
        # Rule 4: Coordinates must be valid (NYC area)
        invalid_coordinates = df.filter(
            (col("start_station_latitude") < 40.0) |
            (col("start_station_latitude") > 41.0) |
            (col("start_station_longitude") < -74.5) |
            (col("start_station_longitude") > -73.5)
        ).count()
        
        total_violations = (
            invalid_duration +
            invalid_amount +
            invalid_time_order +
            invalid_coordinates
        )
        
        return {
            "total_records": total_count,
            "invalid_duration": invalid_duration,
            "invalid_amount": invalid_amount,
            "invalid_time_order": invalid_time_order,
            "invalid_coordinates": invalid_coordinates,
            "total_violations": total_violations,
            "violation_rate": total_violations / total_count if total_count > 0 else 0,
            "is_valid": total_violations == 0
        }
    
    def generate_quality_report(
        self,
        df: DataFrame,
        layer_name: str,
        key_columns: List[str] = None
    ) -> Dict[str, any]:
        """
        Generate comprehensive quality report
        
        Returns:
            Complete quality metrics dictionary
        """
        self.logger.info(f"Generating quality report for {layer_name}")
        
        report = {
            "layer": layer_name,
            "record_count": df.count(),
            "column_count": len(df.columns)
        }
        
        # Null rate analysis
        null_rates = self.calculate_null_rate(df)
        max_null_rate = max(null_rates.values()) if null_rates else 0
        report["null_rates"] = null_rates
        report["max_null_rate"] = max_null_rate
        report["null_check_passed"] = max_null_rate <= self.quality_thresholds['max_null_rate']
        
        # Duplicate detection
        if key_columns:
            duplicate_count = self.detect_duplicates(df, key_columns)
            duplicate_rate = duplicate_count / df.count() if df.count() > 0 else 0
            report["duplicate_count"] = duplicate_count
            report["duplicate_rate"] = duplicate_rate
            report["duplicate_check_passed"] = duplicate_rate <= self.quality_thresholds['max_duplicate_rate']
        
        # Business rules validation (if applicable columns exist)
        if all(col in df.columns for col in ["trip_duration", "trip_amount", "start_time", "stop_time"]):
            business_rules = self.validate_business_rules(df)
            report["business_rules"] = business_rules
        
        # Overall quality score
        passed_checks = sum([
            report.get("null_check_passed", True),
            report.get("duplicate_check_passed", True),
            report.get("business_rules", {}).get("is_valid", True)
        ])
        total_checks = 3
        report["quality_score"] = passed_checks / total_checks
        report["overall_passed"] = report["quality_score"] == 1.0
        
        self.logger.info(f"Quality score for {layer_name}: {report['quality_score']:.2%}")
        
        return report
    
    def validate_gold_layer(
        self,
        fact_df: DataFrame,
        dim_station_df: DataFrame,
        dim_date_df: DataFrame,
        dim_user_type_df: DataFrame
    ) -> Dict[str, any]:
        """
        Comprehensive validation for Gold layer star schema
        
        Returns:
            Complete validation report
        """
        self.logger.info("Validating Gold layer star schema")
        
        report = {
            "layer": "GOLD",
            "timestamp": self.spark.sql("SELECT CURRENT_TIMESTAMP()").collect()[0][0]
        }
        
        # Validate foreign key integrity
        fk_validations = []
        
        # Start station FK
        fk_validations.append(
            self.validate_foreign_keys(
                fact_df, dim_station_df,
                "start_station_fk", "station_sk"
            )
        )
        
        # End station FK
        fk_validations.append(
            self.validate_foreign_keys(
                fact_df, dim_station_df,
                "end_station_fk", "station_sk"
            )
        )
        
        # Start date FK
        fk_validations.append(
            self.validate_foreign_keys(
                fact_df, dim_date_df,
                "start_date_fk", "date_sk"
            )
        )
        
        # User type FK
        fk_validations.append(
            self.validate_foreign_keys(
                fact_df, dim_user_type_df,
                "user_type_fk", "user_type_sk"
            )
        )
        
        report["foreign_key_validations"] = fk_validations
        report["all_fk_valid"] = all(v["is_valid"] for v in fk_validations)
        
        # Aggregate validation
        total_trips = fact_df.count()
        total_revenue = fact_df.agg({"trip_amount": "sum"}).collect()[0][0]
        
        report["aggregate_metrics"] = {
            "total_trips": total_trips,
            "total_revenue": float(total_revenue) if total_revenue else 0,
            "avg_trip_amount": float(total_revenue / total_trips) if total_trips > 0 else 0
        }
        
        # Overall validation
        report["validation_passed"] = report["all_fk_valid"]
        
        return report
