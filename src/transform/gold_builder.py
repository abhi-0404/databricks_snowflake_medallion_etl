"""
Gold Layer - Star Schema Builder
Creates fact and dimension tables optimized for analytics
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, row_number, monotonically_increasing_id,
    coalesce, max as spark_max, lit, round as spark_round,
    radians, sin, cos, atan2, sqrt
)
from pyspark.sql.window import Window
import logging


class GoldBuilder:
    """Builds star schema for Gold layer"""
    
    def __init__(self, spark: SparkSession, config: dict):
        self.spark = spark
        self.config = config
        self.gold_path = config['paths']['gold']
        self.logger = logging.getLogger(__name__)
    
    def build_dim_station(self, df_trips: DataFrame) -> DataFrame:
        """
        Build station dimension from trip data
        Combines start and end stations, deduplicates
        
        Returns:
            Dimension table with station details
        """
        self.logger.info("Building dim_station")
        
        # Extract start stations
        df_start = df_trips.select(
            col("start_station_id").alias("station_id"),
            col("start_station_name").alias("station_name"),
            col("start_station_latitude").alias("latitude"),
            col("start_station_longitude").alias("longitude")
        ).distinct()
        
        # Extract end stations
        df_end = df_trips.select(
            col("end_station_id").alias("station_id"),
            col("end_station_name").alias("station_name"),
            col("end_station_latitude").alias("latitude"),
            col("end_station_longitude").alias("longitude")
        ).distinct()
        
        # Union and deduplicate
        df_all_stations = df_start.union(df_end)
        
        # Aggregate to handle any remaining duplicates
        df_stations = df_all_stations.groupBy("station_id").agg(
            spark_max("station_name").alias("station_name"),
            spark_max("latitude").alias("latitude"),
            spark_max("longitude").alias("longitude")
        )
        
        # Generate surrogate key
        window_spec = Window.orderBy("station_id")
        df_with_sk = df_stations.withColumn(
            "station_sk",
            row_number().over(window_spec)
        )
        
        # Add metadata
        df_final = df_with_sk.select(
            "station_sk",
            "station_id",
            "station_name",
            "latitude",
            "longitude",
            lit(True).alias("is_active"),
            lit(None).cast("string").alias("region_id"),
            lit(None).cast("int").alias("capacity")
        )
        
        self.logger.info(f"Created dim_station with {df_final.count()} stations")
        
        return df_final
    
    def build_dim_date(self, start_date: str, end_date: str) -> DataFrame:
        """
        Build date dimension for date range
        
        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
        
        Returns:
            Date dimension table
        """
        self.logger.info(f"Building dim_date from {start_date} to {end_date}")
        
        # Generate date range
        df_dates = self.spark.sql(f"""
            SELECT sequence(
                to_date('{start_date}'),
                to_date('{end_date}'),
                interval 1 day
            ) as date_array
        """).selectExpr("explode(date_array) as date")
        
        # Add date attributes
        df_date_dim = df_dates.select(
            col("date"),
            date_format(col("date"), "yyyyMMdd").cast("int").alias("date_sk"),
            year(col("date")).alias("year"),
            quarter(col("date")).alias("quarter"),
            month(col("date")).alias("month"),
            dayofmonth(col("date")).alias("day"),
            dayofweek(col("date")).alias("day_of_week"),
            when(dayofweek(col("date")).isin([1, 7]), True).otherwise(False).alias("is_weekend"),
            lit(False).alias("is_holiday")  # Can be enhanced with holiday calendar
        )
        
        return df_date_dim
    
    def build_dim_user_type(self) -> DataFrame:
        """
        Build user type dimension (static)
        
        Returns:
            User type dimension table
        """
        self.logger.info("Building dim_user_type")
        
        data = [
            (1, "CUSTOMER", "24-hour or 3-day pass", "Short-term user"),
            (2, "SUBSCRIBER", "Annual membership", "Long-term member")
        ]
        
        df = self.spark.createDataFrame(
            data,
            ["user_type_sk", "user_type", "membership_type", "description"]
        )
        
        return df
    
    def calculate_trip_distance(self, df: DataFrame) -> DataFrame:
        """
        Calculate trip distance using Haversine formula
        
        Args:
            df: DataFrame with lat/lon columns
        
        Returns:
            DataFrame with distance_km column
        """
        # Haversine formula for distance calculation
        df_with_distance = df.withColumn(
            "distance_km",
            spark_round(
                2 * 6371 * atan2(
                    sqrt(
                        sin(radians(col("end_station_latitude") - col("start_station_latitude")) / 2) ** 2 +
                        cos(radians(col("start_station_latitude"))) *
                        cos(radians(col("end_station_latitude"))) *
                        sin(radians(col("end_station_longitude") - col("start_station_longitude")) / 2) ** 2
                    ),
                    sqrt(1 - (
                        sin(radians(col("end_station_latitude") - col("start_station_latitude")) / 2) ** 2 +
                        cos(radians(col("start_station_latitude"))) *
                        cos(radians(col("end_station_latitude"))) *
                        sin(radians(col("end_station_longitude") - col("start_station_longitude")) / 2) ** 2
                    ))
                ),
                2
            )
        )
        
        return df_with_distance
    
    def calculate_trip_amount(self, df: DataFrame) -> DataFrame:
        """
        Calculate trip cost based on duration
        Formula: $3.00 unlock fee + $0.18 per minute
        
        Args:
            df: DataFrame with trip_duration column
        
        Returns:
            DataFrame with trip_amount column
        """
        unlock_fee = 3.00
        price_per_minute = 0.18
        
        df_with_amount = df.withColumn(
            "trip_amount",
            spark_round(
                lit(unlock_fee) + (col("trip_duration") / 60 * lit(price_per_minute)),
                2
            )
        )
        
        return df_with_amount
    
    def build_fact_trips(
        self,
        df_trips: DataFrame,
        df_dim_station: DataFrame,
        df_dim_date: DataFrame,
        df_dim_user_type: DataFrame
    ) -> DataFrame:
        """
        Build fact table with foreign keys to dimensions
        
        Returns:
            Fact table with all measures and foreign keys
        """
        self.logger.info("Building fact_trips")
        
        # Calculate derived metrics
        df_with_distance = self.calculate_trip_distance(df_trips)
        df_with_amount = self.calculate_trip_amount(df_with_distance)
        
        # Join with dimensions to get foreign keys
        df_fact = df_with_amount \
            .join(
                df_dim_station.select("station_sk", "station_id").alias("start_station"),
                col("start_station_id") == col("start_station.station_id"),
                "left"
            ) \
            .join(
                df_dim_station.select("station_sk", "station_id").alias("end_station"),
                col("end_station_id") == col("end_station.station_id"),
                "left"
            ) \
            .join(
                df_dim_date.select("date_sk", "date").alias("start_date"),
                to_date(col("start_time")) == col("start_date.date"),
                "left"
            ) \
            .join(
                df_dim_date.select("date_sk", "date").alias("end_date"),
                to_date(col("stop_time")) == col("end_date.date"),
                "left"
            ) \
            .join(
                df_dim_user_type.select("user_type_sk", "user_type"),
                upper(trim(col("user_type"))) == col("dim_user_type.user_type"),
                "left"
            )
        
        # Select final columns
        df_final = df_fact.select(
            col("trip_sk"),
            col("start_station.station_sk").alias("start_station_fk"),
            col("end_station.station_sk").alias("end_station_fk"),
            col("start_date.date_sk").alias("start_date_fk"),
            col("end_date.date_sk").alias("end_date_fk"),
            col("user_type_sk").alias("user_type_fk"),
            col("trip_duration"),
            col("distance_km").alias("trip_distance_km"),
            col("trip_amount"),
            col("bike_id"),
            col("start_time"),
            col("stop_time"),
            col("year"),
            col("month")
        )
        
        self.logger.info(f"Created fact_trips with {df_final.count()} records")
        
        return df_final
    
    def write_to_gold(self, df: DataFrame, table_name: str, partition_cols: list = None) -> None:
        """Write DataFrame to Gold layer"""
        output_path = f"{self.gold_path}/{table_name}"
        
        writer = df.write.format("delta").mode("overwrite")
        
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        
        writer.save(output_path)
        
        self.logger.info(f"Wrote {df.count()} records to {table_name}")
