"""
Spark Session Configuration and Initialization
"""
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import yaml
from pathlib import Path


class SparkSessionManager:
    """Manages Spark session with Delta Lake configuration"""
    
    _instance = None
    
    def __init__(self, config_path: str = "config/config.yaml"):
        """Initialize Spark session with configuration"""
        self.config = self._load_config(config_path)
        self.spark = self._create_spark_session()
    
    @staticmethod
    def _load_config(config_path: str) -> dict:
        """Load configuration from YAML file"""
        config_file = Path(config_path)
        with open(config_file, 'r') as f:
            return yaml.safe_load(f)
    
    def _create_spark_session(self) -> SparkSession:
        """Create and configure Spark session"""
        builder = SparkSession.builder \
            .appName(self.config['spark']['app_name']) \
            .master(self.config['spark']['master'])
        
        # Apply Spark configurations
        for key, value in self.config['spark']['config'].items():
            builder = builder.config(key, value)
        
        # Configure Delta Lake
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        
        # Set log level
        spark.sparkContext.setLogLevel("WARN")
        
        return spark
    
    @classmethod
    def get_instance(cls, config_path: str = "config/config.yaml"):
        """Get singleton Spark session instance"""
        if cls._instance is None:
            cls._instance = cls(config_path)
        return cls._instance.spark
    
    def stop(self):
        """Stop Spark session"""
        if self.spark:
            self.spark.stop()
            SparkSessionManager._instance = None


# Convenience function
def get_spark_session(config_path: str = "config/config.yaml") -> SparkSession:
    """Get configured Spark session"""
    return SparkSessionManager.get_instance(config_path)
