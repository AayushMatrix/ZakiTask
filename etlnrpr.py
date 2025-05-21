import argparse,yaml
from pyspark.sql import SparkSession
from extract import extractnr_pr
from transform import transform_nrpr

# from transform import transform
# from load import load
class ETL:
    def __init__(self,logger):
            with open('script.yml','r') as file:
                config = yaml.safe_load(file)
            self.logger = logger
            self.logger.info("ETL")
                # Spark configurations
            self.spark_driver_memory = config['SPARK']['DRIVER']['MEMORY']
            self.spark_executor_memory = config['SPARK']['EXECUTOR']['MEMORY']
            self.spark_executor_cores = config['SPARK']['EXECUTOR']['CORES']
            self.spark_executor_instances = config['SPARK']['EXECUTOR']['INSTANCES']
            # PostgreSQL configurations
            self.pg_host = config['POSTGRES']['HOST']
            self.pg_port = config['POSTGRES']['PORT']
            self.pg_database = config['POSTGRES']['DATABASE']
            self.pg_user = config['POSTGRES']['USER']
            self.pg_password = config['POSTGRES']['PASSWORD']
            self.spark = SparkSession.builder.appName("ETL Pipeline").config("spark.driver.memory", self.spark_driver_memory).getOrCreate()
    
    def execute(self,zip,prov):
        self.logger.info("Extract")
        new_path=extractnr_pr.extract(zip)

        self.spark = SparkSession.builder.appName("ETL Pipeline").config("spark.driver.memory", self.spark_driver_memory).getOrCreate()

        self.logger.info("Scrub")
        transform_nrpr.trasform_nrpr(new_path,self,prov) 
       