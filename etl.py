import argparse,yaml
from pyspark.sql import SparkSession
from extract import extract
from transform import transform
from load import load

class ETL:

    def __init__(self,logger):
            with open('script.yml','r') as file:
                config = yaml.safe_load(file)
                
            self.logger = logger
            self.logger.info("EETL")

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


    def execute(self,zip_path,provider):
        self.logger.info("Extract")
        provider_path,inetwork_path = extract.extract_it(zip_path)

        self.logger.info("Scrub")
        rate1_path,provider1_path = transform.trasform_loc(provider_path,inetwork_path,self,provider)

        self.logger.info("Load")
        load.load(rate1_path,provider1_path,self)

            