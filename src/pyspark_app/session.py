import os
from os.path import abspath

import findspark 
findspark.init() 
from pyspark.sql import SparkSession


from src.endpoints.orders import Orders
from src.endpoints.restaurants import TopRestaurants

class PysparkSession(Orders,TopRestaurants):

    def __init__(self):
        super().__init__()
        
        """
        start a new pyspark session 

        """

        os.environ['PYSPARK_SUBMIT_ARGS']='--driver-memory 8G --executor-memory 8G pyspark-shell'
        self.spark=SparkSession.builder.appName("ETL pipeline")\
        .config("spark.sql.warehouse.dir", abspath('data/spark-warehouse'))\
        .config('spark.driver.extraJavaOptions',f'-Dderby.system.home={abspath("data/catalog")}')\
        .config("spark.io.encryption.enabled",True)\
        .enableHiveSupport().getOrCreate()

        df=self.spark.read.parquet('data/spark-warehouse/order')
        df.createOrReplaceTempView("order")

