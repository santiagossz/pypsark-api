import os
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
        
        print('----------START CREATION OF SPARK SESSION----------')

        self.spark=SparkSession.builder.appName("API").getOrCreate()
        

        files=['restaurant','consumer','order']
        for file in files:
            print(f'Loading table {file}----------')
            df=self.spark.read.parquet(f'data/spark-warehouse/{file}')
            df.createOrReplaceTempView(file)

