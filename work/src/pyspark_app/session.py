from os import getenv

import findspark 
findspark.init() 
from pyspark.sql import SparkSession


from work.src.endpoints.orders import Orders
from work.src.endpoints.restaurants import TopRestaurants

class PysparkSession(Orders,TopRestaurants):

    def __init__(self):
        super().__init__()
        
        """
        start a new pyspark session 

        """

        self.SPARK_WAREHOUSE = getenv('SPARK_WAREHOUSE')

        print('----------START CREATION OF SPARK SESSION----------')

        self.spark=SparkSession.builder.appName("ETL pipeline")\
        .config("spark.sql.warehouse.dir", self.SPARK_WAREHOUSE)\
        .config('spark.driver.extraJavaOptions',f'-Dderby.system.home=data/catalog')\
        .config("spark.io.encryption.enabled",True)\
        .config('spark.acls.enable',True)\
        .enableHiveSupport().getOrCreate()

        df=self.spark.read.parquet(f'{self.SPARK_WAREHOUSE}/order')
        df.createOrReplaceTempView("order")

