from typing import get_origin
from pyspark.sql import *
from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession.builder.master("local[3]")\
    .appName('sparksqltable').enableHiveSupport().getOrCreate()


    logger = Log4j(spark)

    print(logger)

    flighttimeparq =  spark.read.parquet('datasource/flight-time.parquet')

    print(flighttimeparq)

    spark.sql("CREATE DATABASE IF NOT EXISTS AIRLINE_DB")
    spark.catalog.setCurrentDatabase("AIRLINE_DB")

    flighttimeparq.write.mode('overwrite').saveAsTable('flight_data_tbl')

    logger.info(spark.catalog.listTables('AIRLINE_DB'))


    print(flighttimeparq)







