"""#create the session 
creates a spark driver and does nothing and stops
#the spark app name value is coming from here 
#itll go to logger.py
# 
"""

from itertools import groupby
import sys
from pyspark import SparkConf
from pyspark.sql import *

from lib.logger import Log4j
from lib.utils import count_by_country, get_spark_app_config, load_survey_df, count_by_country


if __name__ == "__main__":
     print("starting hello spark.")

#config for session of our application

     conf = get_spark_app_config()
    

     spark = SparkSession \
        .builder \
        .appName("hellospark") \
        .config(conf=conf)\
        .getOrCreate()
     
     
        
     logger = Log4j(spark)


     if len(sys.argv)!=2:
        logger.error("Usage: hellospark <filename>")
        sys.exit(-1)




     logger.info("starting hellospark")

    


     #conf_out = spark.sparkContext.getConf()
     #logger.info(conf_out.toDebugString())

     
     survey_df =  load_survey_df(spark, sys.argv[1])


     partition_survey_df = survey_df.repartition(2) 



     count_df = count_by_country(partition_survey_df)
     


 

     print(count_df)
     print(sys.argv)

     logger.info(count_df.collect())


     logger.info("finished hellospark")

    


