#this will load config from conf file

import configparser
from pyspark import SparkConf


#create spark_conf object 
#read the file
#set it to spark.conf and return it 
#we will use it in hellosprk 
def get_spark_app_config():
    spark_conf  = SparkConf()
    config  = configparser.ConfigParser()
    config.read("spark.conf")


    for (key, val) in config.items("SPARK_APP_CONFIGS"):
        spark_conf.set(key, val)
    return spark_conf


def load_survey_df(spark, data_file):
    survey_df = spark.read.option("header", "true")\
        .option("inferschema", "true")\
            .csv(data_file)

    return survey_df


def count_by_country(survey_df):
    return survey_df.where("Age < 40") \
        .select("Age", "Gender", "Country", "state") \
        .groupby("Country") \
            .count()