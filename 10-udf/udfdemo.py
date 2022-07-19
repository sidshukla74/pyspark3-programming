from pyspark.sql import *
from pyspark.sql.functions import *
import re
from pyspark.sql.types import *


from lib.logger import Log4j

def parse_gender(gender):
    female_pattern = r"^f$|f.m|w.m"
    male_pattern = r"^m$|ma|m.l"
    if re.search(female_pattern, gender.lower()):
        return 'Female'
    elif re.search(male_pattern , gender.lower()):
        return 'male'
    else:
        return 'unknown'


if __name__ == "__main__":
    spark = SparkSession.builder.appName('udf demo')\
        .master('local[2]').getOrCreate()  

    logger = Log4j(spark)

    survey_df = spark.read.option('header', 'true') \
        .option('inferSchema', 'true') \
        .csv('data/survey.csv')

    print(survey_df.show(10))

    parse_gender_udf = udf(parse_gender, returnType=StringType())
    logger.info("catalog Entry:")
    [logger.info(r) for r in spark.catalog.listFunctions() if 'parse_gender' in r.name]

    survey_df2 = survey_df.withColumn('gender' , parse_gender_udf("Gender"))
    survey_df2.show(10)

    spark.udf.register("parse_gender_udf", parse_gender, StringType())
    logger.info("catalog Entry:")
    [logger.info(r) for r in spark.catalog.listFunctions() if "parse_sparse" in r.name]

    survey_df3 = survey_df.withColumn("Gender", expr("parse_gender_udf(Gender)"))
    survey_df3.show(10)
