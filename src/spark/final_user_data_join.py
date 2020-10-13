# This Python scripts combines 
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.functions import isnan, when, count, col,size
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StructType, StructField, DecimalType,StringType, Row,FloatType,BooleanType
from pyspark.sql.functions import *



#boto3 has a Paginator class, which allows you to iterator over pages of s3 objects, and can easily be used to provide an iterator over items within the pages:


def create_spark_session():
    """
    Description: Create Spark Session 

    Arguments:
         None

    Returns:
        spark session
    """
    spark = SparkSession \
        .builder \
        .appName("Location Mapping and Getting Coordinates") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


# Setup the logging output print settings (logging to a file)logger = logging.getLogger(name)


def user_join_location(spark):


    df_user = spark.read.parquet("s3a://mapthetech/Parquet/Users/users.parquet/") ##only ones that have the location data  - 24 partitons
    #df_user_rep = spark.read.parquet("s3a://mapthetech/Parquet/Users/users.parquet/").repartition('user_location')
    df_user = df_user.drop('user_location_trim','length_user_loc','user_loc_int','special2')

    df_user = df_user.withColumnRenamed("user_location","location_user")

    df_user_ans_tags = spark.read.parquet("s3a://mapthetech/Parquet/users_ans.parquet/") ## users with tag and count  # 37 million rows , 24 partitions


    df_user_loc = spark.read.parquet("s3a://usercoordinates/*/*") ## Have Latitude and longitude information ,zipped 30 k records - partitions
    #df_user_loc = spark.read.parquet("s3a://usercoordinates/*/*").repartition('user_location')
    df_user_loc = df_user_loc.drop('output')


    df_user_full  = df_user.join(df_user_loc ,df_user.location_user == df_user_loc.user_location, "inner") ## geocordinates mapped back to the main location.  23 partitions
    #df_user_full_rep  = df_user_rep.join(df_user_loc_rep ,df_user_rep.user_location == df_user_loc_rep.user_location, "inner")


    #df_user_full_rep  = df_user.join(df_user_loc ,df_user.user_location == df_user_loc.user_location, "inner").repartition('user_id')

    #logger.info(f"{df_user_full.count()} geocordinates mapped back total count ")

    df_final = df_user_full.join(df_user_ans_tags, df_user_ans_tags.post_ans_user_id == df_user_full.user_id,"inner")

    #logger.info(f"{df_final.count()} User details")


    mode = "overwrite"
    url = "jdbc:postgresql://10.0.0.9:5431/stack_db"
    properties = {"user": "postdb","password": "**","driver": "org.postgresql.Driver"}

    df_final.write.jdbc(url=url, table="user_location_geo", mode=mode, properties=properties)
     
               


def main():
    """
    Description: This is the main function that first creats spark session

    Arguments:
         None

    Returns:
        None
    """
    spark = create_spark_session()
   
    user_join_location(spark)



if __name__ == "__main__":
    main()
                 