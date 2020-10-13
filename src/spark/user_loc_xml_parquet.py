## This Python code is to optimize the User data so that it can be passed to Geocoding API
import boto3
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import functions as f
from pyspark.sql.functions import isnan, when, count, col,size
from geopy.geocoders import Nominatim
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StructType, StructField, DecimalType,StringType, Row,FloatType,BooleanType
from pyspark.sql.functions import length
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import re

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
        .appName("Download StackOverflow XML data from S3") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def regex_filter(x):
    regexs =  ["^\w\s*"]  # ["^-?\\d*(\\.\\d+)?$"]
                    
    if x and x.strip():
        for r in regexs:
            if re.match(r, x, re.IGNORECASE):
                return False
    
    return True 


def read_xml_write_parquet(spark):

    region = 'us-west-2'
    bucket = 'mapthetech'
    
    spark._jsc.hadoopConfiguration().set('fs.s3a.endpoint', f's3-{region}.amazonaws.com')

    client = boto3.client('s3')
    paginator = client.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=bucket)
    
    for page in page_iterator:
      if page['KeyCount'] > 0:
        for item in page['Contents']:
            if item['Key'] == 'Users.xml' :
                key = item['Key']
                s3file = f's3a://{bucket}/{key}'
                df_u = spark.read.format("com.databricks.spark.xml").option("rootTag","users").option("rowTag","row").load(s3file)
                df_users = df_u.select(col('_Id').cast('long').alias("user_id"),\
                    col('_CreationDate').cast('timestamp').alias("creation_date"),\
                    col('_Location').cast('string').alias("user_location"),\
                    col('_Reputation').cast('long').alias("user_reputation"),\
                    col('_Views').cast('long').alias("views"),\
                    col('_UpVotes').cast('long').alias("upvotes"),
                    col('_DownVotes').cast('long').alias("downvotes"),
                    col('_WebsiteUrl').cast('string').alias("website_url"))

                df_users = df_u.select(col('_Id').alias("user_id"), col('_CreationDate').cast('timestamp').alias("creation_date"),col('_Location').alias("user_location"),\
                    col('_Reputation').alias("user_reputation"),col('_Views').alias("views"),col('_UpVotes').alias("upvotes"),col('_DownVotes').alias("downvotes"),col('_WebsiteUrl').alias("website_url"))

                df_user_loc = df_users.where(col("user_location").isNotNull())

                df_user_loc = df_user_loc.withColumn('user_location_all_trim', trim(df_user_loc.user_location))

                #df_user_loc = df_user_loc.where(size(col("user_location")) > 1 )

                df_user_loc = df_user_loc.withColumn('length_user_loc', length(df_user_loc.user_location_all_trim))

                df_user_loc = df_user_loc.where(col("length_user_loc") > 1)

                #df_user_loc = df_user_loc.withColumn("user_loc_int",col("user_location_all_trim").cast("int"))

                #df_user_loc = df_user_loc.where(col("user_loc_int").isNull())
            
    
                filter_udf = udf(regex_filter, BooleanType())

                df_filtered = df_user_loc.withColumn("special" , filter_udf(df_user_loc.user_location_all_trim))

                df_final = df_filtered.where(df_filtered1.special == True)

                df_final.drop('user_location_trim','user_location_all_trim','length_user_loc','user_loc_int','special2')

                #df_final.coalesce(4).write.parquet("s3a://mapthetech/Parquet/Users/users.parquet")
                df_final.repartition(4,'user_location').write.parquet("s3a://mapthetech/Parquet/Users/userstestpart.parquet")

                df_loc_detail = df_final.select('user_location')
                df_just_loc = df_loc_detail.dropDuplicates()
                #df_just_loc.coalesce(3).write.parquet("s3a://mapthetech/usersloc.parquet")
                df_just_loc.repartition(4,'user_location').write.parquet("s3a://mapthetech/userloctest1part.parquet")
         

    return 

               
def main():
    """
    Description: This is the main function that first creats spark session

    Arguments:
         None

    Returns:
        None
    """
    spark = create_spark_session()
    read_xml_write_parquet(spark)



if __name__ == "__main__":
    main()
                 
