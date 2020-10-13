#import boto3
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.functions import isnan, when, count, col
from pyspark.sql import Window
from pyspark.sql.functions import year, month
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
        .appName("Questions Over The Years By Each Tag") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def read_posts_save_questions(spark):

    region = 'us-west-2'
    bucket = 'mapthetech'
    key = 'Posts.xml'
    s3file = f's3a://{bucket}/{key}'
    spark._jsc.hadoopConfiguration().set('fs.s3a.endpoint', f's3-{region}.amazonaws.com')

    df_p = spark.read.format("com.databricks.spark.xml").option("rootTag","posts").option("rowTag","row").load(s3file)
    df_posts_question = df_p.select(col('_Id').alias("post_id"),\
        col('_PostTypeId').alias("post_type_id"),\
        col('_CreationDate').cast('timestamp').alias("post_created_date"),\
        col('_Tags').alias("post_tags"))
   
    df_posts_question = df_posts_question.where( (col("post_tags").isNotNull() ) & (col("post_type_id") == 1 ) )
    df_posts_question = df_posts_question.withColumn("year",year(df_posts_question.post_created_date))
    df_posts_question = df_posts_question.withColumn("month",month(df_posts_question.post_created_date))
    df_posts_question = df_posts_question.withColumn('tag1',F.regexp_replace('post_tags', '(><)', ','))
    df_posts_question = df_posts_question.withColumn('tag2',F.translate('tag1', '<>', ''))
    df_posts_question = df_posts_question.withColumn('tag3',F.split('tag2',','))
    df_posts_question = df_posts_question.select(df_posts_question.year,df_posts_question.month,df_posts_question.post_id,F.explode(df_posts_question.tag3).alias("tags_distinct"))

    window = Window.partitionBy("month","year","tags_distinct")
    df_final =  df_posts_question.select(df_posts_question.month,df_posts_question.year,df_posts_question.tags_distinct,F.count(df_posts_question.post_id).over(window).alias("total_count")).distinct()

    #df_final.coalesce(4).write.parquet("s3a://mapthetech/Parquet/Users_questions/users_questions.parquet")

    mode = "overwrite" 
    url = "jdbc:postgresql://10.0.0.9:5431/stack_db"
    properties = {"user": "postdb","password": "db","driver": "org.postgresql.Driver"}
    df_final.write.jdbc(url=url, table="question_tags_year", mode=mode, properties=properties)

    #df_final.coalesce(4).write.parquet("s3a://mapthetech/Parquet/Users_questions/users_questions.parquet")


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
    read_posts_save_questions(spark)



if __name__ == "__main__":
    main()
                 