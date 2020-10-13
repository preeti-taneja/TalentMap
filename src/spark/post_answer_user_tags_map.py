#This Python script maps user and tags , first the answers are mapped to question tags and than each user with the tag.
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.functions import isnan, when, count, col
from pyspark.sql import Window
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
        .appName("Count Of Answers By a User") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def read_posts_group_user_answer(spark):

    region = 'us-west-2'
    bucket = 'mapthetech'
    key = 'Posts.xml'
    s3file = f's3a://{bucket}/{key}'
    spark._jsc.hadoopConfiguration().set('fs.s3a.endpoint', f's3-{region}.amazonaws.com')

    df_p = spark.read.format("com.databricks.spark.xml").option("rootTag","posts").option("rowTag","row").load(s3file)

    df_posts_answers = df_p.select(col('_Id').alias("post_ans_id"),\
        col('_PostTypeId').alias("post_ans_type_id"),\
        col('_ParentId').alias("parent_question_id"),\
        col('_OwnerUserId').alias("post_ans_user_id"))


    df_posts_question = df_p.select(col('_Id').alias("post_ques_id"),\
        col('_PostTypeId').alias("post_ques_type_id"),\
        col('_Tags').alias("post_tags"))

    df_posts_answers = df_posts_answers.where((col("post_ans_type_id") == 2 ) & (col("post_ans_user_id").isNotNull()) )


    df_posts_question = df_posts_question.where((col("post_tags").isNotNull() ) & (col("post_ques_type_id") == 1 ) )

    df_posts_ans_tags = df_posts_answers.join(df_posts_question,df_posts_question.post_ques_id == df_posts_answers.parent_question_id,"inner")

    df_posts = df_posts_ans_tags.select(df_posts_ans_tags.post_ans_id,df_posts_ans_tags.post_ans_user_id,df_posts_ans_tags.post_tags)

    df_posts = df_posts.withColumn('tag1',F.regexp_replace('post_tags', '(><)', ','))
    df_posts = df_posts.withColumn('tag2',F.translate('tag1', '<>', ''))
    df_posts = df_posts.withColumn('tag3',F.split('tag2',','))
    #df_posts.show(truncate = False)
    df_posts_expl = df_posts.select(df_posts.post_ans_id,df_posts.post_ans_user_id,F.explode(df_posts.tag3).alias("tags_distinct"))


    window = Window.partitionBy("post_ans_user_id","tags_distinct")

    df_final =  df_posts_expl.select(df_posts_expl.post_ans_user_id,df_posts_expl.tags_distinct,F.count(df_posts_expl.post_ans_id).over(window).alias("total_count")).distinct()

    df_final.coalesce(3).write.parquet("s3a://mapthetech/Parquet/users_ans.parquet")



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
    read_posts_group_user_answer(spark)



if __name__ == "__main__":
    main()
                 