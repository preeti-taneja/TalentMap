# python script to find Geo COordinates Using GeoPY
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.functions import isnan, when, count, col,size
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StructType, StructField, DecimalType,StringType, Row,FloatType,BooleanType
from pyspark.sql.functions import *
import time
from time import sleep
from random import randint
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError,GeocoderQuotaExceeded


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



"""
def geoparam(s):
    geolocator = Nominatim(user_agent="Test")
    location = geolocator.geocode(s)
    #print(location)
    if location is not None: 
        location_rev = geolocator.reverse((location.latitude, location.longitude))
        address = location_rev.raw['address']
        city = address.get('city','')
        state = address.get('state', '')
        country = address.get('country','')                
        return Row('latitude', 'longitude','city','state','country')(location.latitude,location.longitude,city,state,country)
    else:
        return Row('latitude', 'longitude','city','state','country')(None,None,None,None,None) 

"""

 def geoparam(s):
    #user_agent = 'user_me_{}'.format(randint(10000,99999))
    geolocator = Nominatim(user_agent="test")
    #time.sleep(1.1)
    try:
        location = geolocator.geocode(s,timeout = None)
        if  location is not None:
            return  Row('latitude', 'longitude')(location.latitude,location.longitude)
        else:
            return Row('latitude', 'longitude')(None,None)
    except GeocoderTimedOut:
        #logging.info('TIMED OUT: GeocoderTimedOut: Retrying...')
        sleep(randint(1*100,2*100)/100)
        return geoparam(s)
    except ( GeocoderQuotaExceeded) as e:
        #if GeocoderQuotaExceeded:
         #   print(e)
        #else:
        #:print(f'Location not found: {e}')
        return Row('latitude', 'longitude')(None,None)


def get_coordinates(spark):

    #log = log4jLogger.LogManager.getLogger(__name__) 

    df_user_loc = spark.read.parquet("s3a://userfourteenparts/part-00012-60d1deeb-fd50-416c-8151-25d9005ba4d5-c000.snappy.parquet")

    schema = StructType([StructField('latitude', FloatType(), False),StructField("longitude", FloatType(), False)])

    geoparam_udf = F.udf(geoparam, schema)
    df_user_loc = df_user_loc.withColumn("Output", geoparam_udf(df_user_loc["user_location"]))
    df_user_loc = df_user_loc.select('*', "Output.*")
    #df_user_loc.show(truncate=False)
    df_user_loc.repartition(1).write.parquet("s3a://usercoordinates/user_coordinates_loc12.parquet")
    
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
    get_coordinates(spark)



if __name__ == "__main__":
    main()
                 