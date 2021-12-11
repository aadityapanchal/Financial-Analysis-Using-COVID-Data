import sys
import re
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from math import sqrt
from pyspark.sql import SparkSession, functions, types


def get_commodities_data(spark, market, type):
    
    df_com = spark.read.format("org.apache.spark.sql.cassandra").options(table='commodities', keyspace='dataflix').load().coalesce(1)
    df_com = df_com.select("date", "type", "market", "close")
    # print('../cleaned_data/commodity/'+market+type)
    # df_com.show()
    
    df_com = df_com.filter( df_com["type"] == type) #f_com["market"] == market &
    df_com.write.mode('overwrite').option('header', 'true').csv('../cleaned_data/commodity/'+market+'/'+type)
    return df_com

   
