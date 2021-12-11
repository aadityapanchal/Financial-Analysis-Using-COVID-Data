import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import datetime
from pyspark.sql import SparkSession, functions, types
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import *

forex_schema = types.StructType([
    types.StructField('date', types.StringType()),
    types.StructField('close', types.FloatType()),
    types.StructField('volume', types.FloatType()),
    types.StructField('open', types.FloatType()),
    types.StructField('high', types.FloatType()),
    types.StructField('low', types.FloatType()),
])

def read_data(file, forex_name):
    df = spark.read.option('header', 'true').csv(file, schema = forex_schema).withColumn('forex_type', lit(forex_name))
    df = df.select(df['forex_type'], to_date(df['date'], 'MM/dd/yyyy').alias('date'), df['close'], df['open'], df['volume'], df['high'], df['low'])
    return df

def create_union(*args):
    df = args[0]
    for i in args:
        if (i != args[0]):
            df = df.union(i)
    return df

def main():
    df_USDAUD = read_data('./datasets/forex/USDAUD.csv', 'USDAUD')
    df_USDBRL = read_data('./datasets/forex/USDBRL.csv', 'USDBRL')
    df_USDCAD = read_data('./datasets/forex/USDCAD.csv', 'USDCAD')
    df_USDCHF = read_data('./datasets/forex/USDCHF.csv', 'USDCHF')
    df_USDEUR = read_data('./datasets/forex/USDEUR.csv', 'USDEUR')
    df_USDGBP = read_data('./datasets/forex/USDGBP.csv', 'USDGBP')
    df_USDINR = read_data('./datasets/forex/USDINR.csv', 'USDINR')
    df_USDJPY = read_data('./datasets/forex/USDJPY.csv', 'USDJPY')
    df_USDMXN = read_data('./datasets/forex/USDMXN.csv', 'USDMXN')
    df_USDRUB = read_data('./datasets/forex/USDRUB.csv', 'USDRUB')
    df = create_union(df_USDAUD, df_USDBRL, df_USDCAD, df_USDCHF, df_USDEUR, df_USDGBP, df_USDINR, df_USDJPY, df_USDMXN, df_USDRUB)
    df.write.format("org.apache.spark.sql.cassandra").mode("overwrite").option("confirm.truncate","true").options(keyspace='dataflix',table='forex').save()
    print('saved stocks data to Cassandra')



if __name__ == '__main__':
    cluster_seeds = ['127.0.0.1:9042']
    spark = SparkSession.builder.appName('load forex data').config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main()
