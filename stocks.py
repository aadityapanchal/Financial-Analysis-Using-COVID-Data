import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import datetime
from pyspark.sql import SparkSession, functions, types
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import *


stocks_schema = types.StructType([
    types.StructField('date', types.StringType()),
    types.StructField('close', types.FloatType()),
    types.StructField('volume', types.FloatType()),
    types.StructField('open', types.FloatType()),
    types.StructField('high', types.FloatType()),
    types.StructField('low', types.FloatType()),
])

def read_data(file, stock_name):
    df = spark.read.option('header', 'true').csv(file, schema = stocks_schema).withColumn('stock_type', lit(stock_name))
    df = df.select(df['stock_type'], to_date(df['date'], 'MM/dd/yyyy').alias('date'), df['close'], df['open'], df['volume'], df['high'], df['low'])
    return df

def create_union(*args):
    df = args[0]
    for i in args:
        if (i != args[0]):
            df = df.union(i)
    return df

def main():
    df_dow_jones = read_data('./datasets/stocks/DOWJONES.csv', 'dow_jones')
    df_ndx_composite = read_data('./datasets/stocks/NDX Composite.csv', 'ndx_composite')
    df_ndx = read_data('./datasets/stocks/NDX.csv', 'ndx')
    df_nyse = read_data('./datasets/stocks/NYSE.csv', 'nyse')
    df_rut = read_data('./datasets/stocks/RUT.csv', 'rut')
    df_spx = read_data('./datasets/stocks/SPX.csv', 'spx')
    df = create_union(df_dow_jones, df_ndx_composite, df_ndx, df_nyse, df_rut, df_spx)
    #df.show(10000)
    #df.write.format("org.apache.spark.sql.cassandra").options(table='stocks_shrink', keyspace='final_project').mode('overwrite').save()
    df.write.format("org.apache.spark.sql.cassandra").mode("overwrite").option("confirm.truncate","true").options(keyspace='final_project',table='stocks').save()


if __name__ == '__main__':
    cluster_seeds = ['127.0.0.1:9042']
    spark = SparkSession.builder.appName('load stocks data').config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main()
