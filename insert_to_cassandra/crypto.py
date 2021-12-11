import sys
import uuid
from pyspark.sql.functions import lit
from pyspark.sql.functions import *
from pyspark.sql import SparkSession, functions, types
from pyspark.sql.types import StructType
from datetime import datetime
from uuid import uuid4
from uuid import UUID
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def Crypto_schema():
    Crypto_schema = types.StructType([
        types.StructField('date', types.StringType(), True),
        types.StructField('close', types.FloatType(), True),
        types.StructField('volume', types.StringType(), True),
        types.StructField('open', types.FloatType(), True),
        types.StructField('high', types.FloatType(), True),
        types.StructField('low', types.FloatType(), True),
    ])
    return Crypto_schema

def read_data(input, crypto_name, tablename):
    df = spark.read.option('header', 'true').csv(input, schema = Crypto_schema()).withColumn('cryptotype', lit(crypto_name))
    df = df.select(to_date(df["date"],"M/d/y").alias("date"),df["close"],df["volume"],df["open"],df["high"],df["low"], df["cryptotype"])
    return df

def main():
    df_BIN = read_data('./datasets/crypto/binance.csv', 'Binance', 'binance')
    df_BTC = read_data('./datasets/crypto/bitcoin.csv', 'Bitcoin', 'bitcoin')
    df_DASH = read_data('./datasets/crypto/dash.csv', 'Dash', 'dash')
    df_DOGE = read_data('./datasets/crypto/doge.csv', 'Doge', 'doge')
    df_ETH = read_data('./datasets/crypto/ethereum.csv', 'Ethereum', 'ethereum')
    df_LTC = read_data('./datasets/crypto/litecoin.csv', 'Litecoin', 'litecoin')
    df_RIP = read_data('./datasets/crypto/ripple.csv', 'Ripple', 'ripple')
    df_TRON = read_data('./datasets/crypto/tron.csv', 'Tron', 'tron')
    df_MONERO = read_data('./datasets/crypto/xmr.csv', 'Monero', 'monero')
    df = df_BIN.union(df_BTC).union(df_ETH).union(df_DASH).union(df_DOGE).union(df_LTC).union(df_RIP).union(df_TRON).union(df_MONERO)
    #df.show(10000)
    #df.write.format("org.apache.spark.sql.cassandra").options(table='stocks_shrink', keyspace='final_project').mode('overwrite').save()
    df.write.format("org.apache.spark.sql.cassandra").mode("overwrite").option("confirm.truncate","true").options(keyspace='dataflix',table='crypto').save()


if __name__ == '__main__':
    spark = SparkSession.builder.appName('BTCLoad').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    cluster_seeds = ['127.0.0.1:9042']
    spark = SparkSession.builder.appName('Spark Cassandra example').config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
    main() #, key_space,table























