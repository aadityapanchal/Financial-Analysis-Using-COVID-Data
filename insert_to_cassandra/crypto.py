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
    # maxm_close = df.select(max("close")).collect()[0][0]
    # minm_close = df.select(min("close")).collect()[0][0]
    # mean_close = (maxm_close-minm_close)/2
    # maxm_open = df.select(max("open")).collect()[0][0]
    # minm_open = df.select(min("open")).collect()[0][0]
    # mean_open = (maxm_open-minm_open)/2
    # per_df = df.select(df['date'], df['cryptotype'],((((df['close']-mean_close)/mean_close))*100).alias('close_percentage'), ((((df['open']-mean_open)/mean_open))*100).alias('open_percentage'))
    # per_df.show()
    # per_df.write.option('header', 'true').csv("./percentage_crypto_saved")
    # #df.write.format("org.apache.spark.sql.cassandra").mode("overwrite").option("confirm.truncate","true").options(keyspace='dataflix',table=tablename).save()
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

#   CREATE TABLE crypto (date date, close FLOAT, volume text, open FLOAT, high FLOAT, low FLOAT, cryptotype TEXT, primary key(date));
#   CREATE TABLE bitcoin (date date, close FLOAT, volume text, open FLOAT, high FLOAT, low FLOAT, cryptotype TEXT, primary key(date));
#   CREATE TABLE ripple (date date, close FLOAT, volume text, open FLOAT, high FLOAT, low FLOAT, cryptotype TEXT, primary key(date));
#   CREATE TABLE ethereum (date date, close FLOAT, volume text, open FLOAT, high FLOAT, low FLOAT, cryptotype TEXT, primary key(date));
























# def main(BTC_inputs,ETH_inputs,RIP_inputs): #, key_space, table

#     df_BTC = spark.read.option('header','true').csv(BTC_inputs, schema = Crypto_schema()).withColumn("cryptotype", lit("BTC"))
#     df_BTC1 = df_BTC.select(to_date(df_BTC["date"],"M/d/y").alias("date"),df_BTC["close"],df_BTC["volume"],df_BTC["open"],df_BTC["high"],df_BTC["low"], df_BTC["cryptotype"])
#     df_BTC1.write.format("org.apache.spark.sql.cassandra").mode("overwrite").options(table='temp', keyspace='dataflix').option("confirm.truncate","true") .save()
#     # df_ETH = spark.read.option('header','true').csv(ETH_inputs, schema = Crypto_schema()).withColumn("Crypto_Type", lit("ETH"))
#     # df_ETH1 = df_ETH.select(to_date(df_ETH["Date"],"M/d/y").alias("Date"),df_ETH["Close/Last"],df_ETH["Volume"],df_ETH["Open"],df_ETH["High"],df_ETH["Low"], df_ETH["Crypto_Type"])

#     # df_RIP = spark.read.option('header','true').csv(RIP_inputs, schema = Crypto_schema()).withColumn("Crypto_Type", lit("RIP"))
#     # df_RIP1 = df_RIP.select(to_date(df_RIP["Date"],"M/d/y").alias("Date"),df_RIP["Close/Last"],df_RIP["Volume"],df_RIP["Open"],df_RIP["High"],df_RIP["Low"], df_RIP["Crypto_Type"])

#     # temp = df_BTC1.union(df_ETH1)
#     # CryptoDF = temp.union(df_RIP1)
#     # # CryptoDF.show(10)
#     # CryptoGroupBy = CryptoDF.groupBy(CryptoDF["Date"]).agg(functions.avg(CryptoDF["Close/Last"]).alias("AVG_Close"),
#     #                                                        functions.avg(CryptoDF["Open"]).alias("AVG_Open"),
#     #                                                        ).orderBy(CryptoDF["Date"].desc())
#     # uuidUdf = udf(lambda: str(uuid.uuid4()), StringType()).asNondeterministic()
#     # CryptoGroupBy = CryptoGroupBy.withColumn("id", uuidUdf())
#     # CryptoGroupBy.show()
#     # print(CryptoGroupBy.dtypes)
#     # CryptoGroupBy.write.format("org.apache.spark.sql.cassandra").mode("overwrite").options(table=table, keyspace=key_space).option("confirm.truncate","true") .save()

#     # cluster = Cluster(['node1.local'])
#     # session = cluster.connect(keyspace)
#     # rows = session.execute('SELECT Date, bytes FROM nasalogs WHERE host=%s', [somehost])


# if __name__ == '__main__':
#     spark = SparkSession.builder.appName('BTCLoad').getOrCreate()
#     assert spark.version >= '3.0' # make sure we have Spark 3.0+
#     spark.sparkContext.setLogLevel('WARN')
#     sc = spark.sparkContext
#     BTC_inputs = sys.argv[1]
#     ETH_inputs = sys.argv[2]
#     RIP_inputs = sys.argv[3]
#     # key_space = sys.argv[4]
#     # table = sys.argv[5]
#     cluster_seeds = ['127.0.0.1:9042']
#     spark = SparkSession.builder.appName('Spark Cassandra example').config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
#     main(BTC_inputs,ETH_inputs,RIP_inputs) #, key_space,table







# print("Bitcoin DF---------------------->")
    # print(df_BTC1.dtypes)
    # print("Ethereum DF---------------------->")
    # df_ETH1.show(10)
    # print(df_ETH1.dtypes)# print("Ripple DF---------------------->")
    # df_RIP1.show(10)
    # print(df_RIP1.dtypes)
