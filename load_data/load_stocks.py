import sys
import re
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from math import sqrt
from pyspark.sql import SparkSession, functions, types

# def main():
#
#     df_stocks = spark.read.format("org.apache.spark.sql.cassandra").options(table='forex', keyspace='dataflix').load().coalesce(1)
#     df_stocks = df_stocks.select(df_stocks['date'], df_stocks['forex_type'], df_stocks['close'])
#     USDRUB_df = df_stocks.filter(df_stocks['forex_type'] == 'USDRUB')
#     USDGBP_df = df_stocks.filter(df_stocks['forex_type'] == 'USDGBP')
#     USDCHF_df = df_stocks.filter(df_stocks['forex_type'] == 'USDCHF')
#     USDCAD_df = df_stocks.filter(df_stocks['forex_type'] == 'USDCAD')
#     USDEUR_df = df_stocks.filter(df_stocks['forex_type'] == 'USDEUR')
#     USDJPY_df = df_stocks.filter(df_stocks['forex_type'] == 'USDJPY')
#     USDAUD_df = df_stocks.filter(df_stocks['forex_type'] == 'USDAUD')
#     USDBRL_df = df_stocks.filter(df_stocks['forex_type'] == 'USDBRL')
#     USDINR_df = df_stocks.filter(df_stocks['forex_type'] == 'USDINR')
#     USDMXN_df = df_stocks.filter(df_stocks['forex_type'] == 'USDMXN')
#
#     USDRUB_df.write.mode('overwrite').option('header', 'true').csv('./cleaned_data/forex/USDRUB_df')
#     USDGBP_df.write.mode('overwrite').option('header', 'true').csv('./cleaned_data/forex/USDGBP_df')
#     USDCHF_df.write.mode('overwrite').option('header', 'true').csv('./cleaned_data/forex/USDCHF_df')
#     USDCAD_df.write.mode('overwrite').option('header', 'true').csv('./cleaned_data/forex/USDCAD_df')
#     USDEUR_df.write.mode('overwrite').option('header', 'true').csv('./cleaned_data/forex/USDEUR_df')
#     USDJPY_df.write.mode('overwrite').option('header', 'true').csv('./cleaned_data/forex/USDJPY_df')
#     USDAUD_df.write.mode('overwrite').option('header', 'true').csv('./cleaned_data/forex/USDAUD_df')
#     USDBRL_df.write.mode('overwrite').option('header', 'true').csv('./cleaned_data/forex/USDBRL_df')
#     USDINR_df.write.mode('overwrite').option('header', 'true').csv('./cleaned_data/forex/USDINR_df')
#     USDMXN_df.write.mode('overwrite').option('header', 'true').csv('./cleaned_data/forex/USDMXN_df')
#
#
#
# if __name__ == '__main__':
#     #inputs = sys.argv[1]
#     # key_space = sys.argv[1]
#     # table = sys.argv[2]
#     cluster_seeds = ['127.0.0.1:9042']
#     spark = SparkSession.builder.appName('Spark Cassandra example').config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
#     #session = cluster_seeds.connect(key_space)
#     assert spark.version >= '3.0' # make sure we have Spark 3.0+
#     spark.sparkContext.setLogLevel('WARN')
#     sc = spark.sparkContext
#     # main(key_space,table)
#     main()

def get_forex_data(spark, stocks_type):

    df_stocks = spark.read.format("org.apache.spark.sql.cassandra").options(table='stocks', keyspace='dataflix').load().coalesce(1)
    df_stocks = df_stocks.select(df_stocks['date'], df_stocks['stock_type'], df_stocks['close'])
    # print('./cleaned_data/forex/'+ forex_type)
    # df_stocks.show(10)
    df_stocks = df_stocks.filter(df_stocks['stock_type'] == stocks_type)
    # df_stocks.show(10)
    df_stocks.write.mode('overwrite').option('header', 'true').csv('./cleaned_data/stocks/'+stocks_type)

    return df_stocks
