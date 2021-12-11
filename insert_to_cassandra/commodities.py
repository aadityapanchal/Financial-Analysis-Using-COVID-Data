import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import datetime
from pyspark.sql import SparkSession, functions, types
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import *

sys.path.append('./datasets/commodities')

def data_schema():
    schema = types.StructType([
        types.StructField('date', types.StringType()),
        types.StructField('close', types.FloatType()),
        types.StructField('volume', types.FloatType()),
        types.StructField('open', types.FloatType()),
        types.StructField('high', types.FloatType()),
        types.StructField('low', types.FloatType())
    ])
    return schema

def format_input(data, market, type):
    df = spark.read.option('header', 'true').csv(data, schema = data_schema()).withColumn('market', lit(market)).withColumn('type',lit(type))
    df = df.select(df["market"], df["type"], to_date(df['date'], 'M/d/y').alias('date'), df['close'], df['open'], df['volume'], df['high'], df['low'])
    return df

def unionAll(*args):
    df = args[0]
    for i in args:
        if (i != args[0]):
            df = df.union(i)
    return df

def main():
    # ***** ENERGY *****
    # CL:NMX => Crude Oil
    df_1 = format_input("./datasets/commodities/CLNMX_CrudeOil_HistoricalData.csv", "Energy", "CLNMX")
    # HO:NMX => Heating Oil
    df_2 = format_input("./datasets/commodities/HONMX_HeatingOil_HistoricalData.csv", "Energy", "HONMX")
    # NG:NMX => Natural Gas
    df_3 = format_input("./datasets/commodities/NGNMX_NaturalGas_HistoricalData.csv", "Energy", "NGNMX")

    # ***** GRAINS *****
    # ZR => Rough Rice
    df_4= format_input("./datasets/commodities/ZR_RoughRice_HistoricalData.csv", "Grains", "ZR")
    # ZS => Soybeans
    df_5 = format_input("./datasets/commodities/ZS_Soybeans_HistoricalData.csv", "Grains", "ZS")
    # ZL => Soybean Oil
    df_6 = format_input("./datasets/commodities/ZL_SoybeanOil_HistoricalData.csv", "Grains", "ZL")

    # ***** MEATS *****
    # GF => Feeder Cattle
    df_7 = format_input("./datasets/commodities/GF_FeederCattle_HistoricalData.csv", "Meats", "GF")
    # LE => Live Cattle
    df_8 = format_input("./datasets/commodities/LE_LiveCattle_HistoricalData.csv", "Meats", "LE")
    # DC => Milk
    df_9 = format_input("./datasets/commodities/DC_Milk_HistoricalData.csv", "Meats", "DC")

    # ***** METALS *****
    # HG:CMX => Copper
    df_10 = format_input("./datasets/commodities/HGCMX_Copper_HistoricalData.csv", "Metals", "HGCMX")
    # GC:CMX => Gold
    df_11 = format_input("./datasets/commodities/GCCMX_Gold_HistoricalData.csv", "Metals", "GCCMX")
    # SI:CMX => Silver
    df_12 = format_input("./datasets/commodities/SICMX_Silver_HistoricalData.csv", "Metals", "SICMX")

    # result = df_1.union(df_2).union(df_3).union(df_4).union(df_5)
    result = unionAll(df_1, df_2, df_3, df_4, df_5, df_6, df_7, df_8, df_9, df_10, df_11, df_12)

    result.write.format("org.apache.spark.sql.cassandra").mode("overwrite").option("confirm.truncate","true").options(keyspace='dataflix',table='commodities').save()
    print('saved commodities data to Cassandra')


if __name__ == '__main__':
    cluster_seeds = ['127.0.0.1:9042']
    spark = SparkSession.builder.appName('load commodities data').config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main()    