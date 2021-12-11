from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, functions, types
from datetime import date, timedelta
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+


sys.path.append('./insert_to_cassandra')


def get_covid19_conifrmed_US_schema():
    covid19_conifrmed_US_schema = types.StructType([
        types.StructField('date', types.DateType()),
        types.StructField('city', types.StringType()),
        types.StructField('province', types.StringType()),
        types.StructField('total_cases_us', types.LongType()),
        types.StructField('new_cases_us', types.LongType())
    ])
    return covid19_conifrmed_US_schema

def get_covid19_conifrmed_global_schema():
    covid19_conifrmed_global_schema = types.StructType([
        types.StructField('date', types.DateType()),
        types.StructField('country', types.StringType()),
        #types.StructField('province', types.StringType()),
        types.StructField('total_cases_global', types.LongType()),
        types.StructField('new_cases_global', types.LongType())

    ])
    return covid19_conifrmed_global_schema

def get_covid19_deaths_us_schema():
    covid19_conifrmed_global_schema = types.StructType([
        types.StructField('date', types.DateType()),
        types.StructField('city', types.StringType()),
        types.StructField('province', types.StringType()),
        types.StructField('total_deaths_us', types.LongType()),
        types.StructField('new_deaths_us', types.LongType())
    ])
    return covid19_conifrmed_global_schema


def get_covid19_deaths_global_schema():
    covid19_deaths_global_schema = types.StructType([
        types.StructField('date', types.DateType()),
        types.StructField('country', types.StringType()),
        #types.StructField('province', types.StringType()),
        types.StructField('total_deaths_global', types.LongType()),
        types.StructField('new_deaths_global', types.LongType())
    ])
    return covid19_deaths_global_schema

def get_covid19_recovered_global_schema():
    covid19_recovered_global_schema = types.StructType([
        types.StructField('date', types.DateType()),
        types.StructField('country', types.StringType()),
        #types.StructField('province', types.StringType()),
        types.StructField('total_recovered_global', types.LongType()),
        types.StructField('new_recovered_global', types.LongType())
    ])
    return covid19_recovered_global_schema



def map_function_US(line):
    #print(line)
    data = line.split(",")
    #print(data)
    city = data[5]
    province = data[6]
    start_date = date(2020, 1, 22)
    size = len(data)
    prev_day_cases = int(data[13])
    for i in range(13, size):
        new_cases = (int(data[i])-prev_day_cases) if (int(data[i])-prev_day_cases) > 0 else 0
        #print(new_cases)
        yield(start_date, city, province, int(data[i]), new_cases)
        prev_day_cases = int(data[i])
        if(i == size):
            start_date = date(2020, 1, 22)
        else :
            start_date = start_date + timedelta(days=1)

def map_function_global(line):
    #print(line)
    total_columns = 674
    data = line.split(",")
    country = data[1]
    start_date = date(2020, 1, 22)
    size = len(data)
    extra_columns = size - total_columns
    prev_day_cases = 0
    for i in range(4 + extra_columns, size):
        new_cases = (int(data[i])-prev_day_cases) if (int(data[i])-prev_day_cases) > 0 else 0
        yield(start_date, country, int(data[i]), new_cases)
        prev_day_cases = int(data[i])

        if(i == size):
            start_date = date(2020, 1, 22)
        else:
            start_date = start_date + timedelta(days=1)

def map_function_deaths_US(line):
    #print(line)
    #print(line)
    data = line.split(",")
    #print(data)
    city = data[5]
    province = data[6]
    start_date = date(2020, 1, 22)
    size = len(data)
    prev_day_cases = 0
    for i in range(14, size):
        new_cases = (int(data[i])-prev_day_cases) if (int(data[i])-prev_day_cases) > 0 else 0
        yield(start_date, city, province, int(data[i]), new_cases)
        prev_day_cases = int(data[i])
        if(i == size):
            start_date = date(2020, 1, 22)
        else :
            start_date = start_date + timedelta(days=1)


def main():
    #etl for covid data for us and save to cassandra db
    covid_us_rdd = sc.textFile('./datasets/covid/time_series_covid19_confirmed_US.csv')
    covid_us_rdd_header = covid_us_rdd.first()
    print("inside shell script")
    covid_us_rdd_body = covid_us_rdd.filter(lambda line : line != covid_us_rdd_header)
    covid_data_us_df = spark.createDataFrame(covid_us_rdd_body.flatMap(map_function_US), schema = get_covid19_conifrmed_US_schema())
    covid_data_us_df.write.format("org.apache.spark.sql.cassandra") \
                    .mode("overwrite").option("confirm.truncate","true") \
                    .options(keyspace='dataflix',table='covid19_cases_us').save()
    print("saved to cassandra db " + "covid_data_us_df")

    #etl for covid data global and save to cassandra db
    covid_global_rdd = sc.textFile('./datasets/covid/time_series_covid19_confirmed_global.csv')
    covid_global_rdd_header = covid_global_rdd.first()
    covid_global_rdd_body = covid_global_rdd.filter(lambda line : line != covid_global_rdd_header)
    covid_data_global_df = spark.createDataFrame(covid_global_rdd_body.flatMap(map_function_global), schema = get_covid19_conifrmed_global_schema())
    covid_data_global_df.write.format("org.apache.spark.sql.cassandra") \
                    .mode("overwrite").option("confirm.truncate","true") \
                    .options(keyspace='dataflix',table='covid19_cases_global').save()
    print("saved to cassandra db " + "covid_data_global_df")

    #etl for deaths for us and save to cassandra db
    deaths_us_rdd = sc.textFile('./datasets/covid/time_series_covid19_deaths_US.csv')
    deaths_us_rdd_header = deaths_us_rdd.first()
    deaths_us_rdd_body = deaths_us_rdd.filter(lambda line : line != deaths_us_rdd_header)
    deaths_data_us_df = spark.createDataFrame(deaths_us_rdd_body.flatMap(map_function_deaths_US), schema = get_covid19_deaths_us_schema())
    deaths_data_us_df.write.format("org.apache.spark.sql.cassandra") \
                    .mode("overwrite").option("confirm.truncate","true") \
                    .options(keyspace='dataflix',table='covid19_deaths_us').save()
    print("saved to cassandra db " + "deaths_data_us_df")

    #etl for deaths for global and save to cassandra db
    deaths_global_rdd = sc.textFile('./datasets/covid/time_series_covid19_deaths_global.csv')
    deaths_global_rdd_header = deaths_global_rdd.first()
    deaths_global_rdd_body = deaths_global_rdd.filter(lambda line : line != deaths_global_rdd_header)
    deaths_data_global_df = spark.createDataFrame(deaths_global_rdd_body.flatMap(map_function_global), schema = get_covid19_deaths_global_schema())
    deaths_data_global_df.write.format("org.apache.spark.sql.cassandra") \
                    .mode("overwrite").option("confirm.truncate","true") \
                    .options(keyspace='dataflix',table='covid19_deaths_global').save()
    print("saved to cassandra db " + "deaths_data_global_df")

    #etl for recovered cases for global and save to cassandra db
    recovered_global_rdd = sc.textFile('./datasets/covid/time_series_covid19_recovered_global.csv')
    recovered_global_rdd_header = recovered_global_rdd.first()
    recovered_global_rdd_body = deaths_global_rdd.filter(lambda line : line != recovered_global_rdd_header)
    recovered_global_df = spark.createDataFrame(recovered_global_rdd_body.flatMap(map_function_global), schema = get_covid19_recovered_global_schema())
    recovered_global_df.write.format("org.apache.spark.sql.cassandra") \
                    .mode("overwrite").option("confirm.truncate","true") \
                    .options(keyspace='dataflix',table='covid19_recovered_cases_global').save()
    print("saved to cassandra db " + "recovered_global_df")




if __name__ == '__main__':
    print("main")
    cluster_seeds = ['127.0.0.1:9042']
    spark = SparkSession.builder.appName('Covid -19 Data migration to cassandra') \
        .config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
    #spark = SparkSession.builder.appName('covid cases us confirmed').getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    main()
