from pyspark.sql import SparkSession, functions, types


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
