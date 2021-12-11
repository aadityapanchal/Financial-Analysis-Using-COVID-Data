import sys
import re
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from math import sqrt
from pyspark.sql import SparkSession, functions, types

def get_individual_covid_data(spark):
    #df_stock = spark.read.format("org.apache.spark.sql.cassandra").options(table='stocks', keyspace='dataflix').load().coalesce(1)
    covid19_cases_global_df = spark.read.format("org.apache.spark.sql.cassandra").options(table='covid19_cases_global', keyspace='dataflix').load().coalesce(1)
    covid19_deaths_us_df = spark.read.format("org.apache.spark.sql.cassandra").options(table='covid19_deaths_us', keyspace='dataflix').load().coalesce(1)
    covid19_cases_us_df = spark.read.format("org.apache.spark.sql.cassandra").options(table='covid19_cases_us', keyspace='dataflix').load().coalesce(1)
    covid19_recovered_cases_global_df = spark.read.format("org.apache.spark.sql.cassandra").options(table='covid19_recovered_cases_global', keyspace='dataflix').load().coalesce(1)
    covid19_deaths_global_df = spark.read.format("org.apache.spark.sql.cassandra").options(table='covid19_deaths_global', keyspace='dataflix').load().coalesce(1)
    # covid19_cases_global_df.show(100, False)
    return covid19_cases_us_df, covid19_deaths_us_df, covid19_cases_global_df, covid19_deaths_global_df, covid19_recovered_cases_global_df



def get_covid_data(spark):

    covid19_cases_us_df, covid19_deaths_us_df, covid19_cases_global_df, covid19_deaths_global_df, covid19_recovered_cases_global_df = get_individual_covid_data(spark)

    cases_us_df = covid19_cases_us_df.groupBy(covid19_cases_us_df['date'], covid19_cases_us_df['province']).agg(functions.sum('total_cases_us').alias('sum_total_cases_us'), functions.sum('new_cases_us').alias('sum_new_cases_us'))
    deaths_us_df = covid19_deaths_us_df.groupBy(covid19_deaths_us_df['date'], covid19_deaths_us_df['province']).agg(functions.sum('total_deaths_us').alias('sum_total_deaths_us'), functions.sum('new_deaths_us').alias('sum_new_deaths_us'))
    final_us_states_df = cases_us_df.join(deaths_us_df, ['date', 'province'], 'inner')

    cases_us_date_df = covid19_cases_us_df.groupBy(covid19_cases_us_df['date']).agg(functions.sum('total_cases_us').alias('sum_total_cases_us'), functions.sum('new_cases_us').alias('sum_new_cases_us'))
    deaths_us_date_df = covid19_deaths_us_df.groupBy(covid19_deaths_us_df['date']).agg(functions.sum('total_deaths_us').alias('sum_total_deaths_us'), functions.sum('new_deaths_us').alias('sum_new_deaths_us'))
    final_us_date_df = cases_us_date_df.join(deaths_us_date_df, ['date'], 'inner')

    cases_global_date_df = covid19_cases_global_df.groupBy(covid19_cases_global_df['date']).agg(functions.sum('total_cases_global').alias('sum_total_cases_global'), functions.sum('new_cases_global').alias('sum_new_cases_global'))
    deaths_global_date_df = covid19_deaths_global_df.groupBy(covid19_deaths_global_df['date']).agg(functions.sum('total_deaths_global').alias('sum_total_death_global'), functions.sum('new_deaths_global').alias('sum_new_deaths_global'))
    final_global_date_df = cases_global_date_df.join(deaths_global_date_df, ['date'], 'inner')

    final_global_recovered_df = covid19_recovered_cases_global_df.groupBy(covid19_recovered_cases_global_df['date']).agg(functions.sum('total_recovered_global').alias('sum_total_recovered_global'), functions.sum('new_recovered_global').alias('sum_new_recovered_global'))

    #save all dataframes to csv files 
    final_us_states_df.write.mode('overwrite').option('header', 'true').csv('../cleaned_data/covid/final_us_states_df')
    final_us_date_df.write.mode('overwrite').option('header', 'true').csv('../cleaned_data/covid/final_us_date_df')
    final_global_date_df.write.mode('overwrite').option('header', 'true').csv('../cleaned_data/covid/final_global_date_df')
    final_global_recovered_df.write.mode('overwrite').option('header', 'true').csv('../cleaned_data/covid/final_global_recovered_df')

    #prepare the final dataframe for ml models and return the data frame     

    final_us_states_df = final_us_states_df.groupBy(final_us_states_df['date']).agg(functions.sum('sum_total_cases_us').alias('sum_total_cases_us'), functions.sum('sum_new_cases_us').alias('sum_new_cases_us'), functions.sum('sum_total_deaths_us').alias('sum_total_deaths_us'), functions.sum('sum_new_deaths_us').alias('sum_new_deaths_us'))
    final_df = final_us_states_df.join(final_global_date_df, ['date']) \
                           .join(final_global_recovered_df, ['date'])

    return final_df


# CREATE TABLE covid (date date, state text, fip Integer, cases Integer, deaths integer, primary key(date, state));
