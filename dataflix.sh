#!/bin/bash
echo "##### Saving all the datasets into cassandra db #####"

# for types in covid stocks crypto commodities forex
# #for types in commodities
# do
# 	echo "##### Saving $types to cassandra db #####"
# 	spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.1.0 --conf spark.sql.extensions=com.datastax.spark.connector.CassandraSparkExtensions "insert_to_cassandra/$types.py"
# 	echo "##### Saved $types data to cassandra db"
# 	sleep
# done

echo "##### Loading the data from db and implementing ml models #####"

# for types_model in stocks_model crypto_model commodities_model forex_model
for types_model in commodities_model
do
	echo "##### Creating $model ML models #####"
	spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.1.0 --conf spark.sql.extensions=com.datastax.spark.connector.CassandraSparkExtensions "model_train/$types_model.py"
	echo "##### Saved $model ML model"
done
