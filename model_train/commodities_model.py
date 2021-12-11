import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types
from pyspark.ml import PipelineModel
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import StringIndexer, VectorAssembler, SQLTransformer
from pyspark.sql.functions import dayofyear
from pyspark.ml.regression import LinearRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator

from pyspark.ml import Pipeline
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.feature import VectorIndexer
from pyspark.ml.evaluation import RegressionEvaluator


sys.path.append('./load_data')
import load_commodities, load_covid

def main():

    # commodites = ['CLNMX', 'HONMX','NGNMX', 'ZR', 'ZS', 'ZL', 'GF','LE','DC', 'HGCMX','GCCMX','SICMX']

    commodities = {'grains':['ZR', 'ZS', 'ZL'],
                'energy': ['CLNMX', 'HONMX','NGNMX'],
                'meats': ['GF','LE','DC'],
                'metals': ['HGCMX','GCCMX','SICMX']}

    #print(commodities.keys())
    market_types = commodities.keys()

    for market_type in market_types:
        for commodity in commodities.get(market_type):
            print(market_type+"->"+commodity)

            commodity_df = load_commodities.get_commodities_data(spark, market_type, commodity)
            covid19_df = load_covid.get_covid_data(spark)
            join_covid19_com_df = covid19_df.join(commodity_df, ['date'])

            train, validate = join_covid19_com_df.randomSplit([0.75, 0.25])
            train = train.cache()
            validate = validate.cache()

            covid_assembler = VectorAssembler(inputCols=[ 'sum_total_cases_us', 'sum_new_cases_us', 'sum_total_deaths_us', 'sum_new_deaths_us', 'sum_total_cases_global', 'sum_new_cases_global', 'sum_total_death_global', 'sum_new_deaths_global', 'sum_total_recovered_global', 'sum_new_recovered_global'], outputCol='features')
            #lr = LinearRegression(maxIter = 20, regParam=0.2, elasticNetParam=0.9, fitIntercept=True, standardization=True, featuresCol='features', labelCol = 'close')
            gbt = GBTRegressor(maxIter = 20, maxDepth=5, maxBins=32, featuresCol='features', labelCol = 'close')
            pipeline = Pipeline(stages=[covid_assembler, gbt])
            model = pipeline.fit(train)

            predictions = model.transform(validate)
            predictions.select(predictions.date,predictions.close, predictions.prediction).show(100)

            evaluator = RegressionEvaluator(labelCol = 'close', predictionCol = 'prediction')
            r2_score = evaluator.evaluate(model.transform(validate), {evaluator.metricName: "r2"})
            rmse_score = evaluator.evaluate(model.transform(validate), {evaluator.metricName: "rmse"})

            print('r2 score on (validation set): %g' %(r2_score,))
            print('rmse score on (validation set): %g' %(rmse_score,))

            model.write().overwrite().save('models/commodity/'+market_type+'/'+commodity)
            print("Model saved for commodity type ----------> "+commodity)



if __name__ == '__main__':
    cluster_seeds = ['127.0.0.1:9042']
    spark = SparkSession.builder.appName('load commodity').config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
    assert spark.version >= '2.3' # make sure we have Spark 2.3+
    spark.sparkContext.setLogLevel('WARN')
    main()
