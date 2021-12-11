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
import load_covid, load_forex

def main():

    # USDRUB_df, USDGBP_df, USDCHF_df, USDCAD_df, USDEUR_df, USDJPY_df, USDAUD_df, USDBRL_df, USDINR_df, USDMXN_df = load_forex.get_forex_data(spark)
    covid19_df = load_covid.get_covid_data(spark)

    # forex = [USDRUB_df, USDGBP_df, USDCHF_df, USDCAD_df, USDEUR_df, USDJPY_df, USDAUD_df, USDBRL_df, USDINR_df, USDMXN_df]
    forex = ['USDRUB', 'USDGBP', 'USDCHF', 'USDCAD', 'USDEUR', 'USDJPY', 'USDAUD', 'USDBRL', 'USDINR', 'USDMXN']

    for name in forex:
        df_forex = load_forex.get_forex_data(spark, name)
        join_covid19_forex = covid19_df.join(df_forex, ['date'])
        # df_forex.show()
        join_covid19_forex.show(1)

        train, validate = join_covid19_forex.randomSplit([0.75, 0.25])
        train = train.cache()
        validate = validate.cache()
        print(train)
        covid_assembler = VectorAssembler(inputCols=[ 'sum_total_cases_us', 'sum_new_cases_us', 'sum_total_deaths_us', 'sum_new_deaths_us', 'sum_total_cases_global', 'sum_new_cases_global', 'sum_total_death_global', 'sum_new_deaths_global', 'sum_total_recovered_global', 'sum_new_recovered_global'], outputCol='features')
        #lr = LinearRegression(maxIter = 20, regParam=0.2, elasticNetParam=0.9, fitIntercept=True, standardization=True, featuresCol='features', labelCol = 'close')
        gbt = GBTRegressor(maxIter = 20, maxDepth=5, maxBins=32, featuresCol='features', labelCol = 'close')
        pipeline = Pipeline(stages=[covid_assembler, gbt])
        model = pipeline.fit(train)
        predictions = model.transform(validate)
        # predictions.select(predictions.date,predictions.close, predictions.prediction).show(100)
        evaluator = RegressionEvaluator(labelCol = 'close', predictionCol = 'prediction')
        r2_score = evaluator.evaluate(model.transform(validate), {evaluator.metricName: "r2"})
        rmse_score = evaluator.evaluate(model.transform(validate), {evaluator.metricName: "rmse"})
        print('r2 score on (validation set): %g' %(r2_score,))
        print('rmse score on (validation set): %g' %(rmse_score,))
        model.write().overwrite().save('models/forex/' + name)

if __name__ == '__main__':
    cluster_seeds = ['127.0.0.1:9042']
    spark = SparkSession.builder.appName('load forex').config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
    assert spark.version >= '2.3' # make sure we have Spark 2.3+
    spark.sparkContext.setLogLevel('WARN')
    main()
