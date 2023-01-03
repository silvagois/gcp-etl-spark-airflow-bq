import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as functions
from pyspark.sql.functions import regexp_replace, when, year, month, to_date, col
from pyspark.sql.types import StringType, IntegerType, FloatType
import re
from google.cloud import bigquery

def start_or_create_spark():
    from pyspark.sql import SparkSession
    spark = (SparkSession
             .builder
             .appName("Processamento de Dados de Gasolina no Brasil")
             .config('spark.jars', 'gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.23.2.jar')
             .getOrCreate()
             )
    return spark

spark = start_or_create_spark()

df = spark.read.format('csv')\
        .option("header", "true")\
        .option('delimiter', ',')\
        .load(r'gs://bucket-raw/olist/customer/olist_customers_dataset.csv')
df = df.withColumn('customer_city', functions.initcap(functions.col('customer_city')))\
        .withColumn('date_load',functions.current_date()) \
        .withColumn('date_load',functions.to_timestamp(col('date_load'),"yyy-MM-dd"))
#df.limit(5).toPandas()
#df.dtypes

# save in bucket curated in parquet format

#df.repartition(1).write.format('parquet').mode('overwrite').save(r'gs://bucket-curated/olist/customer/customer.parquet')
df.repartition(1)\
    .write.partitionBy('date_load')\
    .format('parquet')\
    .mode('append')\
    .save('gs://bucket-curated/olist/customer')

'''
# Load dados bigquery
bq_dataset='olist'
bq_table='tb_customer'
gcs_tmp_bucket='stack-data-pipeline-gcp-mg-combustiveis-brasil-pyspark-code'

df.write \
    .format("bigquery") \
    .option("table", "{}.{}".format(bq_dataset, bq_table)) \
    .option("temporaryGcsBucket", gcs_tmp_bucket) \
    .mode('overwrite') \
    .save()
'''