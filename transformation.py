from pyspark.sql import SparkSession
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from pyspark.sql.functions import to_date
from pyspark.sql.readwriter import DataFrameWriter


def transformation():
    spark = SparkSession\
        .builder\
        .appName("twitter_transformation")\
        .getOrCreate()
    path = ("C:\\Users\\leand\\Desktop\\Case Verx\\airflow-docker\\datalake")
    tweets_df = spark.read.json("C:\\Users\\leand\\Desktop\\Case Verx\\airflow-docker\\dags\\tweet.json")
    export_df = tweets_df.withColumn("created_date", to_date("created_at")).repartition("created_date")
    export_df.write.mode("overwrite").partitionBy("created_date").parquet(path)
