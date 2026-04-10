import os
import sys
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from configs import kafka_config

os.environ['SPARK_HOME'] = os.path.dirname(pyspark.__file__)
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell'

spark = SparkSession.builder \
    .appName("Sten_IoT_Full_Processor") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# 2. Схема даних сенсорів
schema = StructType([
    StructField("id", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("timestamp", TimestampType(), True)
])

bootstrap_servers = ",".join(kafka_config['bootstrap_servers']) if isinstance(kafka_config['bootstrap_servers'], list) else kafka_config['bootstrap_servers']

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", bootstrap_servers) \
    .option("subscribe", "sten_building_sensors") \
    .option("startingOffsets", "latest") \
    .option("kafka.security.protocol", kafka_config['security_protocol']) \
    .option("kafka.sasl.mechanism", kafka_config['sasl_mechanism']) \
    .option("kafka.sasl.jaas.config", f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_config["username"]}" password="{kafka_config["password"]}";') \
    .load()

parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(F.from_json(F.col("value"), schema).alias("data")) \
    .select("data.*") \
    .withWatermark("timestamp", "10 seconds")

aggregated_df = parsed_df \
    .groupBy(F.window(F.col("timestamp"), "1 minute", "30 seconds")) \
    .agg(
        F.avg("temperature").alias("avg_temp"),
        F.avg("humidity").alias("avg_humidity")
    )

conditions_df = spark.read.csv("alerts_conditions.csv", header=True, inferSchema=True)
alerts_df = aggregated_df.crossJoin(conditions_df)

final_alerts = alerts_df.filter(
    ((F.col("avg_temp") > F.col("temperature_max")) & (F.col("temperature_max") != -999)) |
    ((F.col("avg_temp") < F.col("temperature_min")) & (F.col("temperature_min") != -999)) |
    ((F.col("avg_humidity") > F.col("humidity_max")) & (F.col("humidity_max") != -999)) |
    ((F.col("avg_humidity") < F.col("humidity_min")) & (F.col("humidity_min") != -999))
)

query = final_alerts.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .option("checkpointLocation", "checkpoints_console_final") \
    .start()

print("🚀 СИСТЕМУ ЗАПУЩЕНО!")
print("Чекаємо 60 секунд на формування першого вікна...")
print("Результати (алерти) з'являться нижче у вигляді таблиці.")

query.awaitTermination()