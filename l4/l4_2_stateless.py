from pyspark.sql import SparkSession
from pyspark.functions import col

# Создаем SparkSession
spark = SparkSession.builder \
        .appName("StructuredNetworkWordCount") \
        .getOrCreate()

# Создаем поток данных с использованием источника данных rate
streaming_df = spark \
    .readStream() \
    .format("rate") \
    .option("rowsPerSecond", 1) \
    .load()

result = streaming_df.select(col("value")*2)

# Выводим поток данных
query = result \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()