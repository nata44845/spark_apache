from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Создаем SparkSession
spark = SparkSession.builder \
        .appName("RateSourceExample") \
        .getOrCreate()

# Создаем поток данных с использованием источника данных rate
streaming_df = spark.readStream.format("rate").load()
streaming_df = streaming_df.withColumn("key", col("value"))

# Группируем данные по ключу и считаем количество
grouped_data = streaming_df.groupBy("key").count()

# Выводим поток данных
query = grouped_data.writeStream.outputMode("complete").format("console").start()
query.awaitTermination()