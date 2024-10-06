from pyspark.sql import SparkSession

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

# Создаем batch данных
batch_df = spark.read.csv("file.csv")

# Выполняем соединение
joined_data = streaming_df.join(batch_df, "value")

# Выводим поток данных
query = joined_data.writeStream.outputMode("append").format("console").start()
query.awaitTermination()