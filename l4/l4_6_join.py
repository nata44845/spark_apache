from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, window, lit, rand, shuffle, array
from uuid import uuid4

# Генерация потока
def rate_stream():
    return spark.readStream.format("rate").load()

# Вывод в консоль
def sink(df, mode):
    print(str(uuid4()))
    query = df.writeStream.format("console").outputMode(mode).start()
    query.awaitTermination()

# Генерация случайного числа
def random_int():
    idents = range(1,20)
    column_array = [lit(x) for x in idents]
    spark_array = array(column_array)
    shuffled_array = shuffle(spark_array)
    return shuffled_array[0]

# Создаем SparkSession
spark = SparkSession.builder.appName("Joins").getOrCreate()

# Создаем два потока данных
left = rate_stream().withColumn("ident", random_int()) \
    .withColumn("left", lit("left")) \
    .withWatermark("timestamp", "4 minutes") \
    .withColumn("window", window("timestamp", "1 minutes", "1 minutes")) \
    .alias("left")

right = rate_stream().withColumn("ident", random_int()) \
    .withColumn("right", lit("right")) \
    .withWatermark("timestamp", "5 minutes") \
    .withColumn("window", window("timestamp", "1 minutes", "1 minutes")) \
    .alias("right")

join_expr = expr("left.value = right.value and left.window = right.window")
# Выполняем соединение
joined = left.join(right, join_expr, "inner").select("left.window", "left.value", "left.left", "right.left")

# Выводим поток данных
print(joined.explain(True))
sink(joined,"append")