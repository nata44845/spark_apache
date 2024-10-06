# из list
from pyspark.sql import SparkSession
spark = (SparkSession.builder.master("local[*]").getOrCreate())

data = [("Ivan", 10),("Petr", 20),("Elena", 30)]

columns = ['name', 'age']
df = spark.createDataFrame(data=data, columns=columns)

# из файла
spark = (SparkSession.builder.master("local[*]").getOrCreate())
df = spark.read.option("header", "true").csv("data.csv")

# из hive
spark = (SparkSession.builder.enableHiveSupport().getOrCreate())
df = spark.table("table_name")

# по JDBC
spark = (SparkSession.builder.master("local[*]").getOrCreate())
creds = {"user":"name", "password":"12345"}
df = spark.read.jdbc("url","table_name", properies = creds)

# range
spark = (SparkSession.builder.master("local[*]").getOrCreate())
df = spark.range(100)

# Функции
df.select(col("id"))
df.filter(col("id"<50))
df.withColumn("new", col("id")%2) - создать новый столбец

df.withColumnRenamed("id", "id_rename")
# groupBy
df.withColumn("new", col("id")%2).groupby(col("new"))
# agg
df.withColumn("new", col("id")%2).groupby(col("new")).agg(max(col("id")))

# join
df2 = spark.range(10).withColumnRenamed("id", "id2")
df.join(df2,col("id")==col("id2"), inner)
# orderBy
df.orderBy(col("id").desc)
df.orderBy(col("id").asc)
# distinct
df.distinct()

# группировки и агрегации
df.groupby(col("name")).agg(mean(col("age")).alias("count_age")).show()
df.groupby(col("name")).agg(collect_list(col("age")).alias("count_age")).show()

# Оконные функции
from pyspark.sql.Window import Window
from pyspark.sql.functions import rank
df.withColumn("rank_col",rank().over(Window.partitionBy("partition_col").orderBy("orderBy_col")))

# when и othervise
from pyspark.sql.functions import when,col
spark = SparkSession.builder.getOrCreate()
df = spark.range(100)
df.withColumn("test_col", when(col("id")>50), 'value_more_50').otherwise('value_less_50'))

# pivot
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean

spark = SparkSession.builder("local[*]").getOrCreate()

data = ...
df = spark.createDataFrame(data=data).toDF("name","city","salary")

df.groupby(col("city"),col("name")).agg(mean(col("salary")))
df.groupby("city").pivot("name").agg(mean("salary"))

# cast
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean
rom pyspark.sql.types import StringType

spark = SparkSession.builder("local[*]").getOrCreate()

df = spark.range(100)
df.printSchema()
new_df = df.select(col("id")).cast("string")
new_df = df.select(col("id")).cast(StringType)
new_df = df.withColumn("id",col("id").cast(StringType))
df.printSchema()

# udf
def recognize_model(model: str) -> str:
  if "14" in model:
    return "new model"
  else:
    return "old model"

recognize_model_udf = udf(lambda x: recognize_model(x))

new_df = df.withColumn("version_model", recognize_model_udf("model"))

# Сохранение данных
df.write.csv("path/to/output/folder", header=True)