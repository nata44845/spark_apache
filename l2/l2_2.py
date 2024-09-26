# Группировка по ключу
from pyspark import SparkContext
from pyspark import SparkSession

sc = SparkContext("local", "GroupBy Example")
spark = SparkSession(sc)

data = [(1, "apple"), (2, "banana"), (3, "orange"), (2, "grape")]
rdd = sc.parallelize(data)

grouped_rdd = rdd.groupByKey()
for key, values in grouped_rdd.collect():
    print(f"key: {key}, values: {list(values)}")

# Агрегация данных
sum_rdd = rdd.reduceByKey(lambda x, y: x + y)
for key, value in sum_rdd.collect():
    print(f"key: {key}, sum: {value}")

# join двух rdd
rdd1 = sc.parallelize([(1, "apple"), (2, "banana"), (3, "orange")])
rdd2 = sc.parallelize([(1, "red"), (2, "yellow"), (3, "purple")])

joined_rdd = rdd1.join(rdd2)

for key, (value1, value2) in joined_rdd.collect():
    print(f"key: {key}, value1: {value1}, value2: {value2}"

# Сохранение в файл
rdd.saveAsTextFile("output.txt")