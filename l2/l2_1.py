# Создаем SparkContext

from pyspark import SparkContext

sc = SparkContext("local", "RDD example")

# Создаем RDD из списка чисел
data = [1, 2, 3, 4, 5]
rdd = sc.parallelize(data)

# чтение RDD из файла
rdd = sc.textFile("file.txt")

# Подсчет количества элементов
count = rdd.count()

# Получение первого элемента
first = rdd.first()

# Фильтр
filtered = rdd.filter(lambda x: x>3)

# map
mapped = rdd.map(lambda x: x**2)

# суммирование
mapped = rdd.reduce(lambda x, y: x+y)



