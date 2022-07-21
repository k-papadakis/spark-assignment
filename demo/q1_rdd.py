from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("wordcount").getOrCreate()

sc = spark.sparkContext

'''
## employees (id, onomat/mo, #misthos, #id_tmimatos)
## departments (id_tmimatos, onoma_tmimatos)
'''

'''
Q1 na vrethei o megistos misthos
'''
rdd = sc.textFile("hdfs://master:9000/examples/employees.csv"). \
	map(lambda line : int(line.split(",")[2]))

res_way1 = rdd.sortBy(lambda x: x, ascending = False)

#print(res_way1.take(1)[0])

'''
max = list[0]
for next num in list:
	if num > max: 
		max = num
'''

res_way2 = rdd.map(lambda x : (1, x)). \
	reduceByKey(lambda total_max, next_num : total_max if total_max >= next_num else next_num). \
	map(lambda x : x[1])

print(res_way2.collect())

