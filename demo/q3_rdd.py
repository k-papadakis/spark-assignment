from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("wordcount").getOrCreate()

sc = spark.sparkContext

'''
## employees (id, onomat/mo, #misthos, #id_tmimatos)
## departments (id_tmimatos, onoma_tmimatos)
'''

'''
## Gia kathe tmima na vrethei to plithos twn upallilwn kathw kai oi sunolikes misthologikes dapanes
## tou tmimatos
## Onoma Tmimatos, # Ypallilwn , Eksoda
'''
'''
sum = list[0]
for next val in list:
	sum = sum + val

[(a,b), (c,d), ..., (x,z)]
sum = list[0]
for next val_tuple in list:
	sum[0] = sum[0] + val_tuple[0]
	sum[1] = sum[1] + val_tuple[1]

'''



employees = sc.textFile("hdfs://master:9000/examples/employees.csv"). \
		map(lambda line : (line.split(",")[3], (1, int(line.split(",")[2])))). \
		reduceByKey(lambda x, y : (x[0] + y[0], x[1] + y[1]))

departments = sc.textFile("hdfs://master:9000/examples/departments.csv"). \
		map(lambda x : (x.split(",")[0], x.split(",")[1]))

result = employees.join(departments). \
		map(lambda x : (x[1][1], x[1][0][0], x[1][0][1]))
'''
	(key, ((table1), (table2)))
	(id_tmimatos, ((#ypallilwn, misthoi), onoma_tmimatos))
'''

for i in result.collect():
	print(i)
