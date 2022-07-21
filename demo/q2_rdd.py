from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("wordcount").getOrCreate()

sc = spark.sparkContext

'''
## employees (id, onomat/mo, #misthos, #id_tmimatos)
## departments (id_tmimatos, onoma_tmimatos)
'''

'''
Q2 Na vrethoun oi 5 upalliloi me ton megalitero mistho. Na dwthei to apotelesma
	sti morfh 
	(Epitheto Arxiko_Onomatos., Misthos)
'''
def format_name(name):
	namelist = name.split(" ")
	return namelist[1] + " " + namelist[0][0] + "."

rdd = sc.textFile("hdfs://master:9000/examples/employees.csv"). \
	map(lambda x: (format_name(x.split(",")[1]), int(x.split(",")[2]))). \
	sortBy(lambda x: x[1], ascending = False)


for x in rdd.take(5):
	print(x)


