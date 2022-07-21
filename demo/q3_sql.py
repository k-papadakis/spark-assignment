from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("wordcount").getOrCreate()

'''
## Gia kathe tmima na vrethei to plithos twn upallilwn kathw kai oi sunolikes misthologikes dapanes
## tou tmimatos
## Onoma Tmimatos, # Ypallilwn , Eksoda
'''
'''
## employees (id, onomat/mo, #misthos, #id_tmimatos)
## departments (id_tmimatos, onoma_tmimatos)
'''
employees = spark.read.format("csv").options(header = 'false', inferSchema = 'true').load("hdfs://master:9000/examples/employees.csv")
employees.registerTempTable("employees")

departments = spark.read.format("csv").options(header = 'false', inferSchema = 'true').load("hdfs://master:9000/examples/departments.csv")
departments.registerTempTable("departments")


query = \
	'''
		select d._c1, count(*), sum(e._c2)
		from employees as e, departments as d
		where e._c3 = d._c0
		group by d._c1
	'''

result = spark.sql(query)

result.explain()


result.show()

