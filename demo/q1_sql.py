from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("wordcount").getOrCreate()

'''
Q1 na vrethei o megistos misthos
'''
'''
## employees (id, onomat/mo, #misthos, #id_tmimatos)
## departments (id_tmimatos, onoma_tmimatos)
'''
employees = spark.read.format("csv").options(header = 'false', inferSchema = 'true').load("hdfs://master:9000/examples/employees.csv")
employees.registerTempTable("employees")

query = \
	'''
		select max(_c2)
		from employees
	'''

result = spark.sql(query)

result.show()

