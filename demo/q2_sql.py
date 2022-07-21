from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("wordcount").getOrCreate()

'''
Q2 Na vrethoun oi 5 upalliloi me ton megalitero mistho. Na dwthei to apotelesma
	sti morfh 
	(Epitheto Arxiko_Onomatos., Misthos)
'''
'''
## employees (id, onomat/mo, #misthos, #id_tmimatos)
## departments (id_tmimatos, onoma_tmimatos)
'''
employees = spark.read.format("csv").options(header = 'false', inferSchema = 'true').load("hdfs://master:9000/examples/employees.csv")
employees.registerTempTable("employees")

def format_name(name):
	namelist = name.split(" ")
	return namelist[1] + " " + namelist[0][0] + "."

spark.udf.register("formatter", format_name)

query = \
	'''
		select formatter(_c1), _c2
		from employees
		order by _c2 desc
		limit 5
	'''

result = spark.sql(query)

result.explain()


result.show()

