from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("wordcount").getOrCreate()

sc = spark.sparkContext
'''
def sum_ones(acc, next_val):
	return acc + next_val
'''
'''
("a", [1,1,1,1,...,1,1])
sum = 0 
for each num in list:
	sum = sum + num
'''

rdd = sc.textFile("hdfs://master:9000/examples/text.txt"). \
	flatMap(lambda line : line.split(" ")). \
	filter(lambda word: word != "a"). \
	map(lambda word: (word, 1)). \
	reduceByKey(lambda sum_acc, next_val: sum_acc + next_val). \
	sortBy(lambda x : x[1], ascending = False) 


for i in rdd.take(20):
	print(i)

