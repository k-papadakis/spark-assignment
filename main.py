# %%
import findspark
findspark.init()

import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, size, split, explode

# %%
spark = SparkSession.builder.appName("wordcount").getOrCreate()
sc = spark.sparkContext

# %%
textFile = spark.read.text("hdfs://master:9000/examples/text.txt")

# %%
textFile.count()

# %%
textFile.first()

# %%
textFile.select(size(split(textFile.value, "\s+")).name("numWords"))\
    .agg(max(col("numWords"))).collect()

# %%
textFile.select(explode(split(textFile.value, "\s+")).alias("word"))\
    .groupBy("word").count().sort("count", ascending=False).show()