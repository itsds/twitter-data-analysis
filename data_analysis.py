from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import udf
from pyspark.sql.types import *
from data_extraction import extract_data

def count_hastag():
    spark = SparkSession.builder.appName("Analyzing London Crime Data").getOrCreate()

    data = spark.read\
                .format("csv")\
                .option("header","false")\
                .load("result.csv")

    print(data.count())
    print(data.printSchema())
    words = data.select(explode(split(data._c3," ")).alias("word"))
    words.count()

    def hashtagUDF(word):
        if str(word).startswith('#'):
            return word
        else:
            return 'non_tag'

    extract_tag_udf = udf(hashtagUDF,StringType())
    resultDF = words.withColumn('value',extract_tag_udf(words.word))
    result = resultDF.where(resultDF.value!='non_tag').groupBy('value').count().orderBy('count',ascending=False)
    result.show()

extract_data()
count_hastag()