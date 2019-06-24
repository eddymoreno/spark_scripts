#takes the name of the data_file

import sys

import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import *


print 'setting up spark...'

spark = SparkSession \
        .builder \
        .appName('Join first,middle,last into full_name') \
        .getOrCreate()

print 'loading data (' + sys.argv[1] + ')...'

#hdfsPath = "hdfs:///" + sys.argv[1]
#print "the hdfs path is: " + hdfsPath

path = "hdfs://sandbox-hdp.hortonworks.com:8020/user/script_transfer/" + sys.argv[1]

df = spark.read.format("csv") \
        .option("header", "true") \
        .option('inferSchema',"true") \
        .load(path)

df.printSchema()

print 'doing full_name transformation...'

# adding column called full_name that joins first,middle,last
df = df.withColumn('full_name', concat('first', lit(' '), 'middle', lit(' '), 'last'))

print 'doing average of hw1-hw4 transformation...'

#adding colum with average of 4 homeworks
df = df.withColumn('class_avg', expr('(hw1 + hw2 + hw3 + hw4) / 4'))


df.show()

print 'savig data as ' + sys.argv[1] 

savePath = "hdfs://sandbox-hdp.hortonworks.com:8020/user/sparked_cleaned/" + sys.argv[1]
df.write.option('header', 'true').csv(savePath)

print 'done'

