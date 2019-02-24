from pyspark import SparkContext
sc = SparkContext(master="local",appName="umbler")
rdd = sc.textFile('C://Users//anand//Downloads//convertcsv.csv').map(lambda inp: inp.replace('"', ""))
print(rdd.take(5))
