from pyspark import SparkContext

sc = SparkContext(master="local",appName="sparkmm")
print(sc.textFile("C:\\Users\\anand\\Documents\\PythonProjects\\data.txt").take(2))