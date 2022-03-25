from pyspark.sql import SparkSession
from src.etlpackage import class_create_spark
from src.etlpackage import class_metamodel_extractor

def init_spark():
  spark = SparkSession.builder.appName("HelloWorld").getOrCreate()
  sc = spark.sparkContext
  return spark,sc

def main():
  spark,sc = init_spark()
  nums = sc.parallelize([1,2,3,4])
  print(nums.map(lambda x: x*x).collect())


if __name__ == '__main__':
  class_create_spark.CreateSpark("test", False, True)
  class_metamodel_extractor.MetamodelExtractor()
