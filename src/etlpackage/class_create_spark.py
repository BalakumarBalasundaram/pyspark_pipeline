import logging
import time

from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf


class CreateSpark:
    def __init__(self, pyspark_app_name, local=True, createsparksession=True):

        self.start = time.clock()
        logging.basicConfig(level=logging.INFO,
                            format='%(asctime)s  %(levelname)5s %(filename)19s[line:%(lineno)3d] %(funcName)s %(message)s',
                            datefmt='%y-%m-%d %H:%M:%S',
                            filename='./main.log',
                            filemode='a')
        console = logging.StreamHandler()
        console.setLevel(logging.INFO)

        formatter = logging.Formatter(
            '%(asctime)s  %(levelname)5s %(filename)19s[line:%(lineno)3d] %(funcName)s %(message)s')
        console.setFormatter(formatter)

        logging.getLogger('').addHandler(console)
        logging.info("START CLASS {class_name}.".format(class_name=CreateSpark.__name__))

        if createsparksession:
            if local:
                self.spark = SparkSession.builder.appName("pySpark for Data Science  workshop") \
                    .config("spark.master", "local[*]") \
                    .config("spark.home", spark_home) \
                    .config("spark.pyspark.python", sys.executable).getOrCreate()
                print("Created local SparkSession")
                return self.spark
            else:

                self.spark = SparkSession.builder.appName("workshop").config("spark.master","yarn").enableHiveSupport().getOrCreate()
                print("Created YARN SparkSession")
                return self.spark

        else:
            if local:
                # Configure Spark
                try:
                    conf = SparkConf().setAppName(pyspark_app_name).setMaster("local[8]")
                    self.sc = SparkContext(conf=conf)
                    logging.info("Start pyspark successfully.")
                    logging.info("sc.version:{0}".format(self.sc.version))
                except Exception as e:
                    logging.error("Fail in starting pyspark.")
                    logging.error(e)
