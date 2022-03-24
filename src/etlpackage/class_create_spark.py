import logging
import time
from pyspark import SparkContext, SparkConf


class CreateSpark(object):
    def __init__(self, pyspark_app_name):
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

        # Configure Spark
        try:
            conf = SparkConf().setAppName(pyspark_app_name).setMaster("local[8]")
            self.sc = SparkContext(conf=conf)
            logging.info("Start pyspark successfully.")
            logging.info("sc.version:{0}".format(self.sc.version))
        except Exception as e:
            logging.error("Fail in starting pyspark.")
            logging.error(e)
