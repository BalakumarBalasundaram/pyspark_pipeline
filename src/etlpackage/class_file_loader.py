from src.etlpackage import class_xml_reader


class FileLoader(object):

    def __init__(self, spark):
        self.spark = spark

    def filereader(self, filepath, fileextension):
        if fileextension == '.csv':
            dataframe = self.spark.read.csv(filepath, header=True, inferSchema=True)
        elif fileextension == '.orc':
            dataframe = self.spark.read.orc(filepath)
        elif fileextension == '.parquet':
            dataframe = self.spark.read.parquet(filepath)
        elif fileextension == '.json':
            # reader = jsonReader()
            dataframe = self.spark.read.option("multiline", "true").json(filepath)
            # dataframes = reader.findJSONDataframes()
        elif fileextension == '.tsv':
            dataframe = self.spark.read.option("delimiter", "\t").csv(filepath, header=True)
        elif fileextension == '.xml':
            reader = class_xml_reader.xmlReader(self.spark)
            dataframe = reader.getXMLDataFrame(filepath)

        return dataframe
