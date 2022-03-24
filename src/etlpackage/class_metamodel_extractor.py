from pyspark import SparkContext
from pyspark.sql import DataFrame, SparkSession


def get_metamodel_by_type(metamodel_df: DataFrame, entity_type: str):
    return "dataframe"


class MetamodelExtractor:
    def __init__(self, spark: SparkSession, schema, lookup_filepath):
        self.spark = spark
        self.metamodel_schema = schema
        self.metamodel_lookup = lookup_filepath

    def extract_metamodel_df_by_stream_id(self, src_stream_id):
        try:
            metamodel_full_df = (self.spark.read  # The DataFrameReader
                                 .option("sep", "|")  # Use tab delimiter (default is comma-separator)
                                 # .option("header", "true")   # Use first line of all files as header
                                 .schema(self.metamodel_schema)  # Apply schema
                                 .csv(self.metamodel_lookup)  # Creates a DataFrame from CSV after reading in the file
                                 )
            # metamodel_full_df.show(vertical=True)
            metamodel_df = metamodel_full_df.filter(metamodel_full_df.SRC_STREAM_ID == src_stream_id)
            metamodel_df.show(vertical=True)
            return metamodel_df
        except:
            print("Metamodel doesnt exists for this source")

    def extract_metamodel_df_by_process_type(self, extract_df, process_type):
        try:
            metamodel_df = extract_df.filter(extract_df.PROCESS_TYPE == process_type)
            metamodel_df.show(vertical=True)
            return metamodel_df
        except:
            print("Metamodel doesnt exists for this process type")
