from pyspark import SparkContext
from pyspark.sql import DataFrame, SparkSession


class MetamodelExtractor:
    def __init__(self, spark: SparkSession, schema, lookup_filepath):
        self.spark = spark
        self.metamodel_schema = schema
        self.metamodel_lookup = lookup_filepath
        self.metamodel_full_df = (spark.read  # The DataFrameReader
                                  .option("sep", "|")  # Use tab delimiter (default is comma-separator)
                                  .schema(self.metamodel_schema)  # Apply schema
                                  .csv(self.metamodel_lookup))

    def extract_metamodel_df_by_stream_id(self, src_stream_id):
        try:
            metamodel_df = self.metamodel_full_df.filter(self.metamodel_full_df.SRC_STREAM_ID == src_stream_id)
            metamodel_df.show(vertical=True)
            return metamodel_df
        except:
            print("Metamodel doesnt exists for this source")

    def extract_metamodel_df_by_process_type(self, src_stream_id, process_type):
        try:
            metamodel_by_source_df = self.extract_metamodel_df_by_stream_id(src_stream_id)
            metamodel_filtered_df = metamodel_by_source_df.filter(metamodel_by_source_df.PROCESS_TYPE == process_type)
            metamodel_filtered_df.show(vertical=True)
            return metamodel_filtered_df
        except:
            print("Metamodel doesnt exists for this process type")
