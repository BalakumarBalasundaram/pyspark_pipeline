# define Dropzone to Raw pipeline
from src.etlpackage import class_utils
import os
import re


class DropZoneToRaw:
    def __init__(self, spark, inp_df):
        self.inp_df = inp_df
        self.dbutils = self.get_dbutils(spark)

    @staticmethod
    def _get_dbutils(spark: SparkSession):
        try:
            from pyspark.dbutils import DBUtils  # noqa
            if "dbutils" not in locals():
                utils = DBUtils(spark)
                return utils
            else:
                return locals().get("dbutils")
        except ImportError:
            return None

    def get_dbutils(self):
        utils = self._get_dbutils(self.spark)

        if not utils:
            self.logger.warn("No DBUtils defined in the runtime")
        else:
            self.logger.info("DBUtils class initialized")

        return utils

    def runprocess(self, azure_url, raw_odl_loc, create_date):
        print("###########################DropZone to RAW Pipeline ##################################")
        input_file = self.inp_df.select("STREAM_NM").rdd.flatMap(list).collect()[0]
        landing_dir = self.inp_df.select("LANDING_DIR").rdd.flatMap(list).collect()[0]
        azure_land_dir = landing_dir.replace("/mnt/shareddisk", azure_url + "/tmp")
        raw_odl_loc = self.inp_df.select("RAW_ODL_LOC").rdd.flatMap(list).collect()[0]
        azure_raw_dir = azure_url + "/raw" + raw_odl_loc + create_date.replace("-", '')

        print("input file name is :", input_file)
        print("Landing directory is :", landing_dir)
        print("Raw directory is :", raw_odl_loc)
        print("Azure Raw Loc is :", azure_raw_dir)

        # File listing
        #  print(dbutils.fs.ls(Azure_land_dir))
        landing_file = class_utils.db_list_files(azure_land_dir, input_file)[0]
        raw_file_name = landing_file.split(os.sep)[-1]
        # check if directory exists, else create it
        if not class_utils.file_exists(azure_raw_dir):
            print("Creating directory ", azure_raw_dir, "now")
            self.dbutils.fs.mkdirs(azure_raw_dir)
        azure_raw_file = azure_raw_dir + "/" + raw_file_name
        print("RAW FILE:", azure_raw_file)
        if not class_utils.file_exists(azure_raw_file):
            # Copy the file from Landing directory to Raw folder
            print("Copying the file ", landing_file, "to Raw ADLS ", azure_raw_dir)
            self.dbutils.fs.cp(landing_file, azure_raw_dir)

        return azure_raw_file
