# define Raw to Base pipeline
from pyspark.sql.functions import lit
from src.etlpackage import class_utils, class_globalvars
import datetime

INP_SRC_SYS = "bgmax"
SRC_STREAM_ID = "BGMAX001"
PARTITION_DATE = "FROM_FILE"
WRITE_MODE = "Overwrite"
ENV = "dev"
AZURE_URL = "dbfs://" + ENV
CREATE_DATE = datetime.datetime.now().strftime("%Y-%m-%d")
date_time = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
current_year = datetime.datetime.now().strftime("%Y-%m-%d").split("-")[0].lstrip('0')
current_month = datetime.datetime.now().strftime("%Y-%m-%d").split("-")[1].lstrip('0')
current_day = datetime.datetime.now().strftime("%Y-%m-%d").split("-")[2].lstrip('0')
BATCH_ID = SRC_STREAM_ID + "_" + date_time


class RawToBase:

    def __init__(self, spark, metamodel_df):
        self.spark = spark
        self.metamodel_df = metamodel_df
        self.dbutils = self.get_dbutils(spark)

    def raw_to_base(self, azure_url, partition_date, inp_file):
        print("###########################RAW to BASE Pipeline ##################################")
        # INP_DF.show(vertical=True)
        # input_file=INP_DF.select("STREAM_NM").rdd.flatMap(list).collect()
        base_odl_loc = self.metamodel_df.select("BASE_ODL_LOC").rdd.flatMap(list).collect()[0]
        base_tbl_name = self.metamodel_df.select("BASE_TBL_NAME").rdd.flatMap(list).collect()[0]
        partition_key = self.metamodel_df.select("PARTITION_KEY").rdd.flatMap(list).collect()[0]
        field_sep = self.metamodel_df.select("FIELD_SEP").rdd.flatMap(list).collect()[0]
        azure_base_dir = azure_url + "/base" + base_odl_loc + base_tbl_name.split("_")[1]

        # Create partition directories
        if partition_date == 'FROM_FILE':
            base_dir, partition_values = class_utils.create_partitions_from_file(azure_base_dir, partition_key)
        else:
            base_dir, partition_values = class_utils.create_partions_with_current_date(azure_base_dir, partition_key)

        print("base_tbl_name  is :", base_tbl_name)
        print("base_odl_loc  is :", azure_base_dir)
        print("partition_key  is :", partition_key)
        print("base dir is :", base_dir)
        print("inp_file is :", inp_file)

        # Read Input file
        raw_df = (self.spark.read
                  .option("sep", field_sep)
                  .option("header", True)
                  .csv(inp_file)
                  )
        # raw_df.show()
        repartitioned_df = raw_df.repartition(4)
        enrich_df = repartitioned_df.select(lit(SRC_STREAM_ID).alias("src_stream_id"),
                                            lit(BATCH_ID).alias("batch_id"),
                                            lit(CREATE_DATE).alias("create_date"),
                                            lit("utc").alias("time_zone"),
                                            lit(CREATE_DATE).alias("src_business_date"),
                                            "*")
        # enrich_df.show()
        print("Writing file ", base_dir, "with ", enrich_df.rdd.getNumPartitions(), " partitions")
        for k in partition_values:
            # print(k,d[k])
            # print(k)
            enrich_df = enrich_df.withColumn(k, lit(partition_values[k]))
        # enrich_df.printSchema()
        enrich_df.write.format("avro").partitionBy(list(partition_values.keys())).mode(WRITE_MODE).save(base_dir)
