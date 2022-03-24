from pyspark.sql.types import StructType, StructField, StringType, IntegerType


class Schema:
    def __init__(self):
        self.schema = StructType([
            StructField("PROCESS_ID", StringType(), True),
            StructField("PROCESS_TYPE", StringType(), True),
            StructField("SRC_ID", StringType(), True),
            StructField("SRC_SYS_NM", StringType(), True),
            StructField("STREAM_NM", StringType(), True),
            StructField("SRC_STREAM_ID", StringType(), True),
            StructField("SRC_SYS_TYPE", StringType(), True),
            StructField("SRC_SYS_DESC", StringType(), True),
            StructField("FIELD_SEP", StringType(), True),
            StructField("LINE_SEP", StringType(), True),
            StructField("HEADER_ROW", StringType(), True),
            StructField("FOOTER_ROW", StringType(), True),
            StructField("LOAD_TYPE", StringType(), True),
            StructField("WHERE_CLAUSE", StringType(), True),
            StructField("PARTITION_KEY", StringType(), True),
            StructField("CUST_DATA_FLG", StringType(), True),
            StructField("CUST_DATA_DESC", StringType(), True),
            StructField("LEGAL_GRAND", StringType(), True),
            StructField("LOAD_FREQ", StringType(), True),
            StructField("LANDING_DIR", StringType(), True),
            StructField("RAW_ODL_LOC", StringType(), True),
            StructField("BASE_TBL_NAME", StringType(), True),
            StructField("BASE_ODL_LOC", StringType(), True),
            StructField("SCHEMA_DIR", StringType(), True),
            StructField("C1_TBL_NAME", StringType(), True),
            StructField("C1_ODL_LOC", StringType(), True),
            StructField("DELIVERY_FORMAT", StringType(), True),
            StructField("SRV_NAME", StringType(), True),
            StructField("SRV_SHRT_NAME", StringType(), True),
            StructField("SRC_TIME_ZONE", StringType(), True)
        ])
