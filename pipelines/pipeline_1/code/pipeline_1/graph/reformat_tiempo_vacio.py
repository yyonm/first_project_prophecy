from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pipeline_1.config.ConfigStore import *
from pipeline_1.udfs.UDFs import *

def reformat_tiempo_vacio(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(to_timestamp(col("tiempo_vacio"), "yyyy-MM-dd HH:mm:ss").alias("tiempo_vacio_ts"))
