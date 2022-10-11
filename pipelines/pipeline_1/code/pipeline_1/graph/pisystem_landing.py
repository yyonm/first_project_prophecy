from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pipeline_1.config.ConfigStore import *
from pipeline_1.udfs.UDFs import *

def pisystem_landing(spark: SparkSession) -> DataFrame:
    return spark.read.format("delta").load("dbfs:/mnt/trusted/dmh/operaciones/pi_system/eventos/OUT/")
