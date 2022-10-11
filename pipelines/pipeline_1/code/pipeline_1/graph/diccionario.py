from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pipeline_1.config.ConfigStore import *
from pipeline_1.udfs.UDFs import *

def diccionario(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"core_pi_system.diccionario")
