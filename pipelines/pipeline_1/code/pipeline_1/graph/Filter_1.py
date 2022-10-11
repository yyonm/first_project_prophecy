from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pipeline_1.config.ConfigStore import *
from pipeline_1.udfs.UDFs import *

def Filter_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter((col("division") == lit("DMH")))
