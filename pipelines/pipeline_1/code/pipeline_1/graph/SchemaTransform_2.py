from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pipeline_1.config.ConfigStore import *
from pipeline_1.udfs.UDFs import *

def SchemaTransform_2(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.withColumnRenamed("tag", "tag_landing")

    return df1.drop("timestamp")
