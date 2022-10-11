from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pipeline_1.config.ConfigStore import *
from pipeline_1.udfs.UDFs import *

def SchemaTransform_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.drop("division")
    df2 = df1.drop("server")
    df3 = df2.drop("description")
    df4 = df3.drop("digital_mapping")

    return df4.drop("aggregation_mapping")
