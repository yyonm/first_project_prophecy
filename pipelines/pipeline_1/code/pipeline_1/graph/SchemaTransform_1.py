from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pipeline_1.config.ConfigStore import *
from pipeline_1.udfs.UDFs import *

def SchemaTransform_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.drop("division")
    df2 = df1.drop("server")
    df3 = df2.drop("units")
    df4 = df3.drop("point_type")
    df5 = df4.drop("digital_set")
    df6 = df5.drop("description")
    df7 = df6.drop("digital_mapping")

    return df7.drop("aggregation_mapping")
