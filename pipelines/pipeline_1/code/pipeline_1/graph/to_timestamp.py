from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pipeline_1.config.ConfigStore import *
from pipeline_1.udfs.UDFs import *

def to_timestamp(spark: SparkSession, in0: DataFrame) -> DataFrame:
    from pyspark.sql import functions as fa

    def timezone_from_timestamp(column_name):
        return (fa.concat(
            fa.lit("GMT"),
            fa.regexp_extract(column_name, "(\+|\-)\d+:\d+$", 1),
            fa.hour(fa.regexp_extract(column_name, "(\d+:\d+)$", 1))
        ))

    out0 = in0.withColumn(
        'timestamp_func',
        fa.from_utc_timestamp(fa.col('timestamp').cast('timestamp'), timezone_from_timestamp(fa.col('timestamp')))
    )

    return out0
