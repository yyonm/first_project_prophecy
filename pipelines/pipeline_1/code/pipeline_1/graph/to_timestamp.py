from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pipeline_1.config.ConfigStore import *
from pipeline_1.udfs.UDFs import *

def to_timestamp(spark: SparkSession, in0: DataFrame) -> DataFrame:

    def timezone_from_timestamp(column_name):
        return (concat(
            lit("GMT"),
            regexp_extract(column_name, "(\+|\-)\d+:\d+$", 1),
            hour(regexp_extract(column_name, "(\d+:\d+)$", 1))
        ))

    out0 = in0.withColumn(
        "timestamp_func",
        from_utc_timestamp(col("timestamp").cast("timestamp"), timezone_from_timestamp(col("timestamp")))
    )

    return out0

    return out0
