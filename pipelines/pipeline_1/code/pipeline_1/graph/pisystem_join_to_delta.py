from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pipeline_1.config.ConfigStore import *
from pipeline_1.udfs.UDFs import *

def pisystem_join_to_delta(spark: SparkSession, in0: DataFrame):
    from delta.tables import DeltaTable, DeltaMergeBuilder

    if DeltaTable.isDeltaTable(spark, "dbfs:/mnt/refined/dmh/operaciones/pi_system/prophecy/"):
        DeltaTable\
            .forPath(spark, "dbfs:/mnt/refined/dmh/operaciones/pi_system/prophecy/")\
            .alias("target")\
            .merge(
              in0.alias("source"),
              ((col("source.tag") == col("target.tag")) & (col("source.timestamp_ts") == col("target.timestamp_ts")))
            )\
            .whenMatchedUpdateAll()\
            .whenNotMatchedInsertAll()\
            .execute()
    else:
        in0.write.format("delta").mode("overwrite").save("dbfs:/mnt/refined/dmh/operaciones/pi_system/prophecy/")
