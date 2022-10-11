from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pipeline_1.config.ConfigStore import *
from pipeline_1.udfs.UDFs import *

def DeltaTableOperations_1(spark: SparkSession):
    if not ("SColumnExpression" in locals()):
        from delta.tables import DeltaTable
        spark.sql(f"""OPTIMIZE .
                        ZORDER BY {",".join(["tiempo_vacio"])}""")
