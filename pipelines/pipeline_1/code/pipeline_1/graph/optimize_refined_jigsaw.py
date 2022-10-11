from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pipeline_1.config.ConfigStore import *
from pipeline_1.udfs.UDFs import *

def optimize_refined_jigsaw(spark: SparkSession):
    if not ("SColumnExpression" in locals()):
        from delta.tables import DeltaTable
        spark.sql(f"""OPTIMIZE dmh_minco.table_refined_jigsaw
                        ZORDER BY {",".join(["tiempo_vacio"])}""")
