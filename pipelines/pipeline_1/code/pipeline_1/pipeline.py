from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pipeline_1.config.ConfigStore import *
from pipeline_1.udfs.UDFs import *
from prophecy.utils import *
from pipeline_1.graph import *

def pipeline(spark: SparkSession) -> None:
    optimize_refined_jigsaw(spark)
    df_jigsaw_mdt = jigsaw_mdt(spark)
    df_reformat_tiempo_vacio = reformat_tiempo_vacio(spark, df_jigsaw_mdt)
    df_pisystem_landing = pisystem_landing(spark)
    df_SchemaTransform_2 = SchemaTransform_2(spark, df_pisystem_landing)
    df_diccionario = diccionario(spark)
    df_Filter_1 = Filter_1(spark, df_diccionario)
    df_SchemaTransform_1 = SchemaTransform_1(spark, df_Filter_1)
    df_to_timestamp = to_timestamp(spark, df_SchemaTransform_2)
    df_Join_1 = Join_1(spark, df_SchemaTransform_1, df_to_timestamp)
    pisystem_join_to_delta(spark, df_Join_1)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
    spark.conf.set("prophecy.metadata.pipeline.uri", "3633/pipelines/pipeline_1")
    MetricsCollector.start(spark = spark, pipelineId = "3633/pipelines/pipeline_1")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
