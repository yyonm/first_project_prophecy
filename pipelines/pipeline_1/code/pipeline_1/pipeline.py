from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pipeline_1.config.ConfigStore import *
from pipeline_1.udfs.UDFs import *
from prophecy.utils import *
from pipeline_1.graph import *

def pipeline(spark: SparkSession) -> None:
    df_jigsaw_mdt = jigsaw_mdt(spark)
    df_Reformat_1 = Reformat_1(spark, df_jigsaw_mdt)
    DeltaTableOperations_1(spark)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "3633/pipelines/pipeline_1")
    MetricsCollector.start(spark = spark, pipelineId = "3633/pipelines/pipeline_1")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
