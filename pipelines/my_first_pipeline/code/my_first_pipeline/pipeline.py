from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from my_first_pipeline.config.ConfigStore import *
from my_first_pipeline.udfs.UDFs import *
from prophecy.utils import *
from my_first_pipeline.graph import *

def pipeline(spark: SparkSession) -> None:
    df_Source_0 = Source_0(spark)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "3633/pipelines/my_first_pipeline")
    MetricsCollector.start(spark = spark, pipelineId = "3633/pipelines/my_first_pipeline")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
