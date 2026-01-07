import os
import sys

## Set PYSPARK_PYTHON to the current Python executable
os.environ["PYSPARK_PYTHON"] = sys.executable

## Set Spark's timezone to UTC for DST issue
os.environ["TZ"] = "UTC"
os.environ["SPARK_LOCALITY_WAIT"] = "30000"

## RTDIP / spark config
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, dayofmonth
from rtdip_sdk.pipelines.sources import MISOHistoricalLoadISOSource


## Delta configuration
builder = SparkSession.builder \
    .appName("MISO Load Production") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")  \
    .config("spark.sql.session.timeZone", "UTC")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

for lt in ["actual", "forecast"]:

    try:

        miso_source = MISOHistoricalLoadISOSource(
            spark,
            options={
                "load_type": lt,
                "start_date": "20230106",
                "end_date": "20260105",
                "fill_missing": False
            }
        )

        df = miso_source.read_batch()

        if df.count() == 0:
                    raise ValueError("No records returned - possible API issue or invalid date range")


        print(f"Successfully pulled {df.count()} records")

        ## partitioning
        # 1. Extract date components (year/month/day)
        # 2. Partition by year > month > day
        # 3. Write as Delta Lake 
        df = df.withColumn("year", year("Datetime")) \
            .withColumn("month", month("Datetime")) \
            .withColumn("day", dayofmonth("Datetime"))

        # Write
        file_path = "data/miso_load_" + lt
        (df.write
        .format("delta")  # Delta lake
        .partitionBy("year", "month", "day")  # date partitioning
        .mode("overwrite")
        .save(file_path))


    except Exception as e:
        print(f"ERROR processing {lt} data: {str(e)}")
        continue

spark.stop()
