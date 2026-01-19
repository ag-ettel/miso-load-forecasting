import os
import sys

# Set PYSPARK_PYTHON to the current Python executable
os.environ["PYSPARK_PYTHON"] = sys.executable

# Set Spark's timezone to UTC for DST issue
os.environ["TZ"] = "UTC"
os.environ["SPARK_LOCALITY_WAIT"] = "30000"

## to get last 3 years from today's date
from datetime import datetime, timedelta

# RTDIP / spark config
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, dayofmonth
from miso_load_source_custom import MISOHistoricalLoadSeparated


# Delta configuration
builder = SparkSession.builder \
    .appName("MISO Load Production") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")  \
    .config("spark.sql.session.timeZone", "UTC")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

## function to help get time difference
def add_years(d, years):
    """Return a date that's `years` years after the date (or before if negative).
    Handles leap year edge cases by falling back to Feb 28 if needed."""
    try:
        return d.replace(year=d.year + years)
    except ValueError:
        # Handle February 29 for non-leap years
        return d.replace(year=d.year + years, day=28)

## Calculate today and 3 years ago
today = datetime.now()
start_date = add_years(today, -3)  # Go back 3 years

## Format as required for MISOHistoricalLoadISOSource (YYYYMMDD)
start_date_str = start_date.strftime("%Y%m%d")
end_date_str = today.strftime("%Y%m%d")
actual_end_date_str = (today - timedelta(days=1)).strftime("%Y%m%d")

try:
    miso_source = MISOHistoricalLoadSeparated(
        spark,
        options={
            "start_date": start_date_str,
            "end_date": actual_end_date_str,
            "fill_missing": "false" # must be false to ensure proper forecast error analysis
        }
    )

    # Get actual and forecast as separate dataframes
    df_actual, df_forecast = miso_source.read_batch_separated()

    print(f"Successfully pulled {df_actual.count()} actual records")
    print(f"Successfully pulled {df_forecast.count()} forecast records")

    # Process actual data
    print("\nProcessing actual load data...")
    from pyspark.sql.functions import lit
    df_actual = df_actual.withColumn("load_type", lit("actual"))
    # data quality: filter out load values less than or equal to zero
    actual_count_before = df_actual.count()
    df_actual = df_actual.filter(df_actual["Lrz1"] > 0) 
    actual_count_after = df_actual.count()
    print(f"Filtered out {actual_count_before - actual_count_after} actual records with non-positive load values")
    # flag Lrz1 load values greater than stated installed capacity (20304 MW* for lrz_1)
    # *source: https://cdn.misoenergy.org/PY%202026-2027%20LOLE%20Study%20Report728909.pdf
    # note: this project focuses on lrz_1 (MISO North), but similar checks should be applied if other LRZs are used
    df_actual = df_actual.withColumn("Lrz1_capacity_flag", (df_actual["Lrz1"] > 20304).cast("integer"))
    # add partitioning columns (year, month, day)
    df_actual = df_actual.withColumn("year", year("Datetime")) \
        .withColumn("month", month("Datetime")) \
        .withColumn("day", dayofmonth("Datetime"))
    
    (df_actual.write
    .format("delta")
    .partitionBy("year", "month", "day")
    .mode("overwrite")
    .save("data/miso_load_actual"))
    
    print("Saved actual load data")

    # Process forecast data
    print("\nProcessing forecast load data...")
    df_forecast = df_forecast.withColumn("load_type", lit("forecast"))
    # data quality: filter out load values less than or equal to zero
    forecast_count_before = df_forecast.count()
    df_forecast = df_forecast.filter(df_forecast["Lrz1"] > 0)
    forecast_count_after = df_forecast.count()
    print(f"Filtered out {forecast_count_before - forecast_count_after} forecast records with non-positive load values")
     # flag Lrz1 load values greater than stated installed capacity (20304 MW* for lrz_1)
    df_forecast = df_forecast.withColumn("Lrz1_capacity_flag", (df_forecast["Lrz1"] > 20304).cast("integer"))
    # add partitioning columns (year, month, day)
    df_forecast = df_forecast.withColumn("year", year("Datetime")) \
        .withColumn("month", month("Datetime")) \
        .withColumn("day", dayofmonth("Datetime"))
    
    (df_forecast.write
    .format("delta")
    .partitionBy("year", "month", "day")
    .mode("overwrite")
    .save("data/miso_load_forecast"))
    
    print("Saved forecast load data")

except Exception as e:
    print(f"ERROR processing MISO data: {str(e)}")

spark.stop()
