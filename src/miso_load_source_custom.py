"""
Custom MISO Historical Load Source that returns actual and forecast data separately.
This extends the rtdip_sdk.pipelines.sources.MISOHistoricalLoadISOSource to support
fetching actual and forecast data without any mixing or filling.
"""

import logging
import pandas as pd
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from rtdip_sdk.pipelines.sources import MISOHistoricalLoadISOSource as BaseSource


class MISOHistoricalLoadSeparated(BaseSource):
    """
    Extended MISO Historical Load Source that returns actual and forecast data separately.
    """

    def _prepare_data_separated(self, df: pd.DataFrame, data_type: str = "actual") -> pd.DataFrame:
        """
        Creates a new `Datetime` column, removes null values and pivots the data.
        Modified to return EITHER actual OR forecast data separately (never mixed).

        Args:
            df: Raw form of data received from the API.
            data_type: Either "actual" or "forecast" to specify which data to return.

        Returns:
            Data after basic transformations and pivoting for the specified type.
        """

        df = df[df["MarketDay"] != "MarketDay"]

        # Don't fill missing - keep actual and forecast completely separate
        if data_type == "actual":
            # Keep only actual load, drop rows where it's missing
            df = df[["MarketDay", "HourEnding", "ActualLoad (MWh)", "LoadResource Zone"]].copy()
            df = df.rename(columns={"ActualLoad (MWh)": "load"})
        elif data_type == "forecast":
            # Keep only forecast load, drop rows where it's missing
            df = df[["MarketDay", "HourEnding", "MTLF (MWh)", "LoadResource Zone"]].copy()
            df = df.rename(columns={"MTLF (MWh)": "load"})
        else:
            raise ValueError("data_type must be 'actual' or 'forecast'")

        df = df.rename(
            columns={
                "MarketDay": "date",
                "HourEnding": "hour",
                "LoadResource Zone": "zone",
            }
        )
        df = df.dropna()

        df["date_time"] = pd.to_datetime(df["date"]) + pd.to_timedelta(
            df["hour"].astype(int) - 1, "h"
        )

        df.drop(["hour", "date"], axis=1, inplace=True)
        df["load"] = df["load"].astype(float)

        df = df.pivot_table(
            index="date_time", values="load", columns="zone"
        ).reset_index()

        df.columns = [str(x.split(" ")[0]).upper() for x in df.columns]

        rename_cols = {
            "LRZ1": "Lrz1",
            "LRZ2_7": "Lrz2_7",
            "LRZ3_5": "Lrz3_5",
            "LRZ4": "Lrz4",
            "LRZ6": "Lrz6",
            "LRZ8_9_10": "Lrz8_9_10",
            "MISO": "Miso",
            "DATE_TIME": "Datetime",
        }

        df = df.rename(columns=rename_cols)

        return df

    def read_batch_separated(self):
        """
        Fetches both actual and forecast data as separate Spark DataFrames.
        This ensures no data mixing or filling between the two.

        Returns:
            Tuple of (actual_spark_df, forecast_spark_df)
        """
        raw_df = self._pull_data()
        
        # Process actual data
        df_actual = self._prepare_data_separated(raw_df.copy(), data_type="actual")
        df_actual = self._sanitize_data(df_actual)
        df_actual_spark = self.spark.createDataFrame(df_actual)
        
        # Process forecast data
        df_forecast = self._prepare_data_separated(raw_df.copy(), data_type="forecast")
        df_forecast = self._sanitize_data(df_forecast)
        df_forecast_spark = self.spark.createDataFrame(df_forecast)
        
        return df_actual_spark, df_forecast_spark
