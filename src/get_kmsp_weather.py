import requests
import polars as pl
from datetime import datetime, timedelta
from pathlib import Path
import time

def fetch_kmsp_weather(start_date: str, end_date: str) -> pl.DataFrame:
    """
    Fetch historical hourly weather data for Minneapolis/St. Paul International Airport (KMSP)

    Parameters:
    - start_date: Start date in YYYY-MM-DD format
    - end_date: End date in YYYY-MM-DD format

    Returns:
    - Polars DataFrame with hourly weather data
    """
    # KMSP Airport coordinates
    LATITUDE = 44.88200
    LONGITUDE = -93.22180

    # Open-Meteo Historical Weather API endpoint
    url = "https://archive-api.open-meteo.com/v1/archive"

    # Define hourly variables to fetch
    hourly_variables = [
        "temperature_2m",
        "relative_humidity_2m",
        "dewpoint_2m",
        "apparent_temperature",
        "precipitation",
        "rain",
        "snowfall",
        "pressure_msl",
        "surface_pressure",
        "cloud_cover",
        "wind_speed_10m",
        "wind_direction_10m",
        "wind_gusts_10m"
    ]

    # Prepare API request parameters
    params = {
        "latitude": LATITUDE,
        "longitude": LONGITUDE,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": ",".join(hourly_variables),
        "timezone": "UTC",
        "temperature_unit": "fahrenheit",
        "wind_speed_unit": "mph",
        "precipitation_unit": "inch"
    }

    try:
        ## Make the API request
        response = requests.get(url, params=params)
        response.raise_for_status()

        ## Parse JSON response
        data = response.json()

        ## Extract hourly data
        hourly_data = data.get("hourly", {})
        times = hourly_data.get("time", [])

        ## Create a dictionary for the DataFrame
        df_data = {
            "timestamp": times,
            "latitude": [LATITUDE] * len(times),
            "longitude": [LONGITUDE] * len(times),
            "station_id": ["KMSP"] * len(times)
        }

        ## Add weather variables
        for var in hourly_variables:
            df_data[var] = hourly_data.get(var, [])

        ## Create Polars DataFrame
        df = pl.DataFrame(df_data)

        ## Convert timestamp string to datetime
        df = df.with_columns(
            pl.col("timestamp").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M")
        )

        ## remove duplicates if any
        df = df.unique(subset=["timestamp"])

        ## data quality: handle nulls and validate physical states
        key_vars = ["temperature_2m", "relative_humidity_2m", "dewpoint_2m", "precipitation", "wind_speed_10m"]
        
        # Sort by timestamp to ensure fills work correctly
        df = df.sort("timestamp")
        
        # Strategy: forward-fill up to 6 hours for short gaps, then use rolling mean for 6 to 24 hour gaps
        print("\n=== Null Imputation Strategy ===")
        for var in key_vars:
            nulls_before = df.filter(pl.col(var).is_null()).height
            
            # Forward-fill up to 6 hours (short gaps)
            df = df.with_columns(
                pl.col(var).forward_fill(limit=6).alias(f"{var}_filled")
            )
            
            # For remaining nulls, use 24-hour rolling mean (daily average)
            df = df.with_columns(
                pl.col(f"{var}_filled").fill_null(
                    pl.col(var).rolling_mean(window_size=24)
                ).alias(var)
            )
            
            df = df.drop(f"{var}_filled")
            
            nulls_after = df.filter(pl.col(var).is_null()).height
            print(f"{var}: {nulls_before} nulls → {nulls_after} after imputation")
        
        # Drop only rows with any key variable still null (persistent gaps >24 hrs)
        records_before_null_removal = df.height
        for var in key_vars:
            df = df.filter(pl.col(var).is_not_null())
        records_removed = records_before_null_removal - df.height
        print(f"\nRemoved {records_removed} records with persistent gaps (>24 hrs)")
        print(f"Retained {df.height} records after null handling")   
        
        ## Validate impossible physical states
        df = df.with_columns([
            ## dew point should not exceed temperature
            (pl.col("dewpoint_2m") > pl.col("temperature_2m")).alias("dew_gt_temp_flag"),
            ## temperature bounds -60 to 120 F, flag outside this range
            ((pl.col("temperature_2m") < -60) | (pl.col("temperature_2m") > 120)).alias("temp_out_of_bounds_flag"),
            ## wind speed cannot be negative
            (pl.col("wind_speed_10m") < 0).alias("neg_wind_speed_flag"),
            ## wind speed upper bound (200 mph)
            (pl.col("wind_speed_10m") > 200).alias("excessive_wind_speed_flag"),
            ## relative humidity bounds 0-100%
            ((pl.col("relative_humidity_2m") < 0) | (pl.col("relative_humidity_2m") > 100)).alias("rh_out_of_bounds_flag")
        ])
        
        # Report all anomalies
        anomaly_flags = [
            "dew_gt_temp_flag",
            "temp_out_of_bounds_flag",
            "neg_wind_speed_flag",
            "excessive_wind_speed_flag",
            "rh_out_of_bounds_flag"
        ]
        
        print("\n=== Data Quality Report ===")
        for flag in anomaly_flags:
            flag_count = df.filter(pl.col(flag)).height
            pct = 100 * flag_count / df.height if df.height > 0 else 0
            print(f"{flag}: {flag_count} records ({pct:.2f}%)")
        
        # Filter rows with critical anomalies (not excessive wind, which can occur in severe weather)
        records_before = df.height
        df = df.filter(
            ~(pl.col("dew_gt_temp_flag") | 
              pl.col("temp_out_of_bounds_flag") | 
              pl.col("neg_wind_speed_flag") | 
              pl.col("rh_out_of_bounds_flag"))
        )
        records_filtered = records_before - df.height
        print(f"\nFiltered {records_filtered} anomalous records ({100*records_filtered/records_before:.2f}% of total)")
        
        # Drop flag columns before saving
        df = df.drop(anomaly_flags)


        ## Add partitioning columns
        df = df.with_columns([
            pl.col("timestamp").dt.year().alias("year"),
            pl.col("timestamp").dt.month().alias("month"),
            pl.col("timestamp").dt.day().alias("day")
        ])
        return df

    except requests.exceptions.RequestException as e:
        print(f"Error fetching weather data: {e}")
        return None
    except Exception as e:
        print(f"Error processing data: {e}")
        return None

def get_max_timestamp(output_path: str) -> datetime:
    """
    Read existing parquet files and return the maximum timestamp
    Returns None if no data exists
    """
    try:
        parquet_files = list(Path(output_path).rglob("*.parquet"))
        if not parquet_files:
            print(f"No existing data found in {output_path}")
            return None
        
        # Read all parquet files and get max timestamp
        df_existing = pl.scan_parquet(str(parquet_files[0])).select("timestamp").collect()
        if df_existing.is_empty():
            return None
        
        max_ts = df_existing.select(pl.col("timestamp").max()).item()
        print(f"Found existing data. Max timestamp: {max_ts}")
        return max_ts
    except Exception as e:
        print(f"Error reading existing data: {e}")
        return None

def save_partitioned_parquet(df: pl.DataFrame, output_path: str, append: bool = False):
    """
    Save Polars DataFrame with partitioning compatible with Delta Lake
    If append=True, append to existing data instead of overwriting
    """
    if df is None or df.is_empty():
        print("No data to save")
        return False

    # Create output directory
    Path(output_path).mkdir(parents=True, exist_ok=True)

    try:
        if append:
            # Read existing data
            existing_files = list(Path(output_path).rglob("*.parquet"))
            if existing_files:
                df_existing = pl.scan_parquet(f"{output_path}/**/*.parquet").collect()
                # Combine with new data and deduplicate by timestamp
                df = pl.concat([df_existing, df]).unique(subset=["timestamp"])
                print(f"Appending {df.height - df_existing.height} new records to existing {df_existing.height} records")
            
            # Remove old partitioned data before writing new combined dataset
            import shutil
            for partition_dir in Path(output_path).glob("year=*"):
                shutil.rmtree(partition_dir)
        
        # Write with Hive partitioning: year=YYYY/month=M/day=D
        df.write_parquet(
            output_path,
            compression="zstd",
            statistics=True,
            partition_by=["year", "month", "day"]
        )

        print(f"Successfully saved {df.height} hourly records to {output_path}")
        print(f"Partition structure: year=X/month=X/day=X")
        return True

    except Exception as e:
        print(f"Error saving partitioned Parquet: {e}")
        return False

def main():
    """
    Main function to fetch and save KMSP weather data
    """
    
    # Define output directory (relative to project directory)
    output_directory = "./data/weather_kmsp"

    # Check if data exists and get max timestamp
    max_existing_ts = get_max_timestamp(output_directory)
    
    today = datetime.now()
    
    # If data exists, only fetch from after the max timestamp
    # Otherwise, fetch last 3 years
    if max_existing_ts:
        start_date = max_existing_ts
        append_mode = True
    else:
        start_date = today - timedelta(days=3*366)
        append_mode = False
    
    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = today.strftime("%Y-%m-%d")

    print(f"Fetching hourly weather data for KMSP from {start_date_str} to {end_date_str}")
    print(f"Append mode: {append_mode}")

    ## Fetch the data
    weather_df = fetch_kmsp_weather(start_date_str, end_date_str)

    if weather_df is not None:
            print(f"\nData preview:")
            print(weather_df.head())

            # Save with partitioning (append if data exists)
            success = save_partitioned_parquet(weather_df, output_directory, append=append_mode)

            if success:
                # Show partition structure
                print("\nPartition directories created:")
                for p in Path(output_directory).rglob("*.parquet"):
                    print(f"  {p}")

if __name__ == "__main__":
    main()
