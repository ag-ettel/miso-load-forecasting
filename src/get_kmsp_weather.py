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
