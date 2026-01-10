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

def save_partitioned_parquet(df: pl.DataFrame, output_path: str):
    """
    Save Polars DataFrame with partitioning compatible with Delta Lake
    """
    if df is None or df.is_empty():
        print("No data to save")
        return False

    # Create output directory
    Path(output_path).mkdir(parents=True, exist_ok=True)

    try:
        # Write with Hive partitioning: year=YYYY/month=M/day=D
        df.write_parquet(
            output_path,
            compression="zstd",
            statistics=True,
            partition_by=["year", "month", "day"] #,
           # maintain_order=False
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
    
    # gets last 3 years
    from datetime import datetime, timedelta

    today = datetime.now()
    start_date = datetime.now() - timedelta(days=3*367)
    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = today.strftime("%Y-%m-%d")

    ## Define output directory (relative to project directory)
    output_directory = "./data/weather_kmsp"

    print(f"Fetching hourly weather data for KMSP from {start_date_str} to {end_date_str}")

    ## Fetch the data
    weather_df = fetch_kmsp_weather(start_date_str, end_date_str)

    if weather_df is not None:
            print(f"\nData preview:")
            print(weather_df.head())

            # Save with partitioning
            success = save_partitioned_parquet(weather_df, output_directory)

            if success:
                # Show partition structure
                print("\nPartition directories created:")
                for p in Path(output_directory).rglob("*.parquet"):
                    print(f"  {p}")

if __name__ == "__main__":
    main()
