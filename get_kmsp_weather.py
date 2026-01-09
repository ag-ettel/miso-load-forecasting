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

        return df

    except requests.exceptions.RequestException as e:
        print(f"Error fetching weather data: {e}")
        return None
    except Exception as e:
        print(f"Error processing data: {e}")
        return None

def save_to_parquet(df: pl.DataFrame, output_path: str, filename: str = None):
    """
    Save Polars DataFrame to Parquet file

    Parameters:
    - df: Polars DataFrame
    - output_path: Directory path to save the file
    - filename: Optional custom filename (defaults to weather_data_KMSP_{start}_{end}.parquet)
    """
    if df is None or df.is_empty():
        print("No data to save")
        return False

    # Create output directory if it doesn't exist
    Path(output_path).mkdir(parents=True, exist_ok=True)

    # Generate filename if not provided
    if filename is None:
        min_timestamp = df["timestamp"].min()
        max_timestamp = df["timestamp"].max()
        filename = f"weather_data_KMSP_{min_timestamp.date()}_{max_timestamp.date()}.parquet"

    # Full file path
    file_path = Path(output_path) / filename

    try:
        # Write to Parquet with compression
        df.write_parquet(
            file_path,
            compression="zstd",
            statistics=True
        )
        print(f"Successfully saved weather data to {file_path}")
        print(f"Dataset contains {df.height} hourly records")
        return True

    except Exception as e:
        print(f"Error saving to Parquet: {e}")
        return False

def main():
    """
    Main function to fetch and save KMSP weather data
    """
    ## Define date range here
    ## TO DO: make dynamic
    start_date = "2023-01-01"
    end_date = "2026-01-07"

    ## Define output directory (relative to project directory)
    output_directory = "./data/weather"

    print(f"Fetching hourly weather data for KMSP from {start_date} to {end_date}")

    ## Fetch the data
    weather_df = fetch_kmsp_weather(start_date, end_date)

    if weather_df is not None:
        print(f"\nData preview:")
        print(weather_df.head())
        print(f"\nData types:")
        print(weather_df.dtypes)

        ## Save to Parquet
        success = save_to_parquet(weather_df, output_directory)

        if success:
            # Additional processing example for MISO integration
            print("\nSample integration with MISO data:")
            # Add hour and date columns for easier joins
            enriched_df = weather_df.with_columns([
                pl.col("timestamp").dt.hour().alias("hour"),
                pl.col("timestamp").dt.date().alias("date")
            ])
            print(enriched_df.select(["timestamp", "date", "hour", "temperature_2m", "wind_speed_10m"]).head())

if __name__ == "__main__":
    main()
