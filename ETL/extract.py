import os
import pandas as pd

def extract_country_data(country_code, save_to_csv=False):
    """
    Extracts data for a specific country from both development and disaster datasets.

    Args:
        1. country_code (str): The ISO code of the country to extract (e.g., "USA").
        2. save_to_csv (bool): If True, saves the transformed DataFrames as CSV files.

    Returns:
        1. Two DataFrames for a specified country: development_data and disaster_data.
        2. (Optional) Two CSV files with the data (e.g., USA_disaster_data_.csv and USA_development_data.csv).
    """
    try:
        # Development Data Extraction
        development_file_path = "../data/raw/development_data_raw.csv"
        if not os.path.exists(development_file_path):
            raise FileNotFoundError(f"[EXTRACT ERROR] WDI file not found at {development_file_path}")

        development_data = pd.read_csv(development_file_path)
        development_data = development_data[development_data['Country Code'] == country_code]

        if development_data.empty:
            print(f"[EXTRACT ERROR] Error finding WDI data for Country Code: {country_code}")
        else:
            print(f"[EXTRACT INFO] Extracting WDI data for {country_code}...")

        # Disaster Data Extraction
        disaster_file_path = "../data/raw/disaster_data_raw.csv"
        if not os.path.exists(disaster_file_path):
            raise FileNotFoundError(f"[EXTRACT ERROR] EM-DAT file not found at {disaster_file_path}")

        disaster_data = pd.read_csv(disaster_file_path)
        disaster_data = disaster_data[disaster_data['ISO'] == country_code]

        if disaster_data.empty:
            print(f"[EXTRACT ERROR] Error finding EM-DAT data for ISO: {country_code}")
        else:
            print(f"[EXTRACT INFO] Extracting EM-DAT data for {country_code}...")

        # Save data to CSV if requested
        if save_to_csv:
            os.makedirs("../data/unfiltered_ETL_outputs", exist_ok=True)
            development_data.to_csv(f"../data/unfiltered_ETL_outputs/{country_code}_development_data.csv", index=False)
            disaster_data.to_csv(f"../data/unfiltered_ETL_outputs/{country_code}_disaster_data.csv", index=False)
            print(f"[EXTRACT INFO] Creating two CSVs for {country_code} at ../data/_unfiltered_ETL_outputs: {country_code}_development_data and {country_code}_disaster_data...")


        print(f"[EXTRACT INFO] Creating two DataFrames for {country_code}: development_data and disaster_data...")
        return development_data, disaster_data

    except Exception as e:
        print(f"[EXTRACT ERROR] Error executing extract_country_data for {country_code}: {e}")
        raise

