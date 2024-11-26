import os
import pandas as pd

def extract_country_data(country_code, save_to_csv=False):
    """
    Extracts data for a specific country from both development and disaster datasets.

    Args:
        country_code (str): The ISO code of the country to extract (e.g., "USA").

    Returns:
        1. Two DataFrames for a specified country: the first with development data, the second with disaster data.
        2. (Optional) Two CSV files with the data.
    """
    try:
        # Development data extraction
        development_file_path = "../data/raw/development_data_raw.csv"
        if not os.path.exists(development_file_path):
            raise FileNotFoundError(f"WDI file not found at {development_file_path}")

        development_data = pd.read_csv(development_file_path)
        development_data = development_data[development_data['Country Code'] == country_code]

        if development_data.empty:
            print(f"Error finding WDI data for Country Code: {country_code}")
        else:
            print(f"Successfully extracted WDI data for {country_code}.")

        # Disaster data extraction
        disaster_file_path = "../data/raw/disaster_data_raw.csv"
        if not os.path.exists(disaster_file_path):
            raise FileNotFoundError(f"EM-DAT file not found at {disaster_file_path}")

        disaster_data = pd.read_csv(disaster_file_path)
        disaster_data = disaster_data[disaster_data['ISO'] == country_code]

        if disaster_data.empty:
            print(f"Error finding EM-DAT data for ISO: {country_code}")
        else:
            print(f"Successfully extracted EM-DAT data for {country_code}.")

        # Save data to CSV if requested
        if save_to_csv:
            os.makedirs("../data/unfiltered_ETLoutputs", exist_ok=True)
            development_data.to_csv(f"../data/unfiltered_ETLoutputs/{country_code}_development_data.csv", index=False)
            disaster_data.to_csv(f"../data/unfiltered_ETLoutputs/{country_code}_disaster_data.csv", index=False)
            print(f"Two CSVs created for {country_code} at ../data/outputs: PHL_development_data and PHL_disaster_data.")


        print(f"Two DataFrames created for {country_code}: development_data and disaster_data.")
        return development_data, disaster_data

    except Exception as e:
        print(f"Error executing extract_country_data for {country_code}: {e}")
        raise

## TEST ##
if __name__ == "__main__":
    extract_country_data(country_code="PHL", save_to_csv=True)