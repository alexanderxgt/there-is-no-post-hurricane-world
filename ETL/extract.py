##########################################################
# Name:    extract.py
# Author:  Alexander X. Gonzalez-Torres
# Purpose: For a single country, extract development data
#          from the World Bank's World Development Index.
#          Also, extract natural disaster data from The
#          International Disaster Database.
#
# Usage: Further instructions and functionalities below.
#        The functions in extract.py are incorporated into
#        the main.py pipeline.
###########################################################

import os
import pandas as pd
import wbgapi as wb # World Bank API; they publish WDI

#######################################################################################################################
#######################################################################################################################

def extract_development_data(country_code, save_to_csv=False):
    """
    Fetch all indicators for a country and transform DataFrame into a desirable format.

    Args:
        1. country_code (str): ISO3 code of the country (e.g., 'USA')
        2. save_to_csv (bool): If True, saves the transformed DataFrame as a CSV file.

    Returns:
        1. A DataFrame: development_data.
        2. (Optional) A CSV file with the data (e.g., USA_WDI_data.csv).
    """
    try:
        wb.db = 2  # WDI is database number 2
        batch_size = 100  # Number of indicators to fetch per batch

        # In case save_to_csv = True, these save the CSV in proper directory
        output_dir = "../data/extracted"
        os.makedirs(output_dir, exist_ok=True)
        output_file = os.path.join(output_dir, f"{country_code}_WDI_data.csv")

        # Fetch Indicator Code and Indicator Name
        indicators = list(wb.series.list())
        indicator_names = {ind['id']: ind['value'] for ind in indicators}

        # Fetch Country Code and Country Name
        country_info = wb.economy.get(country_code)
        country_name = country_info['value']

        print(f"[EXTRACT INFO] Building DataFrame for {country_code} in batches of {batch_size} Indicators.")
        batch_dfs = []
        failed_batches = []
        for i in range(0, len(indicators), batch_size):
            batch = [ind['id'] for ind in indicators[i:i + batch_size]]
            print(f"[EXTRACT INFO] Fetching batch {i // batch_size + 1}")
            try:
                df_batch = wb.data.DataFrame(
                    series=batch,
                    economy=country_code,
                    time='all',
                    columns='series'
                )
                if not df_batch.empty:
                    batch_dfs.append(df_batch)
            except Exception as e:
                failed_batches.append(batch)
                print(f"[EXTRACT ERROR] Failed to fetch batch {i // batch_size + 1}: {e}")

        if not batch_dfs:
            print("[EXTRACT ERROR] No data fetched. DataFrame and CSV not created.")
            return None

        # Combine the batches
        combined_df = pd.concat(batch_dfs, axis=1)

        # Reset index to facilitate getting years as columns
        combined_df.reset_index(inplace=True)

        # Melt the DataFrame to get Indicators as rows
        melted_df = combined_df.melt(
            id_vars=['time'],
            var_name='Indicator Code',
            value_name='Value'
        )

        # Convert time format from 'YR2020' to '2020'
        melted_df['time'] = melted_df['time'].str[2:]

        # Pivot to get years as columns
        final_df = melted_df.pivot(
            index='Indicator Code',
            columns='time',
            values='Value'
        ).reset_index()

        # Add Country Name and Indicator Name columns to melted DF
        final_df.insert(0, 'Country Name', country_name)
        final_df.insert(1, 'Country Code', country_code)
        final_df.insert(2, 'Indicator Name', final_df['Indicator Code'].map(indicator_names))

        # Sort columns so years are in chronological order
        year_cols = [col for col in final_df.columns if col.isdigit()]
        ordered_cols = ['Country Name', 'Country Code', 'Indicator Name', 'Indicator Code'] + sorted(year_cols)
        final_df = final_df[ordered_cols]

        # Save to CSV if requested
        if save_to_csv:
            try:
                final_df.to_csv(output_file, index=False)
                print(f"[EXTRACT INFO] Creating a CSV for {country_code} at data/extracted: {country_code}_WDI_data.csv")
            except Exception as e:
                print(f"[EXTRACT ERROR] Error saving CSV for {country_code}: {e}")
                raise

        print(f"[EXTRACT INFO] Creating a DataFrame for {country_code}: development_data")
        return final_df

    except Exception as e:
        print(f"[EXTRACT ERROR] Error executing extract_development_data for {country_code}: {e}")
        raise

#######################################################################################################################
#######################################################################################################################

def extract_disaster_data(country_code, save_to_csv=False):
    """
    Extracts disaster data for a specific country.

    Args:
        1. country_code (str): The ISO code of the country to extract (e.g., "USA").
        2. save_to_csv (bool): If True, saves the transformed DataFrames as CSV files.

    Returns:
        1. A DataFrame: disaster_data.
        2. (Optional) A CSV file with the data (e.g., USA_EMDAT_data.csv).
    """
    try:
        disaster_file_path = "../data/raw/EMDAT_complete.csv"
        if not os.path.exists(disaster_file_path):
            raise FileNotFoundError(f"[EXTRACT ERROR] EM-DAT file not found at {disaster_file_path}")

        disaster_data = pd.read_csv(disaster_file_path)
        disaster_data = disaster_data[disaster_data['ISO'] == country_code]

        if disaster_data.empty:
            print(f"[EXTRACT ERROR] Error finding EM-DAT data for ISO: {country_code}")
        else:
            print(f"[EXTRACT INFO] Extracting EM-DAT data for {country_code}")

        # Save data to CSV if requested
        if save_to_csv:
            os.makedirs("../data/extracted", exist_ok=True)
            disaster_data.to_csv(f"../data/extracted/{country_code}_EMDAT_data.csv", index=False)
            print(f"[EXTRACT INFO] Creating a CSV for {country_code} at /data/extracted: {country_code}_EMDAT_data.csv")

        print(f"[EXTRACT INFO] Creating a DataFrame for {country_code}: disaster_data")
        return disaster_data

    except Exception as e:
        print(f"[EXTRACT ERROR] Error executing extract_disaster_data for {country_code}: {e}")
        raise

#######################################################################################################################
#######################################################################################################################

