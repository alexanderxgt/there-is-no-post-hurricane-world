import os


def transform_disaster_data(
        # all default figures chosen during Exploratory Data Analysis
        country_code,
        disaster_data,
        df_start_year=2008,
        df_end_year=2020,
        min_total_affected=2000000,
        save_to_csv=False
):
    """
    Transforms disaster data for a specific country based on user args.

    Args:
       1. country_code (str): ISO code of the country.
       2. disaster_data (pd.DataFrame): Raw disaster data.
       3. df_start_year (int): Start year for filtering (default is 2008).
       4. df_end_year (int): End year for filtering (default is 2020).
       5. min_total_affected (int): Minimum total affected for filtering (default is 2,000,000).
       6. save_to_csv (bool): If True, saves the transformed DataFrame to a CSV.

    Returns:
        1. A filtered DataFrame for a specified country: disaster_data_transformed.
        2. (Optional) A CSV file with the transformed data (e.g., USA_disaster_data_transformed.csv).

    """
    disaster_data_transformed = disaster_data[(disaster_data['Start Year'] >= df_start_year) & (disaster_data['Start Year'] <= df_end_year)]
    print(f"[TRANSFORM INFO] Filtering {country_code} EM-DAT data to only include disasters between {df_start_year} and {df_end_year}...")

    disaster_data_transformed = disaster_data_transformed[disaster_data_transformed['Total Affected'] >= min_total_affected]
    print(f"[TRANSFORM INFO] Filtering {country_code} EM-DAT data to only include disasters with {min_total_affected} or more affected...")

    # Save data to CSV if requested
    if save_to_csv:
        os.makedirs("../data/filtered_ETL_outputs", exist_ok=True)
        disaster_data_transformed.to_csv(f"../data/filtered_ETL_outputs/{country_code}_disaster_data_transformed.csv", index=False)
        print(f"[TRANSFORM INFO] Creating CSV for disaster data at ../data/filtered_ETL_outputs/{country_code}_disaster_data_transformed.csv...")

    return disaster_data_transformed


