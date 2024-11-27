import os


def transform_development_data(
        # all default figures chosen during Exploratory Data Analysis
        country_code,
        development_data,
        df_min_year=2008,
        df_max_year=2020,
        save_to_csv=False,
):
    """
    Transforms development data for a specific country based on user args.

   Args:
       1. country_code (str): The ISO code of the country to extract (e.g., "USA").
       2. development_data (pd.DataFrame): Development DataFrame returned by extract.py
       3. df_min_year (int): Most distant year one wishes to extract (default is 2008).
       4. df_max_year (int): Most proximate year one wishes to extract (default is 2020).
       5. min_total_affected (int): Minimum total affected for filtering (default is 2,000,000).

       . save_to_csv (bool): If True, saves the transformed DataFrame as CSV file.

    Returns:
        1. A filtered DataFrame for a specified country: development_data_transformed.
        2. (Optional) A CSV file with the transformed data (e.g., USA_development_data_transformed.csv).

    """
    development_data_transformed = development_data.copy()

    development_data_transformed = development_data_transformed.loc[:,
                                   ['Country Code', 'Indicator Name'] +
                                   [str(year) for year in range(df_min_year, df_max_year + 1)]
                                   ]
    print(f"[TRANSFORM INFO] Filtering {country_code} WDI data to only include data between {df_min_year} and {df_max_year}")

    # Eliminate Development Indicators where more than half of the years have no data
    # There has to be a more elegant way of doing this. I don't love the create-and-delete column process.
    # Also, "more than half of the years" doesn't feel sensitive enough, but we can come back to this -- ALEX
    num_columns = development_data_transformed.shape[1]
    nc_half = num_columns // 2






    # Save to CSV if specified
    if save_to_csv:
        os.makedirs("../data/filtered_ETL_outputs", exist_ok=True)
        development_data_transformed.to_csv(f"../data/filtered_ETL_outputs/{country_code}_development_data_transformed.csv", index=False)

        print(f"[TRANSFORM INFO] Creating CSV for {country_code} development data at /data/filtered_ETL_outputs/{country_code}_development_disaster_data_transformed.csv")

    return development_data_transformed


def transform_disaster_data(
        # all default figures chosen during Exploratory Data Analysis
        country_code,
        disaster_data,
        df_min_year=2008,
        df_max_year=2020,
        min_total_affected=2000000,
        save_to_csv=False
):
    """
    Transforms disaster data for a specific country based on user args.

    Args:
       1. country_code (str): The ISO code of the country to extract (e.g., "USA").
       2. disaster_data (pd.DataFrame): Disaster DataFrame returned by extract.py.
       3. df_min_year (int): Most distant year one wishes to extract (default is 2008).
       4. df_max_year (int): Most proximate year one wishes to extract (default is 2020).
       5. min_total_affected (int): Minimum total affected for filtering (default is 2,000,000).
       6. save_to_csv (bool): If True, saves the transformed DataFrame as CSV file.

    Returns:
        1. A filtered DataFrame for a specified country: disaster_data_transformed.
        2. (Optional) A CSV file with the transformed data (e.g., USA_disaster_data_transformed.csv).

    """
    disaster_data_transformed = disaster_data.copy()

    disaster_data_transformed = disaster_data_transformed[(disaster_data_transformed['Start Year'] >= df_min_year)
                                                          & (disaster_data_transformed['Start Year'] <= df_max_year)]
    print(f"[TRANSFORM INFO] Filtering {country_code} EM-DAT data to only include disasters between {df_min_year} and {df_max_year}")

    disaster_data_transformed = disaster_data_transformed[disaster_data_transformed['Total Affected'] >= min_total_affected]
    print(f"[TRANSFORM INFO] Filtering {country_code} EM-DAT data to only include disasters with {min_total_affected} or more affected")

    # Save data to CSV if requested
    if save_to_csv:
        os.makedirs("../data/filtered_ETL_outputs", exist_ok=True)
        disaster_data_transformed.to_csv(f"../data/filtered_ETL_outputs/{country_code}_disaster_data_transformed.csv", index=False)

        print(f"[TRANSFORM INFO] Creating CSV for {country_code} disaster data at /data/filtered_ETL_outputs/{country_code}_disaster_data_transformed.csv")

    return disaster_data_transformed


