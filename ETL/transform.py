##########################################################
# Name:    transform.py
# Author:  Alexander X. Gonzalez-Torres
# Acknowledgments: Laura Brown
#
# Purpose: Clean, transform, and reduce dimensionality of
#          the data extracted from the World Development
#          Index and The International Disaster Database.
#
#
# Usage: Further instructions and functionalities below.
#        The functions in transform.py are incorporated into
#        the main.py pipeline.
###########################################################


import os

#######################################################################################################################
#######################################################################################################################

def transform_development_data(
        # all default figures chosen during Exploratory Data Analysis
        country_code,
        development_data,
        df_min_year=2007,
        df_max_year=2023,
        save_to_csv=False,
):
    """
    Transforms development data for a specific country based on user args.

   Args:
       1. country_code (str): The ISO3 code of the country to extract (e.g., "USA").
       2. development_data (pd.DataFrame): Development DataFrame returned by extract.py
       3. df_min_year (int): Most distant year one wishes to extract (default is 2008).
       4. df_max_year (int): Most proximate year one wishes to extract (default is 2020).
       5. min_total_affected (int): Minimum total affected for filtering (default is 2,000,000).
       6. save_to_csv (bool): If True, saves the transformed DataFrame as CSV file.

    Returns:
        1. A filtered DataFrame for a specified country: development_data_transformed.
        2. (Optional) A CSV file with the transformed data (e.g., USA_development_data_transformed.csv).

    """
    try:
        development_data_transformed = development_data.copy()

        development_data_transformed = development_data_transformed.loc[:,
                                    ['Country Code', 'Indicator Name'] +
                                    [str(year) for year in range(df_min_year, df_max_year + 1)]
                                    ]
        print(f"[TRANSFORM INFO] Filtering {country_code} WDI data to only include data between {df_min_year} "
              f"and {df_max_year}")

    # Eliminates Development Indicators where more than half of the years have no data
    # There has to be a more elegant way of doing this. I don't love the create-and-delete column process.
     # Also, "more than half of the years" doesn't feel sensitive enough, but we can come back to this -- ALEX
        num_year_columns = development_data_transformed.iloc[:, 2:].shape[1]
        nc_half = num_year_columns // 2

        development_data_transformed['NaN_count'] = development_data_transformed.iloc[:, 2:].isna().sum(axis=1)
        development_data_transformed = development_data_transformed[
            development_data_transformed['NaN_count'] <= nc_half
            ]
        development_data_transformed = development_data_transformed.drop(columns=['NaN_count'])

    # Eliminate rows where all year columns contain 0s
        development_data_transformed = development_data_transformed[
            ~(development_data_transformed.iloc[:, 2:] == 0).all(axis=1)
        ]

        print(f'[TRANSFORM INFO] Filtering {country_code} WDI data to only include '
              f'Indicators with data for 50% or more of selected years')

    # Handles NULL values as needed using
    # 1. Backward Fill for minimum year in DataFrame ('bfill')
    # 2. Forward Fill for maximum year in DataFrame ('ffill')
    # 3. Linear Interpolation for NULL values between two valid data points ('linear')
        if development_data_transformed.iloc[:, 2:].isna().any().any():
            development_data_transformed.iloc[:, 2:] = (
            development_data_transformed.iloc[:, 2:]
            .interpolate(axis=1, method='linear')
            .bfill(axis=1)
            .ffill(axis=1)
        )
            print(f'[TRANSFORM INFO] Handling NULL values for {country_code} WDI data')
        else:
            print(f'[TRANSFORM INFO] No NULL values found for {country_code} WDI data; skipping NULL handling')

    # Save to CSV if specified
        if save_to_csv:
            os.makedirs("../data/transformed", exist_ok=True)
            development_data_transformed.to_csv(
                f"../data/transformed/{country_code}_WDI_transformed.csv", index=False)

            print(f"[TRANSFORM INFO] Creating CSV for {country_code} development data "
                  f"at data/transformed/{country_code}_WDI_transformed.csv")

        return development_data_transformed

    except Exception as e:
        print(f"[TRANSFORM ERROR] Error transforming development data for {country_code}: {e}")
        raise

#######################################################################################################################
#######################################################################################################################

def transform_disaster_data(
        # all default figures chosen during Exploratory Data Analysis
        country_code,
        disaster_data,
        df_min_year=2007,
        df_max_year=2023,
        min_total_affected=500000,
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
    try:
        disaster_data_transformed = disaster_data.copy()

        disaster_data_transformed = disaster_data_transformed[(disaster_data_transformed['Start Year'] >= df_min_year)
                                                          & (disaster_data_transformed['Start Year'] <= df_max_year)]
        print(f"[TRANSFORM INFO] Filtering {country_code} "
              f"EM-DAT data to only include disasters between {df_min_year} and {df_max_year}")

        disaster_data_transformed = disaster_data_transformed[
            (disaster_data_transformed['Magnitude Scale'] == 'Kph') &
            (disaster_data_transformed['Magnitude'] >= 178)
            ]
        print(f"[TRANSFORM INFO] Filtering data to only include major hurricanes")

        disaster_data_transformed = disaster_data_transformed[disaster_data_transformed['Total Affected'] >= min_total_affected]
        print(f"[TRANSFORM INFO] Filtering {country_code} EM-DAT data to only include disasters "
              f"with {min_total_affected} or more affected")

    # Save data to CSV if requested
        if save_to_csv:
            os.makedirs("../data/transformed", exist_ok=True)
            disaster_data_transformed.to_csv(
                f"../data/transformed/{country_code}_EMDAT_transformed.csv", index=False)

            print(f"[TRANSFORM INFO] Creating CSV for {country_code} disaster data "
                  f"at data/transformed/{country_code}_EMDAT_transformed.csv")

        return disaster_data_transformed

    except Exception as e:
        print(f"[TRANSFORM ERROR] Error transforming disaster data for {country_code}: {e}")
        raise

#######################################################################################################################
#######################################################################################################################