import wbgapi as wb
import pandas as pd

def extract_development_data(country_code, output_file, batch_size=100, db=2):
    """
    Fetch all indicators for a country and transform DataFrame into a desirable format.

    Args:
        1. country_code (str): ISO code of the country (e.g., 'USA')
        2.  output_file (str): File name to save the CSV
        batch_size (int): Number of indicators to fetch per batch
        db (int): Database ID to query (default is 2 for World Development Indicators)

    Returns:
        pd.DataFrame: The transformed DataFrame
    """
    wb.db = db

    # Get all indicator IDs and their metadata
    print("[INFO] Fetching all indicators and their metadata...")
    indicators = list(wb.series.list())
    print(f"[INFO] Total indicators found: {len(indicators)}")

    # Create a mapping of indicator IDs to their names
    indicator_names = {ind['id']: ind['value'] for ind in indicators}

    # Get country metadata
    country_info = wb.economy.get(country_code)
    country_name = country_info['value']

    # Fetch data in batches
    print(f"[INFO] Fetching data for {country_code} in batches of {batch_size} indicators...")
    batch_dfs = []
    for i in range(0, len(indicators), batch_size):
        batch = [ind['id'] for ind in indicators[i:i + batch_size]]
        print(f"[INFO] Fetching batch {i // batch_size + 1}")
        try:
            df_batch = wb.data.DataFrame(
                series=batch,
                economy=country_code,
                time='all',
                labels=False,  # We'll add labels manually for more control
                columns='series'
            )
            if not df_batch.empty:
                batch_dfs.append(df_batch)
        except Exception as e:
            print(f"[ERROR] Failed to fetch batch {i // batch_size + 1}: {e}")

    if not batch_dfs:
        print("[ERROR] No data fetched. CSV not created.")
        return None

    # Combine all batches
    print("[INFO] Combining and transforming data...")
    combined_df = pd.concat(batch_dfs, axis=1)

    # Reset index to get time as a column
    combined_df.reset_index(inplace=True)

    # Melt the DataFrame to get indicators as rows
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

    # Add country and indicator name columns
    final_df.insert(0, 'Country Name', country_name)
    final_df.insert(1, 'Country Code', country_code)
    final_df.insert(2, 'Indicator Name', final_df['Indicator Code'].map(indicator_names))

    # Sort columns so years are in chronological order
    year_cols = [col for col in final_df.columns if col.isdigit()]
    ordered_cols = ['Country Name', 'Country Code', 'Indicator Name', 'Indicator Code'] + sorted(year_cols)
    final_df = final_df[ordered_cols]

    # Save to CSV
    final_df.to_csv(output_file, index=False)
    print(f"[INFO] Data saved to {output_file}")

    return final_df

# Example usage
if __name__ == '__main__':
    df = fetch_and_transform_indicators('PHL', 'philippines_indicators.csv')