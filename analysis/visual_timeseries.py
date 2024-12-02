##########################################################
# Name:    visual_timeseries.py
# Author:  Alexander X. Gonzalez-Torres
# Acknowledgments: Laura Brown, Joo Young Han
#
# Purpose: Generate a PDF report containing visualizations
#          of time series analysis conducted for every
#          development attribute in the transformed data.
#
# Usage: Further instructions and functionalities below.
#        The functions in visual_timeseries.py
#        are incorporated into the main.py pipeline.
###########################################################

import os
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import seaborn as sns
import itertools

def generate_report(country_code, dev_data, dis_data):
    """
    Generates a time series analysis report for all indicators and saves to a PDF.

    Args:
        1. country_code (str): ISO code of the country (e.g., 'USA')
        2. dev_data (pd.DataFrame): Transformed development data for the country.
        3. dis_data (pd.DataFrame): Transformed disaster data for the country.

    Returns:
        1. A .pdf report with time series graphics for every Indicator in dev_data.
    """
    try:
        # Save routes
        output_dir = "../data/timeseries_reports"
        os.makedirs(output_dir, exist_ok=True)
        output_file = os.path.join(output_dir, f"{country_code}_timeseries_report.pdf")

        # Helps with progress monitoring
        batch_size = 100

        # Extract year columns
        year_cols = [col for col in dev_data.columns if col.isdigit()]
        min_year = int(min(year_cols))
        max_year = int(max(year_cols))

        # Count total indicators
        total_indicators = len(dev_data)
        print(f"[TIMESERIES INFO] {total_indicators} meaningful indicators found "
              f"for {country_code} between {min_year} and {max_year}")

        # Prepare distinct colors for storms in plot
        n_storms = len(dis_data['Event Name'].unique())
        storm_colors = itertools.cycle(sns.color_palette("husl", n_colors=n_storms))

        # Create PDF
        with PdfPages(output_file) as pdf:
            # Loop through each indicator
            for i, row in enumerate(dev_data.iterrows(), start=1):
                indicator_name = row[1]['Indicator Name']
                values = row[1][year_cols].values

                # Log progress every `batch_size`
                if i % batch_size == 0 or i == total_indicators:
                    print(f"[TIMESERIES INFO] Progress: {i} of {total_indicators} Indicators analyzed")

                # Check for missing data, just in case
                if pd.isna(values).all():
                    print(f"[TIMESERIES WARNING] Skipping indicator '{indicator_name}' due to missing data")
                    continue

                try:
                    # Create a figure for the current indicator
                    plt.figure(figsize=(10, 6))
                    plt.plot(year_cols, values, marker='o')

                    # Add disaster year markers with storm names and OFDA/BHA support
                    disaster_years = dis_data['Start Year'].values
                    disaster_names = dis_data['Event Name'].values
                    ofda_support = dis_data['OFDA/BHA Response'].values

                    for year, storm, ofda in zip(disaster_years, disaster_names, ofda_support):
                        if str(year) in year_cols:
                            color = next(storm_colors)  # Get a distinct color for each storm
                            plt.axvline(
                                x=str(year),
                                color=color,
                                linestyle='--',
                                alpha=0.7,
                                label=f"{storm} ({'US Response' if ofda == 'Yes' else 'No US Response'})"
                            )

                    # Customize plot
                    plt.title(f"{country_code}: {indicator_name}")
                    plt.xlabel("Year")
                    plt.ylabel("Value")
                    plt.xticks(rotation=45)
                    plt.grid(True)
                    plt.legend(loc='upper left', fontsize=8)
                    plt.tight_layout()

                    # Save current figure to PDF
                    pdf.savefig()
                    plt.close()

                except Exception as e:
                    print(f"[TIMESERIES ERROR] Failed to plot indicator {indicator_name}: {e}")
                    continue

            print(f"[TIMESERIES INFO] Saved timeseries report for {country_code} at data/timeseries_reports: {country_code}_timeseries_report.pdf")

    except Exception as e:
        print(f"[TIMESERIES ERROR] Error generating report for {country_code}: {e}")
        raise