##############################################################
# Name:    main.py
# Author:  Alexander X. Gonzalez-Torres
#
# Purpose: Pipeline that Extracts, Transforms, Loads
#          development and disaster data for a given country.
#          It then conducts time-series analysis for a number
#          of human development and economic indicators
#          against major hurricanes to aid researchers in
#          determining whether major hurricanes (Cat 3 and above)
#          impact the social and economic futures of countries.
#
# Usage:   This pipeline is currently in a testing phase and the
#          arguments are hardcoded below. Consult extract.py,
#          transform.py, and visual_analytics.py for more information
#          on the arguments for each function below.
#
#          Currently, it's producing a report for
#          PHL between 2007 and 2023
#
# Notes: Be sure to change all country_codes for pipeline to work.
#        Also be mindful of the fact that not every country has
#        similar populations, so be sure to change min_total_affected
#        for transform_disaster_data. We chose 2M after EDA but that
#        using PHL and JPN but doesn't work for Puerto Rico, etc.
##################################################################

from ETL.extract import extract_development_data, extract_disaster_data
from ETL.transform import transform_development_data, transform_disaster_data
from analysis.visual_timeseries import generate_report

# PIPELINE
if __name__ == "__main__":
    try:
        print("[INFO] Extracting development data:")
        development_data = extract_development_data("PRI", save_to_csv=True)

        print("[INFO] Extracting disaster data:")
        disaster_data = extract_disaster_data("PRI",save_to_csv=True)

        print("[INFO] Transforming development data:")
        transformed_development_data = transform_development_data(
            "PRI", development_data, save_to_csv=True
        )

        print("[INFO] Transforming disaster data:")
        transformed_disaster_data = transform_disaster_data(
            "PRI", disaster_data, min_total_affected= 500000, save_to_csv=True
        )

        print("[INFO] Generating time series report:")
        generate_report(
            country_code="PRI",
            dev_data=transformed_development_data,
            dis_data=transformed_disaster_data
        )

        print("[SUCCESS] Pipeline test completed successfully!")

    except Exception as e:
        print(f"[ERROR] Pipeline test failed.")
