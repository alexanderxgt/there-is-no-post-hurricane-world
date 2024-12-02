##########################################################
# Name:    main.py
# Author:  Alexander X. Gonzalez-Torres
#
# Purpose: Pipeline that Extracts, Transforms, Loads
#          development and disaster data for a given country.
#          It then conducts time-series analysis for a number
#          of human development and economic indicators
#          against major hurricanes to aid researchers in
#          determining whether major hurricanes (Cat 3 and above)
#          impact the social and economic
#
# Usage:   This pipeline is currently in a testing phase and the
#          arguments are hardcoded below. Consult extract.py,
#          transform.py, and visual_analytics.py for more information
#          on the arguments for each function below.
#
#          Currently, it's producing a report for
#          PHL between 2017 and 2020.
#
###########################################################

from ETL.extract import extract_development_data, extract_disaster_data
from ETL.transform import transform_development_data, transform_disaster_data
from analysis.visual_timeseries import generate_report

# TEST PIPELINE
if __name__ == "__main__":
    try:
        print("[TEST INFO] Extracting development data:")
        development_data = extract_development_data("PHL", save_to_csv=True)

        print("[TEST INFO] Extracting disaster data:")
        disaster_data = extract_disaster_data("PHL", save_to_csv=True)

        print("[TEST INFO] Transforming development data:")
        transformed_development_data = transform_development_data(
            "PHL", development_data, 2017, 2020, save_to_csv=True
        )

        print("[TEST INFO] Transforming disaster data:")
        transformed_disaster_data = transform_disaster_data(
            "PHL", disaster_data, 2017, 2020, save_to_csv=True
        )

        print("[TEST INFO] Generating time series report:")
        generate_report(
            country_code="PHL",
            dev_data=transformed_development_data,
            dis_data=transformed_disaster_data
        )

        print("[TEST SUCCESS] Pipeline test completed successfully!")

    except Exception as e:
        print(f"[TEST ERROR] Pipeline test failed.")
