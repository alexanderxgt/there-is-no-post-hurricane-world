from extract import extract_country_data
from transform import transform_disaster_data

# TEST PIPELINE
if __name__ == "__main__":
    print("[TEST INFO] Extracting data:")
    development_data, disaster_data = extract_country_data("PHL", save_to_csv=True)

    print("[TEST INFO] Transforming data:")
    transformed_disaster_data = transform_disaster_data("PHL", disaster_data, 2017, 2020, 100000, save_to_csv=True)