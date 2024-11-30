from ETL.extract import extract_development_data, extract_disaster_data
from ETL.transform import transform_development_data, transform_disaster_data

# TEST PIPELINE
if __name__ == "__main__":
    print("[TEST INFO] Extracting development data:")
    development_data = extract_development_data("PHL", save_to_csv=True)

    print("[TEST INFO] Extracting disaster data:")
    disaster_data = extract_disaster_data("PHL", save_to_csv=True)

    print("[TEST INFO] Transforming development data:")
    transformed_development_data = transform_development_data("PHL", development_data, 2017, 2020, save_to_csv=True)
    print("[TEST INFO] Transforming development data:")
    transformed_disaster_data = transform_disaster_data("PHL", disaster_data, 2017, 2020, 100000, save_to_csv=True)
