def transform_data(development_data, disaster_data, save_to_csv=False):
    """
    Transforms development and disaster DataFrames for a specific country based
    on parameters decided during Exploratory Data Analysis phase of this project.

    Args:
        development_data (pd.DataFrame): Raw development data.
        disaster_data (pd.DataFrame): Raw disaster data.

    Returns:
        1. Two filtered DataFrames for a specified country: transformed_development_data, transformed_disaster_data.
        2. (Optional) Two CSV files with the transformed data.

    """
