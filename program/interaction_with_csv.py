import pandas as pd


def get_unique_countries(csv_file_path):
    """
    Get a list of all unique countries from a CSV file.

    Parameters:
    - csv_file_path (str): The path to the CSV file.

    Returns:
    - list: A list of unique countries.
    """

    # Read data from CSV into a DataFrame
    df = pd.read_csv(csv_file_path, delimiter=';')

    # Extract unique countries from the 'country' column
    unique_countries = (
        df['country']
        .str.split(',')
        .explode()
        .str.strip()
        .dropna()
        .unique()
        .tolist()
    )
    unique_countries = [country for country in unique_countries if country]

    return unique_countries
