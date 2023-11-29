import logging
import requests
import time
import pandas as pd


def fetch_gdp_per_capita(country_list, api_url, api_key, csv_file_path, is_test=False, max_retries=3):
    """
    Fetch GDP per capita for each country from the ninja API and save to a CSV file.

    Parameters:
    - country_list (list): List of countries.
    - is_test (bool): Optional parameter to test the function with only the first 10 countries.

    Returns:
    - None
    """
    if is_test:
        country_list = country_list[:10]  # If it's a test, use only the first 10 countries

    gdp_data = []

    for country in country_list:
        name_country = country
        api_url = f'{api_url}?name={name_country}'
        retries = 0
        while retries < max_retries:
            try:
                response = requests.get(api_url, headers={'X-Api-Key': api_key})
                response.raise_for_status()  # Checking for errors

                country_data_list = response.json()
                logging.debug(f"Country Data for {name_country}: {country_data_list}")
                if country_data_list:
                    # Use the first (and only) element of the list
                    country_data = country_data_list[0]

                gdp_per_capita = country_data.get('gdp_per_capita', None)
                gdp_data.append({'Country': country, 'GDP_per_capita': gdp_per_capita})
                break  # Exiting the loop after a successful request
            except requests.exceptions.RequestException as err:
                logging.error(f"Error for {name_country}: {err}")
                retries += 1
                logging.warning(f"Retrying {retries}/{max_retries}...")
                time.sleep(5)  # Pause between retries

    # Convert the data to a DataFrame
    gdp_df = pd.DataFrame(gdp_data)

    # Save the DataFrame to a CSV file
    gdp_df.to_csv(csv_file_path, index=False)

    print(f'Data has been successfully exported to {csv_file_path}')
