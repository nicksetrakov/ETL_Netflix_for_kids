import sqlite3
import pandas as pd
from nltk.sentiment import SentimentIntensityAnalyzer


def filter_kids_friendly_movies_from_sql(database_path):
    """
    Filters kids-friendly movies from an SQLite database based on rating and content description.

    Parameters:
    - database_path (str): The path to the SQLite database.

    Returns:
    - pd.DataFrame: A DataFrame containing kids-friendly movies.

    Steps:
    1. Connect to the SQLite database.
    2. Filter out movies not suitable for kids based on rating.
    3. Remove movies about war or violence from the filtered data.
    4. Display the result or further process the DataFrame.

    Example:
    ```python
    kids_movies = filter_kids_friendly_movies_from_sql('path/to/database.db')
    ```

    Note:
    The function assumes a table named 'NETFLIX_COMBINED_CLEANED' in the database with columns 'rating' and 'description'.
    """

    # Connect to the SQLite database
    connection = sqlite3.connect(database_path)

    # Step 1: Filter out movies not suitable for kids based on rating
    query = '''
  SELECT *
  FROM NETFLIX_COMBINED_CLEANED
  WHERE rating NOT IN  ('NC-17', 'TV-MA', 'NR', 'UR')
  '''

    kids_friendly_data = pd.read_sql_query(query, connection)

    # Step 2: Remove movies about war or violence
    kids_friendly_data = kids_friendly_data[
        ~kids_friendly_data['description'].str.contains('War') & ~kids_friendly_data['description'].str.contains(
            'Violence')]

    # Display the result or further process the kids_friendly_data DataFrame
    print(
        'Filter out movies where the rating says its not for kids and emove all movies which are about war or violence.\n')
    print(kids_friendly_data)
    return kids_friendly_data


def create_shows_for_kids_recommendation_table(netflix_data, database_path):
    """
    Creates a recommendation table for kids' shows based on specified criteria and saves it to an SQLite database and CSV file.

    Parameters:
    - netflix_data (pd.DataFrame): The DataFrame containing Netflix data.
    - database_path (str): The path to the SQLite database.

    Steps:
    1. Create a new column "popularity" with a default value of 2.
    2. Load the list of popular directors from 'popular_directors.csv'.
    3. Load information about GDP from 'gdp_per_capita.csv'.
    4. Assign popularity values based on conditions: directors' popularity and countries with low GDP.
    5. Perform sentiment analysis on movie descriptions to identify movies with positive and uplifting content.
    6. Filter shows based on the 'listed_in' column for children and family content.
    7. Save the final DataFrame as an SQL table named "SHOWS_FOR_KIDS_RECOMMENDATION" in the specified database.
    8. Save the final DataFrame as a CSV file in 'program/data_export/shows_for_kids_recommendation.csv'.

    Example:
    ```python
    create_shows_for_kids_recommendation_table(netflix_data, 'path/to/database.db')
    ```

    Note:
    - Assumes the presence of 'popular_directors.csv' and 'gdp_per_capita.csv' in the 'program/data_sources/' directory.
    - Requires the 'pandas' and 'sqlite3' libraries.
    """

    # Create a new column "popularity" with a default value of 2
    netflix_data['popularity'] = 2

    # Load the list of popular directors
    popular_directors = pd.read_csv("program/data_sources/popular_directors.csv", delimiter=';')
    popular_directors['director'] = popular_directors['director'].str.split(',')
    popular_directors = popular_directors.explode('director')
    popular_directors['director'] = popular_directors['director'].str.strip()

    # Load information about GDP
    gdp_per_capita = pd.read_csv("program/data_sources/gdp_per_capita.csv")

    # Assign popularity values based on conditions
    netflix_data.loc[netflix_data['director'].isin(popular_directors['director']), 'popularity'] = 3
    netflix_data.loc[netflix_data['country'].isin(
        gdp_per_capita['Country'][gdp_per_capita['GDP_per_capita'] < 30000]), 'popularity'] = 0

    # First Extra ideas: Perform sentiment analysis on movie descriptions to identify movies with positive and uplifting content

    sia = SentimentIntensityAnalyzer()
    netflix_data['sentiment_score'] = netflix_data['description'].apply(lambda x: sia.polarity_scores(x)['compound'])
    positive_threshold = 0.2
    netflix_data['is_positive'] = netflix_data['sentiment_score'] > positive_threshold

    # Second Extra ideas: filter for column "listed_in"

    keywords_for_kids = ['Children & Family Movies', "Kids' TV"]

    netflix_data = netflix_data[
        netflix_data['listed_in'].apply(lambda x: any(keyword in x for keyword in keywords_for_kids))]

    # Save the final DataFrame as an SQL table
    print('Saving the final DataFrame as an SQL table shows_for_kids_recommendation...')
    connection = sqlite3.connect(database_path)
    netflix_data[['show_id', 'title', 'popularity']].to_sql("SHOWS_FOR_KIDS_RECOMMENDATION", connection,
                                                            if_exists='replace', index=False, index_label='show_id')

    # Save the final DataFrame as a CSV file
    print('Saving the final DataFrame as a CSV file shows_for_kids_recommendation...')
    netflix_data[['show_id', 'title', 'popularity']].to_csv("program/data_export/shows_for_kids_recommendation.csv",
                                                            index=False)
