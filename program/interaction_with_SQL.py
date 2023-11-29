import sqlite3
import pandas as pd
from program.helper_functions import show_all_tables, show_table_schema, get_relations, show_data_from_table


def create_sql_tables(database_path):
    """
    Create SQL tables for NETFLIX_SHOWS, RATINGS, and GDP_PER_CAPITA.
  
    Args:
    - database_path (str): Path to the SQLite database.
  
    Returns:
    None
    """

    # Connect to the SQLite database
    connection = sqlite3.connect(database_path)
    cursor = connection.cursor()

    # Create table RATINGS
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS RATINGS (
        id INTEGER PRIMARY KEY,
        name VARCHAR(50)
    );
    """)

    # Create table GDP_PER_CAPITA
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS GDP_PER_CAPITA (
        Country VARCHAR(50) PRIMARY KEY,
        GDP_per_capita FLOAT
    );
    """)

    # Define the schema for the NETFLIX_SHOWS table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS NETFLIX_SHOWS (
        show_id VARCHAR(50) PRIMARY KEY,
        type VARCHAR(50),
        title VARCHAR(100),
        director VARCHAR(100),
        cast TEXT,
        country VARCHAR(150),
        date_added DATE,
        release_year INTEGER,
        rating_id INTEGER,
        duration VARCHAR(50),
        listed_in VARCHAR(150),
        description TEXT,
        FOREIGN KEY (rating_id) REFERENCES RATINGS(id)
        FOREIGN KEY (country) REFERENCES GDP_PER_CAPITA(Country)
    );
    """)

    # Commit the changes and close the connection
    connection.commit()

    # Check if tables were created as intended
    show_all_tables(connection)

    # Check the schema of each table
    show_table_schema(connection, 'NETFLIX_SHOWS')
    show_table_schema(connection, 'RATINGS')
    show_table_schema(connection, 'GDP_PER_CAPITA')

    # Check relations between tables
    get_relations(connection)


def insert_data_into_tables(database_path):
    """
    Insert data into SQL tables NETFLIX_SHOWS, RATINGS, and GDP_PER_CAPITA.

    Args:
    - database_path (str): Path to the SQLite database.

    Returns:
    None
    """
    # Connect to the SQLite database

    connection = sqlite3.connect(database_path)

    # Insert data into tables (Make sure to adapt this based on your actual data)
    netflix_shows_data = pd.read_csv("program/data_sources/netflix_shows.csv", delimiter=";")
    ratings_data = pd.read_csv("program/data_sources/ratings.csv")
    gdp_per_capita_data = pd.read_csv("program/data_sources/gdp_per_capita.csv")

    netflix_shows_data.columns = ['show_id', 'type', 'title', 'director', 'cast', 'country', 'date_added',
                                  'release_year', 'rating_id', 'duration', 'listed_in', 'description']

    # Write data into tables
    netflix_shows_data.to_sql("NETFLIX_SHOWS", connection, if_exists="replace", index=False)
    ratings_data.to_sql("RATINGS", connection, if_exists="replace", index=False)
    gdp_per_capita_data.to_sql("GDP_PER_CAPITA", connection, if_exists="replace", index=False)

    # Commit the changes
    connection.commit()

    # Check if data was inserted into tables
    show_data_from_table(connection, 'NETFLIX_SHOWS')
    show_data_from_table(connection, 'RATINGS')
    show_data_from_table(connection, 'GDP_PER_CAPITA')

    # Close the connection
    connection.close()


def join_tables(database_path, new_table):
    """
    Join NETFLIX_SHOWS and RATINGS tables and create a new table.

    Args:
    - database_path (str): Path to the SQLite database.
    - new_table (str): Name of the new table to be created.

    Returns:
    None
    """

    # Connect to the SQLite database
    connection = sqlite3.connect(database_path)

    sql_query = '''
    SELECT ns.show_id, ns.type, ns.title, ns.director, ns.cast, ns.country, ns.date_added, ns.release_year, r.name as rating, ns.duration, ns.listed_in, ns.description
    FROM NETFLIX_SHOWS AS ns
    LEFT JOIN RATINGS AS r
    ON ns.rating_id = r.id
    '''
    # Join tables
    netflix_shows_ratings = pd.read_sql_query(sql_query, connection, index_col='show_id')

    # Write data into tables
    netflix_shows_ratings.to_sql(new_table, connection, if_exists="replace", index=True, index_label='show_id')

    # Commit the changes
    connection.commit()

    # Check if data was inserted into tables
    show_data_from_table(connection, new_table)

    # Close the connection
    connection.close()


def create_view(database_path, new_view):
    """
    Create a new view.
        Args:
    - database_path (str): Path to the SQLite database.
    - new_view (str): Name of the new view to be created.
    
    Returns:
    None
    """
    # Connect to the SQLite database
    connection = sqlite3.connect(database_path)

    # Create a new view
    sql_query = '''
    CREATE VIEW IF NOT EXISTS {} AS
    SELECT ns.show_id, ns.type, ns.title, ns.director, ns.cast, ns.country, ns.date_added, ns.release_year, r.name as rating, ns.duration, ns.listed_in, ns.description
    FROM NETFLIX_SHOWS AS ns
    LEFT JOIN RATINGS AS r
    ON ns.rating_id = r.id
    '''.format(new_view)

    # Create a new view
    connection.execute(sql_query)

    # Commit the changes
    connection.commit()

    sql_query = '''SELECT show_id, rating FROM {}'''.format(new_view)
    view_data = pd.read_sql_query(sql_query, connection)
    print(view_data)

    # Close the connection
    connection.close()


def clean_and_create_table(database_path, view):
    """
    Function: clean_and_create_table

    Description:
    This function connects to an SQLite database specified by the `database_path`, loads data from the specified `view`,
    applies various cleaning operations on the data, and creates a new table named NETFLIX_COMBINED_CLEANED in the same database.

    Cleaning Operations:
    1. Removes rows where the "cast" column is empty.
    2. Replaces missing values in the "country" column with 'unknown'.
    3. Replaces multiple countries in the "country" column with 'many'.
    4. Removes "|TITLE|" from the "title" column.
    5. Adds a new column "release_2000_or_newer" indicating whether the movie was released in 2000 or later.

    Parameters:
    - database_path: Path to the SQLite database.
    - view: The name of the view from which data will be loaded.

    Usage Example:
    clean_and_create_table('your_database.db', 'your_view_name')
    """

    connection = sqlite3.connect(database_path)
    # Load data from the VIEW into a pandas DataFrame
    view_data = pd.read_sql_query(f'SELECT * FROM {view}', connection)

    # Remove rows where "cast" is empty
    view_data = view_data[view_data['cast'].notna() & (view_data['cast'] != '')]

    # Replace missing values in "country" with 'unknown'
    view_data['country'].fillna('unknown', inplace=True)

    # Replace multiple countries in "country" with 'many'
    view_data['country'] = view_data['country'].apply(lambda x: 'many' if ',' in str(x) else x)

    # Remove "|TITLE|" from "title"
    view_data['title'] = view_data['title'].str.replace('|TITLE|', '')

    # Add a new column "release_2000_or_newer"
    view_data['release_2000_or_newer'] = view_data['release_year'].apply(lambda year: 'yes' if year >= 2000 else 'no')

    # Write data to a new table
    view_data.to_sql('NETFLIX_COMBINED_CLEANED', connection, if_exists='replace', index=False, index_label='show_id')
    show_data_from_table(connection, 'NETFLIX_COMBINED_CLEANED')
    connection.close()
