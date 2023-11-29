from program.interaction_with_SQL import create_sql_tables, insert_data_into_tables, join_tables, create_view, \
    clean_and_create_table
from program.interaction_with_GCP import download_blob, read_from_bigquery_and_save_csv
from program.interaction_with_csv import get_unique_countries
from program.interaction_with_API import fetch_gdp_per_capita
from program.authorization.api_key import API_KEY, API_URL
from program.data_transformation import filter_kids_friendly_movies_from_sql, create_shows_for_kids_recommendation_table

database_path = "program/database/netflix_database.db"
project_id = "python-rocket-1"
bucket_name = "python-rocket-source-data-4s23"
source_blob_name = "etl-netflix/netflix_shows.csv"
destination_file_name = "program/data_sources/netflix_shows.csv"
service_user_key_file_path = "program/authorization/service_user_read_file.example.json"
service_user_key_bigquery_path = "program/authorization/service_user_read_bigquery.example.json"
dataset_id = "etl_netflix"
table_id = "ratings"
csv_file_path_ratings = "program/data_sources/ratings.csv"
csv_file_path_netflix_shows = "program/data_sources/netflix_shows.csv"
csv_file_path_gdp_per_capita = "program/data_sources/gdp_per_capita.csv"
name_view = "VIEW_NETFLIX_SHOWS_WITH_RATING"


def main():
    print("---------------------")
    print("netflix for kids")
    print("---------------------")
    download_blob(bucket_name=bucket_name, source_blob_name=source_blob_name,
                  destination_file_name=destination_file_name, project_id=project_id,
                  service_account_file=service_user_key_file_path)
    read_from_bigquery_and_save_csv(project_id=project_id, dataset_id=dataset_id, table_id=table_id,
                                    service_user_key_path=service_user_key_bigquery_path,
                                    csv_file_path=csv_file_path_ratings)
    unique_countries = get_unique_countries(csv_file_path_netflix_shows)
    fetch_gdp_per_capita(unique_countries, api_key=API_KEY, api_url=API_URL, csv_file_path=csv_file_path_gdp_per_capita,
                         is_test=False)
    create_sql_tables(database_path)
    insert_data_into_tables(database_path)
    join_tables(database_path=database_path, new_table="NETFLIX_META_WITH_RATING")
    create_view(database_path=database_path, new_view=name_view)
    clean_and_create_table(database_path=database_path, view=name_view)
    filter_for_kids = filter_kids_friendly_movies_from_sql(database_path=database_path)
    create_shows_for_kids_recommendation_table(database_path=database_path, netflix_data=filter_for_kids)


if __name__ == "__main__":
    main()
