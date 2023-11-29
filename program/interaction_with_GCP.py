import pandas as pd
from google.cloud import storage
from google.oauth2 import service_account


def download_blob(bucket_name, source_blob_name, destination_file_name, project_id, service_account_file):
    """
    Downloads a blob from Google Cloud Storage (GCS) bucket.

    Parameters:
    - bucket_name (str): The ID of the GCS bucket.
    - source_blob_name (str): The ID of the GCS object (blob) to be downloaded.
    - destination_file_name (str): The local path to which the file should be downloaded.
    - project_id (str): The ID of the Google Cloud project.
    - service_account_file (str): The path to the service account key file.

    Returns:
    - None

    This function uses the Google Cloud Storage client library to download a blob
    from the specified GCS bucket to the local file system.

    Example:
    download_blob(
        bucket_name="your-bucket-name",
        source_blob_name="storage-object-name",
        destination_file_name="local/path/to/file",
        project_id="your-project-id",
        service_account_file="path/to/service/account/key.json"
    )
    """

    credentials = service_account.Credentials.from_service_account_file(service_account_file)

    """Downloads a blob from the bucket."""
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"

    # The ID of your GCS object
    # source_blob_name = "storage-object-name"

    # The path to which the file should be downloaded
    # destination_file_name = "local/path/to/file"

    storage_client = storage.Client(project=project_id, credentials=credentials)

    bucket = storage_client.bucket(bucket_name)

    # Construct a client side representation of a blob.
    # Note `Bucket.blob` differs from `Bucket.get_blob` as it doesn't retrieve
    # any content from Google Cloud Storage. As we don't need additional data,
    # using `Bucket.blob` is preferred here.
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)

    print(
        "Downloaded storage object {} from bucket {} to local file {}.".format(
            source_blob_name, bucket_name, destination_file_name
        )
    )


def read_from_bigquery_and_save_csv(project_id, dataset_id, table_id, service_user_key_path, csv_file_path):
    """
    Export data from a BigQuery table to a CSV file, rename a specified column, and save it.

    Parameters:
    - project_id (str): The Google Cloud project ID.
    - dataset_id (str): The BigQuery dataset ID.
    - table_id (str): The BigQuery table ID.
    - service_user_key_path (str): The path to the service user JSON key file for BigQuery authorization.
    - csv_file_path (str): The path where the CSV file will be saved.
    - new_column_name (str): The new name for the specified column (default is 'name').

    Returns:
    - None
    """

    # Set up BigQuery credentials
    credentials = service_account.Credentials.from_service_account_file(service_user_key_path)

    # Set up the BigQuery query string
    query = f'SELECT * FROM `{project_id}.{dataset_id}.{table_id}`'

    # Read data from BigQuery into a DataFrame
    df = pd.read_gbq(query, project_id=project_id, credentials=credentials)

    # Rename the specified column
    df = df.rename(columns={'rating': 'name'})

    # Save the DataFrame to a CSV file
    df.to_csv(csv_file_path, index=False)

    print(f'Data has been successfully exported to {csv_file_path}')
