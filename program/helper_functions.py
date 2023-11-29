
def get_relations(conn):
  # Create a cursor object to execute SQL queries
  cursor = conn.cursor()
  # Get all tables in the database
  cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
  tables = cursor.fetchall()
  # Retrieve table relations and key information
  relations = {}
  for table in tables:
    table_name = table[0]
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = cursor.fetchall()
    cursor.execute(f"PRAGMA foreign_key_list({table_name})")
    foreign_keys = cursor.fetchall()
    relations[table_name] = {
      'related_tables': [fk[2] for fk in foreign_keys],
      'primary_key': [col[1] for col in columns if col[5] == 1],
      'foreign_key':
      [col[1] for col in columns if col[1] in [fk[3] for fk in foreign_keys]]
    }
  # Print the table relations with key information
  print()
  for table, data in relations.items():
    print(f"Table: {table}")
    print("Related Tables: ", ", ".join(data['related_tables']))
    print("Primary Key: ", ", ".join(data['primary_key']))
    print("Foreign Key: ", ", ".join(data['foreign_key']))
    print()
  # Close the cursor and connection
  cursor.close()


def show_table_schema(conn, table_name):
  import pandas as pd
  print("")
  print(f"schema for table {table_name}")
  print("")
  # Query the schema information
  query = f"PRAGMA table_info({table_name})"
  schema_df = pd.read_sql_query(query, conn)
  # Display the schema DataFrame
  print(schema_df)


def show_all_tables(conn):
  import pandas as pd
  # Query the schema information
  query = "SELECT name FROM sqlite_schema WHERE type ='table' AND name NOT LIKE 'sqlite_%'"
  all_tables = pd.read_sql_query(query, conn)
  # Display the schema DataFrame
  print(all_tables)


def show_data_from_table(conn, table):
  import pandas as pd
  query = f"SELECT * FROM {table}"
  data = pd.read_sql_query(query, conn)
  print(f"showing data columns sql table {table}")
  print(data.columns)
  print(f"showing data from sql table {table}")
  print(data)


def drop_table(conn, table_name, view=False):
  # Create a cursor object
  cursor = conn.cursor()
  # Define the table name to drop
  # Drop the table if it exists
  if view:
    TABLE_OR_VIEW = "VIEW"
  else:
    TABLE_OR_VIEW = "TABLE"
  drop_table_query = f'DROP {TABLE_OR_VIEW} IF EXISTS {table_name}'
  cursor.execute(drop_table_query)
  # Commit the changes
  conn.commit()
  # Close the cursor and the connection
  cursor.close()
  print(f"The '{table_name}' table has been dropped successfully.")
