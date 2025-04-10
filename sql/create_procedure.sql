CREATE OR REPLACE PROCEDURE data_profiling_py(
    database_name STRING,
    schema_name STRING,
    table_name STRING)
RETURNS TABLE (
    TableName STRING,
    ColumnName STRING,
    TotalCount INT,
    PopCount INT,
    PopPercentage FLOAT,
    DistinctCount INT,
    DuplicateCount INT,
    BlankAndNullCount INT,
    MinValue STRING,
    MaxValue STRING
)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'execute'
AS
$$
import snowflake.snowpark as snowpark

def execute(session: snowpark.Session, database_name: str, schema_name: str, table_name: str):
    # Fully qualified table name
    full_table_name = f"{database_name}.{schema_name}.{table_name}"

    # Get column names and data types for the specified table
    columns_query = f"""
        SELECT COLUMN_NAME, DATA_TYPE 
        FROM {database_name}.INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA = '{schema_name}' AND TABLE_NAME = '{table_name}'
    """
    columns = session.sql(columns_query).collect()

    # Query to get the total count of the table
    total_count_query = f"SELECT COUNT(*) AS total_count FROM {full_table_name}"
    total_count = session.sql(total_count_query).collect()[0]['TOTAL_COUNT']

    # Temporary list to store profiling results
    profiling_results = []

    # Loop through each column and calculate the required metrics
    for column in columns:
        col_name = column['COLUMN_NAME']
        data_type = column['DATA_TYPE'].upper()  # Get the data type in uppercase
        
        # Query for populated (non-null, non-blank) count
        pop_count_query = f"SELECT COUNT(*) AS pop_count FROM {full_table_name} WHERE {col_name} IS NOT NULL"
        if data_type.startswith("VARCHAR") or data_type == "STRING":
            pop_count_query += f" AND {col_name} <> ''"
        pop_count = session.sql(pop_count_query).collect()[0]['POP_COUNT']

        # Query for distinct count
        distinct_count_query = f"SELECT COUNT(DISTINCT {col_name}) AS distinct_count FROM {full_table_name} WHERE {col_name} IS NOT NULL"
        if data_type.startswith("VARCHAR") or data_type == "STRING":
            distinct_count_query += f" AND {col_name} <> ''"
        distinct_count = session.sql(distinct_count_query).collect()[0]['DISTINCT_COUNT']

        # Duplicate count = total count - distinct count
        duplicate_count = total_count - distinct_count

        # Blank and null count
        blank_and_null_count = total_count - pop_count

        # Min/Max value queries, handling based on data type
        if data_type.startswith("VARCHAR") or data_type == "STRING":
            # For string types, exclude empty strings
            min_value_query = f"SELECT MIN({col_name}) AS min_value FROM {full_table_name} WHERE {col_name} IS NOT NULL AND {col_name} <> ''"
            max_value_query = f"SELECT MAX({col_name}) AS max_value FROM {full_table_name} WHERE {col_name} IS NOT NULL AND {col_name} <> ''"
        else:
            # For numeric or other types, exclude only nulls
            min_value_query = f"SELECT MIN({col_name}) AS min_value FROM {full_table_name} WHERE {col_name} IS NOT NULL"
            max_value_query = f"SELECT MAX({col_name}) AS max_value FROM {full_table_name} WHERE {col_name} IS NOT NULL"
        
        # Get min and max values
        min_value = session.sql(min_value_query).collect()[0]['MIN_VALUE']
        max_value = session.sql(max_value_query).collect()[0]['MAX_VALUE']

        # Append the results to the list
        profiling_results.append({
            "TableName": table_name,
            "ColumnName": col_name,
            "TotalCount": total_count,
            "PopCount": pop_count,
            "PopPercentage": (pop_count / total_count) * 100,
            "DistinctCount": distinct_count,
            "DuplicateCount": duplicate_count,
            "BlankAndNullCount": blank_and_null_count,
            "MinValue": str(min_value),
            "MaxValue": str(max_value)
        })
    
    # Convert profiling results into a Snowpark DataFrame and return it
    return session.create_dataframe(profiling_results)
$$;
