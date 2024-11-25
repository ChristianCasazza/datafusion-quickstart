from pathlib import Path
from datafusion import SessionContext


class DataFusionWrapper:
    def __init__(self):
        self.con = SessionContext()
        self.registered_tables = []

    def register_data(self, paths, table_names):
        """
        Registers data files (Parquet, CSV, JSON) to DataFusion context with specified table names.
        Automatically detects the file type based on the file extension.
        
        Args:
            paths (list): List of paths to data files.
            table_names (list): List of table names corresponding to the paths.
        """
        if len(paths) != len(table_names):
            raise ValueError("The number of paths must match the number of table names.")
        
        for path, table_name in zip(paths, table_names):
            file_extension = Path(path).suffix.lower()

            if file_extension == ".parquet":
                self.con.register_parquet(table_name, path)
            elif file_extension == ".csv":
                self.con.register_csv(table_name, path)
            elif file_extension == ".json":
                self.con.register_json(table_name, path)
            else:
                raise ValueError(f"Unsupported file type '{file_extension}' for file: {path}")

            self.registered_tables.append(table_name)

    def run_query(self, sql_query):
        """
        Runs a SQL query on the registered tables in the DataFusion context.
        Args:
            sql_query (str): The SQL query string to execute.
        Returns:
            DataFrame: Query result as a DataFusion DataFrame.
        """
        return self.con.sql(sql_query)

    def _construct_path(self, path, base_path, file_name, extension):
        """
        Constructs the full file path based on input parameters.
        """
        if path:
            return Path(path)
        elif base_path and file_name:
            return Path(base_path) / f"{file_name}.{extension}"
        else:
            # Default file path: "output.<extension>" in the current directory
            return Path(f"output.{extension}")

    def export(self, dataframe, file_type, path=None, base_path=None, file_name=None, with_header=True):
        """
        Exports a DataFrame to the specified file type.
        Args:
            dataframe: DataFusion DataFrame.
            file_type (str): Type of file to export ('parquet', 'csv', 'json').
            path: Full path to the file (optional).
            base_path: Directory path (optional).
            file_name: Name of the file (without extension) (optional).
            with_header: Include header row for CSV files (default: True).
        """
        file_type = file_type.lower()
        if file_type not in ["parquet", "csv", "json"]:
            raise ValueError("file_type must be one of 'parquet', 'csv', or 'json'.")

        # Construct file path
        full_path = self._construct_path(path, base_path, file_name, file_type)

        # Export based on file type
        if file_type == "csv":
            dataframe.write_csv(full_path, with_header=with_header)
        elif file_type == "json":
            dataframe.write_json(full_path)
        elif file_type == "parquet":
            dataframe.write_parquet(full_path)
        
        print(f"File written to: {full_path}")


def process_queries(con, sql_folder, export_base_path, export_file_type):
    """
    Processes SQL files in the specified folder and exports query results.

    Args:
        con (DataFusionWrapper): Initialized DataFusionWrapper instance.
        sql_folder (Path): Path to the folder containing .sql files.
        export_base_path (Path): Base path for exporting the result files.
        export_file_type (str): File type for exports ('parquet', 'csv', 'json').
    """
    print(f"\nStarting to process SQL queries from folder: {sql_folder}")

    # Ensure the export base path exists
    export_base_path.mkdir(parents=True, exist_ok=True)

    # Process each SQL file in the folder
    for sql_file in sql_folder.glob("*.sql"):
        print(f"\nExecuting query from file: {sql_file.name}")
        sql_query = sql_file.read_text()  # Read the SQL query from the file

        # Run the query
        result_df = con.run_query(sql_query)

        # Generate the output file name based on the SQL file name
        file_name = sql_file.stem  # Extract the base name without extension
        export_path = export_base_path / f"{file_name}.{export_file_type}"

        # Export the query results
        con.export(result_df, file_type=export_file_type, path=export_path)

        print(f"Exported results to: {export_path}")

    print("\nFinished processing all SQL queries.")

def main():
    # Define the base paths
    repo_base_path = Path(__file__).parents[1].resolve()  # Root of the repo
    data_base_path = repo_base_path / "data"
    sql_base_path = repo_base_path / "python_files/sql"
    export_base_path = data_base_path / "exports"

    # Initialize the DataFusionWrapper
    con = DataFusionWrapper()

    # Paths to data files and corresponding table names
    paths = [
        data_base_path / "examples/mta_operations_statement/file_1.parquet",
        data_base_path / "examples/mta_hourly_subway_socrata/*.parquet",
        data_base_path / "examples/mta_daily_ridership/*.parquet",
        data_base_path / "examples/mta_bus_wait_time/*.parquet",
        data_base_path / "examples/daily_weather_asset/*.parquet",
    ]
    table_names = [
        "mta_operations_statement",
        "mta_hourly_subway_socrata",
        "mta_daily_ridership",
        "mta_bus_wait_time",
        "daily_weather_asset",
    ]

    print("\nRegistering data tables...")
    # Register data files (automatically detects file type)
    con.register_data(paths, table_names)
    for table_name in table_names:
        print(f"Table registered: {table_name}")

    # Specify export file type
    export_file_type = "parquet"

    # Process the SQL queries and export results
    process_queries(con, sql_folder=sql_base_path, export_base_path=export_base_path, export_file_type=export_file_type)

if __name__ == "__main__":
    main()