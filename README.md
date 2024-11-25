
# DataFusion Quickstart Repo

## Overview

This repository provides a quickstart guide for conducting base operations with **DataFusion**, including registering datasets, executing SQL queries, exporting results, and inspecting table schemas. The core functionality is encapsulated in the `DataFusionWrapper` class, which simplifies common tasks, like specifying the file type when reading a dataset, and provides a user-friendly interface for working with DataFusion. The example .ipynb file contains an example of reading different files from the data/examples folder, and then conducting some SQL queries.

---

## Step 0: Install `uv`

To manage your Python environment and dependencies, we recommend using `uv`, a tool that enhances `pip` with features like better dependency management and simplified virtual environment creation.

### Install `uv`:
```bash
pip install uv
```

### Why `uv`?
- **Simplified Virtual Environments**: Easily create and manage virtual environments.
- **Improved Package Management**: Handles dependencies more effectively than `pip`.
- **Intuitive Commands**: Streamlines workflows for Python projects.

---

## Step 1: Create a Virtual Environment and Install Dependencies

### Create and Activate a Virtual Environment
1. **Create a virtual environment:**
   ```bash
   uv venv
   ```
   This will create a virtual environment in the `.venv` folder.

2. **Activate the virtual environment:**
   - On Linux/Mac:
     ```bash
     source .venv/bin/activate
     ```
   - On Windows:
     ```bash
     .venv\Scripts\activate
     ```

### Install Required Packages
1. Add `datafusion` and `ipykernel` to your virtual environment:
   ```bash
   uv add datafusion ipykernel
   ```

2. Your environment is now ready for running the examples in this repository. When using the .ipynb file, you will need to manually set the kernel to use your venv.

---

## Examples and Usage

### Explore the Jupyter Notebook
- **File**: `datafusion_quickstart.ipynb`
- This notebook demonstrates how to use the `DataFusionWrapper` class for:
  - Registering datasets.
  - Executing queries.
  - Inspecting schemas.
  - Exporting results.

### Multi-Query SQL Pipeline Example
- **File**: `python_files/sql_pipeline.py`
- This script provides an example of running a multi-query SQL pipeline using the `DataFusionWrapper` class:
  - Registers datasets from `data/examples/`.
  - Reads SQL queries from `python_files/sql/`.
  - Exports query results to `data/exports/` as Parquet files.

---

## Key Features of `DataFusionWrapper`
- **Dataset Registration**: Simplifies adding datasets (Parquet, CSV, JSON) as tables.
- **SQL Query Execution**: Run SQL queries on registered tables with ease.
- **Export Results**: Save query results in `Parquet`, `CSV`, or `JSON` formats.
- **Table and Schema Inspection**: Use built-in functions to explore the structure of your data.

---

## Example Workflow

### Register and Inspect Tables
```python
from datafusion_wrapper import DataFusionWrapper

con = DataFusionWrapper()

# Register datasets
paths = [
    "data/examples/mta_operations_statement/file_1.parquet",
    "data/examples/mta_hourly_subway_socrata/*.parquet"
]
table_names = [
    "mta_operations_statement",
    "mta_hourly_subway_socrata"
]
con.register_data(paths, table_names)

# Show tables
con.show_tables()

# Show schema of a specific table
con.show_schema("mta_operations_statement")
```

### Run a Multi-Query SQL Pipeline
Refer to `python_files/sql_pipeline.py` for a detailed example of registering datasets, running multiple SQL queries, and exporting results to Parquet files. 

This script:
1. Dynamically resolves paths relative to the repository root.
2. Reads SQL queries from `python_files/sql/`.
3. Exports the results to `data/exports/`.

---

With this setup, you can quickly start working with **DataFusion** for SQL-based operations on structured data!
