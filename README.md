# GeoJSON to PostgreSQL Transformer

## Overview
This project processes GeoJSON files and stores the extracted data into a PostgreSQL database. It is designed to handle luminaries and circuit data efficiently.

## Features
- Connects to a PostgreSQL database.
- Creates a table for storing GeoJSON data if it doesn't exist.
- Processes GeoJSON files from a specified folder.
- Inserts or updates records in the database based on the contents of the GeoJSON files.

## Requirements
- Python 3.x
- psycopg2
- argparse
- JSON

## Installation
1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd <repository-directory>
   ```

2. Install the required packages:
   ```bash
   pip install psycopg2
   ```

## Usage
Run the script with the following command:

bash
python transform_geojson_store.py --db_host <DB_HOST> --db_name <DB_NAME> --db_user <DB_USER> --db_pass <DB_PASS> --folder <FOLDER_PATH>


### Arguments
- `--db_host`: Database host address.
- `--db_name`: Name of the database.
- `--db_user`: Database username.
- `--db_pass`: Database password.
- `--folder`: Path to the folder containing GeoJSON files.

## Example


python transform_geojson_store.py --db_host localhost --db_name mydatabase --db_user myuser --db_pass mypassword --folder /path/to/geojson/files


## License
This project is licensed under the MIT License.