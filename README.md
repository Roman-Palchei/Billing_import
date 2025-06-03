# Billing CSV Importer

This is a CLI tool designed to import billing call data from CSV files into a MySQL database.

## Installation

1. Clone the repository:
   ```bash
   git clone https://your.repo.url.git
   cd import_billing
2. Set up a virtual environment:
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
3. Install dependencies:
   ```bash
   pip install -r requirements.txt

# Configuration

Before running the importer, create a config.ini file in the root directory with the following structure:

    [mysql]
    host = 127.0.0.1
    user = your_user
    password = your_password
    database = your_database

    ⚠️ Do not commit config.ini to version control. It contains sensitive credentials.

# CSV Format

The CSV file must contain the following headers:

    provider – the telecom provider name

    datetime – call start time, in the format DD.MM.YYYY HH:MM:SS

    number – phone number (caller)

    duration – call duration in seconds

    cost – cost of the call in your preferred currency

 Example:

    provider,datetime,number,duration,cost
    lifecell,30.04.2025 14:48:21,380991112233,25,0.50

# Directory Structure

Place your billing CSV files in a subdirectory named billing/ in the root of the project.
The script will scan this folder and prompt you to select the file to import.

    Example structure:

    import_billing/
    ├── billing/
    │   ├── april_2025.csv
    │   └── may_2025.csv
    ├── main.py
    ├── config.ini
    ├── requirements.txt
    └── README.md


# Usage

Run the script:

    python main.py

You will be prompted to select a CSV file located in the billing/ directory. The script will:

    Connect to the MySQL database

    Validate each row

    Insert new providers if missing

    Import all call records into the billing_calls table

A progress bar will display the import status.
Dependencies

This project requires the following Python packages:

    mysql-connector-python

    InquirerPy

    tqdm

Install them using pip install -r requirements.txt.
License

MIT License

