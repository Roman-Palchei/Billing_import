# This is a sample Python script.
import os
from random import choices

from mysql.connector import connect

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
name='Import from csv billing'

def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('PyCharm')

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
import mysql.connector
import csv
from InquirerPy import inquirer
import configparser
from datetime import datetime
from tqdm import tqdm

class data_import:
    def __init__(self):
        self.data = None
        self.db_connection = None
        self.file_path = 'billing'
        self.file_name = None
        self.config = configparser.ConfigParser()
        self.cursor = None
        self.providers_dic = {}


#open file and load data to self.data
    def open_file (self):
        bill_file = os.listdir(self.file_path)
        self.file_name = inquirer.select(
            message = "Select CSV file with billing data: ",
            choices = bill_file
        ).execute()
        path = os.path.join(self.file_path, self.file_name)
        with open(path, 'r') as csvf:
            reader = csv.DictReader(csvf, delimiter=',', quotechar='"')
            self.data = list(reader)
            if not self.data:
                raise ValueError('Billing file is empty. Please check your file!!!')



    def connection_db (self):
        self.config.read('config.ini')
        try:
            self.db_connection = mysql.connector.connect(
            host = self.config.get('mysql', 'host'),
            user = self.config.get('mysql', 'user'),
            passwd = self.config.get('mysql', 'password'),
            database = self.config.get('mysql', 'database')
            )
            if not self.db_connection.is_connected():
                raise ConnectionError('Connection is not active')
        except mysql.connector.Error as err:
            raise ConnectionError(f'Base not connect {err}')
        self.cursor = self.db_connection.cursor(dictionary=True)


    def load_providers (self):
        self.cursor.execute("SELECT id, name FROM providers")
        result = self.cursor.fetchall()
        self.providers_dic = {row["name"]: row["id"] for row in result}


    def get_provider_id (self, provider_name: str):
        return self.providers_dic.get(provider_name)

    def create_provider (self, provider_name: str):
        if not provider_name.strip():
            raise ValueError("Name can't be empty")
        self.cursor.execute("INSERT INTO providers (name) VALUES (%s)", (provider_name,))
        self.db_connection.commit()
        result = self.cursor.lastrowid
        self.load_providers()
        return result


    def validate_row (self, row: dict):
        success = False
        if row['provider'].strip() == "":
            raise ValueError('Provider is empty')

        if row['datetime'].strip() == "":
            raise ValueError('Date is empty')
        format = ["%d.%m.%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y%m%d%H%M%S"]
        for frm in format:
            try:
                dt = datetime.strptime(row["datetime"], frm)
                row["datetime"] = dt.strftime("%Y-%m-%d %H:%M:%S")
                success = True
                break
            except ValueError:
                continue
        if not success:
            raise ValueError("Unsupported datetime Format")

        if row['number'].strip() == "":
            raise ValueError('Number is empty')

        if row['duration'].strip() == "":
            raise  ValueError('Duration is empty')
        if ":" in row['duration']:
            parts = row['duration'].split(':')
            parts = [int(p) for p in parts]
            if len(parts) == 3:
                h, m, s = parts
            elif len(parts) == 2:
                h = 0
                m, s = parts
            else:
                raise ValueError('Invalid duration format')
            row['duration'] = h * 3600 + m * 60 + s
        else:
            row['duration'] = float(row['duration'])

        if row['duration'] < 0:
             raise ValueError('Duration is smellier than 0')

        if row['cost'] == "":
            raise ValueError('Cost is empty')
        row['cost'] = float(row['cost'])

        if row['cost'] < 0:
            raise ValueError('Cost is smellier than 0 ')
        return True

    def import_data (self):
        self.open_file()
        self.connection_db()
        self.load_providers()
        i = 0
        row_to_inspect = []
        for row in tqdm(self.data, desc="Preparing", unit="rows"):
            try:
                self.validate_row(row)
                provider_id = self.get_provider_id(row['provider'])
                if provider_id is None:
                    provider_id = self.create_provider(row['provider'])

                i += 1
                row_to_inspect.append((
                    provider_id,
                    row['datetime'],
                    row['number'],
                    row['duration'],
                    row['cost']
                ))
            except ValueError:
                continue

        self.cursor.executemany(
            "INSERT INTO billing_calls (provider_id, call_datetime, phone_a, duration_seconds, cost) VALUES (%s, %s, %s, %s, %s)",
            row_to_inspect,)
        self.db_connection.commit()
        print(f"{i} rows")


def main():
    bill = data_import()
    bill.import_data()


main()