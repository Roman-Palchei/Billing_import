import mysql.connector
import csv
import re
import configparser
import io
from datetime import datetime



class data_import:
    def __init__(self):
        self.data = None
        self.db_connection = None
        self.config = configparser.ConfigParser()
        self.cursor = None
        self.providers_dic = {}
        self.config.read('config.ini')

    def connection_db(self):
        try:
            self.db_connection = mysql.connector.connect(
                host=self.config.get('mysql', 'host'),
                user=self.config.get('mysql', 'user'),
                passwd=self.config.get('mysql', 'password'),
                database=self.config.get('mysql', 'database')
            )
            self.cursor = self.db_connection.cursor(dictionary=True)
        except mysql.connector.Error as err:
            raise ConnectionError(f'DB Error: {err}')

    def load_providers(self):
        self.cursor.execute("SELECT id, name FROM providers")
        result = self.cursor.fetchall()
        self.providers_dic = {row["name"]: row["id"] for row in result}

    def get_provider_id(self, provider_name: str):
        return self.providers_dic.get(provider_name)

    def create_provider(self, provider_name: str):
        if not provider_name.strip():
            raise ValueError("Name can't be empty")
        self.cursor.execute("INSERT INTO providers (name) VALUES (%s)", (provider_name,))
        self.db_connection.commit()
        result = self.cursor.lastrowid
        self.load_providers()
        return result

    def validate_row(self, row: dict):
        success = False
        if row['provider'].strip() == "": raise ValueError('Provider is empty')
        if row['datetime'].strip() == "": raise ValueError('DateTime is empty')
        format = ["%d.%m.%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y%m%d%H%M%S", "%m/%d/%Y %H:%M:%S"]
        for frm in format:
            try:
                dt = datetime.strptime(row["datetime"], frm)
                row["datetime"] = dt.strftime("%Y-%m-%d %H:%M:%S")
                success = True
                break
            except ValueError:
                continue
        if not success: raise ValueError("Unsupported datetime Format")

        row["number"] = re.sub(r'\D', '', row["number"])
        if not row["number"]: raise ValueError("Number is empty")

        if ":" in str(row["duration"]):
            parts = [int(p) for p in row["duration"].strip(":")]
            if len(parts) == 3: h, m, s = parts
            elif len(parts) == 2: h=0; m, s = parts
            else: h=0; m=0; s=0
            row["duration"] = h * 3600 + m * 60 + s
        else:
            row["duration"] = float(row["duration"])
            return True
        if not row["duration"]: raise ValueError("Durattion is empty")

        row["cost"] = float(row["cost"])
        if not row["cost"]: raise ValueError("Cost is empty")

    def import_data_from_file(self, file_storage):
        self.connection_db()
        self.load_providers()

        try:
            stream = io.TextIOWrapper(file_storage.stream, encoding='utf-8')
            reader = csv.DictReader(stream, delimiter=',', quotechar='"')

            self.data = list(reader)
            if not self.data:
                return "File is empty!"

            row_to_inspect = []
            imported_count = 0
            errors = []

            for index, row in enumerate(self.data):
                try:
                    self.validate_row(row)
                    provider_id = self.get_provider_id(row["provider"])
                    if provider_id is None:
                        provider_id = self.create_provider(row["provider"])

                    row_to_inspect.append((
                        provider_id,
                        row["datetime"],
                        row["number"],
                        row["duration"],
                        row["cost"]
                    ))
                    imported_count += 1
                except ValueError as e:
                    errors.append(f"Row {index+1}: {e}")
                    continue

            if row_to_inspect:
                self.cursor.executemany(
                    "INSERT INTO billig_calls (provider_id, call_datetime, phone_a, duration_seconds, cost) VALUE (%s, %s, %s, %s, %S)",
                    row_to_inspect
                )
                self.db_connection.commit()
            return f"Success! Imported {imported_count} rows. Errors: {len(errors)}"
        finally:
            if self.db_connection and self.db_connection.is_connected():
                self.cursor.close()
                self.db_connection.close()

    def get_preview(self, file_storage):
        file_storage.stream.seek(0)

        try:
            stream = io.TextIOWrapper(file_storage.stream, encoding='utf-8')
            reader = csv.DictReader(stream, delimiter=',', quotechar='"')

            preview_data=[]

            for i, row in enumerate(reader):
                if i >= 10:
                    break
                preview_data.append(row)

            file_storage.stream.seek(0)
            return preview_data

        except Exception as e:
            file_storage.stream.seek(0)
            return [{'Error': str(e)}]