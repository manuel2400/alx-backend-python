#!/usr/bin/python3
import csv
import uuid
import mysql.connector
from mysql.connector import errorcode
from typing import Generator, Tuple, Any

def connect_db() -> mysql.connector.MySQLConnection:
    """Connect to the MySQL server"""
    try:
        return mysql.connector.connect(
            host="localhost",
            user="root",
            password="your_password"
        )
    except mysql.connector.Error as err:
        print(f"Error connecting to MySQL: {err}")
        return None

def create_database(connection: mysql.connector.MySQLConnection) -> None:
    """Create ALX_prodev database if it doesn't exist"""
    cursor = connection.cursor()
    try:
        cursor.execute("CREATE DATABASE IF NOT EXISTS ALX_prodev")
        print("Database ALX_prodev created successfully")
    except mysql.connector.Error as err:
        print(f"Failed creating database: {err}")
    finally:
        cursor.close()

def connect_to_prodev() -> mysql.connector.MySQLConnection:
    """Connect to ALX_prodev database"""
    try:
        return mysql.connector.connect(
            host="localhost",
            user="root",
            password="your_password",
            database="ALX_prodev"
        )
    except mysql.connector.Error as err:
        print(f"Error connecting to ALX_prodev: {err}")
        return None

def create_table(connection: mysql.connector.MySQLConnection) -> None:
    """Create user_data table if it doesn't exist"""
    cursor = connection.cursor()
    table_description = (
        "CREATE TABLE IF NOT EXISTS `user_data` ("
        "  `user_id` VARCHAR(36) PRIMARY KEY,"
        "  `name` VARCHAR(255) NOT NULL,"
        "  `email` VARCHAR(255) NOT NULL,"
        "  `age` DECIMAL(3,0) NOT NULL,"
        "  INDEX `idx_user_id` (`user_id`)"
        ") ENGINE=InnoDB"
    )
    try:
        cursor.execute(table_description)
        print("Table user_data created successfully")
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print("Table already exists.")
        else:
            print(f"Failed creating table: {err}")
    finally:
        cursor.close()

def insert_data(connection: mysql.connector.MySQLConnection, filename: str) -> None:
    """Insert data from CSV file into database"""
    cursor = connection.cursor()
    try:
        with open(filename, 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                # Generate UUID if not present in CSV
                user_id = row.get('user_id', str(uuid.uuid4()))
                cursor.execute(
                    "INSERT IGNORE INTO user_data (user_id, name, email, age) "
                    "VALUES (%s, %s, %s, %s)",
                    (user_id, row['name'], row['email'], int(row['age']))
                )
        connection.commit()
        print(f"Data inserted successfully from {filename}")
    except Exception as e:
        connection.rollback()
        print(f"Error inserting data: {e}")
    finally:
        cursor.close()

def stream_users() -> Generator[Tuple[Any, ...], None, None]:
    """Generator that streams users one by one from database"""
    connection = connect_to_prodev()
    if not connection:
        yield None
        return
        
    cursor = connection.cursor(buffered=True)
    try:
        cursor.execute("SELECT * FROM user_data")
        while True:
            row = cursor.fetchone()
            if row is None:
                break
            yield row
    except mysql.connector.Error as err:
        print(f"Error streaming users: {err}")
    finally:
        cursor.close()
        connection.close()

if __name__ == "__main__":
    # For testing the generator
    for user in stream_users():
        print(user)