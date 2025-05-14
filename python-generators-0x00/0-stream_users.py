#!/usr/bin/python3
import mysql.connector
from typing import Dict, Generator

def stream_users() -> Generator[Dict[str, str|int], None, None]:
    """
    Generator function that streams users one by one from the user_data table
    
    Yields:
        dict: A dictionary containing user data with keys:
              'user_id', 'name', 'email', 'age'
    """
    try:
        # Connect to the database
        connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password="your_password",
            database="ALX_prodev"
        )
        
        # Use a server-side cursor for efficient streaming
        cursor = connection.cursor(dictionary=True)
        
        # Execute the query
        cursor.execute("SELECT * FROM user_data")
        
        # Stream rows one by one
        while True:
            row = cursor.fetchone()
            if row is None:
                break
            yield row
            
    except mysql.connector.Error as err:
        print(f"Database error: {err}")
        yield None
    finally:
        # Clean up resources
        try:
            cursor.close()
        except:
            pass
        try:
            connection.close()
        except:
            pass


if __name__ == "__main__":
    # Test the generator
    for user in stream_users():
        print(user)