#!/usr/bin/python3
import mysql.connector
from typing import Dict, Generator, List

def stream_users_in_batches(batch_size: int) -> Generator[List[Dict[str, str|int]], None, None]:
    """
    Generator function that fetches users in batches from the database
    
    Args:
        batch_size: Number of records to fetch per batch
        
    Yields:
        list: A list of user dictionaries for each batch
    """
    try:
        # Connect to the database
        connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password="your_password",
            database="ALX_prodev"
        )
        
        # Use a server-side cursor for efficient batch fetching
        cursor = connection.cursor(dictionary=True)
        
        # Execute the query
        cursor.execute("SELECT * FROM user_data")
        
        # Fetch rows in batches
        while True:
            batch = cursor.fetchmany(batch_size)
            if not batch:
                break
            yield batch
            
    except mysql.connector.Error as err:
        print(f"Database error: {err}", file=sys.stderr)
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


def batch_processing(batch_size: int = 50) -> Generator[Dict[str, str|int], None, None]:
    """
    Processes users in batches and filters those over 25 years old
    
    Args:
        batch_size: Number of records to process per batch
        
    Yields:
        dict: Users over 25 years old one by one
    """
    # Loop through batches
    for batch in stream_users_in_batches(batch_size):
        # Filter users over 25 in the current batch
        for user in batch:
            if user['age'] > 25:
                yield user


if __name__ == "__main__":
    import sys
    try:
        # Print processed users in batches of specified size
        for user in batch_processing(50):
            print(user)
    except BrokenPipeError:
        # Handle pipe closure gracefully
        sys.stderr.close()