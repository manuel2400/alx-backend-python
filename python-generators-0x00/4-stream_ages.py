#!/usr/bin/python3
import seed
from typing import Generator

def stream_user_ages() -> Generator[int, None, None]:
    """
    Generator function that streams user ages one by one from the database
    
    Yields:
        int: User ages one at a time
    """
    connection = seed.connect_to_prodev()
    cursor = connection.cursor()
    
    try:
        cursor.execute("SELECT age FROM user_data")
        while True:
            row = cursor.fetchone()
            if row is None:
                break
            yield row[0]
    finally:
        cursor.close()
        connection.close()

def calculate_average_age() -> float:
    """
    Calculates the average age of users using the stream_user_ages generator
    
    Returns:
        float: The average age of users
    """
    total = 0
    count = 0
    
    # Single loop to process all ages
    for age in stream_user_ages():
        total += age
        count += 1
    
    return total / count if count > 0 else 0

if __name__ == "__main__":
    average_age = calculate_average_age()
    print(f"Average age of users: {average_age:.2f}")