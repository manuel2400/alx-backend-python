#!/usr/bin/python3
""""1-Batch_processing - enerator to fetch and process data in batches from the users database
"""
seed = __import__('seed')



def stream_users_in_batches(batch_size):
    """Function to fetch data in batch
    """
    connector = seed.connect_to_prodev()
    cursor = connector.cursor(dictionary=True)
    batch = []
    cursor.execute("SELECT * FROM user_data")
    for row in cursor:
        batch.append(row)
        if len(batch) == batch_size:
            return batch
    cursor.close()
    connector.close()
    return batch

def batch_processing(batch_size):
    """Function processes each batch to
    filter users over the age of25
    """
    for batch in stream_users_in_batches(batch_size):
            if batch["age"] > 25:
                yield batch
