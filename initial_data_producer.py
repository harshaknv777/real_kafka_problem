import time
import random
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError


def producer():
    '''
    Pushes random integers (between 1 and max_int_value) to all partitions of "data-input" Topic one by one in ascending order
    '''
    count = 0
    final_list = []
    no_of_partitions = 10
    msg_cnt = 10
    max_int_value = 10

    # Kafka Producer
    producer = KafkaProducer(
        bootstrap_servers=["localhost:19092",
                           "localhost:29092"],
        value_serializer=str.encode
    )

    # Loop to determine the partition into which message needs to be inserted
    for partition in range(no_of_partitions):
        # creating a random list of 10 integers in ascending order
        val_list = sorted([random.randint(1, max_int_value)
                          for _ in range(msg_cnt)])
        final_list.extend(val_list)
        for val in val_list:
            # Send it to 'data-input' topic
            print(
                f'Producing message @ {datetime.now()} | Message = {str(val)} to partition {partition}')
            future = producer.send('data-input', value=str(val),
                                   partition=partition)
            # Block for 'synchronous' sends
            try:
                record_metadata = future.get(timeout=10)
                count += 1
            except KafkaError:
                # Decide what to do if produce request failed...
                print("message insertion failed")
                pass
            # Successful result returns assigned partition and offset
            print(
                f'Record inserted into {record_metadata.partition} partition with offset {record_metadata.offset}')

            # Sleep for a random number of seconds
            time_to_sleep = random.randint(1, 5)
            time.sleep(time_to_sleep)
    print(f"{count} Messages inserted")
    print(sorted(final_list))


if __name__ == '__main__':
    producer()
