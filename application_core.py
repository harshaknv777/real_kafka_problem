import json
from datetime import datetime
from kafka import KafkaConsumer
from kafka import KafkaProducer
from kafka.structs import TopicPartition
import time


def consumer(group_identifier):
    '''
    The function accepts the consumer group identifier and polls on the topic "data-input"
    Returns a dictionary with integer keys and corresponding counts of each integer in the Topic (or partition)
    The messages (integers) are added to a dictionary and value is incremented to count the no of occurences of each integer
    The no of messages read from each partition is also maintained similarily (not in requirements)
    Each poll event is checked for presence of any data; on continous defined number of empty polls, polling is exited 
    '''
    print("Inside Consumer")
    # Kafka Consumer
    consumer = KafkaConsumer(
        'data-input',
        group_id=group_identifier,
        bootstrap_servers=["localhost:19092",
                           "localhost:29092"],
        auto_offset_reset='earliest',
        # max_poll_records=1,
        # max_partition_fetch_bytes=10,
        # fetch_max_bytes=10,
        consumer_timeout_ms=5000
    )

    time.sleep(3)
    data_dict = {}
    partition_dict = {}
    count = 0
    empty_poll_count = 0

    while True:

        # polling on the topic
        consumer.poll()

        # check the poll output for data
        if (bool(consumer)):
            print("Poll is empty")
            empty_poll_count += 1
            # exit if poll results are empty for 3 continous polls
            if(empty_poll_count > 3):
                print(f'Sorted data dictionary - {sorted(data_dict.items())}')
                print(
                    f'Sorted partition dictionary - {sorted(partition_dict.items())}')
                print(f"{count} Messages read")
                return data_dict
        else:
            empty_poll_count = 0

        for record in consumer:
            # reading the message
            msg = json.loads(record.value)

            # maintaining a dictionary with no of occurences of each integer
            if msg in data_dict:
                data_dict[msg] = data_dict[msg]+1
            else:
                data_dict[msg] = 1

            # maintaining a dictionary with no of messages read from each partition
            part = record.partition
            if part in partition_dict:
                partition_dict[part] = partition_dict[part]+1
            else:
                partition_dict[part] = 1

            count += 1


def producer_output(data_dict):
    '''
    Accepts a dictionary with keys and corresponding no of occurences
    Pushes the keys as messages to the topic "data-output" for the no of times as defined in the value
    '''
    # Kafka Producer
    producer = KafkaProducer(
        bootstrap_servers=["localhost:19092",
                           "localhost:29092"],
        value_serializer=str.encode,
    )
    count_producer = 0
    # Looping the dictionary in sorted order
    for key in sorted(data_dict):
        # Check if key and value are integers
        if isinstance(data_dict[key], int) and isinstance(key, int):
            for _ in range(data_dict[key]):
                try:
                    print(
                        f'Producing message @ {datetime.now()} | Message = {str(key)}')
                    producer.send('data-output', value=str(key))
                    count_producer += 1
                except Exception as exception:
                    print(
                        f'Message push into Output topic failed with exception - {exception}')
        else:
            print(f'The key or value is not integer, so skipping the message push')

    # Printing the no of messages pushed into output topic
    print(count_producer)


if __name__ == '__main__':
    # Calling the consumer function with group identifier
    consumed_data_dict = consumer('group_test')

    # Calling the function to push messages into the output topic with the data dictionary from above step
    producer_output(consumed_data_dict)
