import concurrent.futures
from logging import exception
import application_core as app
from collections import Counter
from functools import reduce
from operator import add

# List to hold the result from each thread of execution
result_list = []
no_of_threads = 10
consumer_group_id = 'my_multi_group'


if __name__ == '__main__':
    try:
        # calling the consumer function of the application with multiple threads
        with concurrent.futures.ThreadPoolExecutor() as executor:
            results = [executor.submit(app.consumer, consumer_group_id)
                       for _ in range(no_of_threads)]
            # looping over the results of each thread
            for f in concurrent.futures.as_completed(results):
                result = f.result()
                # appending the result of each thread to the final result list
                result_list.append(result)
    except Exception as exception:
        print(f'Multi thread process failed with exception - {exception}')

    print(result_list)
    # merging the mutiple dictionaries of the result list by adding the values of the matching keys
    result = dict(reduce(add, map(Counter, result_list)))
    print(result)
    # calling the function to produce into output topic
    if result:
        app.producer_output(result)
