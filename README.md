# Kafka - Cluster creation with Topics, Producing and Consuming from Kafka cluster with Python 

This repository is built for creating and answering the below problem statement

Suppose you have one Kafka topic data-input with 10 partitions. Within each partition, all messages are integers sorted in ascending order.

For example:
Partition 1: [1,1,1,1,1,1,1,1,2,2,2,2,2,3,3,3,3,3,3]

Partition 2: [1,1,1,1,1,2,2,2,2,2,3,3,3,3,4]

Partition 3: [2,2,2,2,2,3]

…

Partition 10: [2,2,2,2,4,4,6,8]

**Need an application to read all those messages and write them to another topic data-output which
only has one partition and still has all messages ordered increasingly:**

Partition 1:
[1,1,1,1,1,1,1,1,1,1,1,1,1,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,3,3,3,3,3,4,4,4,6,8]

The partition content from above is only given as an example. When designing your solution, assume that
the number of messages can exceed the amount of memory available on the machine executing the code.

## Solution
Download the attached scripts and follow the steps listed for each header

## Reproducible Environment
Create a reproducible (docker-based) development environment for the problem. The environment should include at least a two-node kafka “cluster” and the data-input topic with appropriate sample data.

Execute the below command **(prerequisite to have docker composer installed)**  to create a two node Kafka cluster along with a zookeeper container 

`docker-compose -f docker_compose.yml up -d`

The above command also creates below two topics:
  - Topic 1 - "data-input" with 10 partitions
  - Topic 2 - "data-output" with 1 partitions
 
**Initial data**

Execute the below python script in an (virtual) environment where **kafka** package is installed.

`python initial_data_producer.py` 

This will push random integers (between 1 and 10) to all partitions of "data-input" Topic one by one in ascending order

## Simple Consumer
a) Come up with a simple algorithm to read the messages and write them to the data-output topic in the desired way

Approach followed is to read the messages from the Topic and maintain a dictionary with integers present as keys and count of occurences as values.
Once all messages are read, push messages into output Topic from the above created dictionary in sorted order with value determining the no of times an integer needs to be pushed 

b) Implement your algorithm from a) in python3 using the implemented environment and a client library of your choosing.

Execute the below python script in an (virtual) environment where **kafka** package is installed.

`python application_core.py`

## Scalable Consumer
a) Extend the algorithm from 2a) to work concurrently. Each partition should be read by exactly one service

Approach is to use the concept of **ConsumerGroups** such that each group reads from one partition of the Topic.
Used **Multi-Threading** approach to launch multiple consumers with same group id to achieve the same

b) Extend your implementation from 2b) to include the version of the algorithm proposed in 3a).
Execute the below python script in an (virtual) environment where **kafka** package is installed.

`python application_multi_thread.py`

This script 
  - is a wrapper on top of the above `application_core.py` script to launch multiple instances of the consumer 
  - consolidate the results from each thread of consumer
  - call the producer to push messages into `data-output` Topic using the consolidate results
