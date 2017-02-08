This is a Zookeeper Kafka connector implementation for both snk and the source..

#### Instructions for building
-------------------------
```
$ git clone https://github.com/s8sg/kafka-connect-zookeeper
$ (cd kafka-connect-zookeeper && mvn clean package)
```
    
#### Instructions for running
------------------------
1.  Run zookeeper somehow.
    ```
    Here are instructions on how to run it using docker.
    $ docker run -d --name zookeeper -p 2181:2181 confluent/zookeeper
    ```
    We are going to use te same zk for Kafka and for the ZK connector.

2.  Download and run kafka 0.9.0 locally by following the instructions at http://kafka.apache.org/documentation.html#quickstart
    ```
    $ curl -o kafka_2.11-0.9.0.0.tgz http://www.us.apache.org/dist/kafka/0.9.0.0/kafka_2.11-0.9.0.0.tgz
    $ tar xvfz kafka_2.11-0.9.0.0.tgz
    $ cd kafka_2.11-0.9.0.0
    $ ./bin/kafka-server-start.sh config/server.properties
    ```
   
##### Run ZK Connect Source
    
3.  Run your connect-zookeeper-source plugin
    ```
    $ export CLASSPATH=/path/to/kafka-connect-zookeeper/target/kafka-connect-zookeeper-1.0.jar
    $ kafka_2.11-0.9.0.0/bin/connect-standalone.sh kafka-connect-zookeeper/connect-standalone.properties  kafka-connect-zookeeper/connect-zk-source.properties
    ```
    
4.  Write stuff to zookeeper node (that is the that this connector will read from, as configured in connect-zk-source.properties)
    You could set the data using `zkcli.sh`. The current repo comes with an utility (zk_util.py) to upload data to zk node
    ```
    $ pip install kazoo
    $ python zk_util.py upload localhost:2181 /test/data this_is_a_test_data
    ```
    
5.  Read the data out from the kafka topic named 'test' (that is the that this connector will write to, as configured in connect-zk-source.properties)
    ```
    $ kafka_2.11-0.9.0.0/bin/kafka-console-consumer.sh  --zookeeper localhost:2181 --topic test
    {"schema":{"type":"string","optional":false},"payload":"this_is_a_test_data"}
    ```
   
##### Run ZK Connect Sink
    
6.  Run your connect-zookeeper-sink plugin
    ```
    $ export CLASSPATH=/path/to/kafka-connect-zookeeper/target/kafka-connect-zookeeper-1.0.jar
    $ kafka_2.11-0.9.0.0/bin/connect-standalone.sh kafka-connect-zookeeper/connect-standalone.properties  kafka-connect-zookeeper/connect-zk-sink.properties
    ```

7.  Check that the zookeeper-sink plugin has written the data to the zookeeper
    ```
    $ python zk_util.py download localhost:2181 /test/data
    this_is_a_test_data
    ```
   
#### TODO: 
* Write Test Cases
* Integration with build system (Travis)

#### Note:
* This repo is under devolpment as not yet production ready (Please Contribute !)
* This repo use zk watch feature which is async call, but connector `poll` call is sync in nature. Each poll call register an watch. Watch stores the chaged data in a queue, which is unloaded in poll call itself  
  ```
  poll ---> watch
     watch ---> concurrent_queue
  concurrent_queue ---> poll
  ```
* For any contribution or suggestions, please create pr or issues
