This is a Zookeeper Kafka connector implementation for both sink and the source.

#### Instructions for building
-------------------------
```
$ git clone https://github.com/s8sg/kafka-connect-zookeeper
$ (cd kafka-connect-zookeeper && mvn clean package)
```
    
#### Instructions for running
------------------------
1.  Run zookeeper somehow
    ```
    Here are instructions on how to run it using docker.
    $ docker run -d --name zookeeper -p 2181:2181 confluent/zookeeper
    ```
    We are going to use te same zk for Kafka and for the ZK connector

2.  Download and run kafka 0.9.0 locally by following the instructions at http://kafka.apache.org/documentation.html#quickstart
    ```
    $ curl -o kafka_2.11-0.9.0.0.tgz http://www.us.apache.org/dist/kafka/0.9.0.0/kafka_2.11-0.9.0.0.tgz
    $ tar xvfz kafka_2.11-0.9.0.0.tgz
    $ cd kafka_2.11-0.9.0.0
    $ ./bin/kafka-server-start.sh config/server.properties
    ```
##### Configure
    
3.  Configure The Source and Sink properties  
connect-zk-source.properties        
<table class="data-table">
        <tbody>
            <tr>
                <th>Name</th>
                <th>Description</th>
            </tr>
            <tr>
                <td>zk-hosts</td>
                <td>Single ZooKeeper Host, or Comma Seperated List of ZooKeeper Hosts in a Cluster</td>
            </tr>
            <tr>
                <td>zk-nodes</td>
                <td>Single ZooKeeper Data Node, or Comma Seperated List of Zookeeper Nodes</td>
            </tr>
        </tbody>
</table>
connect-zk-sink.properties   
<table class="data-table">
        <tbody>
            <tr>
                <th>Name</th>
                <th>Description</th>
            </tr>
            <tr>
                <td>zk-hosts</td>
                <td>Single ZooKeeper Host, or Comma Seperated List of ZooKeeper Hosts in a Cluster</td>
            </tr>
            <tr>
                <td>zk-node</td>
                <td>ZooKeeper Data Node</td>
            </tr>
        </tbody>
 </table>
    
   
##### Run ZK Connect Source
    
1.  Run your connect-zookeeper-source plugin
    ```
    $ export CLASSPATH=/path/to/kafka-connect-zookeeper/target/kafka-connect-zookeeper-1.0.jar
    $ kafka_2.11-0.9.0.0/bin/connect-standalone.sh kafka-connect-zookeeper/connect-standalone.properties  kafka-connect-zookeeper/config/connect-zk-source.properties
    ```
    
2.  Write stuff to zookeeper node (the connector will read from, as configured in connect-zk-source.properties)
    You could set the data using `zkcli.sh`. The current repo comes with an utility (zk_util.py) to upload data to zk node
    ```
    $ pip install kazoo
    $ python zk_util.py upload localhost:2181 /test/test-data this_is_a_test_data
    ```
    
3.  Read the data out from the kafka topic named 'test' (that is the that this connector will write to, as configured in connect-zk-source.properties)
    ```
    $ kafka_2.11-0.9.0.0/bin/kafka-console-consumer.sh  --zookeeper localhost:2181 --topic test
    {"schema":{"type":"string","optional":false},"payload":"this_is_a_test_data"}
    ```
   
##### Run ZK Connect Sink
    
1.  Run your connect-zookeeper-sink plugin
    ```
    $ export CLASSPATH=/path/to/kafka-connect-zookeeper/target/kafka-connect-zookeeper-1.0.jar
    $ kafka_2.11-0.9.0.0/bin/connect-standalone.sh kafka-connect-zookeeper/connect-standalone.properties  kafka-connect-zookeeper/config/connect-zk-sink.properties
    ```

2.  Check that the zookeeper-sink plugin has written the data to the zookeeper
    ```
    $ python zk_util.py download localhost:2181 /test/data
    this_is_a_test_data
    ```
   
#### TODO 
* Write Test Cases
* Integration with build system (Travis)

#### Note
* This repo is under active devolpment
* This repo use zk watch feature which is async call, but connector `poll` call is sync in nature. Each poll call register an watch. Watch stores the chaged data in a queue, which is unloaded in poll call itself  
```
   poll                             watch_call
  register---> watch ----------            
                               | --> puts data
                                        | 
              ---->[concurrent_queue]<--  
             |
  getdata---- 
   |
  send-------------> [kafka]
```
* For avoiding multiple registrstion of watch semaphore(1) is used, each callback to watch release semaphore for the specific node
```
 poll                        watch
acquire---> semaphore(1) <--release
```
* For any contribution or suggestions, please create PR or Issues
