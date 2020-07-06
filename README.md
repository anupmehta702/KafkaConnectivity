# KafkaConnectivity
This contains details to connect to Kafka 


**INSTRUCTIONS -**

**-- COMMANDS TO RUN FROM CON EMU --**
1) zkServer   //start zoo keeper service folder 
    OR //this is not working
    cd kafka_2.11-2.4.0
    .\bin\windows\zookeeper-server-start.bat config\zookeeper.properties   

2) .\bin\windows\kafka-server-start.bat .\config\server.properties 
// in folder C:\kafka_2.11-2.4.0

**--  Start three servers --**
  - (change broker.id ,listeners and logs.dir )
  .\bin\windows\kafka-server-start.bat .\config\server.properties 
  .\bin\windows\kafka-server-start.bat .\config\server-1.properties 
  .\bin\windows\kafka-server-start.bat .\config\server-2.properties 

 **-- PARTITION TOPIC --**
 1) .\bin\windows\kafka-topics.bat --create --topic my_partition_topic --zookeeper localhost:2181 --replication-factor 3 --partitions 3
    .\bin\windows\kafka-topics.bat --describe --topic my_partition_topic --zookeeper localhost:2181 
 o/p -->Topic: my_partition_topic       PartitionCount: 3       ReplicationFactor: 3    Configs:
        Topic: my_partition_topic       Partition: 0    Leader: 2       Replicas: 2,0,1 Isr: 2,0,1
        Topic: my_partition_topic       Partition: 1    Leader: 0       Replicas: 0,1,2 Isr: 0,1,2
        Topic: my_partition_topic       Partition: 2    Leader: 1       Replicas: 1,2,0 Isr: 1,2,0
        
**-- COMMANDS FOR CYGWIN BASHRC --**
set JAVA_HOME = /cygdrive/c/Program Files/Java/jdk1.8.0_73
export JAVA_HOME
set PATH=%PATH%;$JAVA_HOME\bin


How to install zookeeper on windows
https://medium.com/@shaaslam/installing-apache-zookeeper-on-windows-45eda303e835

How to install and start kafka on windows 
https://medium.com/@shaaslam/installing-apache-kafka-on-windows-495f6f2fd3c8



**-- COMMANDS TO RUN FROM WINDOWS POWERSHELL --**
1) zkServer   //start zoo keeper service folder 
    OR //this is not working
    cd kafka_2.11-2.4.0
    .\bin\windows\zookeeper-server-start.bat config\zookeeper.properties   

2) .\bin\windows\kafka-server-start.bat .\config\server.properties 
// in folder C:\kafka_2.11-2.4.0
 
 
**-- CREATE TOPICS --**
    .\bin\windows\kafka-topics.bat --create --topic my_topic --zookeeper localhost:2181 --replication-factor 1 --partitions 1
o/p --> Created topic my_topic.
kafka logs
[2020-04-13 17:41:38,045] INFO [Partition my_topic-0 broker=0] No checkpointed highwatermark is found for partition my_topic-0 (kafka.cluster.Partition)
[2020-04-13 17:41:38,046] INFO [Partition my_topic-0 broker=0] Log loaded for partition my_topic-0 with initial high watermark 0 (kafka.cluster.Partition)
[2020-04-13 17:41:38,048] INFO [Partition my_topic-0 broker=0] my_topic-0 starts at Leader Epoch 0 from offset 0. Previous Leader Epoch was: -1 (kafka.cluster.Partition)
To list topic use below command 
	    .\bin\windows\kafka-topics.bat --list --zookeeper localhost:2181 
		.\bin\windows\kafka-topics.bat --describe --topic my_topic --zookeeper localhost:2181 
		o/p -> Topic: my_topic PartitionCount: 1       ReplicationFactor: 1    Configs:
               Topic: my_topic Partition: 0    Leader: 0       Replicas: 0     Isr: 0 

**-- CREATE PRODUCERS --**
 .\bin\windows\kafka-console-producer.bat --broker-list localhost:9092 --topic my_topic \\run this in normal windows powershell and not ISE
 
**-- CREATE CONSUMERS --**
 .\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic my_topic --from-beginning
 
 
 
 **-- REPLICATED TOPICS --**
 1) Start three servers - (change broker.id ,listeners and logs.dir )
  .\bin\windows\kafka-server-start.bat .\config\server.properties 
  .\bin\windows\kafka-server-start.bat .\config\server-1.properties 
  .\bin\windows\kafka-server-start.bat .\config\server-2.properties 
  
 2) .\bin\windows\kafka-topics.bat --create --topic my_replicated_topic --zookeeper localhost:2181 --replication-factor 3 --partitions 1
 
 3) .\bin\windows\kafka-topics.bat --describe --topic my_replicated_topic --zookeeper localhost:2181 
	o/p -->  Topic: my_replicated_topic      PartitionCount: 1       ReplicationFactor: 3    Configs: Topic: my_replicated_topic      Partition: 0    Leader: 0       Replicas: 0,2,1 Isr: 0,2,1
	
4) .\bin\windows\kafka-console-producer.bat --broker-list localhost:9092 --topic my_replicated_topic

5) .\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic my_replicated_topic --from-beginning
 
 
 **-- PARTITION TOPIC --**
 1) .\bin\windows\kafka-topics.bat --create --topic my_partition_topic --zookeeper localhost:2181 --replication-factor 3 --partitions 3
    .\bin\windows\kafka-topics.bat --describe --topic my_partition_topic --zookeeper localhost:2181 
 o/p -->Topic: my_partition_topic       PartitionCount: 3       ReplicationFactor: 3    Configs:
        Topic: my_partition_topic       Partition: 0    Leader: 2       Replicas: 2,0,1 Isr: 2,0,1
        Topic: my_partition_topic       Partition: 1    Leader: 0       Replicas: 0,1,2 Isr: 0,1,2
        Topic: my_partition_topic       Partition: 2    Leader: 1       Replicas: 1,2,0 Isr: 1,2,0
 
 2) .\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic my_partition_topic --from-beginning
 TO consume only events from a partition
    .\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic my_partition_topic --partition 0 --from-beginning // Processed a total of 60 messages
	--partition 1//Processed a total of 45 messages
	--partition 2//Processed a total of 45 messages
 
**-- WINDOWS COMMANDS --**
   https://stackoverflow.com/questions/12737293/how-do-i-resolve-the-java-net-bindexception-address-already-in-use-jvm-bind
 1) ls command similiar to ll 
 
 2) to find process with a port
 netstat -ano |findStr :2181
 -->  TCP    0.0.0.0:2181           0.0.0.0:0              LISTENING       10748
      TCP    [::]:2181              [::]:0                 LISTENING       10748
	  
 3) to kill a process//ensure you run IDE as admin
 taskkill /pid 10748 /f 
 
 
 **-- KAFKA PERF --**
  .\bin\windows\kafka-producer-perf-test.bat --topic my_partition_topic_one --num-records 50 --record-size 1 --throughput 10 --producer-props bootstrap.servers=localhost:9092 key.serializer=org.apache.kafka.common.serialization.StringSerializer value.serializer=org.apache.kafka.common.serialization.StringSerializer
 