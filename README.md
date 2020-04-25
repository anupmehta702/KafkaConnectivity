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
