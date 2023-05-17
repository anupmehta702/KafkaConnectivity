package multithreadedConsumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class MultithreadedKafkaConsumer implements Runnable, ConsumerRebalanceListener {

    private final KafkaConsumer<String, String> kafkaConsumer;
    private final ExecutorService executor = Executors.newFixedThreadPool(8);
    private final Map<TopicPartition, Task> activeTasksMap = new HashMap<>();
    private final Map<TopicPartition, OffsetAndMetadata> offsetsToCommitMap = new HashMap<>();
    private final AtomicBoolean stopped = new AtomicBoolean(false);
    private long lastCommitTime = System.currentTimeMillis();
    private final Logger log = LoggerFactory.getLogger(MultithreadedKafkaConsumer.class);

    public static void main(String[] args) {
        MultithreadedKafkaConsumer mks = new MultithreadedKafkaConsumer();
    }

    public MultithreadedKafkaConsumer() {
        Properties config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "multithreaded-kafkaConsumer-demo");
        kafkaConsumer = new KafkaConsumer<>(config);
        new Thread(this).start();
    }


    @Override
    public void run() {
        try {
            kafkaConsumer.subscribe(Collections.singleton("my_big_topic"), this);
            while (!stopped.get()) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.of(100, ChronoUnit.MILLIS));
                handleFetchedRecords(records);
                checkActiveTasks();
                commitOffsets();
            }
        } catch (WakeupException we) {
            if (!stopped.get())
                throw we;
        } finally {
            kafkaConsumer.close();
        }
    }


    private void handleFetchedRecords(ConsumerRecords<String, String> records) {
        if (records.count() > 0) {
            List<TopicPartition> partitionsToPauseList = new ArrayList<>();
            records.partitions().forEach(partition -> {
                List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                System.out.println(String.format("Processing  Topic:%s Partition:%s"
                        , partition.topic(), partition.partition()));
                Task task = new Task(partitionRecords);
                partitionsToPauseList.add(partition);
                executor.submit(task);
                activeTasksMap.put(partition, task);
            });
            System.out.println("Pausing partitions -->" + partitionsToPauseList);
            kafkaConsumer.pause(partitionsToPauseList);
        }
    }

    private void printRecordDetails(ConsumerRecord record) {
        System.out.println(String.format("Topic:%s Partition:%s Offset:%s Key:%s Value:%s"
                , record.topic(), record.partition(), record.offset(), record.key(), record.value()));
    }

    private void commitOffsets() {
        try {
            long currentTimeMillis = System.currentTimeMillis();
            if (currentTimeMillis - lastCommitTime > 2000) {
                if (!offsetsToCommitMap.isEmpty()) {
                    System.out.println("Committing offset -->" + offsetsToCommitMap);
                    kafkaConsumer.commitSync(offsetsToCommitMap);
                    offsetsToCommitMap.clear();
                }
                lastCommitTime = currentTimeMillis;
            }
        } catch (Exception e) {
            log.error("Failed to commit offsets!", e);
        }
    }


    private void checkActiveTasks() {
        List<TopicPartition> finishedTasksPartitionsList = new ArrayList<>();
        activeTasksMap.forEach((partition, task) -> {
            if (task.isFinished()) {
                System.out.println("Resuming partitions -->" + finishedTasksPartitionsList);
                finishedTasksPartitionsList.add(partition);
            }
            long offset = task.getCurrentOffset();
            if (offset > 0)
                offsetsToCommitMap.put(partition, new OffsetAndMetadata(offset));
        });
        finishedTasksPartitionsList.forEach(partition -> activeTasksMap.remove(partition));

        kafkaConsumer.resume(finishedTasksPartitionsList);
    }


    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {

        // 1. Stop all tasks handling records from revoked partitions
        Map<TopicPartition, Task> stoppedTask = new HashMap<>();
        for (TopicPartition partition : partitions) {
            Task task = activeTasksMap.remove(partition);
            if (task != null) {
                task.stop();
                stoppedTask.put(partition, task);
            }
        }

        // 2. Wait for stopped tasks to complete processing of current record
        stoppedTask.forEach((partition, task) -> {
            long offset = task.waitForCompletion();
            if (offset > 0)
                offsetsToCommitMap.put(partition, new OffsetAndMetadata(offset));
        });


        // 3. collect offsets for revoked partitions
        Map<TopicPartition, OffsetAndMetadata> revokedPartitionOffsets = new HashMap<>();
        partitions.forEach(partition -> {
            OffsetAndMetadata offset = offsetsToCommitMap.remove(partition);
            if (offset != null)
                revokedPartitionOffsets.put(partition, offset);
        });

        // 4. commit offsets for revoked partitions
        try {
            kafkaConsumer.commitSync(revokedPartitionOffsets);
        } catch (Exception e) {
            log.warn("Failed to commit offsets for revoked partitions!");
        }
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        kafkaConsumer.resume(partitions);
    }


    public void stopConsuming() {
        stopped.set(true);
        kafkaConsumer.wakeup();
    }

}


/*
Logs for just one message --->
Processing  Topic:my_big_topic Partition:2
Pausing partitions -->[my_big_topic-2]
processing record Topic:my_big_topic Partition:2 Offset:19 Key:0 Value:message - 0- abc
Resuming partitions -->[]
Committing offset -->{my_big_topic-2=OffsetAndMetadata{offset=20, leaderEpoch=null, metadata=''}}


Logs for 6 msgs -->
Processing  Topic:my_big_topic Partition:1
Pausing partitions -->[my_big_topic-1]
processing record Topic:my_big_topic Partition:1 Offset:33 Key:4 Value:message - 4- abc
Processing  Topic:my_big_topic Partition:0
Processing  Topic:my_big_topic Partition:2
Pausing partitions -->[my_big_topic-0, my_big_topic-2]
Resuming partitions -->[]
processing record Topic:my_big_topic Partition:0 Offset:42 Key:1 Value:message - 1- abc
processing record Topic:my_big_topic Partition:0 Offset:43 Key:5 Value:message - 5- abc
processing record Topic:my_big_topic Partition:2 Offset:23 Key:0 Value:message - 0- abc
processing record Topic:my_big_topic Partition:2 Offset:24 Key:2 Value:message - 2- abc
processing record Topic:my_big_topic Partition:2 Offset:25 Key:3 Value:message - 3- abc
Resuming partitions -->[]
Resuming partitions -->[my_big_topic-0]
Committing offset -->{my_big_topic-0=OffsetAndMetadata{offset=44, leaderEpoch=null, metadata=''},
 my_big_topic-1=OffsetAndMetadata{offset=34, leaderEpoch=null, metadata=''},
  my_big_topic-2=OffsetAndMetadata{offset=26, leaderEpoch=null, metadata=''}}



 */