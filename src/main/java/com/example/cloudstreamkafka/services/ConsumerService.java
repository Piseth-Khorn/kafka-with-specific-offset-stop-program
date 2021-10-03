package com.example.cloudstreamkafka.services;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.example.cloudstreamkafka.config.ConfigProperties.consumerPropertiesConfig;

@Slf4j
public class ConsumerService<K,V> implements Runnable {
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private String topic;
    private long offset;
    private int partition;
    private ConsumerRecord<K,V> record;

    public ConsumerRecord<K,V> startConsumer(String topic, long offset, int partition) {
        this.topic = topic;
        this.offset = offset;
        this.partition = partition;
        this.run();
        return this.record;
    }

    public void process(ConsumerRecord<K,V> record) {
        System.out.println("Received message: " + record);
        this.record = record;
    }


    @Override
    public void run() {

        try (KafkaConsumer<K,V> consumer = new KafkaConsumer<>(consumerPropertiesConfig())) {
            consumer.subscribe(Collections.singleton(topic), new SaveOffsetOnRebalanced<>(consumer, this.offset));
            consumer.poll(Duration.ofMillis(0));
            for (TopicPartition partition : consumer.assignment())
                consumer.seek(partition, this.offset);
            while (!this.closed.get()) {
                ConsumerRecords<K,V> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<K,V> record : records) {
                    process(record);
                    if (record.offset() == this.offset) {
                        System.out.println("Offset Api :" + this.offset);
                        this.shutdown(consumer);
                        break;
                    }
                }
            }
        } catch (WakeupException ignored) {
        }
    }

    public void shutdown(KafkaConsumer<K,V> consumer) {
        this.closed.set(true);
        consumer.wakeup();
    }
}
