package com.example.cloudstreamkafka.services;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;

public class SaveOffsetOnRebalanced<K,V> implements ConsumerRebalanceListener {
    private final KafkaConsumer<K,V> consumer;
    private final long offset;

    public SaveOffsetOnRebalanced(KafkaConsumer<K,V> consumer, long offset) {
        this.consumer = consumer;
        this.offset = offset;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {

    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        for (TopicPartition partition : partitions) {
            consumer.seek(partition, this.offset);
        }
    }
}
