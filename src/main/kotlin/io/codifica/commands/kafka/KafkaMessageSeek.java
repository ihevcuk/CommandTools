package io.codifica.commands.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class KafkaMessageSeek {
    private final String server;
    private final String topic;
    private final String sinceUtc;
    private final String untilUtc;
    private final int partitions;
    private final String grep;

    public KafkaMessageSeek(String server, String topic, String sinceUtc, String untilUtc, int partitions, String grep) {
        this.server = server;
        this.topic = topic;
        this.sinceUtc = sinceUtc;
        this.untilUtc = untilUtc;
        this.partitions = partitions;
        this.grep = grep.length() == 0 ? null : grep;
    }

    public void seek() {
        Properties props = new Properties();
        props.put("bootstrap.servers", this.server);
        props.put("group.id", "someId");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        try (org.apache.kafka.clients.consumer.KafkaConsumer<String, String> consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(props)) {
            Map<TopicPartition, OffsetAndTimestamp> fromOffset = this.readOffsetByTimestamp(this.toTimestampUtc(this.sinceUtc), consumer);
            Map<TopicPartition, OffsetAndTimestamp> toOffset = this.readOffsetByTimestamp(this.toTimestampUtc(this.untilUtc), consumer);
            Map<TopicPartition, Boolean> shouldProceedPartition = this.calculatePartitionsToProceed(fromOffset, toOffset, consumer);

            consumer.subscribe(Collections.singletonList(this.topic), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {}

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    log.info("onPartitionsAssigned");
                    for (TopicPartition topicPartition : partitions) {
                        if (shouldProceedPartition.get(topicPartition)) {
                            OffsetAndTimestamp from = fromOffset.get(topicPartition);
                            consumer.seek(topicPartition, from.offset());
                        }
                    }
                }
            });

            boolean run = true;
            while (run) {
                if (shouldProceedPartition.entrySet().stream().allMatch(entry -> !entry.getValue())) {
                    log.info("All messages are read for all partitions. Closing program...");
                    break;
                }

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    int partition = record.partition();
                    long offset = record.offset();

                    TopicPartition topicPartition = new TopicPartition(this.topic, partition);
                    long maxOffsetPerPartition = toOffset.get(topicPartition).offset();

                    //log.info("Partition = {}, Offset = {}, MaxOffset = {}", partition, offset, maxOffsetPerPartition);
                    if (offset >= maxOffsetPerPartition - 1) {
                        shouldProceedPartition.put(topicPartition, false);
                        //continue;
                    }

                    if (this.grep != null) {
                        String value = record.value();
                        if (value.contains(this.grep)) {
                            this.print(record, partition, offset);
                        }
                    }
                    else {
                        this.print(record, partition, offset);
                    }
                }
            }
        }
    }

    private Map<TopicPartition, OffsetAndTimestamp> readOffsetByTimestamp(long timestamp, org.apache.kafka.clients.consumer.KafkaConsumer<String, String> consumer) {
        Map<TopicPartition, Long> from = new HashMap<>();
        for (int i = 0; i < this.partitions; i++) {
            TopicPartition topicPartition = new TopicPartition(this.topic, i);
            from.put(topicPartition, timestamp);
        }

        return consumer.offsetsForTimes(from);
    }

    private Map<TopicPartition, Boolean> calculatePartitionsToProceed(Map<TopicPartition, OffsetAndTimestamp> fromOffset, Map<TopicPartition, OffsetAndTimestamp> toOffset, org.apache.kafka.clients.consumer.KafkaConsumer<String, String> consumer) {
        List<TopicPartition> unboundedFromOffset =
                fromOffset.entrySet().stream().filter(entry -> entry.getValue() == null).map(Map.Entry::getKey).collect(Collectors.toList());

        List<TopicPartition> unboundedToOffset =
                toOffset.entrySet().stream().filter(entry -> entry.getValue() == null).map(Map.Entry::getKey).collect(Collectors.toList());
        Map<TopicPartition, Long> endOffsets = consumer.endOffsets(unboundedToOffset);
        endOffsets.forEach((topicPartition, offset) -> toOffset.put(topicPartition, new OffsetAndTimestamp(offset, 0L)));

        Map<TopicPartition, Boolean> shouldProceedPartition = new HashMap<>();
        for (int i = 0; i < this.partitions; i++) {
            TopicPartition topicPartition = new TopicPartition(this.topic, i);

            boolean proceedPartition = !unboundedFromOffset.contains(topicPartition);
            shouldProceedPartition.put(topicPartition, proceedPartition);
        }

        return shouldProceedPartition;
    }

    private LocalDateTime toReadableDate(long timestamp) {
        return LocalDateTime.ofEpochSecond(timestamp/1000, 0, ZoneOffset.UTC);
    }

    private long toTimestampUtc(String date) {
        return LocalDateTime.parse(date).toEpochSecond(ZoneOffset.UTC) * 1000;
    }

    private void print(ConsumerRecord<String, String> record, int partition, long offset) {
        log.info("[{}] value = {}, offset = {}, date = {} timestamp = {}", partition, record.value(), offset, this.toReadableDate(record.timestamp()), record.timestamp());
    }

}