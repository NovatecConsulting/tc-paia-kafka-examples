package de.novatec.tc.test.client;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toSet;
import static org.apache.kafka.common.utils.Utils.closeQuietly;

public class TopicConsumer<K, V> implements AutoCloseable {

    private static final Duration DEFAULT_REQUEST_TIMEOUT = Duration.ofSeconds(2);

    private final String topic;
    private final Duration timeout;
    private final Consumer<K, V> consumer;

    public TopicConsumer(final String topic,
                         final Deserializer<K> keyDeserializer,
                         final Deserializer<V> valueDeserializer,
                         final Map<String, ?> configs,
                         final Duration requestTimeout) {
        this(topic,
             ofNullable(requestTimeout).orElse(DEFAULT_REQUEST_TIMEOUT),
             createConsumer(topic, keyDeserializer, valueDeserializer, configs, requestTimeout));
    }

    TopicConsumer(String topic, Duration timeout, Consumer<K, V> consumer) {
        this.topic = topic;
        this.timeout = timeout;
        this.consumer = consumer;
    }

    public ConsumerRecord<K, V> readKeyValue() {
        ConsumerRecords<K, V> consumerRecords = consumer.poll(timeout);
        if (consumerRecords.isEmpty()) {
            throw new NoSuchElementException("Empty topic: " + topic);
        }
        return consumerRecords.iterator().next();
    }

    public TopicConsumer<K, V> toEarliest() {
        Set<TopicPartition> assignment = consumer.partitionsFor(topic, timeout).stream()
            .map(p -> new TopicPartition(p.topic(), p.partition()))
            .collect(toSet());
        consumer.seekToBeginning(assignment);
        // jump to offset
        assignment.forEach(p -> consumer.position(p, timeout));
        return this;
    }

    public TopicConsumer<K, V> toLatest() {
        Set<TopicPartition> assignment = consumer.partitionsFor(topic, timeout).stream()
                .map(p -> new TopicPartition(p.topic(), p.partition()))
                .collect(toSet());
        consumer.seekToEnd(assignment);
        // jump to offset
        assignment.forEach(p -> consumer.position(p, timeout));
        return this;
    }

    @Override
    public void close() {
        closeQuietly(() -> consumer.close(timeout), consumer.getClass().getSimpleName());
    }

    private static <K, V> Consumer<K, V> createConsumer(final String topic,
                                                        final Deserializer<K> keyDeserializer,
                                                        final Deserializer<V> valueDeserializer,
                                                        final Map<String, ?> configs,
                                                        final Duration requestTimeout) {
        Consumer<K, V> consumer = new KafkaConsumer<>(createConsumerConfigs(configs, requestTimeout), keyDeserializer, valueDeserializer);
        Set<TopicPartition> assignment = consumer.partitionsFor(topic, requestTimeout).stream()
                        .map(p -> new TopicPartition(p.topic(), p.partition()))
                        .collect(toSet());
        consumer.assign(assignment);
        return consumer;
    }

    private static Map<String, Object> createConsumerConfigs(final Map<String, ?> configs,
                                                             final Duration requestTimeout) {
        Map<String, Object> actualConfigs = new HashMap<>(configs);
        actualConfigs.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
        actualConfigs.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, (int) requestTimeout.toMillis());
        actualConfigs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        actualConfigs.put(ConsumerConfig.CLIENT_ID_CONFIG, TopicConsumer.class.getSimpleName());
        return actualConfigs;
    }
}