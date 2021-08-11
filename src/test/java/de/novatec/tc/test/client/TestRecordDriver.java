package de.novatec.tc.test.client;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static java.util.Optional.ofNullable;
import static org.apache.kafka.common.utils.Utils.closeQuietly;

public class TestRecordDriver implements AutoCloseable {

    private static final Duration DEFAULT_DELIVERY_TIMEOUT = Duration.ofSeconds(8);
    private static final Duration DEFAULT_REQUEST_TIMEOUT = Duration.ofSeconds(4);
    private static final Duration DEFAULT_MAX_BLOCK = Duration.ofSeconds(2);

    private final Map<String, ?> configs;
    private final Duration deliveryTimeout;
    private final Duration requestTimeout;
    private final Duration maxBlock;

    private final Collection<AutoCloseable> closeables = new CopyOnWriteArrayList<>();

    public TestRecordDriver(Map<String, ?> configs) {
        this(configs, null, null, null);
    }

    public TestRecordDriver(Map<String, ?> configs,
                            Duration deliveryTimeout,
                            Duration requestTimeout,
                            Duration maxBlock) {
        this.configs = ofNullable(configs).orElseGet(HashMap::new);
        this.deliveryTimeout = ofNullable(deliveryTimeout).orElse(DEFAULT_DELIVERY_TIMEOUT);
        this.requestTimeout = ofNullable(requestTimeout).orElse(DEFAULT_REQUEST_TIMEOUT);
        this.maxBlock = ofNullable(maxBlock).orElse(DEFAULT_MAX_BLOCK);
    }

    @Override
    public void close() {
        closeables.forEach(c -> closeQuietly(c, c.getClass().getSimpleName()));
    }

    public <K, V> TopicProducer<K,V> topicProducer(final String topic,
                                                   final Serializer<K> keySerializer,
                                                   final Serializer<V> valueSerializer) {
        TopicProducer<K,V> topicProducer =
                new TopicProducer<>(topic, keySerializer, valueSerializer, configs, deliveryTimeout, requestTimeout, maxBlock);
        closeables.add(topicProducer);
        return topicProducer;
    }

    public <K, V> TopicConsumer<K, V> topicConsumer(final String topic,
                                                    final Deserializer<K> keyDeserializer,
                                                    final Deserializer<V> valueDeserializer) {
        TopicConsumer<K, V> topicConsumer = new TopicConsumer<>(topic, keyDeserializer, valueDeserializer, configs, requestTimeout);
        closeables.add(topicConsumer);
        return topicConsumer;
    }
}
