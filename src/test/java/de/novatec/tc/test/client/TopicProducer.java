package de.novatec.tc.test.client;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.test.TestRecord;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static java.util.Optional.ofNullable;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.kafka.clients.producer.ProducerConfig.*;
import static org.apache.kafka.common.utils.Utils.closeQuietly;

public class TopicProducer<K, V> implements AutoCloseable {

    private static final Duration DEFAULT_DELIVERY_TIMEOUT = Duration.ofSeconds(8);
    private static final Duration DEFAULT_REQUEST_TIMEOUT = Duration.ofSeconds(4);
    private static final Duration DEFAULT_MAX_BLOCK = Duration.ofSeconds(2);

    private final String topic;
    private final Duration timeout;
    private final Producer<K, V> producer;

    public TopicProducer(final String topic,
                         final Serializer<K> keySerializer,
                         final Serializer<V> valueSerializer,
                         final Map<String, ?> configs) {
        this(topic, keySerializer, valueSerializer, configs, null, null, null);
    }

    public TopicProducer(final String topic,
                         final Serializer<K> keySerializer,
                         final Serializer<V> valueSerializer,
                         final Map<String, ?> configs,
                         final Duration deliveryTimeout,
                         final Duration requestTimeout,
                         final Duration maxBlock) {
        this(topic,
             ofNullable(deliveryTimeout).orElse(DEFAULT_DELIVERY_TIMEOUT).plus(ofNullable(maxBlock).orElse(DEFAULT_MAX_BLOCK)),
             new KafkaProducer<>(createProducerConfigs(
                    ofNullable(configs).orElseGet(HashMap::new),
                    ofNullable(deliveryTimeout).orElse(DEFAULT_DELIVERY_TIMEOUT),
                    ofNullable(requestTimeout).orElse(DEFAULT_REQUEST_TIMEOUT),
                    ofNullable(maxBlock).orElse(DEFAULT_MAX_BLOCK)),
                keySerializer, valueSerializer));
    }

    TopicProducer(String topic, Duration timeout, Producer<K, V> producer) {
        this.topic = topic;
        this.timeout = timeout;
        this.producer = producer;
    }

    public RecordMetadata pipeInput(final TestRecord<K, V> record) {
        Future<RecordMetadata> produceFuture = producer.send(new ProducerRecord<>(
                topic, null, ofNullable(record.getRecordTime()).map(Instant::toEpochMilli).orElse(null), record.key(), record.value(), record.getHeaders()));
        producer.flush();
        return getResult(produceFuture, timeout);
    }

    public RecordMetadata pipeInput(final V value) {
        return pipeInput(new TestRecord<>(value));
    }

    public RecordMetadata pipeInput(final K key, final V value) {
        return pipeInput(new TestRecord<>(key, value));
    }

    @Override
    public void close() {
        closeQuietly(() -> producer.close(timeout), producer.getClass().getSimpleName());
    }

    private static <T> T getResult(final Future<T> future, final Duration timeout) {
        try {
            return future.get(timeout.toMillis(), MILLISECONDS);
        } catch (InterruptedException e) {
            throw new InterruptException("Interrupted during waiting for result.", e);
        } catch (final ExecutionException e) {
            if (e.getCause() instanceof KafkaException) {
                throw (KafkaException) e.getCause();
            }
            throw new KafkaException(e.getCause().getMessage(), e.getCause());
        } catch (final java.util.concurrent.TimeoutException e) {
            throw new org.apache.kafka.common.errors.TimeoutException(e.getMessage(), e);
        }
    }

    private static Map<String, Object> createProducerConfigs(final Map<String, ?> configs,
                                                             final Duration deliveryTimeout,
                                                             final Duration requestTimeout,
                                                             final Duration maxBlock) {
        Map<String, Object> actualConfigs = new HashMap<>(configs);
        actualConfigs.put(DELIVERY_TIMEOUT_MS_CONFIG, (int) deliveryTimeout.toMillis());
        actualConfigs.put(REQUEST_TIMEOUT_MS_CONFIG, (int) requestTimeout.toMillis());
        actualConfigs.put(MAX_BLOCK_MS_CONFIG, (int) maxBlock.toMillis());
        actualConfigs.put(ACKS_CONFIG, "all");
        actualConfigs.put(MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        actualConfigs.put(CLIENT_ID_CONFIG, TestRecordDriver.class.getSimpleName());
        return actualConfigs;
    }

}
