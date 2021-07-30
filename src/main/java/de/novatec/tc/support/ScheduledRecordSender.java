package de.novatec.tc.support;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static java.lang.String.format;
import static java.util.Objects.requireNonNullElseGet;
import static java.util.Optional.ofNullable;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.kafka.clients.producer.ProducerConfig.*;
import static org.apache.kafka.common.utils.Utils.closeQuietly;

public class ScheduledRecordSender implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ScheduledRecordSender.class);

    private final Map<String, Object> configs;
    private final int maxFailedSends;
    private final ScheduledExecutorService executor;

    private final CopyOnWriteArrayList<Pair<RunnableSender<?, ?>, ScheduledFuture<?>>> scheduled = new CopyOnWriteArrayList<>();
    private final AtomicBoolean closeOnNoScheduledSend = new AtomicBoolean(false);

    public ScheduledRecordSender(final Map<String, ?> configs) {
        this(configs, null, null, null);
    }

    public ScheduledRecordSender(final Map<String, ?> configs,
                                 final Duration deliveryTimeout,
                                 final Duration requestTimeout,
                                 final Integer maxFailedSends) {
        this(configs, deliveryTimeout, requestTimeout, maxFailedSends, null);
    }

    public ScheduledRecordSender(final Map<String, ?> configs,
                                 final Duration deliveryTimeout,
                                 final Duration requestTimeout,
                                 final Integer maxFailedSends,
                                 final ScheduledExecutorService executor) {
        this.executor = requireNonNullElseGet(executor, Executors::newSingleThreadScheduledExecutor);
        this.configs = new HashMap<>();
        if (configs != null) {
            this.configs.putAll(configs);
        }
        if (deliveryTimeout != null) {
            this.configs.put(DELIVERY_TIMEOUT_MS_CONFIG, (int) deliveryTimeout.toMillis());
        }
        if (requestTimeout != null) {
            this.configs.put(REQUEST_TIMEOUT_MS_CONFIG, (int) requestTimeout.toMillis());
        }
        this.maxFailedSends = requireNonNullElseGet(maxFailedSends, () -> Integer.MAX_VALUE);
        initSupervision();
    }

    private void initSupervision() {
        executor.scheduleAtFixedRate(() -> {
            for (Pair<RunnableSender<?, ?>, ScheduledFuture<?>> pair : scheduled) {
                if (pair.getKey().isCompleted() && !pair.getValue().isCancelled()) {
                    pair.getValue().cancel(false);
                }
                if (pair.getValue().isDone()) {
                    scheduled.remove(pair);
                    LOG.info("Removed completed runnable send.");
                }
            }
            if (closeOnNoScheduledSend.get() && scheduled.isEmpty()) {
                executor.shutdown();
            }
        }, 0, 1000, MILLISECONDS);
    }

    public <K, V> ScheduledProduce sendAtFixedRate(
            final Supplier<ProducerRecord<K, V>> recordSupplier,
            final Serializer<K> keySerializer,
            final Serializer<V> valueSerializer,
            final Duration period) {
        ensureReady();
        Map<String, Object> actualConfig = new HashMap<>(configs);
        actualConfig.put(MAX_BLOCK_MS_CONFIG, (int) period.toMillis());
        RunnableSender<K, V> runnableSender = new RunnableSender<>(actualConfig, keySerializer, valueSerializer, maxFailedSends, recordSupplier);
        ScheduledFuture<?> scheduledFuture = executor.scheduleAtFixedRate(runnableSender, 0, period.toMillis(), MILLISECONDS);
        scheduled.add(Pair.of(runnableSender, scheduledFuture));
        return runnableSender;
    }

    public <K, V, KS extends Serializer<? super K>, VS extends Serializer<? super V>> ScheduledProduce sendAtFixedRate(
            final Supplier<ProducerRecord<K, V>> recordSupplier,
            final Class<KS> keySerializer,
            final Class<VS> valueSerializer,
            final Duration period) {
        ensureReady();
        Map<String, Object> actualConfig = new HashMap<>(configs);
        actualConfig.put(MAX_BLOCK_MS_CONFIG, (int) period.toMillis());
        RunnableSender<K, V> runnableSender = new RunnableSender<>(actualConfig, keySerializer, valueSerializer, maxFailedSends, recordSupplier);
        ScheduledFuture<?> scheduledFuture = executor.scheduleAtFixedRate(runnableSender, 0, period.toMillis(), MILLISECONDS);
        scheduled.add(Pair.of(runnableSender, scheduledFuture));
        return runnableSender;
    }

    public void closeOnNoScheduledSend() {
        closeOnNoScheduledSend.set(true);
    }

    @Override
    public void close() {
        close(Duration.ofMillis(Long.MAX_VALUE));
    }

    public void close(Duration timeout) {
        long startMs = System.currentTimeMillis();

        scheduled.forEach(p -> p.getKey().stop());
        for (Pair<RunnableSender<?, ?>, ?> pair : scheduled) {
            final Duration remainingTimeout = timeout.minus(Duration.ofMillis(System.currentTimeMillis() - startMs));
            closeQuietly(() -> pair.getKey().awaitCompletion(remainingTimeout.compareTo(Duration.ZERO) >= 0 ? remainingTimeout : Duration.ZERO), pair.getKey().getClass().getSimpleName());
        }

        executor.shutdown();
        try {
            if(!executor.awaitTermination(timeout.toMillis(), MILLISECONDS)) {
                LOG.info("Could not shutdown executor within given timeout.");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.info("Interrupted during awaiting executor shutdown", e);
        }
    }

    public boolean awaitTermination(Duration timeout) throws InterruptedException {
        return executor.awaitTermination(timeout.toMillis(), MILLISECONDS);
    }

    private void ensureReady() {
        if (executor.isShutdown()) {
            throw new IllegalStateException(format("%s has already been closed.", ScheduledRecordSender.class));
        }
    }

    public interface ScheduledProduce {

        Deque<ProduceCompletion> produces();

        boolean lastSendCompleted();

        int lastConsecutiveFailedSends();

        void stop();

        boolean isStopped();

        boolean isCompleted();

        boolean awaitCompletion(Duration timeout) throws InterruptedException;
    }

    private static class RunnableSender<K, V> implements ScheduledProduce, Runnable {

        private static final Logger LOG = LoggerFactory.getLogger(RunnableSender.class);

        private final Producer<K, V> producer;
        private final int maxFailedSends;
        private final Supplier<ProducerRecord<K, V>> recordSupplier;

        private final Deque<ProduceCompletion> produces = new ArrayDeque<>();
        private final AtomicBoolean stopped = new AtomicBoolean(false);
        private final CountDownLatch completionLatch = new CountDownLatch(1);

        public RunnableSender(final Map<String, ?> configs,
                              final Serializer<K> keySerializer,
                              final Serializer<V> valueSerializer,
                              final int maxFailedSends,
                              final Supplier<ProducerRecord<K, V>> recordSupplier) {
            this(new KafkaProducer<>(new HashMap<>(configs), keySerializer, valueSerializer), maxFailedSends, recordSupplier);
        }

        private static  Map<String, Object> withSerializer(final Map<String, ?> configs,
                                                          final Class<? extends  Serializer<?>> keySerializer,
                                                          Class<? extends  Serializer<?>> valueSerializer) {
            Map<String, Object> actualConfig = new HashMap<>(configs);
            actualConfig.put(KEY_SERIALIZER_CLASS_CONFIG, keySerializer);
            actualConfig.put(VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer);
            return actualConfig;
        }

        public RunnableSender(final Map<String, ?> configs,
                              final Class<? extends  Serializer<? super K>> keySerializer,
                              final Class<? extends  Serializer<? super V>> valueSerializer,
                              final int maxFailedSends,
                              final Supplier<ProducerRecord<K, V>> recordSupplier) {
            this(new KafkaProducer<>(withSerializer(configs, keySerializer, valueSerializer)), maxFailedSends, recordSupplier);
        }

        RunnableSender(final Producer<K, V> producer,
                              final int maxFailedSends,
                              final Supplier<ProducerRecord<K, V>> recordSupplier) {
            this.producer = producer;
            this.maxFailedSends = maxFailedSends;
            this.recordSupplier = recordSupplier;
        }

        @Override
        public void run() {
            if (isCompleted()) {
                if (LOG.isInfoEnabled())
                    LOG.info("This runnable sender has already been completed.");
                return;
            }

            if (!isStopped()) {
                if (lastSendCompleted()) {
                    if (lastConsecutiveFailedSends() < maxFailedSends) {
                        ProduceCompletionCallback produceResult = new ProduceCompletionCallback();
                        try {
                            producer.send(recordSupplier.get(), produceResult);
                        } catch (Exception e) {
                            produceResult.fail(e);
                            if (LOG.isWarnEnabled())
                                LOG.warn("Produce failed with exception.", e);
                        }
                        produces.add(produceResult);
                    } else {
                        stop();
                        if (LOG.isWarnEnabled())
                            LOG.warn("Stopped runnable sender, because {} consecutive sends failed.", maxFailedSends);
                    }
                } else {
                    if (LOG.isWarnEnabled())
                        LOG.warn("Skipped send because last send has not been completed until now.");
                }
            }

            if (isStopped()) {
                if (LOG.isInfoEnabled())
                    LOG.info("Closing runnable sender.");
                closeQuietly(producer, producer.getClass().getSimpleName());
                completionLatch.countDown();
            }
        }

        @Override
        public Deque<ProduceCompletion> produces() {
            return new ArrayDeque<>(produces);
        }

        @Override
        public boolean lastSendCompleted() {
            if (produces.isEmpty()) {
                return true;
            }
            return produces.getLast().isDone();
        }

        @Override
        public int lastConsecutiveFailedSends() {
            int numFailed = 0;
            Iterator<ProduceCompletion> it = produces.descendingIterator();
            while(it.hasNext()) {
                ProduceCompletion produceResult = it.next();
                if (produceResult.isDone()) {
                    if (produceResult.isFailed()) {
                        numFailed++;
                    } else {
                        break;
                    }
                }
            }
            return numFailed;
        }

        @Override
        public void stop() {
            stopped.set(true);
        }

        @Override
        public boolean isStopped() {
            return stopped.get();
        }

        @Override
        public boolean isCompleted() {
            return completionLatch.getCount() <= 0;
        }

        @Override
        public boolean awaitCompletion(Duration timeout) throws InterruptedException {
            return completionLatch.await(timeout.toMillis(), MILLISECONDS);
        }

    }

    public interface ProduceCompletion {

        Optional<RecordMetadata> getMetadata();

        Optional<Exception> getException();

        boolean isDone();

        boolean isFailed();

        boolean isSuccess();
    }

    private static class ProduceCompletionCallback implements ProduceCompletion, Callback {

        private static final Logger LOG = LoggerFactory.getLogger(ProduceCompletionCallback.class);

        private static class ResultHolder {
            final RecordMetadata recordMetadata;
            final Exception exception;
            public ResultHolder(final RecordMetadata recordMetadata, final Exception exception) {
                this.recordMetadata = recordMetadata;
                this.exception = exception;
            }
        }

        private final AtomicReference<ResultHolder> result = new AtomicReference<>();

        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            this.result.set(new ResultHolder(metadata, exception));
            if (exception == null) {
                if (LOG.isDebugEnabled())
                    LOG.debug("Sent message to {}-{} at offset {}.", metadata.topic(), metadata.partition(), metadata.offset());
            } else {
                if (LOG.isWarnEnabled())
                    LOG.warn("Failed to sent message: {}", exception.toString());
            }
        }

        public void fail(Exception e) {
            result.set(new ResultHolder(null, e));
        }

        @Override
        public Optional<RecordMetadata> getMetadata() {
            return ofNullable(result.get()).map(r -> r.recordMetadata);
        }

        @Override
        public Optional<Exception> getException() {
            return ofNullable(result.get()).map(r -> r.exception);
        }

        public boolean isDone() {
            return ofNullable(result.get()).isPresent();
        }

        public boolean isFailed() {
            return isDone() && getException().isPresent();
        }

        public boolean isSuccess() {
            return isDone() && getException().isEmpty();
        }

    }
}
