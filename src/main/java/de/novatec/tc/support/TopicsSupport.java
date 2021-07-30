package de.novatec.tc.support;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.awaitility.core.ConditionTimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toSet;
import static org.awaitility.Awaitility.await;

public class TopicsSupport {

    private static final Logger LOG = LoggerFactory.getLogger(TopicsSupport.class);

    private final Map<String, Object> configs;

    public TopicsSupport(final Map<String, ?> configs) {
        this.configs = new HashMap<>(configs);
    }

    public TopicsSupport createTopicsIfNotExists(final Set<NewTopic> topics, final Duration timeout) throws TimeoutException {
        try (final AdminClient client = AdminClient.create(configs)) {
            getResult(client.createTopics(topics, new CreateTopicsOptions().timeoutMs((int) timeout.toMillis())).all(), timeout);
        } catch (final TopicExistsException e) {
            LOG.debug("At leas one of the following topic already exists: {}", topics.stream().map(NewTopic::name).collect(toSet()));
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.info("Interrupted during topic creation.");
        }
        return this;
    }

    public TopicsSupport waitUntilTopicsExist(final Set<String> topics, final Duration timeout) throws TimeoutException {
        try (final AdminClient client = AdminClient.create(configs)) {
            await()
                .timeout(timeout)
                .until(() -> topicExists(client, topics, timeout));
        } catch (ConditionTimeoutException e) {
            throw new TimeoutException(e.getMessage(), e);
        }
        return this;
    }

    private boolean topicExists(final AdminClient client, final Set<String> topics, final Duration timeout) throws TimeoutException, InterruptedException {
        final Set<String> actualTopics =
                getResult(client.listTopics(new ListTopicsOptions().timeoutMs((int) timeout.toMillis())).names(), timeout);
        return actualTopics.containsAll(topics);
    }

    private static <T> T getResult(final KafkaFuture<T> future, final Duration timeout) throws TimeoutException, InterruptedException {
        try {
            return future.get(timeout.toMillis(), MILLISECONDS);
        } catch (final ExecutionException e) {
            if (e.getCause() instanceof KafkaException) {
                throw (KafkaException) e.getCause();
            } else {
                throw new KafkaException(e.getCause().getMessage(), e.getCause());
            }
        } catch (java.util.concurrent.TimeoutException e) {
            throw new TimeoutException(e.getMessage(), e);
        }
    }
}
