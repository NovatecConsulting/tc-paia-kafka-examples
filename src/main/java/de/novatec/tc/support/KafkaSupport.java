package de.novatec.tc.support;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.TopicExistsException;

import java.time.Duration;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class KafkaSupport {

    private final Properties settings;

    public KafkaSupport(Properties settings) {
        this.settings = settings;
    }

    public void createTopicsIfNotExists(Set<NewTopic> topics, Duration timeout) throws TimeoutException {
        try (AdminClient client = AdminClient.create(settings)) {
            client.createTopics(topics).all().get(timeout.toMillis(), MILLISECONDS);
        } catch (final ExecutionException e) {
            if (e.getCause() instanceof KafkaException) {
                if (!(e.getCause() instanceof TopicExistsException)) {
                    throw (KafkaException) e.getCause();
                }
            } else {
                throw new KafkaException(e.getCause().getMessage(), e.getCause());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

}
