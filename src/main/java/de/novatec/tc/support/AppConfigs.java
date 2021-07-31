package de.novatec.tc.support;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.String.format;
import static java.util.Arrays.stream;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

public class AppConfigs {

    private static final Pattern TOPIC_CONFIG_PATTERN = Pattern.compile("^(?<name>[^.]+).topic..*$");

    private final Map<String, ?> configs;

    public AppConfigs(final Properties properties) {
        this(asMap(properties));
    }

    public AppConfigs(final Map<String, ?> configs) {
        this.configs = new HashMap<>(configs);
    }

    public static AppConfigs of(final Properties properties) {
        return new AppConfigs(properties);
    }

    public static AppConfigs of(final Map<String, ?> configs) {
        return new AppConfigs(configs);
    }

    public Properties asProperties() {
        return asProperties(configs);
    }

    public static Properties asProperties(final Map<String, ?> configs) {
        final Properties properties = new Properties();
        properties.putAll(configs);
        return properties;
    }

    public Map<String, Object> asMap() {
        return new HashMap<>(configs);
    }

    public static Map<String, Object> asMap(final Properties properties) {
        return properties.entrySet().stream()
                .map(e -> Pair.of(String.valueOf(e.getKey()), e.getValue()))
                .collect(toMap(Pair::getKey, Pair::getValue));
    }

    public String storeName(final String configName) {
        return ofNullable(configs.get(format("%s.store.name", configName))).map(this::asStringOrNull).orElse(configName);
    }

    public String topicName(final String configName) {
        return ofNullable(configs.get(format("%s.topic.name", configName))).map(this::asStringOrNull).orElse(configName);
    }

    public Optional<Integer> topicPartitions(final String configName) {
        return ofNullable(configs.get(format("%s.topic.partitions", configName))).map(this::asStringOrNull).map(Integer::valueOf);
    }

    public Optional<Short> topicReplicationFactor(final String configName) {
        return ofNullable(configs.get(format("%s.topic.replication.factor", configName))).map(this::asStringOrNull).map(Short::valueOf);
    }

    public Set<String> topicNames(final String... configNames) {
        return stream(configNames)
                .map(this::topicName)
                .collect(toSet());
    }

    public Set<NewTopic> topics(final String... configNames) {
        return stream(configNames)
                .map(this::topic)
                .collect(toSet());
    }

    public Set<NewTopic> topics() {
        final Set<NewTopic> topics = new HashSet<>();
        for (final String configName : findTopicConfigNames()) {
            topics.add(topic(configName));
        }
        return topics;
    }

    public NewTopic topic(final String configName) {
        final String topicName = topicName(configName);
        final Optional<Integer> partitions = topicPartitions(configName);
        final Optional<Short> replicationFactor = topicReplicationFactor(configName);
        return new NewTopic(topicName, partitions, replicationFactor);
    }

    private Set<String> findTopicConfigNames() {
        final Set<String> configName = new HashSet<>();
        for (final Object key : configs.keySet()) {
            final String config = String.valueOf(key);
            final Matcher matcher = TOPIC_CONFIG_PATTERN.matcher(config);
            if (matcher.matches()) {
                configName.add(matcher.group("name"));
            }
        }
        return configName;
    }

    private String asStringOrNull(final Object value) {
        return value != null ? String.valueOf(value) : null;
    }

}
