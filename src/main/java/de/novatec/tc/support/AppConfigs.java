package de.novatec.tc.support;

import org.apache.kafka.clients.admin.NewTopic;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.String.format;
import static java.util.Arrays.stream;
import static java.util.Objects.requireNonNull;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toSet;

public class AppConfigs {

    private static final Pattern TOPIC_CONFIG_PATTERN = Pattern.compile("^(?<name>[^.]+).topic..*$");

    private final Map<String, ?> configs;

    public AppConfigs(Map<String, ?> configs) {
        this.configs = requireNonNull(configs);
    }

    public AppConfigs newWithProperty(String key, Object value) {
        Map<String, Object> newConfigs = new HashMap<>(configs);
        newConfigs.put(key, value);
        return new AppConfigs(newConfigs);
    }

    public Properties asProperties() {
        Properties properties = new Properties();
        properties.putAll(configs);
        return properties;
    }

    public Map<String, Object> asMap() {
        return new HashMap<>(configs);
    }

    public String topicName(String configName) {
        return ofNullable(configs.get(format("%s.topic.name", configName))).map(this::asStringOrNull).orElse(configName);
    }

    public Optional<Integer> topicPartitions(String configName) {
        return ofNullable(configs.get(format("%s.topic.partitions", configName))).map(this::asStringOrNull).map(Integer::valueOf);
    }

    public Optional<Short> topicReplicationFactor(String configName) {
        return ofNullable(configs.get(format("%s.topic.replication.factor", configName))).map(this::asStringOrNull).map(Short::valueOf);
    }

    public Set<String> topicNames(String... configNames) {
        return stream(configNames)
                .map(this::topicName)
                .collect(toSet());
    }

    public Set<NewTopic> topics(String... configNames) {
        return stream(configNames)
                .map(this::topic)
                .collect(toSet());
    }

    public Set<NewTopic> topics() {
        Set<NewTopic> topics = new HashSet<>();
        for (String configName : findTopicConfigNames()) {
            topics.add(topic(configName));
        }
        return topics;
    }

    public NewTopic topic(String configName) {
        String topicName = topicName(configName);
        Optional<Integer> partitions = topicPartitions(configName);
        Optional<Short> replicationFactor = topicReplicationFactor(configName);
        return new NewTopic(topicName, partitions, replicationFactor);
    }

    private Set<String> findTopicConfigNames() {
        Set<String> configName = new HashSet<>();
        for (Object key : configs.keySet()) {
            String config = String.valueOf(key);
            Matcher matcher = TOPIC_CONFIG_PATTERN.matcher(config);
            if (matcher.matches()) {
                configName.add(matcher.group("name"));
            }
        }
        return configName;
    }

    private String asStringOrNull(Object value) {
        return value != null ? String.valueOf(value) : null;
    }

}
