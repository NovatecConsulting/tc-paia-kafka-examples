package de.novatec.tc.support;

import org.apache.kafka.clients.admin.NewTopic;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toSet;

public class Settings {

    private static final Pattern TOPIC_CONFIG_PATTERN = Pattern.compile("^(?<name>[^.]+).topic..*$");

    private final Properties settings;

    public Settings(Properties settings) {
        this.settings = settings;
    }

    public static Settings fromResource(String resourceName) throws IOException {
        return new Settings(loadPropertiesFromResource(resourceName));
    }

    public static Properties loadPropertiesFromResource(String resourceName) throws IOException {
        Properties allProps = new Properties();
        try(InputStream input = Settings.class.getClassLoader().getResourceAsStream(resourceName)) {
            allProps.load(input);
        }
        return allProps;
    }

    public Settings withProperty(String key, String value) {
        settings.setProperty(key, value);
        return this;
    }

    public Properties getAll() {
        return settings;
    }

    public Map<String, ?> getAsMap() {
        Map<String, Object> map = new HashMap<>();
        for (Map.Entry<Object, Object> entry : settings.entrySet()) {
            map.put(String.valueOf(entry.getKey()), entry.getValue());
        }
        return map;
    }

    public String topicName(String configName) {
        return settings.getProperty(format("%s.topic.name", configName), configName);
    }

    public Set<NewTopic> topics(String... configNames) {
        return topics().stream()
                .filter(t -> asList(configNames).contains(t.name()))
                .collect(toSet());
    }

    public Set<NewTopic> topics() {
        Set<NewTopic> topics = new HashSet<>();
        for (String configName : findTopicConfigNames()) {
            String topicName = settings.getProperty(format("%s.topic.name", configName), configName);
            Optional<Integer> partitions = ofNullable(settings.getProperty(format("%s.topic.partitions", configName)))
                    .map(Integer::valueOf);
            Optional<Short> replicationFactor = ofNullable(settings.getProperty(format("%s.topic.replication.factor", configName)))
                    .map(Short::valueOf);
            topics.add(new NewTopic(topicName, partitions, replicationFactor));
        }
        return topics;
    }

    private Set<String> findTopicConfigNames() {
        Set<String> configName = new HashSet<>();
        for (Object key : settings.keySet()) {
            String config = String.valueOf(key);
            Matcher matcher = TOPIC_CONFIG_PATTERN.matcher(config);
            if (matcher.matches()) {
                configName.add(matcher.group("name"));
            }
        }
        return configName;
    }

}
